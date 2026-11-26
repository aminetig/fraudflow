"""Chargement quotidien : data lake -> entrepôt PostgreSQL, pour une date donnée.

- raw.transactions : les transactions silver de la journée ;
- raw.alerts : la dernière version de chaque alerte de la journée ;
- ops.lake_daily : volumes du jour (bronze, quarantaine, silver, alertes), pour le seuil qualité.

Idempotent : les lignes de la date sont remplacées dans une seule transaction SQL.
Relancer le chargement d'une date donne exactement le même résultat.

Lancement : python -m jobs.batch_load_warehouse --date 2026-11-08
"""

import argparse
import logging
from datetime import date

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from lib.config import Settings, create_spark, load_settings
from lib.fraud_rules import ALERT_COLUMNS, latest_per_alert

log = logging.getLogger("batch")

TRANSACTION_COLUMNS = (
    "transaction_id", "card_hash", "card_last4", "customer_id", "merchant_id", "amount",
    "currency", "amount_eur", "country", "channel", "mcc", "event_time", "event_date",
    "_injected_fraud", "kafka_partition", "kafka_offset", "ingested_at",
)
ALERT_TABLE_COLUMNS = (*ALERT_COLUMNS, "detected_at", "alert_date")


def read_or_empty(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except AnalysisException:  # dossier encore inexistant : aucune donnée
        return None


def stage(df: DataFrame, settings: Settings, table: str) -> None:
    """Écrit le DataFrame dans une table de transit (vidée puis remplie)."""
    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    day = parser.parse_args(argv).date
    settings = load_settings()
    spark = create_spark("fraudflow-batch-load", settings, memory="768m", ui=False)

    silver = spark.read.parquet(settings.silver_path).filter(F.col("event_date") == day)
    transactions = silver.select(*TRANSACTION_COLUMNS).cache()

    # Une alerte peut avoir plusieurs versions (fenêtre mise à jour) : on garde la dernière
    gold = read_or_empty(spark, settings.gold_alerts_path)
    alerts = None
    if gold is not None:
        alerts = (
            latest_per_alert(gold, [F.desc("detected_at")])
            .withColumn("alert_date", F.to_date("first_event_time"))
            .filter(F.col("alert_date") == day)
            .select(*ALERT_TABLE_COLUMNS)
            .cache()
        )

    bronze_rows = spark.read.parquet(settings.bronze_path).filter(F.col("ingest_date") == day)
    quarantine = read_or_empty(spark, settings.quarantine_path)
    counts = {
        "bronze": bronze_rows.count(),
        "quarantine": 0 if quarantine is None else quarantine.filter(
            F.col("ingest_date") == day
        ).count(),
        "silver": transactions.count(),
        "alerts": 0 if alerts is None else alerts.count(),
    }
    log.info("Volumes du %s : %s", day, counts)

    stage(transactions, settings, "raw._stage_transactions")
    if alerts is not None:
        stage(alerts, settings, "raw._stage_alerts")

    columns = ", ".join(TRANSACTION_COLUMNS)
    alert_columns = ", ".join(ALERT_TABLE_COLUMNS)
    alert_select = alert_columns.replace("evidence", "evidence::jsonb")
    rate = counts["quarantine"] / counts["bronze"] if counts["bronze"] else 0
    with psycopg.connect(**settings.postgres_params) as conn:  # une seule transaction
        conn.execute("DELETE FROM raw.transactions WHERE event_date = %s", (day,))
        conn.execute(
            f"INSERT INTO raw.transactions ({columns}) "
            f"SELECT {columns} FROM raw._stage_transactions"
        )
        conn.execute("DELETE FROM raw.alerts WHERE alert_date = %s", (day,))
        if alerts is not None:
            conn.execute(
                f"INSERT INTO raw.alerts ({alert_columns}) "
                f"SELECT {alert_select} FROM raw._stage_alerts"
            )
        conn.execute(
            """
            INSERT INTO ops.lake_daily
                (day, bronze_rows, quarantine_rows, silver_rows, alerts, quarantine_rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (day) DO UPDATE SET
                bronze_rows = EXCLUDED.bronze_rows,
                quarantine_rows = EXCLUDED.quarantine_rows,
                silver_rows = EXCLUDED.silver_rows,
                alerts = EXCLUDED.alerts,
                quarantine_rate = EXCLUDED.quarantine_rate,
                computed_at = now()
            """,
            (day, counts["bronze"], counts["quarantine"], counts["silver"], counts["alerts"], rate),
        )
    log.info("Chargement du %s terminé, taux de quarantaine %.2f %%", day, 100 * rate)
    spark.stop()


if __name__ == "__main__":
    main()
