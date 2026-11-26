"""Mesure la qualité de la détection, en comparant les alertes aux fraudes injectées.

- rappel : part des transactions frauduleuses couvertes par au moins une alerte ;
- précision : part des alertes qui couvrent au moins une transaction frauduleuse ;
- délai : temps entre la dernière transaction en cause et l'émission de l'alerte.

C'est le seul endroit où la colonne cachée `_injected_fraud` est lue.
Lancement : make evaluate
"""

from pyspark.sql import functions as F

from lib.config import create_spark, load_settings
from lib.fraud_rules import latest_per_alert

# Les dernières minutes sont écartées : leurs fenêtres ne sont peut-être pas encore closes
SETTLING_MINUTES = 20


def main() -> None:
    settings = load_settings()
    spark = create_spark("fraudflow-evaluate", settings, memory="512m", ui=False)
    silver = spark.read.parquet(settings.silver_path)
    alerts = latest_per_alert(
        spark.read.parquet(settings.gold_alerts_path), [F.desc("detected_at")]
    ).cache()

    first_alert = alerts.agg(F.min("first_event_time")).first()[0]
    horizon = silver.agg(F.max("event_time")).first()[0]
    period = (F.col("event_time") >= F.lit(first_alert)) & (
        F.col("event_time") <= F.lit(horizon) - F.expr(f"INTERVAL {SETTLING_MINUTES} MINUTES")
    )
    transactions = silver.filter(period).select(
        "transaction_id", "card_hash", "event_time", "_injected_fraud"
    )
    frauds = transactions.filter(F.col("_injected_fraud").isNotNull())

    # Une alerte couvre les transactions de sa carte entre sa première et sa dernière preuve
    covers = (F.col("t.card_hash") == F.col("a.card_hash")) & F.col("t.event_time").between(
        F.col("a.first_event_time"), F.col("a.last_event_time")
    )
    covered = frauds.alias("t").join(alerts.alias("a"), covers, "left")

    print(f"\n== Période évaluée : {first_alert} -> {horizon} (moins {SETTLING_MINUTES} min)")
    print("== Rappel : fraudes injectées couvertes par une alerte")
    (
        covered.groupBy(F.col("t._injected_fraud").alias("scenario"), "t.transaction_id")
        .agg(F.max(F.col("a.alert_id").isNotNull().cast("int")).alias("found"))
        .groupBy("scenario")
        .agg(
            F.count("*").alias("fraudes"),
            F.sum("found").alias("detectees"),
            F.round(100 * F.avg("found"), 1).alias("rappel_pct"),
        )
        .orderBy("scenario")
        .show(truncate=False)
    )

    print("== Précision : alertes qui couvrent au moins une fraude injectée")
    evaluated = alerts.filter(
        F.col("last_event_time").between(
            F.lit(first_alert), F.lit(horizon) - F.expr(f"INTERVAL {SETTLING_MINUTES} MINUTES")
        )
    )
    (
        evaluated.alias("a")
        .join(frauds.alias("t"), covers, "left")
        .groupBy("a.rule", "a.alert_id")
        .agg(F.max(F.col("t.transaction_id").isNotNull().cast("int")).alias("correct"))
        .groupBy("rule")
        .agg(
            F.count("*").alias("alertes"),
            F.sum("correct").alias("justes"),
            F.round(100 * F.avg("correct"), 1).alias("precision_pct"),
        )
        .orderBy("rule")
        .show(truncate=False)
    )

    print("== Délai d'alerte, en secondes (alertes émises en direct, hors rattrapage)")
    delay = F.unix_timestamp("detected_at") - F.unix_timestamp("last_event_time")
    (
        alerts.withColumn("delay_s", delay)
        .filter(F.col("delay_s") < 300)
        .groupBy("rule")
        .agg(
            F.count("*").alias("alertes"),
            F.percentile_approx("delay_s", 0.5).alias("median_s"),
            F.percentile_approx("delay_s", 0.95).alias("p95_s"),
        )
        .orderBy("rule")
        .show(truncate=False)
    )
    spark.stop()


if __name__ == "__main__":
    main()
