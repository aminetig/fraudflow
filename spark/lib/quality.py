"""Contrôles qualité de la couche silver.

Chaque ligne reçoit au plus un motif de rejet : le premier contrôle en échec, dans l'ordre
de la liste. Les lignes valides sont typées et enrichies ; les autres partent en quarantaine,
avec leur motif, pour être analysées plutôt que perdues.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lib.reference import CHANNELS, EUR_RATES, ISO_COUNTRIES, ISO_CURRENCIES
from lib.schemas import CORRUPT_COLUMN, TRANSACTION_JSON_SCHEMA

AMOUNT_TYPE = "decimal(14,2)"  # jamais de nombre à virgule flottante pour de l'argent
REQUIRED_FIELDS = (
    "transaction_id",
    "card_hash",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "channel",
    "mcc",
    "event_time",
)


def parse(bronze: DataFrame) -> DataFrame:
    """Lit le JSON brut avec le schéma explicite."""
    parsed = F.from_json(
        "raw_value", TRANSACTION_JSON_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT_COLUMN}
    )
    return bronze.withColumn("_parsed", parsed).select("*", "_parsed.*").drop("_parsed")


def rejection_reason() -> Column:
    """Motif de rejet de la ligne, ou null si elle passe tous les contrôles."""
    amount = F.col("amount").cast(AMOUNT_TYPE)
    event_time = F.col("event_time").cast("timestamp")
    checks = [
        ("invalid_json", F.col(CORRUPT_COLUMN).isNotNull()),
        *[
            (f"missing_{name}", F.col(name).isNull() | (F.trim(F.col(name)) == ""))
            for name in REQUIRED_FIELDS
        ],
        ("invalid_amount", amount.isNull()),
        ("non_positive_amount", amount <= 0),
        ("invalid_currency", ~F.col("currency").isin(*sorted(ISO_CURRENCIES))),
        ("invalid_country", ~F.col("country").isin(*sorted(ISO_COUNTRIES))),
        ("invalid_channel", ~F.col("channel").isin(*CHANNELS)),
        ("invalid_event_time", event_time.isNull()),
        # Date plus de 5 minutes après l'arrivée : horloge du terminal déréglée.
        # On compare à l'heure d'arrivée enregistrée, pas à l'heure actuelle :
        # retraiter les mêmes données donne toujours le même résultat.
        ("future_event_time", event_time > F.col("ingested_at") + F.expr("INTERVAL 5 MINUTES")),
    ]
    reason = F.lit(None).cast("string")
    for name, failed in reversed(checks):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    return reason


def validate(bronze: DataFrame) -> DataFrame:
    """Ajoute les champs du JSON et la colonne `rejection_reason`."""
    return parse(bronze).withColumn("rejection_reason", rejection_reason())


def to_silver(checked: DataFrame) -> DataFrame:
    """Lignes valides, typées, avec le montant converti en euros."""
    rates = F.create_map(*[F.lit(value) for pair in EUR_RATES.items() for value in pair])
    amount = F.col("amount").cast(AMOUNT_TYPE)
    return (
        checked.filter(F.col("rejection_reason").isNull())
        .select(
            "transaction_id",
            "card_hash",
            "card_last4",
            "customer_id",
            "merchant_id",
            amount.alias("amount"),
            "currency",
            F.round(amount / rates[F.col("currency")], 2).cast(AMOUNT_TYPE).alias("amount_eur"),
            "country",
            "channel",
            "mcc",
            F.col("event_time").cast("timestamp").alias("event_time"),
            "_injected_fraud",
            "kafka_partition",
            "kafka_offset",
            "ingested_at",
        )
        .withColumn("event_date", F.to_date("event_time"))
    )


def to_quarantine(checked: DataFrame) -> DataFrame:
    """Lignes rejetées : le message brut, son motif de rejet et sa provenance."""
    return checked.filter(F.col("rejection_reason").isNotNull()).select(
        "raw_value",
        "card_hash",
        "card_last4",
        "rejection_reason",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        "ingest_date",
    )
