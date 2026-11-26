"""Règles de détection de fraude.

Fonctions pures : des transactions en entrée, des alertes en sortie, au même format pour
toutes les règles. Elles marchent en continu comme sur un lot, d'où des tests sans Kafka.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BURST_MIN_TRANSACTIONS = 5  # R1 : à partir de 5 paiements en 2 minutes
SPIKE_FACTOR = 10  # R2 : plus de 10 fois le montant habituel du client
NIGHT_END_HOUR = 5  # R4 : de 0 h à 4 h 59, heure de Paris
NIGHT_MIN_AMOUNT_EUR = 300  # R4 : à partir de 300 €
TIMEZONE = "Europe/Paris"

ALERT_COLUMNS = (
    "alert_id",
    "rule",
    "card_hash",
    "card_last4",
    "customer_id",
    "transaction_id",
    "score",
    "level",
    "first_event_time",
    "last_event_time",
    "window_start",
    "window_end",
    "evidence",
)


def enrich(transactions: DataFrame, customers: DataFrame, merchants: DataFrame) -> DataFrame:
    """Ajoute le montant habituel du client et le niveau de risque du commerçant."""
    return transactions.join(
        F.broadcast(customers.select("customer_id", "avg_amount_eur")), "customer_id", "left"
    ).join(F.broadcast(merchants.select("merchant_id", "is_risky")), "merchant_id", "left")


def level(score: Column) -> Column:
    return F.when(score >= 70, "HIGH").when(score >= 50, "MEDIUM").otherwise("LOW")


def _alerts(
    df: DataFrame,
    rule: str,
    *,
    key: Column,
    score: Column,
    first_event_time: Column,
    last_event_time: Column,
    evidence: Column,
    transaction_id: Column | None = None,
    window: Column | None = None,
) -> DataFrame:
    """Met les alertes d'une règle au format commun.

    `alert_id` est calculé à partir de la règle, de la carte et d'une clé stable : rejouer un
    lot, ou retrouver la même fraude dans deux fenêtres qui se chevauchent, donne la même alerte.
    """
    score = F.least(F.lit(100), score).cast("int")
    null_ts = F.lit(None).cast("timestamp")
    return df.select(
        F.sha2(F.concat_ws("|", F.lit(rule), "card_hash", key.cast("string")), 256).alias(
            "alert_id"
        ),
        F.lit(rule).alias("rule"),
        "card_hash",
        "card_last4",
        "customer_id",
        (transaction_id if transaction_id is not None else F.lit(None).cast("string")).alias(
            "transaction_id"
        ),
        score.alias("score"),
        level(score).alias("level"),
        first_event_time.alias("first_event_time"),
        last_event_time.alias("last_event_time"),
        (window.start if window is not None else null_ts).alias("window_start"),
        (window.end if window is not None else null_ts).alias("window_end"),
        F.to_json(evidence).alias("evidence"),
    )


def burst_alerts(transactions: DataFrame) -> DataFrame:
    """R1 : au moins 5 paiements sur la même carte en 2 minutes (fenêtres glissantes)."""
    windows = transactions.groupBy(
        F.window("event_time", "2 minutes", "1 minute"), "card_hash"
    ).agg(
        # Ensemble des identifiants : une transaction reçue deux fois ne compte qu'une fois
        F.collect_set("transaction_id").alias("ids"),
        F.collect_set("merchant_id").alias("merchants"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
        F.max("card_last4").alias("card_last4"),
        F.max("customer_id").alias("customer_id"),
    )
    count = F.size("ids")
    return _alerts(
        windows.filter(count >= BURST_MIN_TRANSACTIONS),
        "R1_BURST",
        key=F.col("first_event_time"),
        score=40 + 10 * (count - BURST_MIN_TRANSACTIONS),
        first_event_time=F.col("first_event_time"),
        last_event_time=F.col("last_event_time"),
        evidence=F.struct(
            count.alias("transactions"), F.size("merchants").alias("distinct_merchants")
        ),
        window=F.col("window"),
    )


def travel_alerts(transactions: DataFrame) -> DataFrame:
    """R3 : paiements en magasin ou au distributeur dans 2 pays en moins de 30 minutes.

    Seuls les paiements avec la carte présente comptent : un achat en ligne chez un
    commerçant étranger ne dit rien de l'endroit où se trouve le client.
    """
    windows = (
        transactions.filter(F.col("channel").isin("POS", "ATM"))
        .groupBy(F.window("event_time", "30 minutes", "5 minutes"), "card_hash")
        .agg(
            F.collect_set("country").alias("countries"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
            F.max("card_last4").alias("card_last4"),
            F.max("customer_id").alias("customer_id"),
        )
    )
    distinct_countries = F.size("countries")
    minutes = (F.unix_timestamp("last_event_time") - F.unix_timestamp("first_event_time")) / 60
    return _alerts(
        windows.filter(distinct_countries > 1),
        "R3_IMPOSSIBLE_TRAVEL",
        key=F.col("first_event_time"),
        score=75 + 10 * (distinct_countries - 2),
        first_event_time=F.col("first_event_time"),
        last_event_time=F.col("last_event_time"),
        evidence=F.struct(
            F.array_sort("countries").alias("countries"),
            F.round(minutes, 1).alias("minutes_between"),
        ),
        window=F.col("window"),
    )


def amount_spike_alerts(enriched: DataFrame) -> DataFrame:
    """R2 : un paiement de plus de 10 fois le montant habituel du client."""
    ratio = F.col("amount_eur") / F.col("avg_amount_eur")
    return _alerts(
        enriched.filter(ratio > SPIKE_FACTOR),
        "R2_AMOUNT_SPIKE",
        key=F.col("transaction_id"),
        score=30 + 3 * ratio,
        first_event_time=F.col("event_time"),
        last_event_time=F.col("event_time"),
        transaction_id=F.col("transaction_id"),
        evidence=F.struct(
            F.col("amount_eur").cast("double").alias("amount_eur"),
            F.col("avg_amount_eur").cast("double").alias("usual_amount_eur"),
            F.round(ratio, 1).alias("ratio"),
            "merchant_id",
            "mcc",
        ),
    )


def risky_night_alerts(enriched: DataFrame) -> DataFrame:
    """R4 : au moins 300 € la nuit, heure de Paris, chez un commerçant à risque."""
    local_hour = F.hour(F.from_utc_timestamp("event_time", TIMEZONE))
    suspicious = (
        F.col("is_risky")
        & (local_hour < NIGHT_END_HOUR)
        & (F.col("amount_eur") >= NIGHT_MIN_AMOUNT_EUR)
    )
    return _alerts(
        enriched.filter(suspicious),
        "R4_RISKY_NIGHT",
        key=F.col("transaction_id"),
        score=50 + F.col("amount_eur") / 100,
        first_event_time=F.col("event_time"),
        last_event_time=F.col("event_time"),
        transaction_id=F.col("transaction_id"),
        evidence=F.struct(
            F.col("amount_eur").cast("double").alias("amount_eur"),
            local_hour.alias("local_hour"),
            "merchant_id",
            "mcc",
        ),
    )


def instant_alerts(enriched: DataFrame) -> DataFrame:
    """Règles qui jugent une transaction seule, sans mémoire : R2 et R4."""
    return amount_spike_alerts(enriched).unionByName(risky_night_alerts(enriched))


def latest_per_alert(alerts: DataFrame, order: list[Column]) -> DataFrame:
    """Garde une ligne par alert_id : la plus récente selon `order`."""
    from pyspark.sql import Window

    rank = F.row_number().over(Window.partitionBy("alert_id").orderBy(*order))
    return alerts.withColumn("_rank", rank).filter("_rank = 1").drop("_rank")
