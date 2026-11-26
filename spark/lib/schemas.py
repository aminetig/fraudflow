"""Schémas explicites : Spark ne devine jamais la structure des données."""

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Champs du message JSON envoyé par le système de paiement. Tous sont lus comme du texte :
# les types sont vérifiés ensuite, pour dire précisément pourquoi une ligne est rejetée.
JSON_FIELDS = (
    "transaction_id",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "channel",
    "mcc",
    "event_time",
    "_injected_fraud",
)
# Colonne qui reçoit le texte d'un message JSON illisible
CORRUPT_COLUMN = "_corrupt_record"
TRANSACTION_JSON_SCHEMA = StructType(
    [StructField(name, StringType()) for name in JSON_FIELDS]
    + [StructField(CORRUPT_COLUMN, StringType())]
)

# Couche bronze : le message brut (carte masquée) et sa provenance dans Kafka
BRONZE_SCHEMA = StructType(
    [
        StructField("raw_value", StringType()),
        StructField("card_hash", StringType()),
        StructField("card_last4", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)
