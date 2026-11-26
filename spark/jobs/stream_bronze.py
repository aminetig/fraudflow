"""Kafka -> bronze : chaque message est gardé tel quel, avec son numéro de carte masqué."""

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lib.config import Settings
from lib.pseudonymize import to_bronze


def start(spark: SparkSession, settings: Settings) -> StreamingQuery:
    messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic)
        # Au tout premier lancement, on lit le topic depuis le début ; ensuite, le checkpoint
        # indique où reprendre
        .option("startingOffsets", "earliest")
        # Après une panne, on rattrape par lots de taille raisonnable
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )
    return (
        to_bronze(messages, settings.card_hash_salt)
        .coalesce(1)  # un fichier par lot, plutôt qu'un par partition Kafka
        .writeStream.queryName("bronze")
        .format("parquet")
        .option("path", settings.bronze_path)
        .option("checkpointLocation", settings.checkpoint("bronze"))
        .partitionBy("ingest_date")
        .trigger(processingTime=settings.trigger)
        .start()
    )
