"""Bronze -> silver : contrôle, typage et dédoublonnage. Les lignes rejetées vont en quarantaine."""

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lib.config import Settings
from lib.quality import to_quarantine, to_silver, validate
from lib.schemas import BRONZE_SCHEMA


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    bronze = spark.readStream.schema(BRONZE_SCHEMA).parquet(settings.bronze_path)
    checked = validate(bronze)

    # Les lignes invalides sont écartées AVANT le watermark : une date dans le futur
    # ferait sinon croire à Spark que le temps a avancé, et il jetterait les vraies données
    clean = (
        to_silver(checked)
        # Retard accepté : 15 minutes. Au-delà, Spark oublie les identifiants déjà vus
        .withWatermark("event_time", "15 minutes")
        .dropDuplicatesWithinWatermark(["transaction_id"])
    )
    silver = (
        clean.writeStream.queryName("silver")
        .format("parquet")
        .option("path", settings.silver_path)
        .option("checkpointLocation", settings.checkpoint("silver"))
        .partitionBy("event_date")
        .trigger(processingTime=settings.trigger)
        .start()
    )
    quarantine = (
        to_quarantine(checked)
        .writeStream.queryName("quarantine")
        .format("parquet")
        .option("path", settings.quarantine_path)
        .option("checkpointLocation", settings.checkpoint("quarantine"))
        .partitionBy("ingest_date")
        .trigger(processingTime=settings.trigger)
        .start()
    )
    return [silver, quarantine]
