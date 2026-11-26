"""Bilan du data lake : volumes, pertes, doublons, quarantaine et numéros de carte en clair.

Lancement : make lake-report
"""

from pyspark.sql import functions as F

from lib.config import create_spark, load_settings

CLEAR_PAN = r"(?<!\d)\d{13,19}(?!\d)"


def main() -> None:
    settings = load_settings()
    spark = create_spark("fraudflow-lake-report", settings, memory="512m", ui=False)
    bronze = spark.read.parquet(settings.bronze_path)
    silver = spark.read.parquet(settings.silver_path)
    quarantine = spark.read.parquet(settings.quarantine_path)

    print("\n== Bronze : chaque message Kafka une fois, ni plus ni moins")
    per_partition = (
        bronze.groupBy("kafka_partition")
        .agg(
            F.count("*").alias("lignes"),
            F.countDistinct("kafka_offset").alias("offsets_distincts"),
            F.min("kafka_offset").alias("premier"),
            F.max("kafka_offset").alias("dernier"),
        )
        .withColumn("doublons", F.col("lignes") - F.col("offsets_distincts"))
        .withColumn("trous", F.col("dernier") - F.col("premier") + 1 - F.col("offsets_distincts"))
        .orderBy("kafka_partition")
    )
    per_partition.show(truncate=False)
    clear = bronze.filter(F.col("raw_value").rlike(CLEAR_PAN)).count()
    print(f"Numéros de carte en clair dans le bronze : {clear}")

    n_bronze, n_silver, n_quarantine = bronze.count(), silver.count(), quarantine.count()
    duplicates = n_silver - silver.select("transaction_id").distinct().count()
    print("\n== Silver")
    print(f"Lignes : {n_silver} ; transaction_id en double : {duplicates}")
    print(f"Fraudes injectées présentes : {silver.filter('_injected_fraud IS NOT NULL').count()}")

    rate = 100 * n_quarantine / max(n_bronze, 1)
    print(f"\n== Quarantaine : {n_quarantine} lignes, {rate:.2f} % du bronze")
    quarantine.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
