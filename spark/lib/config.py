"""Configuration, lue dans les variables d'environnement, et création de la session Spark."""

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    kafka_topic: str
    alerts_topic: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    card_hash_salt: str
    checkpoint_root: str
    trigger: str
    fraud_trigger: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    mongo_uri: str

    # Emplacements du data lake : un bucket par couche
    bronze_path = "s3a://bronze/transactions"
    silver_path = "s3a://silver/transactions"
    quarantine_path = "s3a://silver/quarantine"
    gold_alerts_path = "s3a://gold/alerts"

    def checkpoint(self, query_name: str) -> str:
        return f"{self.checkpoint_root}/{query_name}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def postgres_params(self) -> dict:
        """Paramètres de connexion pour psycopg."""
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit(f"Variable d'environnement manquante : {name}")
    return value


def load_settings() -> Settings:
    salt = _required("CARD_HASH_SALT")
    if len(salt) < 16:
        raise SystemExit("CARD_HASH_SALT doit contenir au moins 16 caractères")
    env = os.environ.get
    return Settings(
        kafka_bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=env("KAFKA_TOPIC", "transactions.raw"),
        alerts_topic=env("KAFKA_ALERTS_TOPIC", "fraud.alerts"),
        s3_endpoint=env("S3_ENDPOINT", "http://minio:9000"),
        s3_access_key=_required("S3_ACCESS_KEY"),
        s3_secret_key=_required("S3_SECRET_KEY"),
        card_hash_salt=salt,
        checkpoint_root=env("CHECKPOINT_ROOT", "/checkpoints"),
        trigger=env("STREAMING_TRIGGER", "30 seconds"),
        fraud_trigger=env("FRAUD_TRIGGER", "10 seconds"),
        postgres_host=env("POSTGRES_HOST", "postgres"),
        postgres_port=int(env("POSTGRES_PORT", "5432")),
        postgres_db=env("POSTGRES_DB", "warehouse"),
        postgres_user=env("POSTGRES_USER", ""),
        postgres_password=env("POSTGRES_PASSWORD", ""),
        mongo_uri=env("MONGO_URI", ""),
    )


def create_spark(
    app_name: str, settings: Settings, memory: str | None = None, ui: bool = True
) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", memory or os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.enabled", str(ui).lower())
        .config("spark.ui.showConsoleProgress", "false")  # pas de barres de progression
        # Peu de données : 3 partitions après un regroupement, au lieu de 200 par défaut
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        # Stockage S3 : MinIO en local, AWS S3 en production en changeant seulement l'adresse
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(settings.s3_endpoint.startswith("https")).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_postgres(spark: SparkSession, settings: Settings, table: str):
    """Lit une table PostgreSQL avec le pilote JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
