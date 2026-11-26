"""Détection de fraude en temps réel : Kafka -> 4 règles -> alertes.

La détection lit Kafka directement, et non le silver : on gagne le délai d'écriture des
fichiers, et une alerte part en quelques secondes. Elle applique en mémoire exactement
les mêmes contrôles que la couche silver (même code), donc elle ignore les lignes invalides.

Chaque alerte est écrite à trois endroits :
- MongoDB : pour les analystes fraude, un document par alerte, avec ses preuves ;
- le topic Kafka fraud.alerts : pour d'autres systèmes (blocage de carte, SMS au client...) ;
- le data lake (gold/alerts) : pour l'historique et l'entrepôt de données.
"""

import json
import logging
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from jobs.ops_metrics import OpsMetrics
from lib.config import Settings, read_postgres
from lib.fraud_rules import burst_alerts, enrich, instant_alerts, travel_alerts
from lib.pseudonymize import to_bronze
from lib.quality import to_silver, validate

log = logging.getLogger("fraud")

# Deux alertes d'une règle à fenêtre, sur la même carte et à moins de cet écart l'une de
# l'autre, forment un seul incident : les fenêtres glissantes se chevauchent, et sans ce
# regroupement une même fraude produirait plusieurs alertes
INCIDENT_GAP = {
    "R1_BURST": timedelta(minutes=2),
    "R3_IMPOSSIBLE_TRAVEL": timedelta(minutes=30),
}


def merge_incidents(alerts: list[dict], find_existing: Callable[[dict], dict | None]) -> list[dict]:
    """Regroupe les alertes d'une même fraude en un seul incident.

    `find_existing` cherche un incident déjà enregistré (dans MongoDB) proche de l'alerte.
    L'incident garde son identifiant, s'étend dans le temps et garde les preuves les plus fortes.
    """
    merged: list[dict] = []
    for alert in sorted(alerts, key=lambda a: (a["rule"], a["card_hash"], a["first_event_time"])):
        gap = INCIDENT_GAP.get(alert["rule"])
        previous = merged[-1] if merged else None
        same_incident = previous is not None and (
            previous["alert_id"] == alert["alert_id"]  # même transaction reçue deux fois
            or (
                gap is not None
                and (previous["rule"], previous["card_hash"]) == (alert["rule"], alert["card_hash"])
                and alert["first_event_time"] <= previous["last_event_time"] + gap
            )
        )
        if same_incident:
            merged[-1] = _combine(previous, alert)
            continue
        existing = find_existing(alert) if gap is not None else None
        if existing is not None:
            alert = _combine({**existing, "detected_at": alert["detected_at"]}, alert)
        merged.append(alert)
    return merged


def _combine(incident: dict, alert: dict) -> dict:
    strongest = alert if alert["score"] >= incident["score"] else incident
    return {
        **strongest,
        "alert_id": incident["alert_id"],
        "first_event_time": min(incident["first_event_time"], alert["first_event_time"]),
        "last_event_time": max(incident["last_event_time"], alert["last_event_time"]),
        "detected_at": max(incident["detected_at"], alert["detected_at"]),
    }


class AlertSink:
    """Écrit les alertes d'un micro-batch. Rejouer un lot ne crée aucun doublon."""

    def __init__(
        self,
        settings: Settings,
        query_name: str,
        rules: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        self.settings = settings
        self.query_name = query_name
        self.rules = rules  # règles appliquées au lot, pour les règles sans mémoire
        self.metrics = OpsMetrics(settings)
        self._client: MongoClient | None = None

    def _collection(self):
        if self._client is None:
            self._client = MongoClient(self.settings.mongo_uri, tz_aware=False)
            alerts = self._client.fraudflow.alerts
            alerts.create_index([("card_hash", ASCENDING)])
            alerts.create_index([("created_at", ASCENDING)])
            alerts.create_index([("rule", ASCENDING), ("level", ASCENDING)])
            alerts.create_index(
                [("rule", ASCENDING), ("card_hash", ASCENDING), ("last_event_time", DESCENDING)]
            )
        return self._client.fraudflow.alerts

    def _find_incident(self, alert: dict) -> dict | None:
        gap = INCIDENT_GAP[alert["rule"]]
        found = self._collection().find_one(
            {
                "rule": alert["rule"],
                "card_hash": alert["card_hash"],
                "last_event_time": {"$gte": alert["first_event_time"] - gap},
                "first_event_time": {"$lte": alert["last_event_time"] + gap},
            },
            sort=[("last_event_time", DESCENDING)],
        )
        if found is None:
            return None
        found["evidence"] = json.dumps(found["evidence"])
        return found

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        source = batch.persist()  # le lot est relu par les règles : on le garde en mémoire
        try:
            alerts = self.rules(source) if self.rules else source
            alerts = alerts.withColumn("detected_at", F.current_timestamp())
            columns = alerts.columns
            found = [row.asDict() for row in alerts.collect()]  # quelques alertes par lot
            incidents = merge_incidents(found, self._find_incident)
            if incidents:
                self._to_mongo(incidents)
                result = alerts.sparkSession.createDataFrame(
                    [tuple(i[c] for c in columns) for i in incidents], alerts.schema
                )
                self._to_kafka(result)
                # Un dossier par lot, écrasé si le lot est rejoué : pas de doublon
                result.write.mode("overwrite").parquet(
                    f"{self.settings.gold_alerts_path}/query={self.query_name}/batch_id={batch_id}"
                )
            self.metrics.record_alerts(
                self.query_name, batch_id, Counter(i["rule"] for i in incidents)
            )
        finally:
            source.unpersist()

    def _to_mongo(self, incidents: list[dict]) -> None:
        now = datetime.now(UTC).replace(tzinfo=None)
        operations = []
        for incident in incidents:
            doc = {k: v for k, v in incident.items() if k not in ("_id", "created_at")}
            doc["evidence"] = json.loads(doc["evidence"])  # preuves : leur forme dépend de la règle
            operations.append(
                UpdateOne(
                    {"_id": doc["alert_id"]},  # upsert : créée si nouvelle, mise à jour sinon
                    {"$set": {**doc, "updated_at": now}, "$setOnInsert": {"created_at": now}},
                    upsert=True,
                )
            )
        self._collection().bulk_write(operations, ordered=False)

    def _to_kafka(self, alerts: DataFrame) -> None:
        (
            alerts.select(
                F.col("alert_id").alias("key"), F.to_json(F.struct(*alerts.columns)).alias("value")
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap)
            .option("topic", self.settings.alerts_topic)
            .save()
        )


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    # Référentiel lu une fois au démarrage, puis gardé en mémoire (jointure flux / table)
    customers = read_postgres(spark, settings, "ref.customers").cache()
    merchants = read_postgres(spark, settings, "ref.merchants").cache()
    log.info("Référentiel : %d clients, %d commerçants", customers.count(), merchants.count())

    def transactions() -> DataFrame:
        messages = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 20000)
            .load()
        )
        valid = to_silver(validate(to_bronze(messages, settings.card_hash_salt)))
        # Les fenêtres attendent les retardataires 15 minutes, puis Spark libère leur mémoire
        return enrich(valid, customers, merchants).withWatermark("event_time", "15 minutes")

    def start_query(name: str, stream: DataFrame, mode: str, sink: AlertSink) -> StreamingQuery:
        return (
            stream.writeStream.queryName(name)
            .outputMode(mode)
            .foreachBatch(sink)
            .option("checkpointLocation", settings.checkpoint(name))
            .trigger(processingTime=settings.fraud_trigger)
            .start()
        )

    # Règles à fenêtre : mises à jour à chaque lot (update), l'alerte part dès le seuil atteint.
    # Règles sans mémoire (R2, R4) : appliquées au lot dans le sink, qui n'est ainsi lu qu'une fois
    queries = [
        ("fraud_burst", burst_alerts(transactions()), "update", None),
        ("fraud_travel", travel_alerts(transactions()), "update", None),
        ("fraud_instant", transactions(), "append", instant_alerts),
    ]
    return [
        start_query(name, stream, mode, AlertSink(settings, name, rules))
        for name, stream, mode, rules in queries
    ]
