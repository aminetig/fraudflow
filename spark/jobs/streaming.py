"""Lance les traitements en continu.

- bronze : Kafka -> data lake, numéro de carte masqué ;
- silver et quarantine : contrôle qualité du bronze ;
- fraud_burst, fraud_travel, fraud_instant : détection de fraude, directement depuis Kafka.

Toutes les requêtes tournent dans une seule application Spark, donc une seule machine Java :
c'est plus économe en mémoire. Chacune a son propre checkpoint et reprend seule après une panne.
"""

import logging
import sys

from pyspark.sql.streaming import StreamingQueryListener

from jobs import stream_bronze, stream_fraud_detection, stream_silver
from jobs.ops_metrics import OpsMetrics
from lib.config import create_spark, load_settings

log = logging.getLogger("streaming")


class ProgressLogger(StreamingQueryListener):
    """À chaque micro-batch qui a reçu des données : une ligne de log et une ligne de métriques."""

    def __init__(self, metrics: OpsMetrics) -> None:
        self.metrics = metrics

    def onQueryStarted(self, event) -> None:
        log.info("Requête %s démarrée", event.name)

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        if not progress.numInputRows:
            return
        duration_ms = progress.durationMs.get("triggerExecution", 0)
        # Lignes lues en entrée : la quarantaine lit tout le bronze, puis n'en garde qu'une part
        log.info(
            "%-13s lot %-5d %6d lignes lues en %5d ms",
            progress.name,
            progress.batchId,
            progress.numInputRows,
            duration_ms,
        )
        self.metrics.record_progress(
            progress.name,
            progress.batchId,
            progress.numInputRows,
            duration_ms,
            progress.processedRowsPerSecond,
        )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("Requête %s arrêtée sur une erreur : %s", event.id, event.exception)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    spark = create_spark("fraudflow-streaming", settings)
    spark.streams.addListener(ProgressLogger(OpsMetrics(settings)))

    stream_bronze.start(spark, settings)
    stream_silver.start(spark, settings)
    stream_fraud_detection.start(spark, settings)
    log.info("Requêtes actives : %s", ", ".join(q.name for q in spark.streams.active))

    # Si une requête échoue, le programme s'arrête en erreur et Docker le relance :
    # chaque requête reprend alors depuis son checkpoint
    spark.streams.awaitAnyTermination()
    return 1


if __name__ == "__main__":
    sys.exit(main())
