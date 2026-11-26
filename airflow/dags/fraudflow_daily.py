"""DAG quotidien FraudFlow : data lake -> entrepôt -> dbt, avec un seuil qualité bloquant.

1. check_silver_available : les données de la journée existent dans le data lake ;
2. load_warehouse : Spark charge la journée dans PostgreSQL (idempotent) ;
3. quality_gate : si plus de 5 % des lignes du jour sont en quarantaine, on s'arrête là,
   et les tables métier gardent leur dernière version saine ;
4. dbt_source_freshness : les données chargées sont récentes ;
5. dbt_build : modèles et tests dbt ;
6. quality_report : bilan de l'exécution dans ops.pipeline_runs.

Airflow orchestre mais ne calcule pas : Spark et dbt tournent dans leurs propres conteneurs.
En production, ces tâches seraient envoyées sur un cluster (Kubernetes, EMR, Databricks).
"""

import logging
import os
from datetime import datetime, timedelta

import boto3
from airflow.exceptions import AirflowFailException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger(__name__)

MAX_QUARANTINE_RATE = float(os.environ.get("QUALITY_MAX_QUARANTINE_RATE", "0.05"))
NETWORK = os.environ.get("FRAUDFLOW_NETWORK", "fraudflow_default")

# Secrets transmis aux conteneurs Spark et dbt. `private_environment` : ils n'apparaissent
# ni dans les logs ni dans l'interface d'Airflow
SECRETS = ("S3_ACCESS_KEY", "S3_SECRET_KEY", "CARD_HASH_SALT", "POSTGRES_USER", "POSTGRES_PASSWORD")


def container(task_id: str, image: str, command: str) -> DockerOperator:
    """Une tâche qui lance un conteneur sur le réseau de la plateforme, puis le supprime."""
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        private_environment={name: os.environ[name] for name in SECRETS},
        environment={"POSTGRES_HOST": "postgres"},
        network_mode=NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )


def record_failure(context) -> None:
    """Appelé quand une tâche échoue définitivement : message clair et trace dans ops."""
    ti = context["task_instance"]
    log.error(
        "ÉCHEC FraudFlow : tâche %s, exécution %s. Voir docs/runbook.md.", ti.task_id, ti.run_id
    )
    PostgresHook("warehouse").run(
        """
        INSERT INTO ops.pipeline_runs (dag_id, run_id, logical_date, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (dag_id, run_id) DO UPDATE SET status = EXCLUDED.status, finished_at = now()
        """,
        parameters=(ti.dag_id, ti.run_id, context["ds"], f"failed:{ti.task_id}"),
    )


@dag(
    dag_id="fraudflow_daily",
    # Le run de minuit traite la journée qui vient de se terminer : sa date logique (ds) est
    # celle de la veille. Attention, avec schedule="@daily", Airflow 3 donnerait la date
    # du jour qui commence, donc une journée encore vide.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 7),
    catchup=True,  # rejoue automatiquement les journées passées non traitées
    max_active_runs=1,  # une journée à la fois : la machine a peu de mémoire
    default_args={
        "retries": 2,  # une erreur passagère (base indisponible...) est réessayée
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": record_failure,
    },
    doc_md=__doc__,
    tags=["fraudflow"],
)
def fraudflow_daily():
    @task
    def check_silver_available(ds: str | None = None) -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        )
        found = s3.list_objects_v2(
            Bucket="silver", Prefix=f"transactions/event_date={ds}/", MaxKeys=1
        )
        if not found.get("KeyCount"):
            raise ValueError(f"Aucune donnée silver pour le {ds} : nouvel essai plus tard")
        log.info("Données silver présentes pour le %s", ds)

    load_warehouse = container(
        "load_warehouse", "fraudflow/spark", "python -m jobs.batch_load_warehouse --date {{ ds }}"
    )

    @task
    def quality_gate(ds: str | None = None) -> dict:
        row = PostgresHook("warehouse").get_first(
            "SELECT bronze_rows, quarantine_rows, alerts, quarantine_rate "
            "FROM ops.lake_daily WHERE day = %s",
            parameters=(ds,),
        )
        if row is None:
            raise AirflowFailException(f"Pas de volumes pour le {ds} : chargement incomplet")
        bronze, quarantined, alerts, rate = row
        log.info(
            "Le %s : %d lignes, %d en quarantaine (%.2f %%)", ds, bronze, quarantined, rate * 100
        )
        if rate > MAX_QUARANTINE_RATE:
            # AirflowFailException : échec immédiat, sans nouvel essai (il ne changerait rien)
            raise AirflowFailException(
                f"Taux de quarantaine {rate:.2%} au-dessus du seuil de {MAX_QUARANTINE_RATE:.0%}. "
                "dbt n'est pas lancé : les tables métier gardent leur dernière version saine."
            )
        return {"bronze": bronze, "quarantined": quarantined, "alerts": alerts, "rate": float(rate)}

    dbt_source_freshness = container("dbt_source_freshness", "fraudflow/dbt", "source freshness")
    dbt_build = container("dbt_build", "fraudflow/dbt", "build")

    @task
    def quality_report(volumes: dict, ds: str | None = None, run_id: str | None = None) -> None:
        PostgresHook("warehouse").run(
            """
            INSERT INTO ops.pipeline_runs (dag_id, run_id, logical_date, status, ingested_rows,
                                           quarantined_rows, quarantine_rate, alerts)
            VALUES ('fraudflow_daily', %s, %s, 'success', %s, %s, %s, %s)
            ON CONFLICT (dag_id, run_id) DO UPDATE SET
                status = 'success',
                ingested_rows = EXCLUDED.ingested_rows,
                quarantined_rows = EXCLUDED.quarantined_rows,
                quarantine_rate = EXCLUDED.quarantine_rate,
                alerts = EXCLUDED.alerts,
                finished_at = now()
            """,
            parameters=(
                run_id, ds, volumes["bronze"], volumes["quarantined"], volumes["rate"],
                volumes["alerts"],
            ),
        )

    volumes = quality_gate()
    check_silver_available() >> load_warehouse >> volumes
    volumes >> dbt_source_freshness >> dbt_build >> quality_report(volumes)


fraudflow_daily()
