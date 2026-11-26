"""Écrit les métriques des traitements en continu dans PostgreSQL (schéma ops).

Une panne de la base de supervision ne doit jamais arrêter le traitement des données :
en cas d'erreur, on prévient dans les logs et on continue.
"""

import logging

import psycopg

from lib.config import Settings

log = logging.getLogger("ops")


class OpsMetrics:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._conn: psycopg.Connection | None = None

    def _execute_many(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg.connect(
                    **self.settings.postgres_params, autocommit=True, connect_timeout=5
                )
            with self._conn.cursor() as cur:
                cur.executemany(sql, rows)
        except psycopg.Error as exc:
            log.warning("Métriques non enregistrées : %s", exc)
            self._conn = None

    def record_progress(
        self, query: str, batch_id: int, rows: int, duration_ms: int, rows_per_s: float
    ) -> None:
        # ON CONFLICT : un lot rejoué après une panne remplace sa ligne au lieu de la doubler
        self._execute_many(
            """
            INSERT INTO ops.streaming_metrics
                (query_name, batch_id, input_rows, duration_ms, rows_per_second)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (query_name, batch_id) DO UPDATE SET
                input_rows = EXCLUDED.input_rows,
                duration_ms = EXCLUDED.duration_ms,
                rows_per_second = EXCLUDED.rows_per_second,
                recorded_at = now()
            """,
            [(query, batch_id, rows, duration_ms, rows_per_s)],
        )

    def record_alerts(self, query: str, batch_id: int, counts: dict[str, int]) -> None:
        self._execute_many(
            """
            INSERT INTO ops.alert_metrics (query_name, batch_id, rule, alerts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (query_name, batch_id, rule) DO UPDATE SET
                alerts = EXCLUDED.alerts, recorded_at = now()
            """,
            [(query, batch_id, rule, n) for rule, n in counts.items()],
        )
