"""Écrit le référentiel dans PostgreSQL : tables ref.customers et ref.merchants.

Lancé une fois au démarrage de la plateforme, avant le générateur et la détection de fraude.
Idempotent : tout le contenu est remplacé dans une seule transaction.
"""

import logging
import os
import sys

import psycopg

from referential import RISKY_MCCS, build_referential

log = logging.getLogger("referential")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ref = build_referential(int(os.environ.get("GENERATOR_SEED", "42")))
    params = {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "warehouse"),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }
    with psycopg.connect(**params) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE ref.customers, ref.merchants")
        # COPY : chargement en masse, bien plus rapide que des INSERT ligne par ligne
        columns = "customer_id, full_name, email, home_country, avg_amount_eur"
        with cur.copy(f"COPY ref.customers ({columns}) FROM STDIN") as copy:
            for c in ref.customers.values():
                copy.write_row(
                    (c.customer_id, c.full_name, c.email, c.home_country, c.avg_amount_eur)
                )
        columns = "merchant_id, name, mcc, category, country, is_risky"
        with cur.copy(f"COPY ref.merchants ({columns}) FROM STDIN") as copy:
            for m in ref.merchants:
                copy.write_row(
                    (m.merchant_id, m.name, m.mcc, m.category, m.country, m.mcc in RISKY_MCCS)
                )
    log.info(
        "Référentiel écrit : %d clients, %d commerçants", len(ref.customers), len(ref.merchants)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
