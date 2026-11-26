"""Générateur FraudFlow : simule les paiements par carte et les envoie en continu dans Kafka.

Lancement depuis la machine (Kafka doit tourner) :
    python generator/producer.py
Aperçu sans Kafka :
    python generator/producer.py --dry-run --count 5
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import logging
import os
import random
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from dirty_data import DirtyInjector, serialize
from fraud_patterns import FraudInjector
from referential import Referential, build_referential
from transactions import TransactionFactory, format_event_time

PARIS = ZoneInfo("Europe/Paris")
REPORT_EVERY_S = 10

log = logging.getLogger("generator")


class EventStream:
    """Assemble le flux : transactions normales, fraudes programmées et données abîmées."""

    def __init__(
        self, referential: Referential, rng: random.Random, fraud_rate: float, dirty_rate: float
    ) -> None:
        self.factory = TransactionFactory(referential, rng)
        self.fraud = FraudInjector(self.factory, rng, fraud_rate)
        self.dirty = DirtyInjector(rng, dirty_rate)
        self.stats: Counter[str] = Counter()
        # Fraudes en attente, triées par heure d'envoi : (heure, numéro d'ordre, transaction)
        self._pending: list[tuple[float, int, dict]] = []
        self._order = itertools.count()

    def tick(self, now: float) -> list[tuple[str, bytes]]:
        """Renvoie les messages (clé, valeur) à envoyer à l'instant `now`."""
        for scheduled in self.fraud.maybe_start(datetime.fromtimestamp(now, PARIS)):
            heapq.heappush(
                self._pending, (now + scheduled.delay_s, next(self._order), scheduled.event)
            )

        messages: list[tuple[str, bytes]] = []
        while self._pending and self._pending[0][0] <= now:
            _, _, event = heapq.heappop(self._pending)
            event["event_time"] = format_event_time(now)
            messages.append((event["card_number"], serialize(event)))
            self.stats["fraud"] += 1
            self.stats[f"fraud:{event['_injected_fraud']}"] += 1

        event = self.factory.normal()
        event["event_time"] = format_event_time(now)
        key = event["card_number"]  # la clé décide de la partition : une carte = une partition
        anomaly, payloads = self.dirty.maybe_apply(event, now)
        if anomaly:
            self.stats["dirty"] += 1
            self.stats[f"dirty:{anomaly}"] += 1
        messages.extend((key, payload) for payload in payloads)

        self.stats["messages"] += len(messages)
        return messages


def log_stats(stats: Counter[str], delivery: Counter[str], elapsed_s: float) -> None:
    total = stats["messages"] or 1
    log.info(
        "%d messages envoyés (%.1f/s) | fraudes %.1f %% | anomalies %.1f %% | "
        "confirmés par Kafka %d | erreurs %d",
        stats["messages"],
        stats["messages"] / max(elapsed_s, 1e-9),
        100 * stats["fraud"] / total,
        100 * stats["dirty"] / total,
        delivery["ok"],
        delivery["error"],
    )


def run_kafka(stream: EventStream, args: argparse.Namespace, stop: threading.Event) -> None:
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "client.id": "fraudflow-generator",
            "acks": "all",  # un message n'est confirmé qu'une fois écrit par Kafka
            "enable.idempotence": True,  # un envoi réessayé ne crée pas de doublon
            "linger.ms": 50,  # attend 50 ms pour envoyer les messages par lots
            "compression.type": "lz4",  # lots compressés : moins de disque et de réseau
        }
    )
    delivery: Counter[str] = Counter()

    def on_delivery(err, _msg) -> None:
        # Appelé par Kafka pour chaque message : confirmé ou en échec
        if err is None:
            delivery["ok"] += 1
            return
        delivery["error"] += 1
        if delivery["error"] <= 5:
            log.error("Échec d'envoi : %s", err)

    interval = 1.0 / args.rate
    start = next_tick = last_report = time.monotonic()
    while not stop.is_set():
        for key, value in stream.tick(time.time()):
            producer.produce(args.topic, key=key.encode(), value=value, on_delivery=on_delivery)
        producer.poll(0)  # traite les confirmations reçues
        if args.count and stream.stats["messages"] >= args.count:
            break

        now = time.monotonic()
        if now - last_report >= REPORT_EVERY_S:
            log_stats(stream.stats, delivery, now - start)
            last_report = now
        # Cadence régulière ; après une pause (veille du PC), on repart sans rattrapage
        next_tick = max(next_tick + interval, now - 1)
        stop.wait(max(0.0, next_tick - time.monotonic()))

    log.info("Arrêt : envoi des derniers messages...")
    unsent = producer.flush(10)
    log_stats(stream.stats, delivery, time.monotonic() - start)
    if unsent:
        log.warning("%d messages n'ont pas été confirmés par Kafka", unsent)


def run_dry(stream: EventStream, args: argparse.Namespace) -> None:
    """Affiche les messages au lieu de les envoyer, avec une horloge simulée (sans attente)."""
    now = time.time()
    count = args.count or 10
    while stream.stats["messages"] < count:
        for key, value in stream.tick(now):
            print(f"{key} | {value.decode('utf-8', errors='replace')}")
        now += 1.0 / args.rate


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    parser = argparse.ArgumentParser(description="Générateur de transactions FraudFlow")
    parser.add_argument("--bootstrap", default=env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=env("KAFKA_TOPIC", "transactions.raw"))
    parser.add_argument(
        "--rate", type=float, default=float(env("GENERATOR_RATE", "10")),
        help="transactions normales par seconde",
    )
    parser.add_argument(
        "--fraud-rate", type=float, default=float(env("GENERATOR_FRAUD_RATE", "0.02"))
    )
    parser.add_argument(
        "--dirty-rate", type=float, default=float(env("GENERATOR_DIRTY_RATE", "0.03"))
    )
    parser.add_argument(
        "--seed", type=int, default=int(env("GENERATOR_SEED", "42")),
        help="graine du référentiel (mêmes clients à chaque lancement)",
    )
    parser.add_argument(
        "--count", type=int, default=int(env("GENERATOR_COUNT", "0")),
        help="s'arrête après N messages (0 : sans fin)",
    )
    parser.add_argument("--dry-run", action="store_true", help="affiche les messages, sans Kafka")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr
    )
    args = parse_args(argv)
    referential = build_referential(args.seed)
    log.info(
        "Référentiel : %d clients, %d cartes, %d commerçants",
        len(referential.customers), len(referential.cards), len(referential.merchants),
    )
    stream = EventStream(referential, random.Random(), args.fraud_rate, args.dirty_rate)

    if args.dry_run:
        run_dry(stream, args)
        return 0

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):  # Ctrl+C, ou arrêt du conteneur par Docker
        signal.signal(sig, lambda *_: stop.set())
    log.info(
        "Envoi vers %s, topic %s, %.0f transactions/s", args.bootstrap, args.topic, args.rate
    )
    run_kafka(stream, args, stop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
