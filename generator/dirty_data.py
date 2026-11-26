"""Données volontairement abîmées, comme dans la vraie vie.

La plupart devront être repérées et mises en quarantaine par la couche silver. Les doublons
devront être supprimés, et les transactions en retard acceptées malgré tout.
"""

from __future__ import annotations

import json
import random

from referential import weighted_choice
from transactions import format_event_time

# Type d'anomalie : poids de tirage
DIRTY_KINDS: dict[str, float] = {
    "negative_amount": 2,  # montant négatif
    "null_amount": 2,  # montant vide
    "invalid_currency": 2,  # devise qui n'existe pas
    "invalid_country": 1,  # pays qui n'existe pas
    "missing_field": 2,  # champ obligatoire absent
    "future_event_time": 1,  # date dans le futur (horloge du terminal déréglée)
    "broken_json": 2,  # message coupé en plein milieu
    "duplicate": 3,  # même transaction envoyée deux fois
    "late_event": 3,  # transaction qui arrive 5 à 15 minutes en retard
}


def serialize(event: dict) -> bytes:
    """Transforme la transaction en JSON compact, encodé en UTF-8."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class DirtyInjector:
    def __init__(self, rng: random.Random, dirty_rate: float) -> None:
        self.rng = rng
        self.dirty_rate = dirty_rate

    def maybe_apply(self, event: dict, now: float) -> tuple[str | None, list[bytes]]:
        """Abîme parfois la transaction. Renvoie l'anomalie appliquée et les messages à envoyer."""
        if self.rng.random() >= self.dirty_rate:
            return None, [serialize(event)]
        kind = weighted_choice(self.rng, DIRTY_KINDS)
        return kind, self.apply(kind, event, now)

    def apply(self, kind: str, event: dict, now: float) -> list[bytes]:
        event = dict(event)  # on travaille sur une copie
        if kind == "negative_amount":
            event["amount"] = -abs(event["amount"])
        elif kind == "null_amount":
            event["amount"] = None
        elif kind == "invalid_currency":
            event["currency"] = self.rng.choice(["EURO", "€", "XYZ", ""])
        elif kind == "invalid_country":
            event["country"] = self.rng.choice(["FRA", "France", "ZZ", ""])
        elif kind == "missing_field":
            field = self.rng.choice(["transaction_id", "customer_id", "merchant_id", "event_time"])
            del event[field]
        elif kind == "future_event_time":
            event["event_time"] = format_event_time(now + self.rng.uniform(3600, 72 * 3600))
        elif kind == "late_event":
            event["event_time"] = format_event_time(now - self.rng.uniform(5 * 60, 15 * 60))
        elif kind == "duplicate":
            payload = serialize(event)
            return [payload, payload]
        elif kind == "broken_json":
            payload = serialize(event)
            return [payload[: self.rng.randint(10, len(payload) - 10)]]
        else:
            raise ValueError(f"Anomalie inconnue : {kind}")
        return [serialize(event)]
