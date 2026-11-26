"""Scénarios de fraude injectés dans le flux, pour pouvoir mesurer la détection.

Chaque transaction frauduleuse porte la colonne cachée `_injected_fraud`, avec le nom de
son scénario. La détection ne doit jamais la lire : elle sert seulement à calculer, à la
fin, la part des fraudes trouvées (rappel) et la part des alertes justes (précision).
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from referential import HIGH_VALUE_MCCS, MERCHANT_COUNTRIES, RISKY_MCCS, weighted_choice
from transactions import TransactionFactory

# Pays lointains : impossible d'y payer en magasin quelques minutes après un achat en Europe
FAR_COUNTRIES = ("US", "JP", "MA", "GB")
# La nuit, à l'heure de Paris : de 0 h à 4 h 59
NIGHT_HOURS = range(0, 5)


@dataclass(frozen=True)
class ScheduledEvent:
    """Une transaction à envoyer dans `delay_s` secondes."""

    delay_s: float
    event: dict


@dataclass(frozen=True)
class Scenario:
    name: str
    weight: float  # fréquence relative du scénario
    expected_size: float  # nombre moyen de transactions produites


SCENARIOS = (
    Scenario("burst", 3, 6.5),
    Scenario("amount_spike", 4, 1),
    Scenario("impossible_travel", 3, 2),
    Scenario("risky_night", 3, 1),
)


class FraudInjector:
    def __init__(self, factory: TransactionFactory, rng: random.Random, fraud_rate: float) -> None:
        self.factory = factory
        self.ref = factory.ref
        self.rng = rng
        self.fraud_rate = fraud_rate
        self._builders: dict[str, Callable[[], list[ScheduledEvent]]] = {
            "burst": self.burst,
            "amount_spike": self.amount_spike,
            "impossible_travel": self.impossible_travel,
            "risky_night": self.risky_night,
        }

    def maybe_start(self, now_paris: datetime) -> list[ScheduledEvent]:
        """Appelé à chaque transaction normale : démarre parfois un scénario de fraude."""
        active = [s for s in SCENARIOS if s.name != "risky_night" or now_paris.hour in NIGHT_HOURS]
        weights = [s.weight for s in active]
        mean_size = sum(s.weight * s.expected_size for s in active) / sum(weights)
        # Probabilité choisie pour que les fraudes forment `fraud_rate` du flux total
        probability = self.fraud_rate / ((1 - self.fraud_rate) * mean_size)
        if self.rng.random() >= probability:
            return []
        scenario = self.rng.choices(active, weights=weights)[0]
        return self._builders[scenario.name]()

    def burst(self) -> list[ScheduledEvent]:
        """Carte volée testée en ligne : 5 à 8 petits achats en moins d'une minute."""
        card = self.rng.choice(self.ref.cards)
        events = []
        for _ in range(self.rng.randint(5, 8)):
            country = weighted_choice(self.rng, MERCHANT_COUNTRIES)
            merchant = self.rng.choice(self.ref.merchants_for("ECOM", country))
            event = self.factory.build(card, merchant, "ECOM", self.rng.uniform(1, 15), "burst")
            events.append(ScheduledEvent(self.rng.uniform(0, 45), event))
        return events

    def amount_spike(self) -> list[ScheduledEvent]:
        """Achat de 12 à 30 fois le montant moyen du client, chez un vendeur d'articles chers."""
        card = self.rng.choice(self.ref.cards)
        customer = self.ref.customers[card.customer_id]
        channel = self.rng.choice(["POS", "ECOM"])
        merchants = self.ref.merchants_for(
            channel, customer.home_country, HIGH_VALUE_MCCS
        ) or self.ref.merchants_for(channel, customer.home_country)
        merchant = self.rng.choice(merchants)
        amount = customer.avg_amount_eur * self.rng.uniform(12, 30)
        event = self.factory.build(card, merchant, channel, amount, "amount_spike")
        return [ScheduledEvent(0, event)]

    def impossible_travel(self) -> list[ScheduledEvent]:
        """Carte clonée : un achat en magasin chez soi, puis très loin 1 à 10 minutes après."""
        card = self.rng.choice(self.ref.cards)
        customer = self.ref.customers[card.customer_id]
        far_country = self.rng.choice([c for c in FAR_COUNTRIES if c != customer.home_country])
        here = self.rng.choice(self.ref.merchants_for("POS", customer.home_country))
        there = self.rng.choice(self.ref.merchants_for("POS", far_country))
        usual = self.factory.usual_amount_eur(customer, "POS")
        first = self.factory.build(card, here, "POS", usual, "impossible_travel")
        second = self.factory.build(
            card, there, "POS", self.rng.uniform(50, 400), "impossible_travel"
        )
        return [ScheduledEvent(0, first), ScheduledEvent(self.rng.uniform(60, 600), second)]

    def risky_night(self) -> list[ScheduledEvent]:
        """Gros paiement en ligne, en pleine nuit, chez un commerçant à risque."""
        card = self.rng.choice(self.ref.cards)
        merchant = self.rng.choice(self.ref.merchants_for("ECOM", mccs=RISKY_MCCS))
        amount = self.rng.uniform(300, 1500)
        event = self.factory.build(card, merchant, "ECOM", amount, "risky_night")
        return [ScheduledEvent(0, event)]
