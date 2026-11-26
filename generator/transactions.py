"""Fabrique de transactions par carte légitimes, à partir du référentiel."""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime

from referential import (
    CURRENCY_BY_COUNTRY,
    EUR_RATES,
    MERCHANT_COUNTRIES,
    NEIGHBOURS,
    Card,
    Customer,
    Merchant,
    Referential,
    weighted_choice,
)

# Répartition des paiements entre les canaux
CHANNEL_WEIGHTS: dict[str, float] = {"POS": 65, "ECOM": 25, "ATM": 10}
ATM_AMOUNTS = (20, 40, 50, 60, 80, 100, 150, 200)
# Part des achats en magasin faits dans un pays voisin (frontaliers)
BORDER_SHOPPING_RATE = 0.002
# Part des achats en ligne faits chez un commerçant tiré dans n'importe quel pays
ANY_COUNTRY_ECOM_RATE = 0.2


def format_event_time(ts: float) -> str:
    """Horodatage ISO 8601 en UTC, à la milliseconde : 2026-11-07T09:30:00.123Z."""
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_local_amount(amount_eur: float, currency: str) -> float:
    """Convertit un montant en euros dans la devise du commerçant."""
    decimals = 0 if currency == "JPY" else 2  # le yen n'a pas de centimes
    return round(amount_eur * EUR_RATES[currency], decimals)


class TransactionFactory:
    """Crée les messages que le système de paiement envoie à chaque paiement par carte."""

    def __init__(self, referential: Referential, rng: random.Random) -> None:
        self.ref = referential
        self.rng = rng

    def normal(self) -> dict:
        """Une transaction légitime : un client paie comme d'habitude."""
        card = self.rng.choice(self.ref.cards)
        customer = self.ref.customers[card.customer_id]
        channel = weighted_choice(self.rng, CHANNEL_WEIGHTS)
        country = self._merchant_country(customer, channel)
        merchant = self.rng.choice(self.ref.merchants_for(channel, country))
        return self.build(card, merchant, channel, self.usual_amount_eur(customer, channel))

    def build(
        self,
        card: Card,
        merchant: Merchant,
        channel: str,
        amount_eur: float,
        fraud: str | None = None,
    ) -> dict:
        """Assemble le message JSON. `event_time` est rempli au moment de l'envoi."""
        currency = CURRENCY_BY_COUNTRY[merchant.country]
        return {
            "transaction_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "card_number": card.card_number,
            "customer_id": card.customer_id,
            "merchant_id": merchant.merchant_id,
            "amount": to_local_amount(amount_eur, currency),
            "currency": currency,
            "country": merchant.country,
            "channel": channel,
            "mcc": merchant.mcc,
            "event_time": None,
            "_injected_fraud": fraud,
        }

    def usual_amount_eur(self, customer: Customer, channel: str) -> float:
        """Un montant habituel pour ce client."""
        if channel == "ATM":
            ceiling = max(20.0, 4 * customer.avg_amount_eur)
            return float(self.rng.choice([a for a in ATM_AMOUNTS if a <= ceiling]))
        # Variation autour de la moyenne du client (loi log-normale de moyenne 1)
        sigma = 0.6
        return max(0.5, customer.avg_amount_eur * self.rng.lognormvariate(-(sigma**2) / 2, sigma))

    def _merchant_country(self, customer: Customer, channel: str) -> str:
        home = customer.home_country
        if channel == "ECOM" and self.rng.random() < ANY_COUNTRY_ECOM_RATE:
            return weighted_choice(self.rng, MERCHANT_COUNTRIES)
        if channel == "POS" and self.rng.random() < BORDER_SHOPPING_RATE:
            return self.rng.choice(NEIGHBOURS[home])
        return home
