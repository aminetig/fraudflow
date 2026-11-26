"""Référentiel simulé de la banque : clients, cartes et commerçants.

Il est construit à partir d'une graine fixe : relancer le générateur redonne exactement
les mêmes clients, cartes et commerçants. C'est indispensable, car la détection de fraude
comparera chaque transaction au profil habituel de son client.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from faker import Faker

# Canaux de paiement : TPE en magasin, achat en ligne, distributeur de billets (DAB)
CHANNELS = ("POS", "ECOM", "ATM")

# Pays des commerçants, avec leur poids de tirage
MERCHANT_COUNTRIES: dict[str, float] = {
    "FR": 75, "BE": 4, "DE": 4, "ES": 4, "IT": 4,
    "GB": 2.5, "CH": 2, "US": 2, "MA": 1.5, "JP": 1,
}
# Pays de résidence des clients
CUSTOMER_COUNTRIES: dict[str, float] = {"FR": 95, "BE": 1.25, "DE": 1.25, "ES": 1.25, "IT": 1.25}
# Pays voisins : un achat en magasin de l'autre côté de la frontière reste légitime
NEIGHBOURS: dict[str, list[str]] = {
    "FR": ["BE", "DE", "ES", "IT", "CH"],
    "BE": ["FR", "DE"],
    "DE": ["FR", "BE", "CH"],
    "ES": ["FR"],
    "IT": ["FR", "CH"],
}
CURRENCY_BY_COUNTRY: dict[str, str] = {
    "FR": "EUR", "BE": "EUR", "DE": "EUR", "ES": "EUR", "IT": "EUR",
    "GB": "GBP", "CH": "CHF", "US": "USD", "MA": "MAD", "JP": "JPY",
}
# Taux de change fixes et fictifs : nombre d'unités de la devise pour 1 EUR
EUR_RATES: dict[str, float] = {
    "EUR": 1.0, "GBP": 0.85, "CHF": 0.95, "USD": 1.10, "MAD": 11.0, "JPY": 165.0,
}

# Codes MCC (catégorie du commerçant, norme ISO 18245) : libellé et poids de tirage
ATM_MCC = "6011"
MCC_CATALOG: dict[str, tuple[str, float]] = {
    "5411": ("Supermarché", 25),
    "5812": ("Restaurant", 12),
    "5814": ("Restauration rapide", 8),
    "5541": ("Station-service", 8),
    "5912": ("Pharmacie", 6),
    "5651": ("Vêtements", 6),
    "5311": ("Grand magasin", 5),
    "4111": ("Transport", 5),
    "5999": ("Commerce divers", 5),
    "5732": ("Électronique", 4),
    "7011": ("Hôtel", 3),
    "4511": ("Compagnie aérienne", 2),
    "5944": ("Bijouterie", 2),
    ATM_MCC: ("Distributeur de billets", 8),
    "4829": ("Transfert d'argent", 1),
    "5967": ("Vente à distance", 1),
    "6051": ("Crypto-actifs", 1),
    "7995": ("Jeux d'argent", 1),
}
# Commerçants jugés à risque par la banque
RISKY_MCCS = frozenset({"4829", "5967", "6051", "7995"})
# Commerçants qui vendent en ligne, et ceux qui ne vendent qu'en ligne
ONLINE_MCCS = frozenset(
    {"5311", "5651", "5732", "5999", "7011", "4511", "4829", "5967", "6051", "7995"}
)
ONLINE_ONLY_MCCS = frozenset({"5967"})
# Commerçants d'articles chers, cibles des fraudes au montant
HIGH_VALUE_MCCS = frozenset({"5311", "5732", "5944"})


@dataclass(frozen=True)
class Customer:
    customer_id: str
    full_name: str
    email: str
    home_country: str
    avg_amount_eur: float  # montant moyen habituel d'un achat, en euros


@dataclass(frozen=True)
class Card:
    card_number: str  # numéro factice mais au bon format (clé de Luhn valide)
    customer_id: str


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    name: str
    mcc: str
    category: str
    country: str

    def accepts(self, channel: str) -> bool:
        """Dit si ce commerçant peut recevoir un paiement par ce canal."""
        if channel == "ATM":
            return self.mcc == ATM_MCC
        if channel == "ECOM":
            return self.mcc in ONLINE_MCCS
        return self.mcc != ATM_MCC and self.mcc not in ONLINE_ONLY_MCCS


@dataclass
class Referential:
    customers: dict[str, Customer]
    cards: list[Card]
    merchants: list[Merchant]
    _index: dict[tuple[str, str], list[Merchant]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # Index (pays, canal) -> commerçants, pour tirer un commerçant sans tout parcourir
        for merchant in self.merchants:
            for channel in CHANNELS:
                if merchant.accepts(channel):
                    self._index.setdefault((merchant.country, channel), []).append(merchant)

    def merchants_for(
        self, channel: str, country: str | None = None, mccs: frozenset[str] | None = None
    ) -> list[Merchant]:
        """Commerçants qui acceptent ce canal, dans un pays donné ou partout, filtrés par MCC."""
        if country is None:
            candidates = [m for m in self.merchants if m.accepts(channel)]
        else:
            candidates = self._index.get((country, channel), [])
        if mccs is not None:
            candidates = [m for m in candidates if m.mcc in mccs]
        return candidates


def weighted_choice(rng: random.Random, weights: dict[str, float]) -> str:
    """Tire une clé du dictionnaire, en respectant les poids."""
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def luhn_check_digit(partial: str) -> str:
    """Calcule le dernier chiffre d'un numéro de carte (algorithme de Luhn)."""
    total = 0
    for position, char in enumerate(reversed(partial)):
        digit = int(char)
        if position % 2 == 0:  # un chiffre sur deux est doublé, en partant de la droite
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return str((10 - total % 10) % 10)


def is_luhn_valid(number: str) -> bool:
    """Vérifie la clé de Luhn d'un numéro de carte complet."""
    return number.isdigit() and luhn_check_digit(number[:-1]) == number[-1]


def _new_card_number(rng: random.Random, used: set[str]) -> str:
    while True:
        partial = "4" + "".join(rng.choices("0123456789", k=14))
        number = partial + luhn_check_digit(partial)
        if number not in used:
            used.add(number)
            return number


def build_referential(
    seed: int = 42, n_customers: int = 5000, n_merchants: int = 1000
) -> Referential:
    """Construit tout le référentiel à partir de la graine."""
    rng = random.Random(seed)
    fake = Faker("fr_FR")
    fake.seed_instance(seed)

    customers: dict[str, Customer] = {}
    cards: list[Card] = []
    used_numbers: set[str] = set()
    for i in range(1, n_customers + 1):
        customer = Customer(
            customer_id=f"C{i:05d}",
            full_name=fake.name(),
            email=fake.safe_email(),
            home_country=weighted_choice(rng, CUSTOMER_COUNTRIES),
            avg_amount_eur=round(min(max(rng.lognormvariate(math.log(40), 0.5), 8), 250), 2),
        )
        customers[customer.customer_id] = customer
        for _ in range(rng.choice([1, 1, 2])):  # un tiers des clients ont deux cartes
            cards.append(Card(_new_card_number(rng, used_numbers), customer.customer_id))

    # D'abord, dans chaque pays, un distributeur et un commerce (qui vend aussi en ligne),
    # puis un commerçant par MCC à risque : chaque tirage trouvera toujours un candidat
    guaranteed = [(country, mcc) for country in MERCHANT_COUNTRIES for mcc in (ATM_MCC, "5999")]
    guaranteed += [("FR", mcc) for mcc in sorted(RISKY_MCCS)]
    mcc_weights = {mcc: weight for mcc, (_, weight) in MCC_CATALOG.items()}
    merchants: list[Merchant] = []
    while len(merchants) < n_merchants:
        if guaranteed:
            country, mcc = guaranteed.pop(0)
        else:
            country = weighted_choice(rng, MERCHANT_COUNTRIES)
            mcc = weighted_choice(rng, mcc_weights)
        merchants.append(
            Merchant(
                merchant_id=f"M{len(merchants) + 1:04d}",
                name=fake.company(),
                mcc=mcc,
                category=MCC_CATALOG[mcc][0],
                country=country,
            )
        )
    return Referential(customers, cards, merchants)
