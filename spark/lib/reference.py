"""Données de référence utilisées par les contrôles qualité."""

import pycountry

# Codes ISO 4217 (devises) et ISO 3166-1 alpha-2 (pays), tenus à jour par pycountry
ISO_CURRENCIES = frozenset(currency.alpha_3 for currency in pycountry.currencies)
ISO_COUNTRIES = frozenset(country.alpha_2 for country in pycountry.countries)

# Canaux de paiement : TPE en magasin, achat en ligne, distributeur de billets
CHANNELS = ("POS", "ECOM", "ATM")

# Taux de change fictifs, les mêmes que ceux du générateur : nombre d'unités pour 1 EUR
EUR_RATES = {"EUR": 1.0, "GBP": 0.85, "CHF": 0.95, "USD": 1.10, "MAD": 11.0, "JPY": 165.0}
