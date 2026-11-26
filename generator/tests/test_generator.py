"""Tests du générateur : chaque test vérifie une promesse précise du code."""

import json
import random
from datetime import datetime, timedelta

import pytest

from dirty_data import DIRTY_KINDS, DirtyInjector
from fraud_patterns import FraudInjector
from producer import PARIS, EventStream
from referential import (
    ATM_MCC,
    CURRENCY_BY_COUNTRY,
    EUR_RATES,
    RISKY_MCCS,
    build_referential,
    is_luhn_valid,
)
from transactions import TransactionFactory, format_event_time

FIELDS = {
    "transaction_id", "card_number", "customer_id", "merchant_id", "amount", "currency",
    "country", "channel", "mcc", "event_time", "_injected_fraud",
}
NOON = datetime(2026, 11, 7, 12, 0, tzinfo=PARIS)
NIGHT = datetime(2026, 11, 7, 2, 0, tzinfo=PARIS)


@pytest.fixture(scope="module")
def ref():
    return build_referential(seed=42, n_customers=500, n_merchants=200)


@pytest.fixture
def factory(ref):
    return TransactionFactory(ref, random.Random(1))


def amount_eur(event: dict) -> float:
    return event["amount"] / EUR_RATES[event["currency"]]


def test_referential_is_reproducible():
    first = build_referential(seed=7, n_customers=50, n_merchants=40)
    second = build_referential(seed=7, n_customers=50, n_merchants=40)
    assert first.customers == second.customers
    assert first.cards == second.cards
    assert first.merchants == second.merchants


def test_card_numbers_are_valid_and_unique(ref):
    numbers = [card.card_number for card in ref.cards]
    assert len(numbers) == len(set(numbers))
    assert all(len(n) == 16 and is_luhn_valid(n) for n in numbers)
    assert not is_luhn_valid("4111111111111112")


def test_normal_transactions_are_complete_and_consistent(ref, factory):
    owner = {card.card_number: card.customer_id for card in ref.cards}
    for _ in range(2000):
        event = factory.normal()
        assert set(event) == FIELDS
        assert event["amount"] > 0
        assert event["currency"] == CURRENCY_BY_COUNTRY[event["country"]]
        assert (event["channel"] == "ATM") == (event["mcc"] == ATM_MCC)
        assert owner[event["card_number"]] == event["customer_id"]
        assert event["_injected_fraud"] is None


def test_burst_is_five_or_more_payments_on_one_card_within_a_minute(factory):
    scheduled = FraudInjector(factory, random.Random(2), 0.02).burst()
    assert len(scheduled) >= 5
    assert len({s.event["card_number"] for s in scheduled}) == 1
    delays = [s.delay_s for s in scheduled]
    assert max(delays) - min(delays) < 60
    assert all(s.event["_injected_fraud"] == "burst" for s in scheduled)


def test_amount_spike_is_more_than_ten_times_the_usual_amount(ref, factory):
    injector = FraudInjector(factory, random.Random(3), 0.02)
    for _ in range(200):
        (scheduled,) = injector.amount_spike()
        customer = ref.customers[scheduled.event["customer_id"]]
        assert amount_eur(scheduled.event) > 10 * customer.avg_amount_eur


def test_impossible_travel_is_two_countries_within_thirty_minutes(factory):
    injector = FraudInjector(factory, random.Random(4), 0.02)
    for _ in range(200):
        first, second = injector.impossible_travel()
        assert first.event["card_number"] == second.event["card_number"]
        assert first.event["country"] != second.event["country"]
        assert second.delay_s - first.delay_s < 30 * 60
        assert first.event["channel"] == second.event["channel"] == "POS"


def test_risky_night_is_a_large_payment_at_a_risky_merchant(factory):
    (scheduled,) = FraudInjector(factory, random.Random(5), 0.02).risky_night()
    assert scheduled.event["mcc"] in RISKY_MCCS
    assert amount_eur(scheduled.event) >= 300


def test_risky_night_happens_only_at_night(factory):
    injector = FraudInjector(factory, random.Random(6), fraud_rate=0.5)

    def scenarios_at(moment: datetime) -> set[str]:
        started = (s for _ in range(3000) for s in injector.maybe_start(moment))
        return {s.event["_injected_fraud"] for s in started}

    assert "risky_night" not in scenarios_at(NOON)
    assert "risky_night" in scenarios_at(NIGHT)


@pytest.mark.parametrize("kind", sorted(DIRTY_KINDS))
def test_each_anomaly_does_what_it_says(factory, kind):
    now = NOON.timestamp()
    event = factory.normal()
    event["event_time"] = format_event_time(now)
    payloads = DirtyInjector(random.Random(8), 1.0).apply(kind, event, now)

    if kind == "duplicate":
        assert len(payloads) == 2 and payloads[0] == payloads[1]
        return
    (payload,) = payloads
    if kind == "broken_json":
        with pytest.raises(json.JSONDecodeError):
            json.loads(payload)
        return

    dirty = json.loads(payload)
    if kind == "negative_amount":
        assert dirty["amount"] < 0
    elif kind == "null_amount":
        assert dirty["amount"] is None
    elif kind == "invalid_currency":
        assert dirty["currency"] not in EUR_RATES
    elif kind == "invalid_country":
        assert dirty["country"] not in CURRENCY_BY_COUNTRY
    elif kind == "missing_field":
        assert len(dirty) == len(FIELDS) - 1
    else:
        moved = datetime.fromisoformat(dirty["event_time"]) - NOON
        if kind == "future_event_time":
            assert moved >= timedelta(hours=1)
        else:  # late_event
            assert timedelta(minutes=-15) <= moved <= timedelta(minutes=-5)


def test_stream_has_about_two_percent_fraud_and_three_percent_anomalies(ref):
    stream = EventStream(ref, random.Random(9), fraud_rate=0.02, dirty_rate=0.03)
    now = NOON.timestamp()
    for _ in range(60_000):  # 100 minutes de flux à 10 transactions par seconde
        stream.tick(now)
        now += 0.1
    total = stream.stats["messages"]
    assert 0.015 < stream.stats["fraud"] / total < 0.025
    assert 0.025 < stream.stats["dirty"] / total < 0.035
