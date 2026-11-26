import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pyspark.sql import functions as F

from lib.fraud_rules import burst_alerts, enrich, instant_alerts, travel_alerts

NOON = datetime(2026, 11, 8, 12, 0, 0, tzinfo=UTC)
TX_SCHEMA = (
    "transaction_id string, card_hash string, card_last4 string, customer_id string, "
    "merchant_id string, amount_eur decimal(14,2), country string, channel string, "
    "mcc string, event_time timestamp"
)


def tx(spark, rows):
    """rows : (id, carte, montant €, pays, canal, commerçant, heure)."""
    data = [
        (tid, card, "7950", "C1", merchant, Decimal(str(amount)), country, channel, "5411", at)
        for tid, card, amount, country, channel, merchant, at in rows
    ]
    return spark.createDataFrame(data, TX_SCHEMA)


def enriched(spark, rows, avg=40, risky=False):
    customers = spark.createDataFrame(
        [("C1", Decimal(avg))], "customer_id string, avg_amount_eur decimal(10,2)"
    )
    merchants = spark.createDataFrame(
        [("M1", risky), ("M2", True)], "merchant_id string, is_risky boolean"
    )
    return enrich(tx(spark, rows), customers, merchants)


def payments(n, card="K1", start=NOON, every_s=8, channel="ECOM", country="FR"):
    return [
        (f"t{i}", card, 5, country, channel, "M1", start + timedelta(seconds=i * every_s))
        for i in range(n)
    ]


def test_burst_of_five_raises_a_single_alert(spark):
    alerts = burst_alerts(tx(spark, payments(5))).collect()
    assert alerts and {a.rule for a in alerts} == {"R1_BURST"}
    # Les fenêtres qui se chevauchent retrouvent la même fraude : un seul alert_id
    assert len({a.alert_id for a in alerts}) == 1
    assert alerts[0].score == 40 and alerts[0].level == "LOW"
    assert json.loads(alerts[0].evidence)["transactions"] == 5


def test_four_payments_and_a_duplicate_are_not_a_burst(spark):
    rows = payments(4) + [payments(1)[0]]  # t0 reçu deux fois
    assert burst_alerts(tx(spark, rows)).count() == 0


def test_two_countries_in_five_minutes_is_impossible_travel(spark):
    rows = [
        ("a", "K1", 30, "FR", "POS", "M1", NOON),
        ("b", "K1", 200, "US", "POS", "M1", NOON + timedelta(minutes=5)),
    ]
    alerts = travel_alerts(tx(spark, rows)).collect()
    assert len({a.alert_id for a in alerts}) == 1
    assert json.loads(alerts[0].evidence)["countries"] == ["FR", "US"]
    assert alerts[0].level == "HIGH"


def test_online_purchase_abroad_is_not_travel(spark):
    rows = [
        ("a", "K1", 30, "FR", "POS", "M1", NOON),
        ("b", "K1", 20, "US", "ECOM", "M1", NOON + timedelta(minutes=5)),
    ]
    assert travel_alerts(tx(spark, rows)).count() == 0


def test_amount_spike_compares_to_the_usual_amount(spark):
    rows = [("big", "K1", 500, "FR", "POS", "M1", NOON), ("ok", "K1", 300, "FR", "POS", "M1", NOON)]
    (alert,) = instant_alerts(enriched(spark, rows, avg=40)).collect()
    assert alert.rule == "R2_AMOUNT_SPIKE" and alert.transaction_id == "big"
    assert alert.score == 67 and alert.level == "MEDIUM"  # 30 + 3 x 12,5


def test_risky_merchant_at_night_paris_time(spark):
    night = datetime(2026, 11, 8, 1, 0, tzinfo=UTC)  # 2 h du matin à Paris
    rows = [
        ("night", "K1", 350, "FR", "ECOM", "M2", night),
        ("day", "K1", 350, "FR", "ECOM", "M2", NOON),
        ("small", "K1", 100, "FR", "ECOM", "M2", night),
    ]
    alerts = instant_alerts(enriched(spark, rows, avg=100)).filter(
        F.col("rule") == "R4_RISKY_NIGHT"
    ).collect()
    assert [a.transaction_id for a in alerts] == ["night"]
    assert json.loads(alerts[0].evidence)["local_hour"] == 2


def test_alert_id_is_stable_when_replayed(spark):
    first = instant_alerts(enriched(spark, [("big", "K1", 900, "FR", "POS", "M1", NOON)]))
    second = instant_alerts(enriched(spark, [("big", "K1", 900, "FR", "POS", "M1", NOON)]))
    assert first.first().alert_id == second.first().alert_id
