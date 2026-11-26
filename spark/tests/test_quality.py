import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from lib.quality import to_quarantine, to_silver, validate
from lib.schemas import BRONZE_SCHEMA

# Heure avec son fuseau : sans fuseau, Python la lirait à l'heure locale de la machine
INGESTED_AT = datetime(2026, 11, 8, 12, 0, 0, tzinfo=UTC)
VALID = {
    "transaction_id": "5b56fdcc-2c5b-4c55-a25c-46f85044432f",
    "card_number": "************7950",
    "customer_id": "C04877",
    "merchant_id": "M0947",
    "amount": 43.1,
    "currency": "EUR",
    "country": "FR",
    "channel": "POS",
    "mcc": "5411",
    "event_time": "2026-11-08T11:59:58.123Z",
    "_injected_fraud": None,
}
DELETE = object()  # marqueur : supprimer le champ du message


def checked_rows(spark, raw_values, card_hash="empreinte"):
    rows = [
        (raw, card_hash, "7950", 0, offset, INGESTED_AT, INGESTED_AT, INGESTED_AT.date())
        for offset, raw in enumerate(raw_values)
    ]
    return validate(spark.createDataFrame(rows, BRONZE_SCHEMA))


def with_change(**changes) -> str:
    event = {**VALID, **changes}
    return json.dumps({k: v for k, v in event.items() if v is not DELETE})


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (with_change(), None),
        (with_change(event_time="2026-11-08T11:50:00.000Z"), None),  # 10 min de retard : accepté
        ('{"transaction_id":"t1","amount":12.5,"curr', "invalid_json"),
        (with_change(transaction_id=DELETE), "missing_transaction_id"),
        (with_change(merchant_id=DELETE), "missing_merchant_id"),
        (with_change(amount=None), "missing_amount"),
        (with_change(currency=""), "missing_currency"),
        (with_change(amount="abc"), "invalid_amount"),
        (with_change(amount=-43.1), "non_positive_amount"),
        (with_change(currency="EURO"), "invalid_currency"),
        (with_change(country="ZZ"), "invalid_country"),
        (with_change(channel="PHONE"), "invalid_channel"),
        (with_change(event_time="hier soir"), "invalid_event_time"),
        (with_change(event_time="2026-11-09T12:00:00.000Z"), "future_event_time"),
    ],
)
def test_rejection_reason(spark, raw, reason):
    assert checked_rows(spark, [raw]).first().rejection_reason == reason


def test_missing_card_is_rejected(spark):
    assert checked_rows(spark, [with_change()], card_hash=None).first().rejection_reason == (
        "missing_card_hash"
    )


def test_silver_rows_are_typed_and_converted_to_euros(spark):
    checked = checked_rows(spark, [with_change(), with_change(amount=1650, currency="JPY")])
    silver = to_silver(checked).withColumn(
        "event_time_utc", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss.SSS")
    )
    eur, jpy = silver.orderBy("kafka_offset").collect()
    assert eur.amount == eur.amount_eur == Decimal("43.10")
    assert jpy.amount == Decimal("1650.00") and jpy.amount_eur == Decimal("10.00")
    assert eur.event_time_utc == "2026-11-08 11:59:58.123"
    assert str(eur.event_date) == "2026-11-08"


def test_each_row_goes_either_to_silver_or_to_quarantine(spark):
    checked = checked_rows(spark, [with_change(), with_change(amount=-1), "pas du json"])
    assert to_silver(checked).count() == 1
    quarantined = to_quarantine(checked).orderBy("kafka_offset").collect()
    assert [row.rejection_reason for row in quarantined] == ["non_positive_amount", "invalid_json"]
    assert quarantined[1].raw_value == "pas du json"
