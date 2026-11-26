import hashlib
import json
from datetime import datetime

from lib.pseudonymize import to_bronze

SALT = "sel-de-test-0123456789"
PAN = "4104591967507950"
KAFKA_SCHEMA = "value binary, partition int, offset long, timestamp timestamp"


def bronze_row(spark, value: str, salt: str = SALT):
    messages = spark.createDataFrame(
        [(value.encode(), 0, 0, datetime(2026, 11, 8, 12))], KAFKA_SCHEMA
    )
    return to_bronze(messages, salt).first()


def test_card_number_never_reaches_bronze_in_clear(spark):
    row = bronze_row(spark, json.dumps({"transaction_id": "t1", "card_number": PAN}))
    assert PAN not in row.raw_value
    assert '"card_number": "************7950"' in row.raw_value
    assert row.card_last4 == "7950"
    assert row.card_hash == hashlib.sha256((SALT + PAN).encode()).hexdigest()


def test_the_salt_changes_the_hash(spark):
    message = json.dumps({"card_number": PAN})
    assert bronze_row(spark, message).card_hash == bronze_row(spark, message).card_hash
    assert bronze_row(spark, message).card_hash != bronze_row(spark, message, "autre-sel").card_hash


def test_a_broken_message_is_masked_too(spark):
    row = bronze_row(spark, f'{{"transaction_id":"t1","card_number":"{PAN}","amo')
    assert PAN not in row.raw_value
    assert row.card_last4 == "7950"


def test_a_message_without_card_number_has_no_hash(spark):
    row = bronze_row(spark, json.dumps({"transaction_id": "t1"}))
    assert row.card_hash is None
    assert row.card_last4 is None


def test_kafka_origin_is_kept(spark):
    row = bronze_row(spark, json.dumps({"card_number": PAN}))
    assert (row.kafka_partition, row.kafka_offset) == (0, 0)
    assert row.ingested_at is not None and row.ingest_date is not None
