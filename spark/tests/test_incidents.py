from datetime import datetime, timedelta

from jobs.stream_fraud_detection import merge_incidents

T0 = datetime(2026, 11, 8, 12, 0)


def alert(alert_id, rule="R3_IMPOSSIBLE_TRAVEL", card="K1", start=0, end=5, score=75):
    return {
        "alert_id": alert_id,
        "rule": rule,
        "card_hash": card,
        "score": score,
        "first_event_time": T0 + timedelta(minutes=start),
        "last_event_time": T0 + timedelta(minutes=end),
        "detected_at": T0 + timedelta(minutes=end, seconds=8),
        "evidence": "{}",
    }


def nothing_stored(_alert):
    return None


def test_overlapping_windows_of_one_fraud_become_one_incident():
    found = [alert("a", start=0, end=5), alert("b", start=-20, end=5), alert("c", start=3, end=5)]
    (incident,) = merge_incidents(found, nothing_stored)
    assert incident["first_event_time"] == T0 - timedelta(minutes=20)
    assert incident["last_event_time"] == T0 + timedelta(minutes=5)


def test_other_cards_and_distant_frauds_stay_separate():
    found = [alert("a"), alert("b", card="K2"), alert("c", start=120, end=125)]
    assert len(merge_incidents(found, nothing_stored)) == 3


def test_an_incident_already_stored_keeps_its_identifier():
    stored = alert("incident-du-lot-precedent", start=-10, end=0)
    (incident,) = merge_incidents([alert("nouvelle", start=2, end=6)], lambda _: stored)
    assert incident["alert_id"] == "incident-du-lot-precedent"
    assert incident["first_event_time"] == T0 - timedelta(minutes=10)


def test_the_same_transaction_received_twice_gives_one_alert():
    found = [alert("x", rule="R2_AMOUNT_SPIKE"), alert("x", rule="R2_AMOUNT_SPIKE")]
    assert len(merge_incidents(found, nothing_stored)) == 1


def test_single_transaction_rules_are_never_merged():
    found = [alert("x", rule="R2_AMOUNT_SPIKE"), alert("y", rule="R2_AMOUNT_SPIKE", start=1)]
    assert len(merge_incidents(found, nothing_stored)) == 2
