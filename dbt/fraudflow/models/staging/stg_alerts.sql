select
    alert_id,
    rule,
    card_hash,
    card_last4,
    customer_id,
    transaction_id,
    score,
    level,
    first_event_time,
    last_event_time,
    window_start,
    window_end,
    evidence,
    detected_at,
    alert_date,
    loaded_at
from {{ source('raw', 'alerts') }}
