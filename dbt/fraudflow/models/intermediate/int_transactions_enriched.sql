-- Chaque transaction avec son client, son commerçant et les alertes qui la concernent.
-- Une alerte « couvre » les transactions de sa carte entre sa première et sa dernière preuve.
with transactions as (
    select * from {{ ref('stg_transactions') }}
),

alert_hits as (
    select
        t.transaction_id,
        count(*) as alert_count,
        max(a.score) as max_alert_score,
        string_agg(distinct a.rule, ',' order by a.rule) as alert_rules
    from transactions as t
    inner join {{ ref('stg_alerts') }} as a
        on a.card_hash = t.card_hash
        and t.event_time between a.first_event_time and a.last_event_time
    group by t.transaction_id
)

select
    t.transaction_id,
    t.card_hash,
    t.card_last4,
    t.customer_id,
    t.merchant_id,
    m.merchant_name,
    m.category as merchant_category,
    m.is_risky as is_risky_merchant,
    t.amount,
    t.currency,
    t.amount_eur,
    t.merchant_country,
    c.home_country,
    t.merchant_country <> c.home_country as is_foreign,
    t.channel,
    t.mcc,
    t.event_time,
    t.event_date,
    coalesce(h.alert_count, 0) as alert_count,
    h.alert_count is not null as is_alerted,
    h.max_alert_score,
    h.alert_rules,
    t.injected_fraud,
    t.loaded_at
from transactions as t
left join {{ ref('stg_customers') }} as c on c.customer_id = t.customer_id
left join {{ ref('stg_merchants') }} as m on m.merchant_id = t.merchant_id
left join alert_hits as h on h.transaction_id = t.transaction_id
