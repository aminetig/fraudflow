-- Indicateurs de fraude par jour, canal et pays du commerçant
select
    event_date,
    channel,
    merchant_country,
    count(*) as transactions,
    sum(amount_eur) as amount_eur,
    sum(case when is_alerted then 1 else 0 end) as alerted_transactions,
    sum(case when is_alerted then amount_eur else 0 end) as amount_at_risk_eur,
    round(avg(case when is_alerted then 1.0 else 0.0 end), 4) as alert_rate
from {{ ref('fct_transactions') }}
group by event_date, channel, merchant_country
