-- Score combiné par carte et par jour : une carte qui déclenche plusieurs règles
-- est plus inquiétante qu'une carte qui en déclenche une seule
select
    card_hash,
    max(card_last4) as card_last4,
    max(customer_id) as customer_id,
    alert_date,
    count(*) as alerts,
    count(distinct rule) as distinct_rules,
    string_agg(distinct rule, ',' order by rule) as rules,
    least(100, sum(score)) as combined_score,
    case
        when least(100, sum(score)) >= 70 then 'HIGH'
        when least(100, sum(score)) >= 50 then 'MEDIUM'
        else 'LOW'
    end as combined_level
from {{ ref('stg_alerts') }}
group by card_hash, alert_date
