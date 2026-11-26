-- Commerçants les plus signalés : volume, alertes et montant à risque
select
    t.merchant_id,
    m.merchant_name,
    m.category,
    m.merchant_country,
    m.is_risky,
    count(*) as transactions,
    sum(case when t.is_alerted then 1 else 0 end) as alerted_transactions,
    round(avg(case when t.is_alerted then 1.0 else 0.0 end), 4) as alert_rate,
    sum(case when t.is_alerted then t.amount_eur else 0 end) as amount_at_risk_eur,
    rank() over (
        order by sum(case when t.is_alerted then 1 else 0 end) desc, count(*) desc
    ) as risk_rank
from {{ ref('fct_transactions') }} as t
inner join {{ ref('dim_merchants') }} as m on m.merchant_id = t.merchant_id
group by t.merchant_id, m.merchant_name, m.category, m.merchant_country, m.is_risky
having sum(case when t.is_alerted then 1 else 0 end) > 0
