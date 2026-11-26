-- Dimension commerçant : le référentiel, enrichi de l'activité observée
with activity as (
    select
        merchant_id,
        count(*) as transactions,
        sum(amount_eur) as total_amount_eur
    from {{ ref('fct_transactions') }}
    group by merchant_id
)

select
    m.merchant_id,
    m.merchant_name,
    m.mcc,
    m.category,
    m.merchant_country,
    m.is_risky,
    coalesce(a.transactions, 0) as transactions,
    coalesce(a.total_amount_eur, 0) as total_amount_eur
from {{ ref('stg_merchants') }} as m
left join activity as a on a.merchant_id = m.merchant_id
