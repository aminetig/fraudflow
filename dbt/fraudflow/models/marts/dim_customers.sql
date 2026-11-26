-- Dimension client : le référentiel, enrichi de l'activité observée
with activity as (
    select
        customer_id,
        count(*) as transactions,
        sum(amount_eur) as total_amount_eur,
        sum(case when is_alerted then 1 else 0 end) as alerted_transactions,
        min(event_time) as first_transaction_at,
        max(event_time) as last_transaction_at
    from {{ ref('fct_transactions') }}
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.home_country,
    c.usual_amount_eur,
    coalesce(a.transactions, 0) as transactions,
    coalesce(a.total_amount_eur, 0) as total_amount_eur,
    coalesce(a.alerted_transactions, 0) as alerted_transactions,
    a.first_transaction_at,
    a.last_transaction_at
from {{ ref('stg_customers') }} as c
left join activity as a on a.customer_id = c.customer_id
