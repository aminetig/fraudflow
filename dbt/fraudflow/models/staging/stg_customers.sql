select
    customer_id,
    full_name,
    email,
    home_country,
    avg_amount_eur as usual_amount_eur
from {{ source('ref', 'customers') }}
