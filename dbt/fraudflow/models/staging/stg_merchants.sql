select
    merchant_id,
    name as merchant_name,
    mcc,
    category,
    country as merchant_country,
    is_risky
from {{ source('ref', 'merchants') }}
