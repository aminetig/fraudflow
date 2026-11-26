-- Une ligne par transaction : renommage, sans règle métier.
-- Le silver est déjà dédoublonné, mais un doublon arrivé plus de 15 minutes après l'original
-- peut lui échapper : on garde par sécurité la version chargée en dernier.
select distinct on (transaction_id)
    transaction_id,
    card_hash,
    card_last4,
    customer_id,
    merchant_id,
    amount,
    currency,
    amount_eur,
    country as merchant_country,
    channel,
    mcc,
    event_time,
    event_date,
    _injected_fraud as injected_fraud,
    loaded_at
from {{ source('raw', 'transactions') }}
order by transaction_id, loaded_at desc
