-- Table de faits : une ligne par transaction.
-- Incrémentale : chaque jour, seules les lignes chargées depuis la dernière exécution sont
-- traitées, et `unique_key` remplace une transaction déjà présente au lieu de la doubler.
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select * from {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
{% endif %}
