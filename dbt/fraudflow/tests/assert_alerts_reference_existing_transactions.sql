-- Une alerte sur une transaction précise (R2, R4) doit pointer vers une transaction connue.
-- Le test échoue s'il renvoie au moins une ligne : la détection en temps réel et le pipeline
-- silver ne verraient alors pas les mêmes données.
select a.alert_id, a.rule, a.transaction_id
from {{ ref('stg_alerts') }} as a
left join {{ ref('stg_transactions') }} as t on t.transaction_id = a.transaction_id
where a.transaction_id is not null
  and t.transaction_id is null
