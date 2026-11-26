# Dictionnaire de données

La référence complète, colonne par colonne avec le lignage, est la documentation dbt : `make dbt-docs`, puis http://localhost:8082. Elle est générée à partir des fichiers `schema.yml` du projet dbt, donc toujours à jour. Ce document en donne une vue d'ensemble.

Les colonnes marquées 🔒 contiennent des données personnelles (`meta: {pii: true}` dans dbt).

## Message Kafka `transactions.raw`

Envoyé par le système de paiement, ici le générateur. La clé du message est le numéro de carte.

| Champ | Type | Description |
| --- | --- | --- |
| `transaction_id` | texte (UUID) | Identifiant unique de la transaction |
| `card_number` 🔒 | texte | Numéro de carte, factice mais au bon format (clé de Luhn). Masqué dès le bronze |
| `customer_id` | texte | Client titulaire de la carte |
| `merchant_id` | texte | Commerçant |
| `amount` | nombre | Montant, dans la devise du commerçant |
| `currency` | texte | Devise (ISO 4217) |
| `country` | texte | Pays du commerçant (ISO 3166-1) |
| `channel` | texte | `POS` (magasin), `ECOM` (en ligne), `ATM` (distributeur) |
| `mcc` | texte | Catégorie du commerçant (ISO 18245) |
| `event_time` | texte (ISO 8601, UTC) | Date et heure du paiement |
| `_injected_fraud` | texte ou null | Vérité terrain du simulateur. La détection ne la lit jamais |

## Data lake (MinIO, format Parquet)

| Emplacement | Contenu | Partition |
| --- | --- | --- |
| `bronze/transactions` | Message brut avec la carte masquée (`raw_value`), `card_hash` 🔒, `card_last4` 🔒, partition et offset Kafka, heure d'arrivée | `ingest_date` |
| `silver/transactions` | Transactions valides, typées (`amount` en décimal), dédoublonnées, avec `amount_eur` | `event_date` |
| `silver/quarantine` | Lignes rejetées : message brut, `rejection_reason`, provenance Kafka | `ingest_date` |
| `gold/alerts` | Alertes de fraude, une version par micro-batch | `query`, `batch_id` |

Motifs de rejet (`rejection_reason`) : `invalid_json`, `missing_<champ>`, `invalid_amount`, `non_positive_amount`, `invalid_currency`, `invalid_country`, `invalid_channel`, `invalid_event_time`, `future_event_time`.

## Entrepôt PostgreSQL (base `warehouse`)

| Schéma | Table | Grain | Rôle |
| --- | --- | --- | --- |
| `ref` | `customers` | 1 client | Référentiel : nom 🔒, e-mail 🔒, pays, montant habituel |
| `ref` | `merchants` | 1 commerçant | Référentiel : MCC, catégorie, pays, `is_risky` |
| `raw` | `transactions` | 1 transaction | Copie du silver, chargée chaque jour |
| `raw` | `alerts` | 1 alerte | Dernière version de chaque alerte, avec ses preuves en JSONB |
| `staging` | `stg_*` | comme la source | Renommage, sans règle métier |
| `intermediate` | `int_transactions_enriched` | 1 transaction | Jointure avec le client, le commerçant et les alertes |
| `marts` | `fct_transactions` | 1 transaction | Table de faits, incrémentale |
| `marts` | `dim_customers`, `dim_merchants` | 1 client, 1 commerçant | Dimensions, avec l'activité observée |
| `marts` | `mart_fraud_daily` | jour × canal × pays | Volumes, montants, transactions signalées, montant à risque |
| `marts` | `mart_merchant_risk` | 1 commerçant signalé | Classement des commerçants les plus signalés |
| `marts` | `mart_card_risk_daily` | carte × jour | Score combiné de toutes les règles |
| `ops` | `lake_daily` | 1 jour | Volumes du data lake et taux de quarantaine, pour le seuil qualité |
| `ops` | `pipeline_runs` | 1 exécution du DAG | Statut, volumes, alertes |
| `ops` | `streaming_metrics` | 1 micro-batch | Lignes lues, durée, débit, par requête |
| `ops` | `alert_metrics` | micro-batch × règle | Nombre d'alertes émises |

## MongoDB (base `fraudflow`, collection `alerts`)

Un document par incident de fraude, identifié par `_id` (égal à `alert_id`). Ses champs communs sont `rule`, `card_hash` 🔒, `card_last4` 🔒, `customer_id`, `score`, `level`, `first_event_time`, `last_event_time`, `created_at` et `updated_at`. Le champ `evidence` change de forme selon la règle :

| Règle | Exemple de preuves |
| --- | --- |
| R1 rafale | `{"transactions": 7, "distinct_merchants": 6}` |
| R2 montant anormal | `{"amount_eur": 912.4, "usual_amount_eur": 41.2, "ratio": 22.1, "merchant_id": "M0412", "mcc": "5732"}` |
| R3 voyage impossible | `{"countries": ["FR", "US"], "minutes_between": 6.3}` |
| R4 commerçant à risque la nuit | `{"amount_eur": 640.0, "local_hour": 2, "merchant_id": "M0997", "mcc": "7995"}` |

Index : `card_hash`, `created_at`, (`rule`, `level`), et (`rule`, `card_hash`, `last_event_time`) pour regrouper les incidents.
