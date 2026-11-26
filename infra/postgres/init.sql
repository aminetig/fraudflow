-- Exécuté une seule fois, à la création du volume PostgreSQL, dans la base `warehouse`.
-- Une seule instance pour deux bases (économie de mémoire) : l'entrepôt et Airflow.
CREATE DATABASE airflow;

CREATE SCHEMA raw;  -- données chargées depuis le data lake, sans transformation métier
CREATE SCHEMA ref;  -- référentiel de la banque : clients et commerçants
CREATE SCHEMA ops;  -- supervision des pipelines

-- Référentiel ------------------------------------------------------------------------------

CREATE TABLE ref.customers (
    customer_id     text PRIMARY KEY,
    full_name       text NOT NULL,           -- donnée personnelle
    email           text NOT NULL,           -- donnée personnelle
    home_country    char(2) NOT NULL,
    avg_amount_eur  numeric(10, 2) NOT NULL, -- montant habituel d'un achat
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ref.merchants (
    merchant_id text PRIMARY KEY,
    name        text NOT NULL,
    mcc         char(4) NOT NULL,
    category    text NOT NULL,
    country     char(2) NOT NULL,
    is_risky    boolean NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);

-- Données brutes, chargées chaque jour depuis le data lake -----------------------------------

CREATE TABLE raw.transactions (
    transaction_id  text NOT NULL,
    card_hash       text NOT NULL,
    card_last4      char(4),
    customer_id     text NOT NULL,
    merchant_id     text NOT NULL,
    amount          numeric(14, 2) NOT NULL,
    currency        char(3) NOT NULL,
    amount_eur      numeric(14, 2),
    country         char(2) NOT NULL,
    channel         text NOT NULL,
    mcc             char(4) NOT NULL,
    event_time      timestamptz NOT NULL,
    event_date      date NOT NULL,
    _injected_fraud text,                    -- vérité terrain du simulateur, jamais lue par la détection
    kafka_partition integer,
    kafka_offset    bigint,
    ingested_at     timestamptz,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX transactions_event_date_idx ON raw.transactions (event_date);

CREATE TABLE raw.alerts (
    alert_id         text PRIMARY KEY,
    rule             text NOT NULL,
    card_hash        text NOT NULL,
    card_last4       char(4),
    customer_id      text,
    transaction_id   text,                   -- renseigné pour les règles sur une transaction
    score            integer NOT NULL,
    level            text NOT NULL,
    first_event_time timestamptz NOT NULL,
    last_event_time  timestamptz NOT NULL,
    window_start     timestamptz,            -- renseigné pour les règles sur une fenêtre
    window_end       timestamptz,
    evidence         jsonb,                  -- preuves : leur forme dépend de la règle
    detected_at      timestamptz NOT NULL,
    alert_date       date NOT NULL,
    loaded_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX alerts_alert_date_idx ON raw.alerts (alert_date);

-- Supervision ------------------------------------------------------------------------------

-- Volumes de chaque journée dans le data lake, pour le seuil qualité
CREATE TABLE ops.lake_daily (
    day             date PRIMARY KEY,
    bronze_rows     bigint NOT NULL,
    quarantine_rows bigint NOT NULL,
    silver_rows     bigint NOT NULL,
    alerts          bigint NOT NULL,
    quarantine_rate numeric(7, 4) NOT NULL,
    computed_at     timestamptz NOT NULL DEFAULT now()
);

-- Une ligne par exécution du DAG quotidien
CREATE TABLE ops.pipeline_runs (
    dag_id           text NOT NULL,
    run_id           text NOT NULL,
    logical_date     date NOT NULL,
    status           text NOT NULL,
    ingested_rows    bigint,
    quarantined_rows bigint,
    quarantine_rate  numeric(7, 4),
    alerts           bigint,
    finished_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, run_id)
);

-- Une ligne par micro-batch des traitements en continu
CREATE TABLE ops.streaming_metrics (
    query_name      text NOT NULL,
    batch_id        bigint NOT NULL,
    input_rows      bigint NOT NULL,
    duration_ms     bigint NOT NULL,
    rows_per_second numeric,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX streaming_metrics_recorded_at_idx ON ops.streaming_metrics (recorded_at);

-- Nombre d'alertes émises par règle, à chaque micro-batch
CREATE TABLE ops.alert_metrics (
    query_name  text NOT NULL,
    batch_id    bigint NOT NULL,
    rule        text NOT NULL,
    alerts      integer NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id, rule)
);
