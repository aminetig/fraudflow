# Raccourcis pour piloter FraudFlow : tape `make help` pour la liste.

ifeq ($(OS),Windows_NT)
PYTHON_311 := py -3.11
VENV_PYTHON := .venv/Scripts/python.exe
COPY := copy
else
PYTHON_311 := python3.11
VENV_PYTHON := .venv/bin/python
COPY := cp
endif

# Sous Git Bash, empêche la conversion des chemins Linux comme /opt/kafka/bin
export MSYS_NO_PATHCONV := 1

# Les secrets du fichier .env deviennent des variables du Makefile (POSTGRES_USER...)
-include .env

KAFKA_BIN := /opt/kafka/bin
TOPIC := transactions.raw
DATE ?= $(shell $(VENV_PYTHON) -c "import datetime as d; print(d.date.today() - d.timedelta(days=1))")
SPARK_RUN := docker compose run --rm --no-deps spark-streaming python -m

.DEFAULT_GOAL := help
.PHONY: help up down ps logs reset ui batch streaming dag-run sample consume topics alerts \
	lake lake-report evaluate crash-spark dbt dbt-docs psql venv test lint

help:
	@echo Plateforme
	@echo   make up          : lance la plateforme temps reel en arriere-plan
	@echo   make down        : arrete tout, les donnees sont gardees
	@echo   make ps          : etat des services
	@echo   make logs        : suit le generateur et Spark
	@echo   make ui          : lance aussi Kafka UI, sur le port 8080
	@echo   make reset       : arrete tout et SUPPRIME les donnees
	@echo Traitement quotidien
	@echo   make batch       : met Spark en pause et lance Airflow, sur le port 8081
	@echo   make streaming   : arrete Airflow et relance Spark, qui rattrape son retard
	@echo   make dag-run     : rejoue le DAG pour une date, par exemple DATE=2026-11-07
	@echo   make dbt         : lance les modeles et les tests dbt
	@echo   make dbt-docs    : documentation et lignage dbt, sur le port 8082
	@echo Kafka et alertes
	@echo   make sample      : affiche 5 transactions generees, sans Kafka
	@echo   make consume     : affiche 10 transactions qui arrivent dans Kafka
	@echo   make topics      : decrit le topic des transactions
	@echo   make alerts      : dernieres alertes de fraude dans MongoDB
	@echo Data lake et qualite
	@echo   make lake        : taille de chaque couche du data lake
	@echo   make lake-report : bilan bronze, silver et quarantaine
	@echo   make evaluate    : precision, rappel et delai de la detection
	@echo   make crash-spark : coupe Spark brutalement puis le relance
	@echo   make psql        : console SQL de l entrepot
	@echo Code
	@echo   make venv        : cree l environnement Python du projet
	@echo   make test        : lance les tests
	@echo   make lint        : verifie le style du code

# Premier lancement : crée le fichier des secrets à partir du modèle
.env:
	$(COPY) .env.example .env

up: .env
	docker compose --profile tools build
	docker compose up -d --build

down:
	docker compose --profile ui --profile batch down

ps:
	docker compose --profile batch ps

logs:
	docker compose logs -f generator spark-streaming

ui:
	docker compose --profile ui up -d kafka-ui

reset:
	docker compose --profile ui --profile batch down -v

# Avec 8 Go de RAM, Spark et Airflow ne tiennent pas ensemble : on alterne.
# Spark reprend ensuite exactement où il s'était arrêté, grâce à ses checkpoints.
batch:
	docker compose stop spark-streaming
	docker compose --profile batch up -d airflow

streaming:
	docker compose --profile batch stop airflow
	docker compose up -d spark-streaming

dag-run:
	docker compose exec -T airflow airflow dags trigger fraudflow_daily --logical-date $(DATE)T00:00:00+00:00

sample:
	docker compose run --rm --no-deps generator python producer.py --dry-run --count 5

consume:
	docker compose exec -T kafka $(KAFKA_BIN)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(TOPIC) --max-messages 10 --property print.key=true --property print.partition=true --property print.offset=true

topics:
	docker compose exec -T kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic $(TOPIC)

alerts:
	docker compose exec -T mongo mongosh --quiet /scripts/last_alerts.js

lake:
	docker compose exec -T minio mc du --depth 3 lake

lake-report:
	$(SPARK_RUN) jobs.lake_report

evaluate:
	$(SPARK_RUN) jobs.evaluate_rules

crash-spark:
	docker compose kill spark-streaming
	docker compose up -d spark-streaming

dbt:
	docker compose --profile tools run --rm dbt build

dbt-docs:
	docker compose --profile tools run --rm --service-ports --entrypoint sh dbt -c "dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8080"

psql:
	docker compose exec postgres psql -U $(POSTGRES_USER) -d warehouse

venv:
	$(PYTHON_311) -m venv .venv
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt

test:
	$(VENV_PYTHON) -m pytest -q

lint:
	$(VENV_PYTHON) -m ruff check .
