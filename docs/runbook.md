# Runbook : exploiter FraudFlow

Ce document dit quoi faire quand quelque chose ne va pas. Chaque fiche suit le même plan : comment on le voit, comment on diagnostique, comment on corrige.

## Où regarder

| Besoin | Outil |
| --- | --- |
| Le flux arrive-t-il ? | Grafana, tableau « Santé des pipelines » (http://localhost:3000) |
| État des conteneurs | `make ps` |
| Logs du générateur et de Spark | `make logs` |
| Détail des micro-batchs Spark | Spark UI (http://localhost:4040), onglet Structured Streaming |
| Exécutions du DAG quotidien | Airflow (http://localhost:8081), ou la table `ops.pipeline_runs` |
| Contenu du data lake | `make lake`, `make lake-report`, ou la console MinIO (http://localhost:9001) |
| Dernières alertes | `make alerts` |

Requêtes SQL utiles (`make psql`) :

```sql
-- Derniers micro-batchs de chaque traitement en continu
SELECT query_name, max(recorded_at) AS dernier_lot, sum(input_rows) AS lignes_1h
FROM ops.streaming_metrics
WHERE recorded_at > now() - interval '1 hour'
GROUP BY query_name;

-- Volumes et taux de quarantaine par jour
SELECT * FROM ops.lake_daily ORDER BY day DESC;

-- Historique du DAG
SELECT logical_date, status, quarantine_rate, alerts, finished_at
FROM ops.pipeline_runs ORDER BY finished_at DESC;
```

## 1. Le retard de lecture de Kafka augmente

**Symptôme :** dans Kafka UI (`make ui`), le *lag* du traitement grandit, ou les micro-batchs durent plus que leur intervalle (30 s pour bronze et silver, 10 s pour la fraude) dans Grafana.

**Diagnostic :**
- Spark UI, onglet Structured Streaming : quel traitement est lent, et sur quelle étape ?
- `docker stats` : Spark est-il à court de mémoire ou de processeur ?
- Le générateur a-t-il été accéléré (`GENERATOR_RATE` dans `.env`) ?

**Correction :**
- Retard passager, après un redémarrage par exemple : ne rien faire. Spark rattrape par lots de 20 000 messages au plus (`maxOffsetsPerTrigger`).
- Retard durable : donner plus de cœurs à Spark (`SPARK_MASTER: local[4]`), plus de mémoire (`SPARK_DRIVER_MEMORY`), ou réduire le débit du générateur.
- En production : plus de partitions Kafka et plus d'exécuteurs Spark.

**À surveiller :** Kafka garde les messages 3 jours. Un retard plus long ferait perdre des données, et Spark s'arrêterait en erreur (`failOnDataLoss`).

## 2. Le traitement en continu s'arrête

**Symptôme :** alerte Grafana « Aucune nouvelle donnée depuis 10 minutes », ou `make ps` montre `spark-streaming` arrêté.

**Diagnostic :** `docker compose logs --tail 100 spark-streaming`, puis chercher `ERROR` ou `Exception`. Causes fréquentes :
- MinIO, Kafka ou PostgreSQL indisponible ;
- manque de mémoire : le conteneur est tué sans message clair (`docker stats`, `docker inspect fraudflow-spark-streaming-1 --format '{{.State.OOMKilled}}'`) ;
- le générateur est arrêté : alors Spark tourne, mais ne reçoit plus rien.

**Correction :** corriger la cause, puis `docker compose up -d spark-streaming`. Spark reprend seul là où il s'était arrêté, grâce à ses checkpoints : ni perte ni doublon. C'est testé (voir le README, test de panne).

**Ne jamais faire :** supprimer les checkpoints pour « débloquer » Spark. Il relirait alors tout Kafka et recréerait les fichiers du data lake en double.

## 3. Le DAG échoue sur le seuil qualité

**Symptôme :** la tâche `quality_gate` est rouge dans Airflow, et `ops.pipeline_runs` indique `failed:quality_gate`.

**Ce que ça veut dire :** plus de 5 % des lignes de la journée sont en quarantaine. Le DAG s'est arrêté avant dbt **volontairement** : les tables métier gardent leur dernière version saine, et personne ne lit de chiffres douteux.

**Diagnostic :** quels motifs de rejet dominent ?

```bash
make lake-report
```

- Un seul motif en forte hausse (par exemple `invalid_currency`) : un système en amont a changé de format.
- Tous les motifs en hausse : problème global à la source.

**Correction :**
1. Prévenir l'équipe propriétaire de la source, avec les motifs et des exemples tirés de `silver/quarantine`.
2. Une fois la source corrigée, rejouer la journée (fiche 4).
3. Si les données sont acceptées malgré tout, après une décision métier tracée : relancer la tâche avec un seuil relevé (variable `QUALITY_MAX_QUARANTINE_RATE`), puis le remettre à 5 %.

## 4. Rejouer une journée

Tous les traitements sont idempotents : rejouer une date remplace ses données, sans doublon.

```bash
make batch                  # si Airflow ne tourne pas
make dag-run DATE=2026-11-07
```

Ou, dans Airflow : DAG `fraudflow_daily` → l'exécution de la date → *Clear*.

Ce qui se passe : `batch_load_warehouse` supprime puis réinsère les lignes de la date dans une seule transaction, puis dbt met à jour les tables. Le rejeu a été vérifié : mêmes chiffres, au centime près.

## 5. Tout retraiter depuis le bronze

Cas : une règle de qualité change, ou un bug a corrompu le silver. Le bronze contient chaque message brut (carte masquée) : on peut tout reconstruire sans relire Kafka.

1. Arrêter le traitement en continu : `docker compose stop spark-streaming`.
2. Supprimer le silver, la quarantaine et leurs checkpoints, en gardant le bronze :
   ```bash
   docker compose exec -T minio mc rm --recursive --force lake/silver
   docker run --rm -v fraudflow_spark-checkpoints:/c alpine rm -rf /c/silver /c/quarantine
   docker compose exec -T minio mc mb --ignore-existing lake/silver
   ```
3. Relancer : `docker compose up -d spark-streaming`. Les requêtes silver et quarantaine relisent tout le bronze.
4. Rejouer chaque journée concernée dans Airflow (fiche 4).

## 6. Docker manque de mémoire

**Symptôme :** Docker ne répond plus, ou des conteneurs s'arrêtent sans erreur claire.

**Cause :** sur une machine de 8 Go, Docker n'a qu'environ 4 Go. La plateforme temps réel en utilise environ 2,2, et Airflow plus d'1 de plus.

**Correction :**
- Ne pas lancer Airflow en même temps que Spark : `make batch`, puis `make streaming`.
- Arrêter Kafka UI (`docker compose stop kafka-ui`).
- Sous Windows, donner plus de mémoire à Docker dans `%UserProfile%\.wslconfig` (`[wsl2]`, puis `memory=6GB`), puis `wsl --shutdown`.
