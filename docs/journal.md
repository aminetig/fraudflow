# Journal de construction

Les problèmes réellement rencontrés pendant la construction : le symptôme, la cause et la correction. Ce sont de bonnes réponses à « parlez-moi d'une difficulté technique ».

## 5 novembre : une erreur Kafka au redémarrage

- **Symptôme :** après chaque redémarrage, une ligne `ERROR ... NotControllerException: The active controller appears to be node 1` dans les logs.
- **Diagnostic :** testé sur plusieurs redémarrages et deux variantes de configuration. L'erreur apparaît dans les deux cas, mais Kafka fonctionne ensuite normalement, sans perte de message.
- **Conclusion :** c'est un décalage au démarrage du nœud unique, qui fait à la fois broker et controller. J'ai gardé la configuration la plus simple, et vérifié par des tests plutôt que sur une impression.

## 6 novembre : le producteur Kafka s'arrête sur une ligne vide

- **Symptôme :** `KafkaException: No key separator found on line number 1: ''`.
- **Cause :** avec `parse.key=true`, chaque ligne doit contenir `clé:valeur`. Un appui de trop sur Entrée envoyait une ligne vide. L'invite `>>` de PowerShell (« commande pas terminée ») s'ajoutait à la confusion.
- **Leçon :** l'outil de saisie refuse une ligne invalide, mais Kafka, lui, ne vérifie jamais le contenu. C'est aux traitements de le faire, d'où la quarantaine.

## 7 novembre : un débit trop réaliste... pour être réaliste

- **Problème :** le plan prévoyait 50 transactions par seconde. Avec 6 630 cartes, chaque carte paierait alors environ 650 fois par jour. Des clients normaux auraient déclenché la règle des rafales, avec environ 10 fausses alertes par minute selon le calcul.
- **Correction :** 10 transactions par seconde par défaut, réglable dans `.env`.

## 8 novembre : des tests Spark qui échouaient pour une histoire de fuseau horaire

- **Symptôme :** des transactions valides étaient rejetées pour `future_event_time` dans les tests, pas en production.
- **Cause :** une heure Python sans fuseau, `datetime(2026, 11, 8, 12)`, est lue à l'heure locale de la machine (Paris), puis convertie en UTC par Spark. La transaction paraissait donc une heure dans le futur.
- **Correction :** des heures toujours écrites avec leur fuseau (`tzinfo=UTC`), et Spark réglé en UTC.

## 8 novembre : « 39 997 lignes en quarantaine » ?

- **Symptôme :** le log de la quarantaine annonçait presque tout le flux.
- **Cause :** Spark compte les lignes **lues** par une requête (`numInputRows`), pas les lignes écrites. La quarantaine lit tout le bronze, puis n'en garde que 2 %.
- **Correction :** libellé « lignes lues », et vérification par un bilan indépendant (`make lake-report`).

## 8 novembre : une requête qui lisait Kafka deux fois

- **Symptôme :** `fraud_instant` lisait 2 fois plus de lignes que les autres requêtes.
- **Cause :** ses deux règles (R2 et R4) étaient deux branches réunies (`union`) sur la même source. Spark lisait donc la source une fois par branche.
- **Correction :** les règles sont appliquées dans le `foreachBatch`, sur le lot gardé en mémoire (`persist`). Il n'y a plus qu'une seule lecture.

## 8 novembre : des alertes en rafale pour une seule fraude

- **Symptôme :** 482 alertes « voyage impossible » pour environ 150 fraudes injectées.
- **Cause :** des fenêtres de 30 minutes qui glissent de 5 en 5. Une même fraude apparaît dans plusieurs fenêtres, chacune avec un contenu un peu différent, donc un identifiant différent.
- **Correction :** un regroupement en incidents. Une alerte proche d'un incident existant, sur la même carte, le met à jour. Côté métier, un analyste n'est plus noyé sous les doublons.

## 8 novembre : Docker figé au lancement d'Airflow

- **Symptôme :** plus aucune commande `docker` ne répondait.
- **Cause :** la mémoire. Docker n'a que 3,76 Go sur cette machine : le temps réel en prenait 2,2, et Airflow plus d'1 de plus.
- **Correction :** deux modes (`make batch` et `make streaming`). Spark est mis en pause pendant le traitement quotidien, puis reprend sans perte grâce à ses checkpoints. J'ai aussi limité la mémoire de Grafana et MinIO, deux programmes écrits en Go (`GOMEMLIMIT`).

## 8 novembre : la première tâche Airflow tuée

- **Symptôme :** `check_silver_available` tuée (`SIGKILL`) dès son premier essai.
- **Cause :** elle avait démarré 12 secondes avant le serveur d'API interne d'Airflow 3, qu'elle doit contacter.
- **Ce que ça montre :** c'est exactement le cas prévu par `retries=2`. Le deuxième essai a réussi, sans intervention.

## 8 novembre : une vraie panne, absorbée seule

- **Symptôme :** la requête bronze s'arrête sur `TimeoutException: Timeout of 60000ms expired`.
- **Cause :** la machine était saturée par le rattrapage de Spark et une évaluation lancée en même temps. Kafka n'a pas répondu en 60 secondes.
- **Ce qui s'est passé :** le programme Spark s'arrête en erreur, Docker le relance (`restart: on-failure`), et chaque requête reprend depuis son checkpoint. Aucune intervention n'a été nécessaire, et aucune donnée n'a été perdue.

## 8 novembre : 61 % de précision pour la règle « voyage impossible »

- **Constat :** 131 fausses alertes R3 sur 337, en 4 heures.
- **Analyse :** le générateur simule 0,2 % d'achats frontaliers légitimes, par exemple France puis Belgique. Le calcul en prévoyait environ 136 sur la période, très proche des 131 observés. La règle ne voit que « 2 pays différents », sans notion de distance.
- **Suite :** autoriser les pays voisins dans R3, ou calculer une vitesse de déplacement. C'est un bon exemple de compromis entre rappel et précision, à discuter avec le métier.

## 8 novembre : Airflow 3 ne date plus les exécutions comme Airflow 2

- **Symptôme :** l'exécution de minuit le 8 a chargé... la journée du 8, à peine commencée.
- **Cause :** avec `schedule="@daily"`, Airflow 3 donne à l'exécution la date du moment où elle part. Airflow 2 lui donnait la date de la journée écoulée.
- **Correction :** `CronDataIntervalTimetable`. L'exécution de minuit traite la veille, avec `ds` égal à la veille.
