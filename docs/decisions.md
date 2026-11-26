# Choix techniques

Chaque choix suit le même plan : le contexte, la décision, et l'alternative écartée.

## 1. Kafka comme point d'entrée

- **Contexte :** plusieurs traitements (archivage, détection, analyses) ont besoin des mêmes transactions, chacun à son rythme, et aucune ne doit se perdre si l'un d'eux tombe.
- **Décision :** Kafka, en mode KRaft. C'est un journal persistant : lire ne supprime rien, et chaque lecteur garde sa position. Le numéro de carte sert de clé : toutes les transactions d'une carte vont dans la même partition, dans l'ordre.
- **Écarté :** RabbitMQ, une file d'attente où un message lu disparaît. On ne pourrait ni rejouer les données ni les partager entre plusieurs traitements.

## 2. Pseudonymiser la carte dès l'entrée dans le data lake

- **Contexte :** la norme PCI DSS interdit de stocker un numéro de carte lisible sans protection.
- **Décision :** dès le bronze, le numéro est remplacé par une empreinte SHA-256 salée (`card_hash`) et ses 4 derniers chiffres (`card_last4`). Toute suite de 13 à 19 chiffres est aussi masquée dans le message brut, même dans un JSON cassé. Un contrôle (`make lake-report`) vérifie qu'aucun numéro n'est stocké en clair.
- **Écarté :** garder le message brut intact dans le bronze, comme on le fait souvent. On ne peut plus regrouper par carte qu'avec l'empreinte, mais aucun fichier du data lake ne contient de numéro exploitable. En production, le sel serait dans un coffre-fort à secrets, et Kafka chiffré.

## 3. Valider avant le watermark

- **Contexte :** le dédoublonnage utilise un watermark : Spark considère que le temps avance au rythme des dates des transactions.
- **Décision :** les lignes invalides partent en quarantaine **avant** le watermark. Une transaction datée de demain par une horloge déréglée ferait sinon avancer le temps de Spark, qui jetterait ensuite toutes les vraies transactions comme « trop en retard ».
- **Écarté :** dédoublonner d'abord, puis valider. C'est plus simple à écrire, mais une seule ligne aberrante pourrait faire perdre des données.

## 4. La détection lit Kafka, pas le silver

- **Contexte :** une alerte doit partir en quelques secondes, mais le silver n'est écrit que toutes les 30 secondes, après le bronze.
- **Décision :** les requêtes de fraude lisent Kafka directement et appliquent en mémoire **le même code** de contrôle que le silver. Elles voient donc exactement les mêmes données valides, sans attendre deux écritures de fichiers. Un test dbt vérifie que chaque alerte pointe vers une transaction connue de l'entrepôt.
- **Écarté :** lire le silver en continu. C'est plus pur dans l'architecture bronze, silver et gold, mais le délai d'alerte passerait de quelques secondes à plus d'une minute.

## 5. Regrouper les alertes en incidents

- **Contexte :** les fenêtres glissantes se chevauchent. Une même fraude apparaissait dans plusieurs fenêtres, et créait jusqu'à 6 alertes (482 alertes « voyage impossible » pour environ 150 fraudes).
- **Décision :** une nouvelle alerte de la même règle, sur la même carte et proche dans le temps, met à jour l'incident existant au lieu d'en créer un autre. L'identifiant reste stable, donc rejouer un lot ne crée pas de doublon (upsert dans MongoDB).
- **Écarté :** des fenêtres fixes, sans chevauchement. Plus de doublons, mais une fraude à cheval sur deux fenêtres passerait inaperçue.

## 6. MongoDB pour les alertes, PostgreSQL pour l'entrepôt

- **Contexte :** une alerte contient des preuves dont la forme dépend de la règle (nombre de paiements, pays traversés, rapport au montant habituel...).
- **Décision :** MongoDB stocke un document par alerte, avec ses preuves imbriquées. PostgreSQL porte l'entrepôt en étoile, et dbt le transforme en SQL.
- **Écarté :** tout mettre dans PostgreSQL, avec une colonne JSONB. Ça aurait fonctionné, et c'est d'ailleurs ce que fait la table `raw.alerts`. MongoDB permettait de montrer une deuxième famille de bases.

## 7. Airflow orchestre, mais ne calcule pas

- **Contexte :** le chargement quotidien utilise Spark, et les transformations utilisent dbt.
- **Décision :** Airflow lance chaque étape dans son propre conteneur (`DockerOperator`), avec l'image Spark ou dbt du projet. L'image Airflow reste l'image officielle, sans Java ni Spark.
- **Écarté :** installer Spark et dbt dans l'image Airflow. Ce serait une image énorme, avec des conflits de dépendances, et l'orchestrateur ferait les calculs. En production, ces conteneurs tourneraient sur Kubernetes ou un service géré.

## 8. Deux modes sur une petite machine

- **Contexte :** Docker n'a que 3,76 Go. Le temps réel en utilise environ 2,2, et Airflow plus d'1 de plus. Les deux ensemble ont figé Docker.
- **Décision :** `make batch` met Spark en pause pendant le traitement quotidien, et `make streaming` le relance. Spark reprend exactement là où il s'était arrêté, grâce à ses checkpoints.
- **Écarté :** tout faire tourner en même temps en réduisant la mémoire de chacun. Le risque de plantage aléatoire était trop grand. Avec 16 Go, il suffit de lancer `docker compose --profile batch up -d`.
