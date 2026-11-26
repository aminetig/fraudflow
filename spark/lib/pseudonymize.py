"""Pseudonymisation des numéros de carte, dès l'arrivée dans le data lake.

La norme PCI DSS interdit de stocker un numéro de carte lisible sans protection. On garde :
- card_hash : empreinte SHA-256 salée. Elle permet de regrouper les transactions d'une carte
  sans pouvoir retrouver le numéro, tant que le sel reste secret ;
- card_last4 : les 4 derniers chiffres, autorisés, utiles par exemple au service client.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Le numéro de carte dans le JSON, retrouvé même si la fin du message est abîmée
CARD_FIELD_PATTERN = r'"card_number"\s*:\s*"(\d{13,19})"'
# Toute suite de 13 à 19 chiffres ressemble à un numéro de carte : on la masque partout
PAN_PATTERN = r"(?<!\d)\d{9,15}(\d{4})(?!\d)"


def card_hash(card_number: Column, salt: str) -> Column:
    """Empreinte SHA-256 du sel suivi du numéro ; vide si le numéro est absent."""
    return F.when(card_number != "", F.sha2(F.concat(F.lit(salt), card_number), 256))


def mask_card_numbers(text: Column) -> Column:
    """Remplace chaque numéro de carte par des étoiles suivies des 4 derniers chiffres."""
    return F.regexp_replace(text, PAN_PATTERN, "************$1")


def to_bronze(kafka_messages: DataFrame, salt: str) -> DataFrame:
    """Message Kafka -> ligne bronze : contenu brut masqué, et provenance dans Kafka."""
    raw = F.col("value").cast("string")
    card = F.regexp_extract(raw, CARD_FIELD_PATTERN, 1)  # chaîne vide si absent
    return kafka_messages.select(
        mask_card_numbers(raw).alias("raw_value"),
        card_hash(card, salt).alias("card_hash"),
        F.when(card != "", F.substring(card, -4, 4)).alias("card_last4"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingest_date", F.to_date("ingested_at"))
