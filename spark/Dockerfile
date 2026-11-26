FROM python:3.11.18-slim-bookworm

# Spark tourne dans une machine virtuelle Java : il lui faut Java 17
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Connecteurs Java pour Kafka et S3, téléchargés une fois pour toutes à la construction
# de l'image, jamais au lancement. Versions qui vont ensemble avec Spark 3.5.9.
ARG MAVEN=https://repo1.maven.org/maven2
ARG JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ${JARS}/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar ${JARS}/

WORKDIR /app
COPY lib/ lib/
COPY jobs/ jobs/

# Le programme ne tourne pas en administrateur ; il écrit ses checkpoints dans /checkpoints
RUN useradd --create-home --uid 1000 spark \
    && mkdir /checkpoints \
    && chown spark /checkpoints
USER spark

CMD ["python", "-m", "jobs.streaming"]
