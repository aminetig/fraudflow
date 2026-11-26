FROM python:3.11.18-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/dbt \
    DBT_PROJECT_DIR=/dbt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /dbt
COPY fraudflow/ /dbt/

# Les paquets dbt (dbt_utils) sont téléchargés à la construction, jamais au lancement
RUN dbt deps \
    && useradd --create-home --uid 1000 dbt \
    && chown -R dbt /dbt
USER dbt

ENTRYPOINT ["dbt"]
CMD ["build"]
