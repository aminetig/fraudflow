FROM python:3.11.18-slim

# Pas de fichiers .pyc, et des logs affichés immédiatement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Les dépendances d'abord : Docker garde cette étape en cache tant que la liste ne change pas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# Le programme ne tourne pas en administrateur (root)
RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "producer.py"]
