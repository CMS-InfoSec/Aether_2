# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (if any future dependencies require compilation)
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential libssl-dev openssl \
    && addgroup --system appuser \
    && adduser --system --ingroup appuser appuser \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY services/kraken_ws_ingest.py services/kraken_ws_ingest.py

RUN chown -R appuser:appuser /app

USER appuser

CMD ["python", "services/kraken_ws_ingest.py", "--pairs", "XBT/USD", "--kafka-bootstrap", "kafka:9092"]
