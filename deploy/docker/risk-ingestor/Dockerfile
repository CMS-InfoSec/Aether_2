# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential libpq-dev \
    && addgroup --system appuser \
    && adduser --system --ingroup appuser appuser \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .
COPY deploy/docker/risk-ingestor/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && chown -R appuser:appuser /app /entrypoint.sh

USER appuser

ENTRYPOINT ["/entrypoint.sh"]
CMD []
