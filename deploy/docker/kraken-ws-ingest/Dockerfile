# syntax=docker/dockerfile:1
FROM python:3.11.9-slim

ARG PIP_VERSION=24.3.1

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
RUN python -m pip install --no-cache-dir --upgrade "pip==${PIP_VERSION}" \
    && python -m pip install --no-cache-dir -r requirements.txt \
    && python - <<'PY'
import importlib
import json
import sys

modules = ["psycopg", "argon2", "cryptography"]
missing: dict[str, str] = {}
for name in modules:
    try:
        importlib.import_module(name)
    except Exception as exc:  # pragma: no cover - executed only when modules missing
        missing[name] = repr(exc)

if missing:
    payload = {
        "event": "missing_critical_modules",
        "modules": missing,
        "component": "kraken-ws-ingest-image",
    }
    sys.stderr.write(json.dumps(payload) + "\n")
    raise SystemExit(1)

print("Verified critical Python modules:", ", ".join(modules))
PY

COPY services/kraken_ws_ingest.py services/kraken_ws_ingest.py

RUN chown -R appuser:appuser /app

USER appuser

CMD ["python", "services/kraken_ws_ingest.py", "--pairs", "XBT/USD", "--kafka-bootstrap", "kafka:9092"]
