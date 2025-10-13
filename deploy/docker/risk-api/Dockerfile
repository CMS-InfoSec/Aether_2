# syntax=docker/dockerfile:1
FROM python:3.11.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential libpq-dev libssl-dev openssl \
    && rm -rf /var/lib/apt/lists/* \
    && addgroup --system appuser \
    && adduser --system --ingroup appuser appuser

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
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
        "component": "risk-api-image",
    }
    sys.stderr.write(json.dumps(payload) + "\n")
    raise SystemExit(1)

print("Verified critical Python modules:", ", ".join(modules))
PY
    && rm /tmp/requirements.txt

# Copy the repository contents that are needed at runtime. The build context
# must be the repository root (e.g. `docker build -f deploy/docker/risk-api/Dockerfile .`).
COPY . .
RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "services.risk.main:app", "--host", "0.0.0.0", "--port", "8000"]
