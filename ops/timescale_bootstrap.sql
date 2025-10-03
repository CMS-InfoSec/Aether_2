-- Timescale bootstrap for account-scoped telemetry schemas.
-- Usage: psql "$TIMESCALE_DSN" -v schema=acct_example -f ops/timescale_bootstrap.sql

\if :{?schema}
\else
\echo 'Set the target schema via -v schema=<value> when invoking this script.' >&2
\quit 1
\endif

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE SCHEMA IF NOT EXISTS :"schema";

CREATE TABLE IF NOT EXISTS :"schema".acks (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".fills (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".shadow_fills (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".telemetry (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".audit_logs (
    id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".credential_events (
    id BIGSERIAL PRIMARY KEY,
    event TEXT NOT NULL,
    event_type TEXT NOT NULL,
    secret_name TEXT,
    metadata JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS :"schema".credential_rotations (
    id BIGSERIAL PRIMARY KEY,
    secret_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    rotated_at TIMESTAMPTZ NOT NULL,
    kms_key_id TEXT
);

CREATE TABLE IF NOT EXISTS :"schema".risk_configs (
    account_id TEXT PRIMARY KEY,
    config JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable(format('%I.%I', :'schema', 'acks'), 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable(format('%I.%I', :'schema', 'fills'), 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable(format('%I.%I', :'schema', 'shadow_fills'), 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable(format('%I.%I', :'schema', 'events'), 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable(format('%I.%I', :'schema', 'telemetry'), 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS ON :"schema".acks (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".fills (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".shadow_fills (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".events (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".telemetry (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".credential_events (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ON :"schema".credential_rotations (rotated_at DESC);
