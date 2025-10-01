"""Create TimescaleDB hypertables for core data sets."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


CREATE_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS accounts (
        account_id UUID NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        attributes JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (account_id, created_at)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS credentials (
        credential_id UUID NOT NULL,
        account_id UUID NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        rotated_at TIMESTAMPTZ,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (credential_id, created_at)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS universe_whitelist (
        asset_id TEXT NOT NULL,
        as_of TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        approved BOOLEAN NOT NULL,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (asset_id, as_of)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ohlcv_bars (
        market TEXT NOT NULL,
        bucket_start TIMESTAMPTZ NOT NULL,
        open NUMERIC NOT NULL,
        high NUMERIC NOT NULL,
        low NUMERIC NOT NULL,
        close NUMERIC NOT NULL,
        volume NUMERIC NOT NULL,
        PRIMARY KEY (market, bucket_start)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS order_book_events (
        market TEXT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        side TEXT NOT NULL,
        price NUMERIC NOT NULL,
        size NUMERIC NOT NULL,
        event_type TEXT NOT NULL,
        sequence BIGINT NOT NULL,
        raw JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (market, event_time, sequence)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS features (
        feature_name TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        event_timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (feature_name, entity_id, event_timestamp)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS labels (
        label_name TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        event_timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (label_name, entity_id, event_timestamp)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID NOT NULL,
        account_id UUID NOT NULL,
        market TEXT NOT NULL,
        submitted_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        side TEXT NOT NULL,
        price NUMERIC,
        size NUMERIC,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (order_id, submitted_at)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fills (
        fill_id UUID NOT NULL,
        order_id UUID NOT NULL,
        market TEXT NOT NULL,
        fill_time TIMESTAMPTZ NOT NULL,
        price NUMERIC NOT NULL,
        size NUMERIC NOT NULL,
        fee NUMERIC,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (fill_id, fill_time)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id UUID NOT NULL,
        account_id UUID NOT NULL,
        market TEXT NOT NULL,
        as_of TIMESTAMPTZ NOT NULL,
        quantity NUMERIC NOT NULL,
        entry_price NUMERIC,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (position_id, as_of)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pnl (
        account_id UUID NOT NULL,
        market TEXT NOT NULL,
        as_of TIMESTAMPTZ NOT NULL,
        realized NUMERIC NOT NULL DEFAULT 0,
        unrealized NUMERIC NOT NULL DEFAULT 0,
        PRIMARY KEY (account_id, market, as_of)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS risk_events (
        event_id UUID NOT NULL,
        account_id UUID NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        severity TEXT NOT NULL,
        description TEXT,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (event_id, observed_at)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        event_id UUID NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        metadata JSONB DEFAULT '{}'::jsonb,
        PRIMARY KEY (event_id, event_time)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS config_versions (
        config_key TEXT NOT NULL,
        version INTEGER NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL,
        checksum TEXT,
        payload JSONB NOT NULL,
        PRIMARY KEY (config_key, version, applied_at)
    );
    """
]

HYPERTABLE_COMMANDS = [
    "SELECT create_hypertable('accounts', 'created_at', if_not_exists => TRUE);",
    "SELECT create_hypertable('credentials', 'created_at', if_not_exists => TRUE);",
    "SELECT create_hypertable('universe_whitelist', 'as_of', if_not_exists => TRUE);",
    "SELECT create_hypertable('ohlcv_bars', 'bucket_start', if_not_exists => TRUE);",
    "SELECT create_hypertable('order_book_events', 'event_time', if_not_exists => TRUE);",
    "SELECT create_hypertable('features', 'event_timestamp', if_not_exists => TRUE);",
    "SELECT create_hypertable('labels', 'event_timestamp', if_not_exists => TRUE);",
    "SELECT create_hypertable('orders', 'submitted_at', if_not_exists => TRUE);",
    "SELECT create_hypertable('fills', 'fill_time', if_not_exists => TRUE);",
    "SELECT create_hypertable('positions', 'as_of', if_not_exists => TRUE);",
    "SELECT create_hypertable('pnl', 'as_of', if_not_exists => TRUE);",
    "SELECT create_hypertable('risk_events', 'observed_at', if_not_exists => TRUE);",
    "SELECT create_hypertable('audit_log', 'event_time', if_not_exists => TRUE);",
    "SELECT create_hypertable('config_versions', 'applied_at', if_not_exists => TRUE);",
]


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    for statement in CREATE_TABLE_STATEMENTS:
        op.execute(statement)
    for statement in HYPERTABLE_COMMANDS:
        op.execute(statement)


def downgrade() -> None:
    tables = [
        "config_versions",
        "audit_log",
        "risk_events",
        "pnl",
        "positions",
        "fills",
        "orders",
        "labels",
        "features",
        "order_book_events",
        "ohlcv_bars",
        "universe_whitelist",
        "credentials",
        "accounts",
    ]
    for table in tables:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
