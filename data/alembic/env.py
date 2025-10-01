"""Alembic environment configuration for the Aether TimescaleDB schema."""
from __future__ import annotations

from logging.config import fileConfig
from pathlib import Path
from typing import Dict, Optional

from alembic import context
from sqlalchemy import engine_from_config, pool

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def get_url() -> str:
    """Resolve the database URL either from alembic.ini or environment variables."""
    override = context.get_x_argument(as_dictionary=True).get("db_url")
    if override:
        return override
    ini_url: Optional[str] = config.get_main_option("sqlalchemy.url")
    if ini_url and "${" not in ini_url:
        return ini_url
    import os

    env_url = os.getenv("DATABASE_URL")
    if not env_url:
        raise RuntimeError(
            "DATABASE_URL environment variable must be provided when running migrations."
        )
    return env_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_url()
    context.configure(url=url, target_metadata=None, literal_binds=True, dialect_opts={"paramstyle": "named"})

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        url=get_url(),
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=None)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
