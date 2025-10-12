"""Automated restore drill for TimescaleDB backups.

This module is designed to be executed inside the staging cluster where the
``timescaledb-pgdump`` CronJob stores nightly ``pg_dumpall`` archives on the
``timescaledb-backup`` persistent volume.  The drill decompresses the most
recent dump, restores it into a throwaway database, runs a couple of sanity
checks, and then tears the database down again.  Results are posted to the
on-call Slack channel so humans can track the health of disaster-recovery
procedures.

The job relies on the following environment variables:

``PGHOST`` / ``PGPORT`` / ``PGUSER`` / ``PGPASSWORD``
    Connection parameters for the TimescaleDB instance in staging.

``POSTGRES_DB`` (optional)
    Name of the source database recorded in the dump archives.  Defaults to
    ``marketdata``.

``RESTORE_TARGET_DATABASE`` (optional)
    Name for the temporary database that the drill creates.  Defaults to
    ``marketdata_restore_drill``.

``RESTORE_ADMIN_DB`` (optional)
    Database to connect to when creating or dropping the temporary database.
    Defaults to ``postgres``.

``RESTORE_KEEP_DATABASE_ON_FAILURE`` (optional)
    When set to a truthy value, the temporary database is preserved if the
    restore fails so engineers can inspect the state manually.  Defaults to
    ``false``.

``BACKUP_ARCHIVE_DIR`` (optional)
    Filesystem path where ``timescaledb-pgdump`` writes ``*.sql.gz`` archives.
    Defaults to ``/backups``.

``SLACK_WEBHOOK_URL`` (optional)
    Incoming webhook used to post the drill results.  When unset the script
    still performs the restore but only logs the outcome locally.

``RESTORE_VALIDATION_QUERY`` (optional)
    Additional SQL statement executed after the baseline restore checks.  When
    provided the resulting row is included in the Slack notification/logs to
    confirm downstream data integrity.
"""

from __future__ import annotations

import datetime as dt
import gzip
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Tuple

import requests

try:  # pragma: no cover - optional dependency in some environments
    import psycopg2
    from psycopg2 import sql
except Exception as exc:  # pragma: no cover - surfaced during runtime
    raise RuntimeError(
        "psycopg2 is required for restore_drill but is not installed"
    ) from exc


LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


DEFAULT_BACKUP_DIR = Path("/backups")
DEFAULT_TARGET_DB = "marketdata_restore_drill"
DEFAULT_ADMIN_DB = "postgres"


def _connection_kwargs() -> dict[str, str]:
    """Collect libpq connection parameters from the environment."""

    host = os.environ.get("PGHOST")
    user = os.environ.get("PGUSER")
    if not host or not user:
        raise RuntimeError("PGHOST and PGUSER environment variables are required")

    params: dict[str, str] = {"host": host, "user": user}

    port = os.environ.get("PGPORT")
    if port:
        params["port"] = port

    password = os.environ.get("PGPASSWORD")
    if password:
        params["password"] = password

    return params


def _find_latest_backup(backup_dir: Path) -> Path:
    """Return the most recent ``*.sql.gz`` archive in ``backup_dir``."""

    candidates = [
        path
        for path in backup_dir.glob("timescaledb-*.sql.gz")
        if path.is_file()
    ]
    if not candidates:
        raise FileNotFoundError(
            f"No TimescaleDB backup archives found under {backup_dir}"
        )

    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    LOGGER.info("Selected backup %s", latest.name)
    return latest


def _decompress_archive(archive_path: Path, destination: Path) -> Path:
    """Decompress ``archive_path`` into ``destination`` and return the SQL path."""

    sql_path = destination / archive_path.with_suffix("").name
    LOGGER.info("Decompressing %s to %s", archive_path, sql_path)
    destination.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive_path, "rb") as src, sql_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    return sql_path


def _terminate_connections(conn: "psycopg2.extensions.connection", dbname: str) -> None:
    """Terminate active sessions connected to ``dbname``."""

    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """
            ),
            (dbname,),
        )


def _drop_database(conn_kwargs: dict[str, str], admin_db: str, target_db: str) -> None:
    """Drop ``target_db`` using the administrative connection."""

    LOGGER.info("Dropping database %s", target_db)
    with psycopg2.connect(dbname=admin_db, **conn_kwargs) as connection:
        connection.autocommit = True
        _terminate_connections(connection, target_db)
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(target_db))
            )


def _create_database(conn_kwargs: dict[str, str], admin_db: str, target_db: str) -> None:
    """Create ``target_db`` from ``template0``."""

    LOGGER.info("Creating database %s", target_db)
    with psycopg2.connect(dbname=admin_db, **conn_kwargs) as connection:
        connection.autocommit = True
        _terminate_connections(connection, target_db)
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(target_db))
            )
            cursor.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                    sql.Identifier(target_db)
                )
            )


def _run_psql(sql_file: Path, env: dict[str, str], target_db: str) -> None:
    """Replay ``sql_file`` into ``target_db`` using ``psql``."""

    LOGGER.info("Restoring archive into %s", target_db)
    command = [
        "psql",
        "--set",
        "ON_ERROR_STOP=1",
        "--dbname",
        target_db,
        "--file",
        str(sql_file),
    ]
    subprocess.run(command, check=True, env=env)


def _validate_restore(
    conn_kwargs: dict[str, str],
    target_db: str,
    extra_query: str | None = None,
) -> Tuple[int, int, tuple[str, object] | None]:
    """Run basic validation queries on ``target_db``.

    Returns the number of user tables and backup log entries discovered. When
    ``extra_query`` is provided, it is executed after the baseline checks and
    the raw result tuple is returned for reporting.
    """

    LOGGER.info("Validating restored database %s", target_db)
    with psycopg2.connect(dbname=target_db, **conn_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """
            )
            table_count = int(cursor.fetchone()[0])
            if table_count == 0:
                raise RuntimeError("Restored database does not contain any public tables")

            cursor.execute("SELECT COUNT(*) FROM backup_log")
            backup_entries = int(cursor.fetchone()[0])

            extra_result: tuple[str, object] | None = None
            if extra_query:
                cursor.execute(extra_query)
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(
                        "Validation query returned no rows: {query}".format(
                            query=extra_query
                        )
                    )
                extra_result = (extra_query, row[0] if len(row) == 1 else row)

    LOGGER.info(
        "Validation succeeded with %s tables and %s backup_log rows",
        table_count,
        backup_entries,
    )
    if extra_result:
        LOGGER.info(
            "Additional validation query succeeded: %s => %s",
            extra_result[0],
            extra_result[1],
        )
    return table_count, backup_entries, extra_result


def _send_slack_message(webhook: str | None, text: str, emoji: str) -> None:
    """Post ``text`` to Slack if ``webhook`` is configured."""

    if not webhook:
        LOGGER.info("Slack webhook not configured; skipping notification")
        return

    payload = {"text": f"{emoji} {text}"}
    try:
        response = requests.post(webhook, json=payload, timeout=10)
        response.raise_for_status()
    except Exception:  # pragma: no cover - network interaction
        LOGGER.exception("Failed to post restore drill status to Slack")


def _truthy(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "on"}


def run_restore_drill() -> tuple[str, int, int]:
    """Execute the restore drill workflow.

    Returns the archive file name and validation metrics.
    """

    backup_dir = Path(os.environ.get("BACKUP_ARCHIVE_DIR", str(DEFAULT_BACKUP_DIR)))
    target_db = os.environ.get("RESTORE_TARGET_DATABASE", DEFAULT_TARGET_DB)
    admin_db = os.environ.get("RESTORE_ADMIN_DB", DEFAULT_ADMIN_DB)
    keep_on_failure = _truthy(os.environ.get("RESTORE_KEEP_DATABASE_ON_FAILURE"))

    conn_kwargs = _connection_kwargs()

    archive = _find_latest_backup(backup_dir)

    env = os.environ.copy()
    env.setdefault("PGHOST", conn_kwargs["host"])
    if "port" in conn_kwargs:
        env.setdefault("PGPORT", conn_kwargs["port"])
    env.setdefault("PGUSER", conn_kwargs["user"])
    if "password" in conn_kwargs:
        env.setdefault("PGPASSWORD", conn_kwargs["password"])

    sql_dir = Path("/tmp/restore-drill")
    sql_file = _decompress_archive(archive, sql_dir)

    tables = backups = 0
    extra_result: tuple[str, object] | None = None
    created = False
    try:
        _create_database(conn_kwargs, admin_db, target_db)
        created = True
        _run_psql(sql_file, env, target_db)
        validation_query = os.environ.get("RESTORE_VALIDATION_QUERY")
        tables, backups, extra_result = _validate_restore(
            conn_kwargs,
            target_db,
            validation_query,
        )
    except Exception:
        if created and not keep_on_failure:
            try:
                _drop_database(conn_kwargs, admin_db, target_db)
            except Exception:  # pragma: no cover - cleanup best effort
                LOGGER.exception("Failed to drop database %s during cleanup", target_db)
        raise
    else:
        try:
            _drop_database(conn_kwargs, admin_db, target_db)
        except Exception:  # pragma: no cover - cleanup best effort
            LOGGER.exception("Failed to drop database %s after restore", target_db)

    return archive.name, tables, backups, extra_result


def main() -> None:
    started = dt.datetime.utcnow()
    webhook = os.environ.get("SLACK_WEBHOOK_URL")

    try:
        archive_name, table_count, backup_entries, extra_result = run_restore_drill()
    except Exception as exc:
        duration = (dt.datetime.utcnow() - started).total_seconds()
        message = (
            "TimescaleDB restore drill failed after "
            f"{duration:.0f}s: {exc!s}"
        )
        _send_slack_message(webhook, message, ":x:")
        raise

    duration = (dt.datetime.utcnow() - started).total_seconds()
    message = (
        "TimescaleDB restore drill succeeded using archive"
        f" {archive_name}; {table_count} tables and {backup_entries}"
        f" backup_log rows verified in {duration:.0f}s."
    )
    if extra_result:
        message += (
            " Validation query [{query}] returned {value}."
        ).format(query=extra_result[0], value=extra_result[1])
    _send_slack_message(webhook, message, ":white_check_mark:")


if __name__ == "__main__":  # pragma: no cover - CLI invocation
    main()

