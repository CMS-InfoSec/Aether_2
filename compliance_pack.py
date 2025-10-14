"""Compliance pack exporter producing regulator-ready archives of trading activity."""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import logging
import os
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import APIRouter, Depends, HTTPException, Query
    from fastapi.responses import Response
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        Response,
    )

from audit_mode import AuditorPrincipal, require_auditor_identity
from shared.spot import is_spot_symbol, normalize_spot_symbol

try:  # pragma: no cover - optional dependency during unit tests.
    import boto3
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore

try:  # pragma: no cover - psycopg might be unavailable in some environments.
    import psycopg
    from psycopg.rows import dict_row
    from psycopg import errors as psycopg_errors
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    psycopg_errors = None  # type: ignore


LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/compliance", tags=["compliance"])

ExportFormat = str

SUPPORTED_FORMATS: Mapping[str, str] = {
    "sec": "application/json",
    "esma": "text/csv",
    "csv": "text/csv",
}


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency required at runtime is missing."""


@dataclass(frozen=True)
class StorageConfig:
    """Configuration describing how to persist generated packs."""

    bucket: str
    prefix: str = "compliance-packs"
    endpoint_url: str | None = None


@dataclass(frozen=True)
class CompliancePack:
    """Container for a generated compliance pack artifact."""

    export_date: dt.date
    generated_at: dt.datetime
    export_format: str
    content_type: str
    data: bytes
    sha256: str
    s3_bucket: str
    s3_key: str

    @property
    def size(self) -> int:
        return len(self.data)


def _require_psycopg() -> None:
    if psycopg is None:  # pragma: no cover - guard for optional dependency.
        raise MissingDependencyError("psycopg is required for compliance export functionality")


def _require_boto3() -> None:
    if boto3 is None:  # pragma: no cover - guard for optional dependency.
        raise MissingDependencyError("boto3 is required for compliance export uploads")


def _database_dsn() -> str:
    dsn = (
        os.getenv("COMPLIANCE_DATABASE_URL")
        or os.getenv("TRADING_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "COMPLIANCE_DATABASE_URL, TRADING_DATABASE_URL, TIMESCALE_DSN, or DATABASE_URL must be set",
        )
    return dsn


def _storage_config_from_env() -> StorageConfig:
    bucket = os.getenv("COMPLIANCE_EXPORT_BUCKET") or os.getenv("EXPORT_BUCKET")
    if not bucket:
        raise RuntimeError("COMPLIANCE_EXPORT_BUCKET or EXPORT_BUCKET must be configured")
    prefix = os.getenv("COMPLIANCE_EXPORT_PREFIX") or os.getenv("EXPORT_PREFIX", "compliance-packs")
    endpoint_url = os.getenv("COMPLIANCE_EXPORT_ENDPOINT_URL") or os.getenv("EXPORT_S3_ENDPOINT_URL")
    return StorageConfig(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)


class CompliancePackExporter:
    """Coordinates data extraction, formatting, and archival for compliance packs."""

    _DATA_SOURCES: Mapping[str, Tuple[str, str]] = {
        "trades": ("trade_log", "executed_at"),
        "fills": ("fills", "fill_time"),
        "risk_decisions": ("risk_decisions", "decided_at"),
        "overrides": ("risk_overrides", "created_at"),
        "configs": ("risk_configs", "updated_at"),
    }

    def __init__(self, *, config: StorageConfig, dsn: str | None = None) -> None:
        self._config = config
        self._dsn = dsn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(self, *, for_date: dt.date, export_format: ExportFormat) -> CompliancePack:
        export_format = export_format.lower()
        if export_format not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported export format: {export_format}")

        _require_psycopg()
        _require_boto3()

        resolved_dsn = self._dsn or _database_dsn()
        now = dt.datetime.now(dt.timezone.utc)
        start = dt.datetime.combine(for_date, dt.time.min, tzinfo=dt.timezone.utc)
        end = start + dt.timedelta(days=1)

        with psycopg.connect(resolved_dsn) as conn:  # type: ignore[arg-type]
            records = self._collect_records(conn, start, end)
            pack_bytes = self._render(records, for_date, now, export_format)
            sha256 = self._compute_sha256(pack_bytes)
            extension = "json" if export_format == "sec" else "csv"
            run_id = uuid.uuid4().hex
            key = self._build_s3_key(for_date, export_format, run_id, extension)

            client = self._s3_client()
            metadata = {
                "export_date": for_date.isoformat(),
                "format": export_format,
                "sha256": sha256,
                "run_id": run_id,
            }
            client.put_object(
                Bucket=self._config.bucket,
                Key=key,
                Body=pack_bytes,
                ContentType=SUPPORTED_FORMATS[export_format],
                Metadata=metadata,
            )

            self._record_status(conn, for_date, now, export_format, key, sha256)
            conn.commit()

        return CompliancePack(
            export_date=for_date,
            generated_at=now,
            export_format=export_format,
            content_type=SUPPORTED_FORMATS[export_format],
            data=pack_bytes,
            sha256=sha256,
            s3_bucket=self._config.bucket,
            s3_key=key,
        )

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _collect_records(
        self,
        conn: "psycopg.Connection[Any]",
        start: dt.datetime,
        end: dt.datetime,
    ) -> Dict[str, List[Dict[str, Any]]]:
        payload: Dict[str, List[Dict[str, Any]]] = {}
        for name, (table, ts_column) in self._DATA_SOURCES.items():
            rows = self._fetch_table(conn, table, ts_column, start, end)
            if name == "trades":
                payload[name] = self._normalise_trades(rows)
                continue
            if name == "fills":
                payload[name] = self._normalise_fills(rows)
                continue
            payload[name] = rows
        return payload

    def _fetch_table(
        self,
        conn: "psycopg.Connection[Any]",
        table: str,
        ts_column: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> List[Dict[str, Any]]:
        query = (
            f"SELECT * FROM {table} "
            f"WHERE {ts_column} >= %s AND {ts_column} < %s "
            f"ORDER BY {ts_column} ASC"
        )
        params: Tuple[Any, ...] = (start, end)

        try:
            rows = self._execute_fetch(conn, query, params)
        except Exception as exc:  # pragma: no cover - defensive fallback when schema diverges.
            if psycopg_errors is not None and isinstance(
                exc,
                (
                    psycopg_errors.UndefinedTable,
                    psycopg_errors.UndefinedColumn,
                    psycopg_errors.InvalidCatalogName,
                ),
            ):
                return []
            rows = self._execute_fetch(conn, f"SELECT * FROM {table}", tuple())
        return [self._normalise_row(dict(row)) for row in rows]

    def _execute_fetch(
        self,
        conn: "psycopg.Connection[Any]",
        query: str,
        params: Tuple[Any, ...],
    ) -> Sequence[MutableMapping[str, Any]]:
        with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
            cur.execute(query, params)
            result: Iterable[MutableMapping[str, Any]] = cur.fetchall()
        return list(result)

    def _record_status(
        self,
        conn: "psycopg.Connection[Any]",
        for_date: dt.date,
        generated_at: dt.datetime,
        export_format: str,
        s3_key: str,
        sha256: str,
    ) -> None:
        self._ensure_status_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO compliance_export_status (
                    export_date,
                    generated_at,
                    export_format,
                    s3_bucket,
                    s3_key,
                    sha256
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sha256) DO NOTHING
                """.strip(),
                (for_date, generated_at, export_format, self._config.bucket, s3_key, sha256),
            )

    def _ensure_status_table(self, conn: "psycopg.Connection[Any]") -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_export_status (
                    id BIGSERIAL PRIMARY KEY,
                    export_date DATE NOT NULL,
                    generated_at TIMESTAMPTZ NOT NULL,
                    export_format TEXT NOT NULL,
                    s3_bucket TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    sha256 TEXT NOT NULL UNIQUE
                )
                """.strip()
            )

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------

    def _render(
        self,
        records: Mapping[str, List[Dict[str, Any]]],
        for_date: dt.date,
        generated_at: dt.datetime,
        export_format: str,
    ) -> bytes:
        if export_format == "sec":
            return self._render_sec(records, for_date, generated_at)
        if export_format == "esma":
            return self._render_esma_csv(records, for_date)
        return self._render_generic_csv(records, for_date)

    def _render_sec(
        self,
        records: Mapping[str, List[Dict[str, Any]]],
        for_date: dt.date,
        generated_at: dt.datetime,
    ) -> bytes:
        payload = {
            "form": "PF",
            "version": "1.0",
            "as_of": for_date.isoformat(),
            "generated_at": generated_at.isoformat(),
            "sections": {
                "PortfolioInformation": records.get("configs", []),
                "RiskDecisions": records.get("risk_decisions", []),
                "Overrides": records.get("overrides", []),
                "TradingActivity": {
                    "trades": records.get("trades", []),
                    "fills": records.get("fills", []),
                },
            },
        }
        return json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")

    def _render_esma_csv(
        self,
        records: Mapping[str, List[Dict[str, Any]]],
        for_date: dt.date,
    ) -> bytes:
        buffer = io.StringIO()
        fieldnames = [
            "transaction_id",
            "trade_date",
            "execution_time",
            "instrument",
            "side",
            "price",
            "quantity",
            "venue",
            "buyer",
            "seller",
            "decision_id",
            "override_reference",
            "risk_config_version",
            "fill_id",
            "fill_time",
            "fill_price",
            "fill_quantity",
        ]
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()

        fills_by_trade = self._index_by(records.get("fills", []), ["trade_id", "id", "fill_id", "parent_trade_id"])
        overrides_by_decision = self._index_by(records.get("overrides", []), ["decision_id", "id", "override_id"])
        decisions_by_id = self._index_by(records.get("risk_decisions", []), ["id", "decision_id"])
        latest_config = self._latest_config_version(records.get("configs", []))

        for trade in records.get("trades", []):
            try:
                normalized = self._normalise_trade(trade)
            except ValueError:
                LOGGER.warning(
                    "Skipping trade record with non-spot instrument during rendering: %s",
                    trade,
                )
                continue
            trade_id = normalized.get("trade_id")
            decision_id = normalized.get("decision_id")

            fill = None
            if trade_id:
                fill = fills_by_trade.get(str(trade_id))
            override = None
            if decision_id:
                override = overrides_by_decision.get(str(decision_id))
            decision = None
            if decision_id:
                decision = decisions_by_id.get(str(decision_id))

            row = {
                "transaction_id": trade_id or normalized.get("id"),
                "trade_date": normalized.get("trade_date") or for_date.isoformat(),
                "execution_time": normalized.get("execution_time"),
                "instrument": normalized.get("instrument") or normalized.get("symbol"),
                "side": normalized.get("side"),
                "price": normalized.get("price"),
                "quantity": normalized.get("quantity"),
                "venue": normalized.get("venue") or normalized.get("exchange"),
                "buyer": normalized.get("buyer") or normalized.get("account"),
                "seller": normalized.get("seller") or normalized.get("counterparty"),
                "decision_id": decision_id,
                "override_reference": self._extract_override_reference(override),
                "risk_config_version": latest_config,
                "fill_id": self._safe_lookup(fill, ["fill_id", "id"]),
                "fill_time": self._safe_lookup(fill, ["fill_time", "executed_at", "ts"]),
                "fill_price": self._safe_lookup(fill, ["price", "fill_price"]),
                "fill_quantity": self._safe_lookup(fill, ["quantity", "fill_qty", "size"]),
            }

            if decision and not row["decision_id"]:
                row["decision_id"] = decision.get("id") or decision.get("decision_id")

            writer.writerow({key: self._stringify(value) for key, value in row.items()})

        return buffer.getvalue().encode("utf-8")

    def _render_generic_csv(
        self,
        records: Mapping[str, List[Dict[str, Any]]],
        for_date: dt.date,
    ) -> bytes:
        buffer = io.StringIO()
        keys: List[str] = sorted({
            key
            for rows in records.values()
            for row in rows
            for key in row.keys()
        })
        writer = csv.writer(buffer)
        writer.writerow(["record_type", "as_of", *keys])
        for record_type, rows in records.items():
            for row in rows:
                normalized = {key: self._stringify(row.get(key)) for key in keys}
                writer.writerow([
                    record_type,
                    for_date.isoformat(),
                    *[normalized.get(key, "") for key in keys],
                ])
        return buffer.getvalue().encode("utf-8")

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _normalise_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in list(row.items()):
            row[key] = self._normalise_value(value)
        return row

    def _normalise_trades(self, trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for trade in trades:
            try:
                filtered.append(self._normalise_trade(trade))
            except ValueError:
                LOGGER.warning(
                    "Skipping trade record with non-spot instrument: %s",
                    trade,
                )
        return filtered

    def _normalise_fills(self, fills: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for fill in fills:
            try:
                filtered.append(self._normalise_fill(fill))
            except ValueError:
                LOGGER.warning(
                    "Skipping fill record with non-spot instrument: %s",
                    fill,
                )
        return filtered

    @staticmethod
    def _canonical_spot_symbol(values: Mapping[str, Any]) -> str:
        for key in ("instrument", "symbol", "pair", "market", "ticker"):
            candidate = values.get(key)
            normalized = normalize_spot_symbol(candidate)
            if normalized and is_spot_symbol(normalized):
                return normalized
        return ""

    def _normalise_value(self, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc).isoformat()
        if isinstance(value, dt.date):
            return value.isoformat()
        try:
            import decimal

            if isinstance(value, decimal.Decimal):
                return float(value)
        except Exception:  # pragma: no cover - decimal may not be available.
            pass
        if isinstance(value, (dict, list, tuple)):
            return value
        return value

    def _normalise_trade(self, trade: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = dict(trade)
        trade_id = (
            normalized.get("trade_id")
            or normalized.get("id")
            or normalized.get("transaction_id")
            or normalized.get("uuid")
        )
        normalized.setdefault("trade_id", trade_id)
        normalized.setdefault("trade_date", normalized.get("executed_at") or normalized.get("ts"))
        normalized.setdefault("execution_time", normalized.get("executed_at") or normalized.get("ts"))
        normalized.setdefault("instrument", normalized.get("symbol"))
        normalized.setdefault("quantity", normalized.get("qty") or normalized.get("quantity"))
        normalized.setdefault("price", normalized.get("execution_price") or normalized.get("price"))
        decision = (
            normalized.get("decision_id")
            or normalized.get("risk_decision_id")
            or normalized.get("risk_decision")
        )
        normalized.setdefault("decision_id", decision)
        canonical = self._canonical_spot_symbol(normalized)
        if not canonical:
            raise ValueError("Trade references non-spot instrument")
        normalized["instrument"] = canonical
        normalized["symbol"] = canonical
        return {key: self._normalise_value(value) for key, value in normalized.items()}

    def _normalise_fill(self, fill: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = dict(fill)
        normalized.setdefault("instrument", normalized.get("symbol"))
        normalized.setdefault("symbol", normalized.get("instrument"))
        canonical = self._canonical_spot_symbol(normalized)
        if not canonical:
            raise ValueError("Fill references non-spot instrument")
        normalized["instrument"] = canonical
        normalized["symbol"] = canonical
        return {key: self._normalise_value(value) for key, value in normalized.items()}

    def _latest_config_version(self, configs: Sequence[Mapping[str, Any]]) -> str | None:
        latest_ts: Optional[str] = None
        latest_version: Optional[str] = None
        for config in configs:
            timestamp = self._stringify(
                config.get("updated_at")
                or config.get("created_at")
                or config.get("ts")
                or config.get("timestamp"),
            )
            version = self._stringify(
                config.get("version")
                or config.get("id")
                or config.get("config_id"),
            )
            if timestamp and (latest_ts is None or timestamp > latest_ts):
                latest_ts = timestamp
                latest_version = version
        return latest_version

    def _index_by(
        self,
        rows: Sequence[Mapping[str, Any]],
        keys: Sequence[str],
    ) -> Dict[str, Mapping[str, Any]]:
        index: Dict[str, Mapping[str, Any]] = {}
        for row in rows:
            for key in keys:
                value = row.get(key)
                if value:
                    index[str(value)] = row
                    break
        return index

    def _extract_override_reference(self, override: Mapping[str, Any] | None) -> str | None:
        if not override:
            return None
        identifier = (
            override.get("override_id")
            or override.get("id")
            or override.get("reference")
            or override.get("ticket")
        )
        status = override.get("status") or override.get("state")
        if identifier and status:
            return f"{identifier}:{status}"
        if identifier:
            return str(identifier)
        return None

    def _safe_lookup(self, row: Mapping[str, Any] | None, keys: Sequence[str]) -> Any:
        if not row:
            return None
        for key in keys:
            if key in row and row[key] is not None:
                return row[key]
        return None

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (list, tuple, dict)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    def _build_s3_key(
        self,
        for_date: dt.date,
        export_format: str,
        run_id: str,
        extension: str,
    ) -> str:
        prefix = self._config.prefix.rstrip("/")
        return f"{prefix}/{for_date.isoformat()}/compliance-pack-{export_format}-{run_id}.{extension}"

    def _s3_client(self):
        client_kwargs: Dict[str, Any] = {}
        if self._config.endpoint_url:
            client_kwargs["endpoint_url"] = self._config.endpoint_url
        return boto3.client("s3", **client_kwargs)  # type: ignore[return-value]

    def _compute_sha256(self, payload: bytes) -> str:
        import hashlib

        digest = hashlib.sha256()
        digest.update(payload)
        return digest.hexdigest()


@lru_cache(maxsize=1)
def _cached_exporter() -> CompliancePackExporter:
    config = _storage_config_from_env()
    dsn = _database_dsn()
    return CompliancePackExporter(config=config, dsn=dsn)


@router.get("/export")
def export_compliance_pack(
    export_format: str = Query("sec", alias="format"),
    export_date: dt.date = Query(..., alias="date"),
    _: AuditorPrincipal = Depends(require_auditor_identity),
) -> Response:
    try:
        exporter = _cached_exporter()
        pack = exporter.export(for_date=export_date, export_format=export_format)
    except MissingDependencyError as exc:  # pragma: no cover - environment specific.
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive default.
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    headers = {
        "X-Compliance-SHA256": pack.sha256,
        "X-Compliance-Location": f"s3://{pack.s3_bucket}/{pack.s3_key}",
        "X-Compliance-Generated-At": pack.generated_at.isoformat(),
    }

    filename = f"compliance-pack-{export_format}-{export_date.isoformat()}"
    if pack.content_type == "application/json":
        filename += ".json"
    else:
        filename += ".csv"

    headers["Content-Disposition"] = f"attachment; filename={filename}"

    return Response(content=pack.data, media_type=pack.content_type, headers=headers)


__all__ = [
    "CompliancePack",
    "CompliancePackExporter",
    "MissingDependencyError",
    "StorageConfig",
    "export_compliance_pack",
    "router",
]
