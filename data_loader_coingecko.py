"""CoinGecko data loader with configuration-driven behaviour."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional

import requests

LOGGER = logging.getLogger(__name__)

_DEFAULT_VALUES: dict[str, Any] = {
    "base_url": "https://api.coingecko.com/api/v3",
    "rate_limit_qps": 5,
    "max_retries": 5,
    "backoff_initial_ms": 200,
    "timeout_sec": 30,
}

_SYSTEM_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "system.yaml"


@dataclass(frozen=True)
class CoinGeckoConfig:
    """Configuration describing how the CoinGecko loader should behave."""

    base_url: str = _DEFAULT_VALUES["base_url"]
    rate_limit_qps: float = _DEFAULT_VALUES["rate_limit_qps"]
    max_retries: int = _DEFAULT_VALUES["max_retries"]
    backoff_initial_ms: int = _DEFAULT_VALUES["backoff_initial_ms"]
    timeout_sec: int = _DEFAULT_VALUES["timeout_sec"]


def load_coingecko_config(path: Path | None = None) -> CoinGeckoConfig:
    """Load the CoinGecko configuration from ``system.yaml``.

    Parameters
    ----------
    path:
        Optional override for the system configuration path. When omitted the
        loader will look for ``config/system.yaml`` relative to the project
        root. Missing files or malformed entries will fall back to sensible
        defaults defined in :data:`_DEFAULT_VALUES`.
    """

    config_path = path or _SYSTEM_CONFIG_PATH
    data = _load_system_config(config_path)
    section_obj: Any
    if isinstance(data, Mapping):
        section_obj = data.get("coingecko", {})
    else:  # pragma: no cover - defensive fallback
        section_obj = {}

    if not isinstance(section_obj, Mapping):
        LOGGER.warning(
            "Invalid 'coingecko' section in %s; expected a mapping but got %r", config_path, section_obj
        )
        section_obj = {}

    section: Mapping[str, Any] = section_obj

    return CoinGeckoConfig(
        base_url=_coerce(section, "base_url", str, _DEFAULT_VALUES["base_url"]),
        rate_limit_qps=_coerce(section, "rate_limit_qps", float, _DEFAULT_VALUES["rate_limit_qps"]),
        max_retries=_coerce(section, "max_retries", int, _DEFAULT_VALUES["max_retries"]),
        backoff_initial_ms=_coerce(section, "backoff_initial_ms", int, _DEFAULT_VALUES["backoff_initial_ms"]),
        timeout_sec=_coerce(section, "timeout_sec", int, _DEFAULT_VALUES["timeout_sec"]),
    )


class CoinGeckoDataLoader:
    """HTTP client for CoinGecko that respects local and server-side limits."""

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        *,
        config: CoinGeckoConfig | None = None,
        config_path: Path | None = None,
    ) -> None:
        if config is not None and config_path is not None:
            raise ValueError("Provide either 'config' or 'config_path', not both")

        if config is None:
            config = load_coingecko_config(config_path)

        self.config = config
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._min_interval = 0.0
        if self.config.rate_limit_qps > 0:
            self._min_interval = 1.0 / float(self.config.rate_limit_qps)
        self._last_request_ts = 0.0
        self._base_backoff = max(self.config.backoff_initial_ms, 0) / 1000.0

    def __enter__(self) -> "CoinGeckoDataLoader":  # pragma: no cover - convenience
        return self

    def __exit__(self, *exc_info: object) -> None:  # pragma: no cover - convenience
        self.close()

    def close(self) -> None:
        """Release the underlying HTTP session if owned by the loader."""

        if self._owns_session:
            self._session.close()

    def get(self, path: str, *, params: Optional[Mapping[str, Any]] = None) -> Any:
        """Perform a GET request against the CoinGecko API and return JSON."""

        url = self._build_url(path)
        return self._request_json(url, params=params)

    def fetch_market_chart_range(
        self,
        coin_id: str,
        *,
        vs_currency: str,
        start: int,
        end: int,
    ) -> Any:
        """Convenience helper mirroring the CoinGecko ``market_chart`` endpoint."""

        params = {"vs_currency": vs_currency, "from": start, "to": end}
        return self.get(f"/coins/{coin_id}/market_chart/range", params=params)

    def _build_url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/")
        if not path:
            return base
        return f"{base}/{path.lstrip('/')}"

    def _request_json(self, url: str, *, params: Optional[Mapping[str, Any]] = None) -> Any:
        for attempt in range(1, int(self.config.max_retries) + 1):
            self._respect_client_limit()
            try:
                response = self._session.get(url, params=params, timeout=self.config.timeout_sec)
            except requests.RequestException as exc:
                if attempt >= self.config.max_retries:
                    raise
                sleep_for = self._retry_backoff(attempt)
                LOGGER.warning(
                    "CoinGecko request error (attempt %s/%s): %s; retrying in %.2fs",
                    attempt,
                    self.config.max_retries,
                    exc,
                    sleep_for,
                )
                if sleep_for > 0:
                    time.sleep(sleep_for)
                continue

            if response.status_code == 429:
                retry_after = _retry_after_seconds(response, self._base_backoff, attempt)
                LOGGER.warning(
                    "CoinGecko rate limit encountered; sleeping for %.2fs",
                    retry_after,
                    extra={
                        "attempt": attempt,
                        "max_retries": self.config.max_retries,
                        "rate_limit": _extract_rate_limit_headers(response.headers),
                    },
                )
                if attempt >= self.config.max_retries:
                    response.raise_for_status()
                if retry_after > 0:
                    time.sleep(retry_after)
                continue

            if 500 <= response.status_code < 600:
                if attempt >= self.config.max_retries:
                    response.raise_for_status()
                sleep_for = self._retry_backoff(attempt)
                LOGGER.warning(
                    "CoinGecko server error %s (attempt %s/%s); retrying in %.2fs",
                    response.status_code,
                    attempt,
                    self.config.max_retries,
                    sleep_for,
                )
                if sleep_for > 0:
                    time.sleep(sleep_for)
                continue

            response.raise_for_status()

            remaining = response.headers.get("X-RateLimit-Remaining")
            if remaining is not None and remaining in {"0", "0.0"}:
                LOGGER.info(
                    "CoinGecko rate limit nearly exhausted",
                    extra={"rate_limit": _extract_rate_limit_headers(response.headers)},
                )
            return response.json()

        raise RuntimeError("CoinGecko request exhausted retries without success")

    def _respect_client_limit(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        wait_for = self._min_interval - elapsed
        if wait_for > 0:
            LOGGER.debug(
                "Throttling CoinGecko request for %.3fs to respect %.2f qps", wait_for, self.config.rate_limit_qps
            )
            time.sleep(wait_for)
        self._last_request_ts = time.monotonic()

    def _retry_backoff(self, attempt: int) -> float:
        if self._base_backoff <= 0:
            return 0.0
        exponent = max(attempt - 1, 0)
        return self._base_backoff * (2**exponent)


def _retry_after_seconds(response: requests.Response, base_backoff: float, attempt: int) -> float:
    header = response.headers.get("Retry-After")
    if header is not None:
        try:
            return max(float(header), 0.0)
        except ValueError:
            pass
    exponent = max(attempt - 1, 0)
    return max(base_backoff * (2**exponent), 0.0)


def _extract_rate_limit_headers(headers: Mapping[str, str]) -> dict[str, str]:
    extracted: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if "ratelimit" in lowered or lowered == "retry-after":
            extracted[lowered] = value
    return extracted


def _load_system_config(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        LOGGER.debug("System configuration %s not found; using defaults", path)
        return {}
    raw_text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
    except Exception:  # pragma: no cover - fallback when PyYAML is unavailable
        return _parse_naive_yaml(raw_text.splitlines())
    loaded = yaml.safe_load(raw_text) or {}
    if not isinstance(loaded, Mapping):
        raise ValueError("system.yaml must contain a mapping at the top level")
    return loaded


def _parse_naive_yaml(lines: Iterable[str]) -> Mapping[str, Any]:
    root: MutableMapping[str, Any] = {}
    stack: list[tuple[int, MutableMapping[str, Any]]] = [(-1, root)]

    for raw_line in lines:
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        parent = stack[-1][1] if stack else root
        if not value:
            child: MutableMapping[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
            continue
        parent[key] = _coerce_scalar(value)
    return root


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    if lowered in {"null", "none"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_coerce_scalar(item.strip()) for item in inner.split(",")]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return value[1:-1]
    return value


def _coerce(
    section: Mapping[str, Any],
    key: str,
    converter: Callable[[Any], Any],
    default: Any,
) -> Any:
    value = section.get(key, default)
    try:
        return converter(value)  # type: ignore[misc]
    except (TypeError, ValueError):
        LOGGER.warning(
            "Invalid value for coingecko.%s: %r; using default %r", key, value, default
        )
        return converter(default)  # type: ignore[misc]


__all__ = ["CoinGeckoConfig", "CoinGeckoDataLoader", "load_coingecko_config"]

