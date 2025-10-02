"""Monitor trusted FX feeds to detect stablecoin deviations from the USD peg."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Callable, Iterable, Mapping, MutableMapping, Sequence

LOGGER = logging.getLogger(__name__)

_DEFAULT_MONITORED_SYMBOLS: tuple[str, ...] = ("USDC-USD", "USDT-USD")
_DEFAULT_TRUSTED_FEEDS: tuple[str, ...] = ("nyfed_fx", "bloomberg_fx")
_DEFAULT_DEPEG_THRESHOLD_BPS = 75.0
_DEFAULT_RECOVERY_THRESHOLD_BPS = 15.0
_DEFAULT_FEED_MAX_AGE_SECONDS = 120

_SYSTEM_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "system.yaml"


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class StablecoinMonitorConfig:
    """Configuration describing how the stablecoin monitor should behave."""

    depeg_threshold_bps: float = _DEFAULT_DEPEG_THRESHOLD_BPS
    recovery_threshold_bps: float = _DEFAULT_RECOVERY_THRESHOLD_BPS
    feed_max_age_seconds: int = _DEFAULT_FEED_MAX_AGE_SECONDS
    monitored_symbols: tuple[str, ...] = _DEFAULT_MONITORED_SYMBOLS
    trusted_feeds: tuple[str, ...] = _DEFAULT_TRUSTED_FEEDS

    def __post_init__(self) -> None:
        if self.depeg_threshold_bps <= 0:
            raise ValueError("depeg_threshold_bps must be positive")
        if self.recovery_threshold_bps < 0:
            raise ValueError("recovery_threshold_bps must be non-negative")
        if self.recovery_threshold_bps > self.depeg_threshold_bps:
            raise ValueError("recovery threshold cannot exceed the depeg threshold")
        if self.feed_max_age_seconds < 0:
            raise ValueError("feed_max_age_seconds must be non-negative")

        object.__setattr__(
            self,
            "monitored_symbols",
            tuple(symbol.upper() for symbol in self.monitored_symbols),
        )
        object.__setattr__(
            self,
            "trusted_feeds",
            tuple(feed.lower() for feed in self.trusted_feeds),
        )


@dataclass(frozen=True)
class StablecoinSample:
    """Represents the latest FX quote ingested for a stablecoin symbol."""

    symbol: str
    price: float
    feed: str
    timestamp: datetime


@dataclass(frozen=True)
class StablecoinStatus:
    """Computed deviation from the USD peg for a stablecoin."""

    symbol: str
    price: float
    deviation: float
    deviation_bps: float
    feed: str
    timestamp: datetime
    stale: bool
    depegged: bool


class StablecoinMonitor:
    """Track trusted FX feeds and surface stablecoin depeg status."""

    def __init__(
        self,
        *,
        config: StablecoinMonitorConfig | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config or load_stablecoin_monitor_config()
        self._clock = clock or _utcnow
        self._lock = Lock()
        self._samples: MutableMapping[str, StablecoinSample] = {}
        self._depeg_state: MutableMapping[str, bool] = {}

    # ------------------------------------------------------------------
    # Feed ingestion
    # ------------------------------------------------------------------
    def update(
        self,
        symbol: str,
        price: float,
        *,
        feed: str,
        timestamp: datetime | None = None,
    ) -> StablecoinStatus:
        """Ingest a new FX observation for ``symbol`` and return its status."""

        if price <= 0:
            raise ValueError("price must be positive")

        normalised_symbol = symbol.upper()
        feed_key = feed.lower()
        if self.config.trusted_feeds and feed_key not in self.config.trusted_feeds:
            raise ValueError(f"Feed '{feed}' is not part of the trusted set")

        ts = timestamp or self._clock()
        sample = StablecoinSample(
            symbol=normalised_symbol,
            price=float(price),
            feed=feed_key,
            timestamp=ts,
        )

        with self._lock:
            self._samples[normalised_symbol] = sample
            previous_state = self._depeg_state.get(normalised_symbol, False)
            status = self._compute_status(sample, previous_state)
            self._depeg_state[normalised_symbol] = status.depegged
            return status

    # ------------------------------------------------------------------
    # Status queries
    # ------------------------------------------------------------------
    def status(self, symbol: str) -> StablecoinStatus | None:
        """Return the latest status for ``symbol`` if known."""

        normalised_symbol = symbol.upper()
        with self._lock:
            sample = self._samples.get(normalised_symbol)
            if sample is None:
                return None
            previous_state = self._depeg_state.get(normalised_symbol, False)
            status = self._compute_status(sample, previous_state)
            self._depeg_state[normalised_symbol] = status.depegged
            return status

    def active_depegs(self) -> list[StablecoinStatus]:
        """Return a list of currently depegged stablecoins."""

        statuses: list[StablecoinStatus] = []
        for symbol in self.tracked_symbols():
            status = self.status(symbol)
            if status is None:
                continue
            if status.depegged:
                statuses.append(status)
        return statuses

    def tracked_symbols(self) -> Iterable[str]:
        """Return the set of symbols monitored by the instance."""

        with self._lock:
            known = set(self._samples.keys())
        return sorted(set(self.config.monitored_symbols) | known)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _compute_status(
        self, sample: StablecoinSample, previous_state: bool
    ) -> StablecoinStatus:
        deviation = sample.price - 1.0
        deviation_bps = deviation * 10_000.0
        abs_deviation = abs(deviation_bps)

        depegged = previous_state
        if abs_deviation >= self.config.depeg_threshold_bps:
            depegged = True
        elif abs_deviation <= self.config.recovery_threshold_bps:
            depegged = False

        stale = False
        max_age = self.config.feed_max_age_seconds
        if max_age > 0:
            age = self._clock() - sample.timestamp
            if age > timedelta(seconds=max_age):
                stale = True
                depegged = False

        return StablecoinStatus(
            symbol=sample.symbol,
            price=sample.price,
            deviation=deviation,
            deviation_bps=deviation_bps,
            feed=sample.feed,
            timestamp=sample.timestamp,
            stale=stale,
            depegged=depegged,
        )


def format_depeg_alert(
    statuses: Sequence[StablecoinStatus],
    threshold_bps: float,
) -> str:
    """Return a concise alert string summarising depeg status."""

    if not statuses:
        return "Stablecoin deviation exceeds configured threshold; trading halted."

    parts = []
    for status in statuses:
        direction = "below" if status.deviation < 0 else "above"
        parts.append(
            f"{status.symbol} {direction} peg by {abs(status.deviation_bps):.1f} bps"
            f" (price={status.price:.6f}, feed={status.feed})"
        )
    joined = "; ".join(parts)
    return (
        f"Stablecoin deviation exceeds {threshold_bps:.1f} bps threshold: {joined}. "
        "New orders are temporarily disabled until prices recover."
    )


# ---------------------------------------------------------------------------
# Configuration loading helpers
# ---------------------------------------------------------------------------

def load_stablecoin_monitor_config(
    path: Path | None = None,
) -> StablecoinMonitorConfig:
    """Load monitor configuration from ``config/system.yaml`` if available."""

    config_path = path or _SYSTEM_CONFIG_PATH
    data = _load_system_config(config_path)
    section: Mapping[str, object]
    raw_section = data.get("stablecoin_monitor", {}) if isinstance(data, Mapping) else {}
    if not isinstance(raw_section, Mapping):
        LOGGER.warning(
            "Invalid stablecoin_monitor section in %s; falling back to defaults",
            config_path,
        )
        section = {}
    else:
        section = raw_section

    return StablecoinMonitorConfig(
        depeg_threshold_bps=_coerce_float(
            section.get("depeg_threshold_bps"), _DEFAULT_DEPEG_THRESHOLD_BPS
        ),
        recovery_threshold_bps=_coerce_float(
            section.get("recovery_threshold_bps"), _DEFAULT_RECOVERY_THRESHOLD_BPS
        ),
        feed_max_age_seconds=int(
            _coerce_float(section.get("feed_max_age_seconds"), _DEFAULT_FEED_MAX_AGE_SECONDS)
        ),
        monitored_symbols=_coerce_tuple(
            section.get("monitored_symbols"), _DEFAULT_MONITORED_SYMBOLS
        ),
        trusted_feeds=_coerce_tuple(section.get("trusted_feeds"), _DEFAULT_TRUSTED_FEEDS, lower=True),
    )


def _load_system_config(path: Path) -> Mapping[str, object]:
    if not path.exists():
        LOGGER.debug("System configuration %s not found; using defaults", path)
        return {}

    raw_text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
    except Exception:  # pragma: no cover - fallback when PyYAML is unavailable
        LOGGER.warning(
            "PyYAML not available; stablecoin monitor will use default configuration"
        )
        return {}

    loaded = yaml.safe_load(raw_text) or {}
    if not isinstance(loaded, Mapping):
        raise ValueError("system.yaml must contain a mapping at the top level")
    return loaded


def _coerce_float(value: object, default: float) -> float:
    try:
        if isinstance(value, str) and not value:
            return float(default)
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float(default)


def _coerce_tuple(
    value: object, default: Sequence[str], *, lower: bool = False
) -> tuple[str, ...]:
    if isinstance(value, (list, tuple, set)):
        items = [str(item) for item in value]
    elif isinstance(value, str):
        items = [part.strip() for part in value.split(",") if part.strip()]
    else:
        items = list(default)
    if lower:
        return tuple(item.lower() for item in items)
    return tuple(item.upper() for item in items)


_MONITOR: StablecoinMonitor | None = None


def get_global_monitor() -> StablecoinMonitor:
    """Return the process-wide stablecoin monitor singleton."""

    global _MONITOR
    if _MONITOR is None:
        _MONITOR = StablecoinMonitor()
    return _MONITOR


def override_global_monitor(monitor: StablecoinMonitor | None) -> None:
    """Override the global monitor instance (primarily for testing)."""

    global _MONITOR
    _MONITOR = monitor
