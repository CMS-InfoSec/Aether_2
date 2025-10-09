"""Utilities for monitoring Kraken fee tier proximity.

This module encapsulates logic for determining the current and next fee tier
for an account, emitting notifications to the Policy Service when the
30-day notional approaches the next threshold, and persisting alert events for
later analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import logging
from typing import Sequence

try:  # pragma: no cover - httpx is optional in the test environment
    import httpx
except ModuleNotFoundError:  # pragma: no cover - provide a stub when dependency missing
    httpx = None  # type: ignore[assignment]
from sqlalchemy import select
from sqlalchemy.orm import Session

from services.fees.models import AccountVolume30d, FeeTier, FeeTierProgress


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NextTierStatus:
    """Represents progress towards the next Kraken fee tier."""

    current_tier: FeeTier
    next_tier: FeeTier | None
    current_volume: Decimal
    next_threshold: Decimal | None
    notional_to_next: Decimal | None
    progress_ratio: Decimal
    basis_ts: datetime

    @property
    def progress_pct(self) -> Decimal:
        """Return progress towards the next tier expressed as a percentage."""

        return (self.progress_ratio * Decimal("100")).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )


class FeeOptimizer:
    """Monitors 30-day volume relative to Kraken fee tiers."""

    def __init__(
        self,
        *,
        alert_threshold: Decimal,
        policy_service_url: str,
        policy_path: str,
        policy_timeout: float,
    ) -> None:
        self._alert_threshold = alert_threshold
        self._policy_service_url = policy_service_url
        self._policy_path = policy_path
        self._policy_timeout = policy_timeout

    def ordered_tiers(self, session: Session) -> list[FeeTier]:
        stmt = select(FeeTier).order_by(FeeTier.notional_threshold_usd.asc())
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def determine_tier(tiers: Sequence[FeeTier], volume: Decimal) -> FeeTier:
        if not tiers:
            raise ValueError("Fee schedule is empty")

        ordered = sorted(tiers, key=lambda tier: Decimal(tier.notional_threshold_usd or 0))
        match = ordered[0]
        for tier in ordered:
            threshold = Decimal(tier.notional_threshold_usd or 0)
            if volume >= threshold:
                match = tier
            else:
                break
        return match

    @staticmethod
    def _current_and_next(
        tiers: Sequence[FeeTier], volume: Decimal
    ) -> tuple[FeeTier, FeeTier | None]:
        if not tiers:
            raise ValueError("Fee schedule is empty")

        ordered = sorted(tiers, key=lambda tier: Decimal(tier.notional_threshold_usd or 0))
        current = ordered[0]
        next_tier: FeeTier | None = None
        for idx, tier in enumerate(ordered):
            threshold = Decimal(tier.notional_threshold_usd or 0)
            if volume >= threshold:
                current = tier
                next_tier = ordered[idx + 1] if idx + 1 < len(ordered) else None
            else:
                next_tier = tier
                break
        return current, next_tier

    def status_for_volume(
        self,
        tiers: Sequence[FeeTier],
        volume: Decimal,
        basis_ts: datetime,
    ) -> NextTierStatus:
        current, next_tier = self._current_and_next(tiers, volume)

        next_threshold: Decimal | None = None
        notional_to_next: Decimal | None = None
        progress_ratio = Decimal("1") if next_tier is None else Decimal("0")

        if next_tier is not None:
            next_threshold = Decimal(next_tier.notional_threshold_usd or 0)
            if next_threshold > 0:
                progress_ratio = min(volume / next_threshold, Decimal("1"))
                notional_to_next = max(next_threshold - volume, Decimal("0"))
            else:
                progress_ratio = Decimal("0")
                notional_to_next = None

        return NextTierStatus(
            current_tier=current,
            next_tier=next_tier,
            current_volume=volume,
            next_threshold=next_threshold,
            notional_to_next=notional_to_next,
            progress_ratio=progress_ratio,
            basis_ts=basis_ts,
        )

    def status_for_account(self, session: Session, account_id: str) -> NextTierStatus:
        tiers = self.ordered_tiers(session)
        record = session.get(AccountVolume30d, account_id)
        basis_ts = (
            record.updated_at
            if record is not None and record.updated_at is not None
            else datetime.now(timezone.utc)
        )
        volume = Decimal(record.notional_usd_30d or 0) if record is not None else Decimal("0")
        return self.status_for_volume(tiers, volume, basis_ts)

    def monitor_account(
        self,
        session: Session,
        account_id: str,
        volume: Decimal,
        basis_ts: datetime,
    ) -> NextTierStatus:
        tiers = self.ordered_tiers(session)
        status = self.status_for_volume(tiers, volume, basis_ts)
        self._handle_alerts(session, account_id, status)
        return status

    def _handle_alerts(self, session: Session, account_id: str, status: NextTierStatus) -> None:
        if not self._within_alert_window(status):
            return

        self._persist_progress(session, account_id, status)
        self._notify_policy_service(account_id, status)

    def _within_alert_window(self, status: NextTierStatus) -> bool:
        if status.next_tier is None or status.notional_to_next is None:
            return False
        if status.notional_to_next <= 0:
            return False
        return status.progress_ratio >= self._alert_threshold and status.progress_ratio < Decimal("1")

    def _persist_progress(self, session: Session, account_id: str, status: NextTierStatus) -> None:
        try:
            progress = status.progress_pct
            session.add(
                FeeTierProgress(
                    account_id=account_id,
                    current_tier=status.current_tier.tier_id,
                    progress=progress,
                    ts=status.basis_ts,
                )
            )
            session.commit()
        except Exception:  # pragma: no cover - defensive logging
            session.rollback()
            LOGGER.exception(
                "Failed to persist fee tier progress alert for account %s", account_id
            )

    def _notify_policy_service(self, account_id: str, status: NextTierStatus) -> None:
        next_tier = status.next_tier
        next_threshold = status.next_threshold
        notional_to_next = status.notional_to_next

        if next_tier is None or next_threshold is None:
            return

        payload = {
            "account_id": account_id,
            "current_tier": status.current_tier.tier_id,
            "current_volume_30d": float(status.current_volume),
            "next_tier": next_tier.tier_id,
            "next_tier_threshold": float(next_threshold),
            "notional_to_next": float(notional_to_next) if notional_to_next is not None else None,
            "progress_to_next_pct": float(status.progress_pct),
            "basis_ts": status.basis_ts.isoformat(),
        }

        url = self._build_policy_signal_url(self._policy_service_url, self._policy_path)

        if httpx is None:
            LOGGER.debug(  # pragma: no cover - dependency is intentionally optional
                "httpx is unavailable; skipping policy notification for account %s",
                account_id,
            )
            return

        try:
            with httpx.Client(timeout=self._policy_timeout) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network failure guard
            LOGGER.warning(
                "Failed to signal policy service for account %s near fee tier threshold",
                account_id,
                exc_info=exc,
            )

    @staticmethod
    def _build_policy_signal_url(base_url: str, path: str) -> str:
        sanitized_base = base_url.rstrip("/")
        sanitized_path = path if path.startswith("/") else f"/{path}"
        return f"{sanitized_base}{sanitized_path}"


__all__ = ["FeeOptimizer", "NextTierStatus"]
