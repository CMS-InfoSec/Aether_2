"""FastAPI service exposing fee schedule endpoints.

The service provides routes to compute effective fees for a given account and
trading pair as well as to inspect the current fee tier schedule.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, validator
from sqlalchemy.orm import Session


app = FastAPI(title="Fee Schedule Service")


# ---------------------------------------------------------------------------
# Database access layer (stubbed)
# ---------------------------------------------------------------------------


@dataclass
class FeeTierRecord:
    """Representation of a fee tier as fetched from the database."""

    pair: str
    tier: str
    volume_threshold: float
    maker_bps: float
    taker_bps: float


def get_db() -> Iterable[Session]:
    """Yield a database session.

    This is intentionally stubbed: in a production environment this would
    manage the lifecycle of a SQLAlchemy session. For unit tests we can mock
    out the generator to provide a fixture or fake session.
    """

    class _FakeSession:  # pragma: no cover - illustrative stub
        def execute(self, *_args, **_kwargs):
            raise NotImplementedError("Database layer is not implemented")

    session = _FakeSession()
    try:
        yield session
    finally:  # pragma: no branch - symmetrical cleanup stub
        pass


def fetch_fee_tiers(session: Session, pair: str) -> List[FeeTierRecord]:
    """Fetch fee tier mappings for the requested trading pair.

    The implementation is stubbed with in-memory data. Replace with a real
    SQLAlchemy query (e.g., ``session.execute(select(FeeTier).where(...))``) to
    integrate with a database.
    """

    # Example in-memory mapping to illustrate behaviour.
    tier_table: Dict[str, List[FeeTierRecord]] = {
        "BTC-USD": [
            FeeTierRecord("BTC-USD", "VIP-0", 0.0, 8.0, 10.0),
            FeeTierRecord("BTC-USD", "VIP-1", 1_000_000.0, 6.0, 8.0),
            FeeTierRecord("BTC-USD", "VIP-2", 5_000_000.0, 4.0, 6.0),
        ],
        "ETH-USD": [
            FeeTierRecord("ETH-USD", "VIP-0", 0.0, 10.0, 12.0),
            FeeTierRecord("ETH-USD", "VIP-1", 750_000.0, 8.0, 10.0),
        ],
    }

    return tier_table.get(pair, [])


def fetch_all_fee_tiers(session: Session) -> Dict[str, List[FeeTierRecord]]:
    """Return the fee tiers for all supported trading pairs.

    Stubbed to return the same in-memory mapping as ``fetch_fee_tiers``.
    """

    pairs = ["BTC-USD", "ETH-USD"]
    return {pair: fetch_fee_tiers(session, pair) for pair in pairs}


def fetch_rolling_30d_volume(account_id: str, pair: str) -> float:
    """Return the trailing 30-day trading volume for the account.

    This is a stub that simply returns a deterministic float for illustrative
    purposes. Replace this with a call into analytics infrastructure (Feast,
    warehouse query, etc.).
    """

    # Deterministic mock behaviour per pair to keep the example reproducible.
    return {
        "BTC-USD": 2_500_000.0,
        "ETH-USD": 500_000.0,
    }.get(pair, 0.0)


# ---------------------------------------------------------------------------
# Pydantic schema definitions
# ---------------------------------------------------------------------------


LiquiditySide = Literal["maker", "taker"]


class EffectiveFeeQuery(BaseModel):
    """Validated query parameters for the effective fee endpoint."""

    account_id: str = Field(..., description="Unique account identifier")
    pair: str = Field(..., min_length=3, max_length=32, description="Trading pair symbol")
    liquidity: LiquiditySide = Field(..., description="Liquidity side: maker or taker")
    notional: float = Field(..., gt=0.0, description="Order notional value in USD")

    @validator("pair")
    def normalize_pair(cls, value: str) -> str:
        return value.upper()

    @validator("liquidity")
    def normalize_liquidity(cls, value: str) -> LiquiditySide:
        return value.lower()  # type: ignore[return-value]


class EffectiveFeeResponse(BaseModel):
    bps: float = Field(..., description="Fee rate in basis points")
    usd: float = Field(..., description="Fee in USD for the requested notional")
    tier: str = Field(..., description="Name of the matched fee tier")


class FeeTier(BaseModel):
    tier: str
    volume_threshold: float
    maker_bps: float
    taker_bps: float


class FeeTierScheduleResponse(BaseModel):
    pair: str
    tiers: List[FeeTier]


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------


def determine_fee_tier(
    tiers: List[FeeTierRecord],
    effective_volume: float,
    liquidity: LiquiditySide,
) -> Optional[FeeTierRecord]:
    """Return the tier that applies given the effective rolling volume."""

    if not tiers:
        return None

    tiers_sorted = sorted(tiers, key=lambda tier: tier.volume_threshold)
    matched: Optional[FeeTierRecord] = None
    for tier in tiers_sorted:
        if effective_volume >= tier.volume_threshold:
            matched = tier
    return matched or tiers_sorted[0]


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@app.get("/fees/effective", response_model=EffectiveFeeResponse)
def get_effective_fee(
    account_id: str = Query(..., description="Unique account identifier"),
    pair: str = Query(..., description="Trading pair symbol"),
    liquidity: LiquiditySide = Query(..., description="Liquidity side"),
    notional: float = Query(..., gt=0.0, description="Order notional value in USD"),
    session: Session = Depends(get_db),
) -> EffectiveFeeResponse:
    """Calculate the effective fee for an account and order request."""

    params = EffectiveFeeQuery(
        account_id=account_id,
        pair=pair,
        liquidity=liquidity,
        notional=notional,
    )

    tiers = fetch_fee_tiers(session, params.pair)
    if not tiers:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fee schedule not found for pair {params.pair}",
        )

    rolling_volume = fetch_rolling_30d_volume(params.account_id, params.pair)
    effective_volume = rolling_volume + params.notional

    tier = determine_fee_tier(tiers, effective_volume, params.liquidity)
    if tier is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to determine fee tier",
        )

    if params.liquidity == "maker":
        bps = tier.maker_bps
    else:
        bps = tier.taker_bps

    usd_fee = params.notional * bps / 10_000.0

    return EffectiveFeeResponse(bps=bps, usd=usd_fee, tier=tier.tier)


@app.get("/fees/tiers", response_model=List[FeeTierScheduleResponse])
def get_fee_tiers(session: Session = Depends(get_db)) -> List[FeeTierScheduleResponse]:
    """Return the full maker/taker fee tier schedule."""

    schedule = fetch_all_fee_tiers(session)
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No fee tiers available",
        )

    response: List[FeeTierScheduleResponse] = []
    for pair, tiers in schedule.items():
        response.append(
            FeeTierScheduleResponse(
                pair=pair,
                tiers=[
                    FeeTier(
                        tier=tier.tier,
                        volume_threshold=tier.volume_threshold,
                        maker_bps=tier.maker_bps,
                        taker_bps=tier.taker_bps,
                    )
                    for tier in tiers
                ],
            )
        )

    return response
