
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query, status


from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.schemas import FeeBreakdown, FeeDetail, FeeScheduleResponse
from services.common.security import require_admin_account

app = FastAPI(title="Fees Service")


@app.get("/fees/effective", response_model=FeeScheduleResponse)
def get_effective_fees(
    pair: str = Query(..., description="Trading pair symbol, e.g. BTC-USD"),
    liquidity: str = Query(..., description="Requested liquidity side: maker or taker"),
    notional: float = Query(..., gt=0.0, description="Order notional in USD"),
    account_id: str = Depends(require_admin_account),
) -> FeeScheduleResponse:
    normalized_liquidity = liquidity.lower().strip()
    if normalized_liquidity not in {"maker", "taker"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Liquidity must be either 'maker' or 'taker'.",
        )

    redis = RedisFeastAdapter(account_id=account_id)
    timescale = TimescaleAdapter(account_id=account_id)

    fee_tiers = redis.fee_tiers(pair)
    override = redis.fee_override(pair)
    if not fee_tiers:
        fee_tiers = redis.fee_tiers("default")
    if not override:
        override = redis.fee_override("default") or {
            "currency": "USD",
            "maker": 0.1,
            "taker": 0.2,
        }

    rolling = timescale.rolling_volume(pair)
    rolling_volume = float(rolling.get("notional", 0.0))
    basis_ts = rolling.get("basis_ts") or datetime.now(timezone.utc)
    effective_volume = rolling_volume + float(notional)

    matched_tier = None
    if fee_tiers:
        ordered = sorted(
            fee_tiers,
            key=lambda tier: float(tier.get("volume_threshold", 0.0)),
        )
        for tier in ordered:
            threshold = float(tier.get("volume_threshold", 0.0))
            if effective_volume >= threshold:
                matched_tier = tier
        matched_tier = matched_tier or ordered[0]

    currency = (matched_tier or {}).get("currency") or override.get("currency", "USD")
    maker_bps = float((matched_tier or {}).get("maker_bps", override.get("maker", 0.0)))
    taker_bps = float((matched_tier or {}).get("taker_bps", override.get("taker", 0.0)))

    maker_fee_usd = float(notional) * maker_bps / 10_000.0
    taker_fee_usd = float(notional) * taker_bps / 10_000.0

    tier_id = (matched_tier or {}).get("tier_id", "default")
    basis_timestamp = (
        (matched_tier or {}).get("basis_ts")
        or basis_ts
        or datetime.now(timezone.utc)
    )

    fee = FeeBreakdown(
        currency=currency,
        maker=maker_bps,
        taker=taker_bps,
        maker_detail=FeeDetail(
            bps=maker_bps,
            usd=maker_fee_usd,
            tier_id=tier_id,
            basis_ts=basis_timestamp,
        ),
        taker_detail=FeeDetail(
            bps=taker_bps,
            usd=taker_fee_usd,
            tier_id=tier_id,
            basis_ts=basis_timestamp,
        ),
    )

    return FeeScheduleResponse(
        account_id=account_id,
        effective_from=datetime.now(timezone.utc),
        fee=fee,
    )

