from decimal import Decimal, ROUND_HALF_EVEN
import sys
import types
from typing import Dict, List

try:
    import fastapi  # type: ignore  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - test shim for missing optional dependency
    stub = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str | None = None) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - minimal stub
            self.routes = []

        def on_event(self, *_args, **_kwargs):  # pragma: no cover - decorator stub
            def decorator(func):
                return func

            return decorator

        def post(self, *_args, **_kwargs):  # pragma: no cover - decorator stub
            def decorator(func):
                return func

            return decorator

        def get(self, *_args, **_kwargs):  # pragma: no cover - decorator stub
            def decorator(func):
                return func

            return decorator

    def Query(default=None, *args, **kwargs):  # pragma: no cover - stubbed helper
        return default

    stub.FastAPI = FastAPI
    stub.HTTPException = HTTPException
    stub.Query = Query
    sys.modules["fastapi"] = stub

if "cryptography" not in sys.modules:
    crypto_mod = types.ModuleType("cryptography")
    hazmat_mod = types.ModuleType("cryptography.hazmat")
    primitives_mod = types.ModuleType("cryptography.hazmat.primitives")
    ciphers_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers")
    aead_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers.aead")

    class AESGCM:  # pragma: no cover - placeholder
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def encrypt(self, *args: object, **kwargs: object) -> bytes:
            raise NotImplementedError

        def decrypt(self, *args: object, **kwargs: object) -> bytes:
            raise NotImplementedError

    aead_mod.AESGCM = AESGCM

    sys.modules["cryptography"] = crypto_mod
    sys.modules["cryptography.hazmat"] = hazmat_mod
    sys.modules["cryptography.hazmat.primitives"] = primitives_mod
    sys.modules["cryptography.hazmat.primitives.ciphers"] = ciphers_mod
    sys.modules["cryptography.hazmat.primitives.ciphers.aead"] = aead_mod

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")

    def _noop(*args: object, **kwargs: object) -> None:
        return None

    class _DummySpan:
        def __enter__(self) -> "_DummySpan":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def traced_span(*args: object, **kwargs: object) -> _DummySpan:
        return _DummySpan()

    metrics_stub.increment_oms_error_count = _noop
    metrics_stub.increment_oms_auth_failures = _noop
    metrics_stub.increment_oms_child_orders_total = _noop
    metrics_stub.record_oms_latency = _noop
    metrics_stub.setup_metrics = _noop
    metrics_stub.traced_span = traced_span
    metrics_stub._REGISTRY = object()
    sys.modules["metrics"] = metrics_stub

if "services.oms.oms_service" not in sys.modules:
    stub_module = types.ModuleType("services.oms.oms_service")

    class _PrecisionValidator:
        @staticmethod
        def _snap(value: Decimal | None, step: Decimal | None) -> Decimal | None:
            if value is None:
                return None
            if step is None or step <= 0:
                return value
            operand = value if isinstance(value, Decimal) else Decimal(str(value))
            quant = step if isinstance(step, Decimal) else Decimal(str(step))
            return (operand / quant).to_integral_value(rounding=ROUND_HALF_EVEN) * quant

        @staticmethod
        def _maybe_decimal(*values: object) -> Decimal | None:
            for candidate in values:
                if candidate is None:
                    continue
                if isinstance(candidate, Decimal):
                    return candidate
                try:
                    return Decimal(str(candidate))
                except Exception:
                    continue
            return None

        @classmethod
        def _step(cls, metadata: Dict[str, object], keys: List[str]) -> Decimal | None:
            for key in keys:
                if key not in metadata:
                    continue
                value = metadata[key]
                if key.endswith("decimals"):
                    try:
                        decimals = int(value)
                    except Exception:
                        continue
                    return Decimal("1") / (Decimal("10") ** decimals)
                if isinstance(value, Decimal):
                    return value
                try:
                    if isinstance(value, (int, float)):
                        return Decimal(str(value))
                    if isinstance(value, str):
                        return Decimal(value)
                except Exception:
                    continue
            return None

        @classmethod
        def validate(
            cls,
            symbol: str,
            qty: Decimal,
            price: Decimal | None,
            metadata: Dict[str, Dict[str, object]] | None,
        ) -> tuple[Decimal, Decimal | None]:
            if not metadata:
                return qty, price
            pair_meta = _resolve_pair_metadata(symbol, metadata)
            if pair_meta is None:
                return qty, price
            qty_step = cls._step(pair_meta, ["lot_step", "lot_decimals", "step_size"])
            px_step = cls._step(pair_meta, ["price_increment", "pair_decimals", "tick_size"])
            snapped_qty = cls._snap(qty, qty_step) or qty
            snapped_px = cls._snap(price, px_step) if price is not None else None
            min_qty = cls._maybe_decimal(
                pair_meta.get("ordermin"), pair_meta.get("min_qty")
            )
            if min_qty is not None and snapped_qty < min_qty:
                raise ValueError("quantity below minimum")
            return snapped_qty, snapped_px

    def _normalize_symbol(symbol: str) -> str:
        return symbol.replace("-", "/").replace("_", "/").upper()

    def _resolve_pair_metadata(
        symbol: str, metadata: Dict[str, Dict[str, object]] | None
    ) -> Dict[str, object] | None:
        if not metadata:
            return None
        candidates = [symbol, _normalize_symbol(symbol), symbol.replace("/", ""), _normalize_symbol(symbol).replace("/", "")]
        for candidate in candidates:
            value = metadata.get(candidate)
            if isinstance(value, dict):
                return value
        for value in metadata.values():
            if isinstance(value, dict):
                wsname = str(value.get("wsname") or "")
                altname = str(value.get("altname") or "")
                if wsname == _normalize_symbol(symbol) or altname == _normalize_symbol(symbol).replace("/", ""):
                    return value
        return None

    stub_module._PrecisionValidator = _PrecisionValidator
    stub_module._normalize_symbol = _normalize_symbol
    stub_module._resolve_pair_metadata = _resolve_pair_metadata
    sys.modules["services.oms.oms_service"] = stub_module

import oms_service as legacy_oms_service


def test_place_order_request_coerces_decimal_inputs() -> None:
    request = legacy_oms_service.PlaceOrderRequest(
        account_id="acct",
        client_id="CID-DEC",
        symbol="BTC/USD",
        side="buy",
        type="limit",
        qty=0.5,
        limit_px=30000.1234,
        tp="31000.25",
        sl=Decimal("29000.75"),
        trailing=0.5,
    )

    assert isinstance(request.qty, Decimal)
    assert isinstance(request.limit_px, Decimal)
    assert isinstance(request.tp, Decimal)
    assert isinstance(request.sl, Decimal)
    assert isinstance(request.trailing, Decimal)


def test_build_payload_snaps_metadata_precision() -> None:
    service = legacy_oms_service.OMSService()
    metadata = {
        "BTC/USD": {
            "lot_step": "0.0001",
            "pair_decimals": 5,
            "price_increment": "0.1",
            "ordermin": "0.0001",
            "wsname": "BTC/USD",
        }
    }
    request = legacy_oms_service.PlaceOrderRequest(
        account_id="acct",
        client_id="CID-EDGE",
        symbol="BTC/USD",
        side="buy",
        type="limit",
        qty=Decimal("0.00010009"),
        limit_px=Decimal("20100.12345"),
        tp=Decimal("20500.9876"),
        sl=Decimal("19000.4321"),
        trailing=Decimal("15.5555"),
    )

    snapped = service._snap_order_fields(request, metadata)
    payload = service._build_payload(request, snapped)

    assert snapped.qty == Decimal("0.0001")
    assert snapped.limit_px == Decimal("20100.1")
    assert snapped.take_profit == Decimal("20501.0")
    assert snapped.stop_loss == Decimal("19000.4")
    assert snapped.trailing == Decimal("15.6")

    assert payload["pair"] == "BTC/USD"
    assert Decimal(payload["volume"]) == snapped.qty
    assert Decimal(payload.get("price", "0")) == snapped.limit_px
    assert Decimal(payload.get("takeProfit", "0")) == snapped.take_profit
    assert Decimal(payload.get("stopLoss", "0")) == snapped.stop_loss
    assert Decimal(payload.get("trailingStopOffset", "0")) == snapped.trailing
