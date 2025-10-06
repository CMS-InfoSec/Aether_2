from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Tuple
from types import ModuleType

import pytest
from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore


class _ReplayResult:
    def __init__(self) -> None:
        self.run_id = "test-run"
        self.json_report = Path("reports/test.json")
        self.html_report = Path("reports/test.html")


@pytest.fixture
def replay_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Tuple[TestClient, InMemorySessionStore, object]]:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://")
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    removed_modules: dict[str, ModuleType] = {}
    for module_name in list(sys.modules):
        if module_name == "services" or module_name.startswith("services."):
            removed_modules[module_name] = sys.modules.pop(module_name)

    policy_stub = ModuleType("services.policy.main")
    policy_stub.decide_policy = lambda *args, **kwargs: None  # type: ignore[attr-defined]

    risk_stub = ModuleType("services.risk.engine")

    class _RiskEngine:
        def __init__(self, *args: object, **kwargs: object) -> None:  # noqa: D401 - simple stub
            pass

        def validate(self, *args: object, **kwargs: object) -> object:
            raise RuntimeError("RiskEngine stub should not be invoked in tests")

    risk_stub.RiskEngine = _RiskEngine  # type: ignore[attr-defined]

    shadow_stub = ModuleType("services.oms.shadow_oms")

    class _ShadowOMS:
        def generate_shadow_fills(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            return []

    shadow_stub.shadow_oms = _ShadowOMS()  # type: ignore[attr-defined]

    sys.modules.update(
        {
            "services.policy.main": policy_stub,
            "services.risk.engine": risk_stub,
            "services.oms.shadow_oms": shadow_stub,
        }
    )
    module = importlib.import_module("ops.replay.snapshot_replay")
    snapshot_replay = importlib.reload(module)

    store = snapshot_replay.SESSION_STORE
    assert isinstance(store, InMemorySessionStore)

    def _run(self) -> _ReplayResult:  # type: ignore[no-redef]
        return _ReplayResult()

    monkeypatch.setattr(snapshot_replay.SnapshotReplayer, "run", _run)

    client = TestClient(snapshot_replay.app)

    try:
        yield client, store, snapshot_replay
    finally:
        snapshot_replay.security.set_default_session_store(None)
        for name in [
            "services.policy.main",
            "services.risk.engine",
            "services.oms.shadow_oms",
        ]:
            sys.modules.pop(name, None)
        sys.modules.update(removed_modules)


def _auth_headers(store: InMemorySessionStore, account_id: str) -> dict[str, str]:
    session = store.create(account_id)
    token = session.token
    return {
        "Authorization": f"Bearer {token}",
        "X-Account-ID": account_id,
    }


def test_replay_run_requires_authentication(replay_client: Tuple[TestClient, InMemorySessionStore, object]) -> None:
    client, _store, _ = replay_client

    body = {
        "from": datetime.now(timezone.utc).isoformat(),
        "to": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "account_id": "company",
    }

    response = client.post("/replay/run", json=body)
    assert response.status_code == 401


def test_replay_run_rejects_account_mismatch(
    replay_client: Tuple[TestClient, InMemorySessionStore, object]
) -> None:
    client, store, _ = replay_client

    body = {
        "from": datetime.now(timezone.utc).isoformat(),
        "to": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "account_id": "shadow-account",
    }

    headers = _auth_headers(store, "company")

    response = client.post("/replay/run", json=body, headers=headers)
    assert response.status_code == 403


def test_replay_run_allows_admin_session(
    replay_client: Tuple[TestClient, InMemorySessionStore, object]
) -> None:
    client, store, _ = replay_client

    body = {
        "from": datetime.now(timezone.utc).isoformat(),
        "to": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "account_id": "company",
    }

    headers = _auth_headers(store, "company")

    response = client.post("/replay/run", json=body, headers=headers)
    assert response.status_code == 200

    payload = response.json()
    assert payload["run_id"] == "test-run"


def test_replay_event_normalizes_spot_symbol(
    replay_client: Tuple[TestClient, InMemorySessionStore, object]
) -> None:
    _client, _store, snapshot_replay = replay_client
    now = datetime.now(timezone.utc)
    event = snapshot_replay.ReplayEvent(
        timestamp=now,
        order_id="evt-1",
        instrument="eth_usd",
        side="buy",
        quantity=1.0,
        price=1500.0,
        fee=snapshot_replay.FeeBreakdown(currency="USD", maker=0.1, taker=0.2),
        features=[0.1, 0.2],
        book_snapshot=snapshot_replay.BookSnapshot(
            mid_price=1500.0, spread_bps=10.0, imbalance=0.0
        ),
        state=snapshot_replay.PolicyState(
            regime="neutral", volatility=0.2, liquidity_score=0.5, conviction=0.6
        ),
        confidence=None,
        expected_edge_bps=None,
        actual_mid_price=1501.0,
        actual_volume=10.0,
    )

    assert event.instrument == "ETH-USD"


def test_replay_event_rejects_non_spot_symbol(
    replay_client: Tuple[TestClient, InMemorySessionStore, object]
) -> None:
    _client, _store, snapshot_replay = replay_client
    now = datetime.now(timezone.utc)

    with pytest.raises(ValueError):
        snapshot_replay.ReplayEvent(
            timestamp=now,
            order_id="evt-2",
            instrument="ETH-PERP",
            side="buy",
            quantity=1.0,
            price=1500.0,
            fee=snapshot_replay.FeeBreakdown(currency="USD", maker=0.1, taker=0.2),
            features=[0.1],
            book_snapshot=snapshot_replay.BookSnapshot(
                mid_price=1500.0, spread_bps=10.0, imbalance=0.0
            ),
            state=snapshot_replay.PolicyState(
                regime="neutral", volatility=0.2, liquidity_score=0.5, conviction=0.6
            ),
            confidence=None,
            expected_edge_bps=None,
            actual_mid_price=1501.0,
            actual_volume=10.0,
        )


def test_snapshot_loader_skips_non_spot_payload(
    replay_client: Tuple[TestClient, InMemorySessionStore, object]
) -> None:
    _client, _store, snapshot_replay = replay_client
    start = datetime.now(timezone.utc) - timedelta(minutes=1)
    end = datetime.now(timezone.utc) + timedelta(minutes=1)
    config = snapshot_replay.SnapshotReplayConfig(
        start=start,
        end=end,
        account_id="company",
    )
    loader = snapshot_replay.HistoricalDataLoader(config)

    payload_spot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "instrument": "btc_usd",
        "price": 100.0,
    }
    event = loader._event_from_payload(payload_spot)
    assert event is not None
    assert event.instrument == "BTC-USD"

    payload_derivative = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "instrument": "BTC-PERP",
        "price": 100.0,
    }
    assert loader._event_from_payload(payload_derivative) is None
