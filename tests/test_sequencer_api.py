from __future__ import annotations

from importlib import import_module, util
from pathlib import Path
import sys
import types
from types import ModuleType

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")

    def _noop(*args: object, **kwargs: object) -> None:  # pragma: no cover - stub
        return None

    metrics_stub.increment_rejected_intents = _noop
    metrics_stub.increment_trades_submitted = _noop
    metrics_stub.observe_oms_submit_latency = _noop
    metrics_stub.observe_policy_inference_latency = _noop
    metrics_stub.observe_risk_validation_latency = _noop
    metrics_stub.set_pipeline_latency = _noop
    metrics_stub.setup_metrics = _noop

    class _Span(types.SimpleNamespace):
        async def __aenter__(self) -> "_Span":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        def __enter__(self) -> "_Span":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def _span_factory(*args: object, **kwargs: object) -> _Span:
        return _Span()

    metrics_stub.traced_span = _span_factory

    sys.modules["metrics"] = metrics_stub

if "services.common.adapters" not in sys.modules:
    adapters_stub = types.ModuleType("services.common.adapters")

    class KafkaNATSAdapter:  # pragma: no cover - stub for tests
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def publish(self, topic: str, payload: object) -> None:
            return None

    adapters_stub.KafkaNATSAdapter = KafkaNATSAdapter
    sys.modules["services.common.adapters"] = adapters_stub

if "services" not in sys.modules:
    services_spec = util.spec_from_file_location("services", ROOT / "services" / "__init__.py")
    if services_spec and services_spec.loader:
        services_module = util.module_from_spec(services_spec)
        sys.modules["services"] = services_module
        services_spec.loader.exec_module(services_module)

if "services.common" not in sys.modules:
    common_spec = util.spec_from_file_location("services.common", ROOT / "services" / "common" / "__init__.py")
    if common_spec and common_spec.loader:
        common_module = util.module_from_spec(common_spec)
        sys.modules["services.common"] = common_module
        common_spec.loader.exec_module(common_module)

if "services.common.security" not in sys.modules:
    security_spec = util.spec_from_file_location(
        "services.common.security", ROOT / "services" / "common" / "security.py"
    )
    if security_spec and security_spec.loader:
        security_module = util.module_from_spec(security_spec)
        sys.modules["services.common.security"] = security_module
        security_spec.loader.exec_module(security_module)

from auth.service import InMemorySessionStore
from services.common.security import set_default_session_store


@pytest.fixture(scope="module")
def sequencer_module() -> ModuleType:
    return import_module("sequencer")


@pytest.fixture
def sequencer_client(
    sequencer_module: ModuleType,
) -> tuple[TestClient, InMemorySessionStore, ModuleType]:
    store = InMemorySessionStore()
    set_default_session_store(store)
    client = TestClient(sequencer_module.app)
    try:
        yield client, store, sequencer_module
    finally:
        client.app.dependency_overrides.clear()
        set_default_session_store(None)


@pytest.fixture
def stubbed_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    sequencer_module: ModuleType,
) -> dict[str, dict[str, object]]:
    calls: dict[str, dict[str, object]] = {}

    async def _submit(intent: dict[str, object]) -> sequencer_module.PipelineResult:
        calls["intent"] = intent
        return sequencer_module.PipelineResult(
            run_id="run-1",
            status="success",
            latency_ms=1.0,
            stage_latencies_ms={},
            stage_artifacts={},
            fill_event={},
        )

    monkeypatch.setattr(sequencer_module.pipeline, "submit", _submit)
    return calls


def test_submit_intent_requires_authentication(
    sequencer_client: tuple[TestClient, InMemorySessionStore, ModuleType]
) -> None:
    client, _, _ = sequencer_client
    payload = {"intent": {"account_id": "company"}}
    response = client.post("/sequencer/submit_intent", json=payload)
    assert response.status_code == 401


def test_status_requires_authentication(
    sequencer_client: tuple[TestClient, InMemorySessionStore, ModuleType]
) -> None:
    client, _, _ = sequencer_client
    response = client.get("/sequencer/status")
    assert response.status_code == 401


def test_submit_intent_rejects_mismatched_account(
    sequencer_client: tuple[TestClient, InMemorySessionStore, ModuleType],
    stubbed_pipeline: dict[str, dict[str, object]],
) -> None:
    client, store, _ = sequencer_client
    session = store.create("company")
    payload = {"intent": {"account_id": "ops-admin", "order_id": "ord-1"}}
    headers = {"Authorization": f"Bearer {session.token}"}

    response = client.post("/sequencer/submit_intent", json=payload, headers=headers)

    assert response.status_code == 403
    assert "intent" not in stubbed_pipeline


def test_submit_intent_accepts_matching_account(
    sequencer_client: tuple[TestClient, InMemorySessionStore, ModuleType],
    stubbed_pipeline: dict[str, dict[str, object]],
) -> None:
    client, store, _ = sequencer_client
    session = store.create("company")
    payload = {"intent": {"account_id": "company", "order_id": "ord-2"}}
    headers = {"Authorization": f"Bearer {session.token}"}

    response = client.post("/sequencer/submit_intent", json=payload, headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert stubbed_pipeline["intent"] == payload["intent"]
