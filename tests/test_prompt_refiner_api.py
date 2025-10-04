from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "services.common.config" not in sys.modules:
    import types

    services_module = sys.modules.setdefault("services", types.ModuleType("services"))
    common_module = sys.modules.setdefault(
        "services.common", types.ModuleType("services.common")
    )
    config_module = types.ModuleType("services.common.config")

    class _TimescaleSession:
        def __init__(self, account_id: str) -> None:
            self.dsn = f"postgresql:///{account_id}"
            self.account_schema = f"acct_{account_id}"

    def _get_timescale_session(account_id: str) -> _TimescaleSession:
        return _TimescaleSession(account_id)

    config_module.get_timescale_session = _get_timescale_session  # type: ignore[attr-defined]
    sys.modules["services.common.config"] = config_module
    common_module.config = config_module  # type: ignore[attr-defined]
    services_module.common = common_module  # type: ignore[attr-defined]

    security_module = types.ModuleType("services.common.security")

    def _require_admin_account(*args: object, **kwargs: object) -> str:  # pragma: no cover
        raise RuntimeError("require_admin_account stub should be overridden in tests")

    security_module.require_admin_account = _require_admin_account  # type: ignore[attr-defined]
    sys.modules["services.common.security"] = security_module
    common_module.security = security_module  # type: ignore[attr-defined]

import prompt_refiner
from prompt_refiner import PerformanceMetrics, PromptRecord
from tests.helpers.authentication import override_admin_auth


@pytest.fixture(autouse=True)
def _set_service_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompt_refiner, "ACCOUNT_ID", "default")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(prompt_refiner, "_ensure_tables", lambda: None)
    monkeypatch.setattr(prompt_refiner.refiner, "start", lambda: None)

    async def _stop() -> None:
        return None

    monkeypatch.setattr(prompt_refiner.refiner, "stop", _stop)

    with TestClient(prompt_refiner.app) as test_client:
        yield test_client


def test_get_latest_prompt_requires_authentication(client: TestClient) -> None:
    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ):
        response = client.get("/ml/prompts/latest")

    assert response.status_code == 401


def test_get_latest_prompt_rejects_account_mismatch(
    client: TestClient,
) -> None:
    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ) as headers:
        response = client.get(
            "/ml/prompts/latest",
            headers=headers,
            params={"account_id": "other"},
        )

    assert response.status_code == 403


def test_get_latest_prompt_allows_authorized_account(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = PromptRecord(
        run_id="run-123",
        prompt_text="prompt-body",
        created_at=dt.datetime.now(dt.timezone.utc),
    )
    monkeypatch.setattr(prompt_refiner.refiner, "latest_prompt", lambda: record)

    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ) as headers:
        response = client.get(
            "/ml/prompts/latest",
            headers={**headers, "X-Account-ID": "default"},
            params={"account_id": "default"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == record.run_id
    assert body["prompt_text"] == record.prompt_text


def test_post_prompt_test_requires_authentication(client: TestClient) -> None:
    payload = {"prompt_text": "candidate"}
    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ):
        response = client.post("/ml/prompts/test", json=payload)

    assert response.status_code == 401


def test_post_prompt_test_rejects_account_mismatch(client: TestClient) -> None:
    payload = {"prompt_text": "candidate"}
    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ) as headers:
        response = client.post(
            "/ml/prompts/test",
            json=payload,
            headers=headers,
            params={"account_id": "other"},
        )

    assert response.status_code == 403


def test_post_prompt_test_accepts_authorized_request(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored = PromptRecord(run_id="run-456", prompt_text="candidate", created_at=now)

    def _record_prompt(run_id: str, prompt_text: str) -> PromptRecord:
        assert run_id == "provided-run"
        assert prompt_text == "candidate"
        return stored

    def _evaluate_metrics(
        metrics: PerformanceMetrics | None, update_baseline: bool
    ) -> tuple[PerformanceMetrics, list[str]]:
        assert update_baseline is False
        return (
            PerformanceMetrics(
                sharpe=1.1,
                win_rate=0.6,
                drift=0.1,
                collected_at=now,
            ),
            [],
        )

    monkeypatch.setattr(prompt_refiner.refiner, "record_prompt", _record_prompt)
    monkeypatch.setattr(prompt_refiner.refiner, "evaluate_metrics", _evaluate_metrics)

    payload = {"prompt_text": "candidate", "run_id": "provided-run"}
    with override_admin_auth(
        client.app, prompt_refiner.require_admin_account, "default"
    ) as headers:
        response = client.post(
            "/ml/prompts/test",
            json=payload,
            headers={**headers, "X-Account-ID": "default"},
            params={"account_id": "default"},
        )

    assert response.status_code == 201
    body = response.json()
    assert body["run_id"] == payload["run_id"]
    assert body["stored_prompt"]["prompt_text"] == stored.prompt_text
    assert body["generated_prompts"] == []
