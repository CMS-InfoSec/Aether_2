from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, Mapping

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.correlation import CorrelationContext

from accounts.service import AccountsRepository, AccountsService, AdminProfile, Base


class _InMemoryAuditBackend:
    def __init__(self) -> None:
        self._records: list[Dict[str, Any]] = []

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        self._records.append(dict(record))

    def audit_logs(self) -> Iterable[Mapping[str, Any]]:
        return list(self._records)


def _build_store() -> AuditLogStore:
    return AuditLogStore(backend=_InMemoryAuditBackend())


def _build_service(recorder: SensitiveActionRecorder, db_url: str) -> tuple[AccountsService, sessionmaker]:
    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    repository = AccountsRepository(session_factory)
    return AccountsService(recorder, repository=repository), session_factory


def test_dual_approval_flow_requires_two_distinct_admins(tmp_path):
    store = _build_store()
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)
    db_url = f"sqlite:///{tmp_path/'accounts.db'}"
    service, _ = _build_service(recorder, db_url)

    change = service.request_risk_configuration_change(
        "admin-a", {"max_drawdown": 0.5}
    )
    result = service.approve_risk_change("admin-a", change.request_id)
    assert not result.executed

    with CorrelationContext("corr-123"):
        executed_change = service.approve_risk_change("admin-b", change.request_id)

    assert executed_change.executed
    entries = list(store.all())
    actions = [entry.action for entry in entries]
    assert "risk_change_requested" in actions
    assert "risk_change_executed" in actions

    executed_entry = next(entry for entry in entries if entry.action == "risk_change_executed")
    assert executed_entry.after["approvals"] == executed_change.approvals
    assert executed_entry.correlation_id == "corr-123"


def test_profiles_and_pending_approvals_survive_restart(tmp_path):
    store = _build_store()
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)
    db_url = f"sqlite:///{tmp_path/'accounts_persist.db'}"

    service_a, _ = _build_service(recorder, db_url)
    profile = AdminProfile(admin_id="admin-a", email="admin@example.com", display_name="Admin A")
    service_a.upsert_profile(profile)
    updated_profile = service_a.set_kraken_credentials_status("admin-a", True)
    assert updated_profile.kraken_credentials_linked is True

    change = service_a.request_risk_configuration_change("admin-a", {"leverage": 3})
    service_a.approve_risk_change("admin-a", change.request_id)

    service_b, _ = _build_service(recorder, db_url)
    persisted_profile = service_b.get_profile("admin-a")
    assert persisted_profile is not None
    assert persisted_profile.kraken_credentials_linked is True

    finalized = service_b.approve_risk_change("admin-b", change.request_id)
    assert finalized.executed
    assert finalized.approvals.count("admin-a") == 1
    assert finalized.approvals.count("admin-b") == 1


def test_concurrent_approvals_enforce_two_person_rule(tmp_path):
    store = _build_store()
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)
    db_url = f"sqlite:///{tmp_path/'accounts_concurrency.db'}"
    service, _ = _build_service(recorder, db_url)

    change = service.request_risk_configuration_change("admin-a", {"exposure_limit": 1000000})

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(service.approve_risk_change, "admin-a", change.request_id),
            pool.submit(service.approve_risk_change, "admin-a", change.request_id),
        ]

    successes = 0
    duplicates = 0
    for future in futures:
        try:
            future.result()
            successes += 1
        except PermissionError as exc:
            assert str(exc) == "duplicate_approval"
            duplicates += 1

    assert successes == 1
    assert duplicates == 1

    finalized = service.approve_risk_change("admin-b", change.request_id)
    assert finalized.executed
    assert finalized.approvals.count("admin-a") == 1
    assert finalized.approvals.count("admin-b") == 1
