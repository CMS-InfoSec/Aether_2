from __future__ import annotations

import sys
from decimal import Decimal

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("services.common.security")

from tests.helpers.risk import reload_risk_service, risk_service_instance


def _usage_payload() -> dict[str, Decimal]:
    return {
        "net_asset_value": Decimal("1250000.42"),
        "realized_daily_loss": Decimal("4321.10"),
        "fees_paid": Decimal("210.55"),
        "var_95": Decimal("15500.0"),
        "var_99": Decimal("25500.0"),
    }


def test_risk_state_survives_restart_and_replica_visibility(
    tmp_path, monkeypatch
) -> None:
    db_name = "persistence.db"
    account_id = "company"

    with risk_service_instance(tmp_path, monkeypatch, db_filename=db_name) as module_a:
        module_a.set_stub_account_usage(account_id, _usage_payload())
        usage_a = module_a._load_account_usage(account_id)
        assert Decimal(str(usage_a.net_asset_value)) == _usage_payload()["net_asset_value"]

        module_b = reload_risk_service(tmp_path, monkeypatch, db_filename=db_name)
        try:
            usage_b = module_b._load_account_usage(account_id)
            assert Decimal(str(usage_b.net_asset_value)) == Decimal(
                str(usage_a.net_asset_value)
            )
        finally:
            module_b.ENGINE.dispose()
            sys.modules.pop("risk_service", None)

    replica = reload_risk_service(tmp_path, monkeypatch, db_filename=db_name)
    try:
        usage_after_restart = replica._load_account_usage(account_id)
        assert Decimal(str(usage_after_restart.net_asset_value)) == Decimal(
            str(usage_a.net_asset_value)
        )
    finally:
        replica.ENGINE.dispose()
        sys.modules.pop("risk_service", None)
