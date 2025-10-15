import importlib
import sys
from types import ModuleType

import pytest

import shared.common_bootstrap as bootstrap

MODULE_ATTRS = (
    ("auth.session_client", "httpx"),
    ("auth_service", "httpx"),
    ("services.fees.fee_optimizer", "httpx"),
    ("services.oms.reconcile", "httpx"),
    ("services.reports.kraken_reconciliation", "httpx"),
    ("sentiment_ingest", "httpx"),
)


@pytest.fixture()
def simulate_missing_httpx(monkeypatch):
    """Pretend the external httpx dependency is unavailable."""

    monkeypatch.setitem(sys.modules, "httpx", None)
    real_import_module = bootstrap.importlib.import_module

    def fake_import_module(name: str, package: str | None = None):
        if name == "httpx":
            existing = sys.modules.get("httpx")
            if isinstance(existing, ModuleType):
                return existing
            raise ModuleNotFoundError("httpx intentionally unavailable for test")
        return real_import_module(name, package)

    monkeypatch.setattr(bootstrap.importlib, "import_module", fake_import_module)
    yield


def _reload(module_name: str) -> ModuleType:
    for key in list(sys.modules.keys()):
        if key == module_name or key.startswith(f"{module_name}."):
            sys.modules.pop(key)
    return importlib.import_module(module_name)


@pytest.mark.parametrize("module_name, attr", MODULE_ATTRS)
def test_modules_install_httpx_stub(simulate_missing_httpx, module_name: str, attr: str):
    module = _reload(module_name)
    httpx_module = getattr(module, attr)
    assert getattr(httpx_module, "__file__") == "<httpx-stub>"
    for required in ("Client", "AsyncClient", "get", "post"):
        assert hasattr(httpx_module, required)


def test_alert_dedupe_uses_httpx_stub(simulate_missing_httpx):
    module = _reload("services.alerts.alert_dedupe")
    service = module.AlertDedupeService(alertmanager_url="http://alertmanager")
    httpx_module = service._ensure_httpx()
    assert getattr(httpx_module, "__file__") == "<httpx-stub>"
    assert hasattr(httpx_module, "AsyncClient")


def test_httpx_queryparams_stub_round_trip(simulate_missing_httpx):
    httpx_module = bootstrap.ensure_httpx_ready()
    params = httpx_module.QueryParams({"foo": "bar", "multi": ["a", "b"]})
    assert str(params) == "foo=bar&multi=a&multi=b"
    assert list(params.items()) == [("foo", "bar"), ("multi", "a")]
    cloned = httpx_module.QueryParams(params)
    assert str(cloned) == "foo=bar&multi=a&multi=b"
    assert cloned == params


def test_httpx_queryparams_stub_mapping_behaviour(simulate_missing_httpx):
    httpx_module = bootstrap.ensure_httpx_ready()
    params = httpx_module.QueryParams([("multi", "a"), ("multi", "b"), ("foo", "bar")])

    assert params

    assert list(params) == ["multi", "foo"]
    assert len(params) == 2
    assert "multi" in params
    assert params["multi"] == "a"
    assert params.get("missing") is None
    assert params.get("missing", "default") == "default"
    assert params.get_list("multi") == ["a", "b"]
    assert params.items() == [("multi", "a"), ("foo", "bar")]
    assert params.multi_items() == [("multi", "a"), ("multi", "b"), ("foo", "bar")]
    assert repr(params) == "QueryParams('multi=a&multi=b&foo=bar')"

    clone = httpx_module.QueryParams(params)
    assert clone == params

    reordered = httpx_module.QueryParams([("foo", "bar"), ("multi", "b"), ("multi", "a")])
    assert reordered == params

    assert params != "multi=a&multi=b&foo=bar"
    assert params != {"multi": ["a", "b"], "foo": "bar"}

    empty = httpx_module.QueryParams({"empty": None})
    assert str(empty) == "empty="
    assert empty.items() == [("empty", "")]
    assert empty.get_list("empty") == [""]
    assert empty

    blank = httpx_module.QueryParams()
    assert not blank


def test_httpx_queryparams_stub_hashable(simulate_missing_httpx):
    httpx_module = bootstrap.ensure_httpx_ready()
    params = httpx_module.QueryParams("multi=a&multi=b&foo=bar")

    hashed = hash(params)
    assert isinstance(hashed, int)
    assert hash(httpx_module.QueryParams(str(params))) == hashed

    mapping = {params: "value"}
    assert mapping[params] == "value"
