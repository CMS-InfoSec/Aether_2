import pytest

from types import SimpleNamespace
from typing import Mapping, Sequence

from services.policy import model_server


class _StubPyFuncModel:
    def __init__(self, signature):
        self.metadata = SimpleNamespace(signature=signature)
        self.calls = []

    def predict(self, frame, params=None):
        if isinstance(frame, list):
            captured = [dict(row) for row in frame]
        elif hasattr(frame, "to_dict"):
            try:
                captured = frame.to_dict(orient="records")  # pandas DataFrame
            except TypeError:
                captured = frame.to_dict()
            if isinstance(captured, Mapping):
                captured = [captured]
        else:
            captured = [dict(frame)]
        self.calls.append((captured, params))
        return [
            {
                "edge_bps": 25.0,
                "take_profit_bps": 30.0,
                "stop_loss_bps": 10.0,
                "selected_action": "maker",
                "action_templates": [
                    {
                        "name": "maker",
                        "venue_type": "maker",
                        "edge_bps": 25.0,
                        "fee_bps": 0.1,
                        "confidence": 0.9,
                    },
                    {
                        "name": "taker",
                        "venue_type": "taker",
                        "edge_bps": 20.0,
                        "fee_bps": 0.2,
                        "confidence": 0.85,
                    },
                ],
                "confidence": {
                    "model_confidence": 0.9,
                    "state_confidence": 0.85,
                    "execution_confidence": 0.95,
                },
                "approved": True,
                "metadata": {"run_id": "abc123"},
            }
        ]


class _RegistryStub:
    def __init__(self, model):
        self._model = model
        self.calls = []

    def load(self, name: str):
        self.calls.append(name)
        if isinstance(self._model, Exception):
            raise self._model
        return self._model


class _StubType:
    def __init__(self, value: str):
        self._value = value

    def to_string(self) -> str:
        return self._value


class _StubField:
    def __init__(self, name: str, dtype: str):
        self.name = name
        self.type = _StubType(dtype)


class _StubSchema(list):
    def to_dict(self):
        return {
            "fields": [
                {"name": field.name, "type": field.type.to_string()}
                for field in self
            ]
        }


class _StubSignature:
    def __init__(self, fields: Sequence[_StubField]):
        self.inputs = _StubSchema(fields)


@pytest.fixture
def signature():
    return _StubSignature(
        [
            _StubField("alpha", "double"),
            _StubField("beta", "double"),
        ]
    )


def _cached_model(signature):
    stub_model = _StubPyFuncModel(signature)
    cached = model_server.CachedPolicyModel.from_pyfunc(
        "policy-intent::acct::btc-usd",
        "7",
        stub_model,
        signature,
    )
    return cached, stub_model


def test_predict_intent_success(monkeypatch, signature):
    cached_model, stub_model = _cached_model(signature)
    registry = _RegistryStub(cached_model)
    monkeypatch.setattr(model_server, "_MODEL_REGISTRY", registry)

    intent = model_server.predict_intent(
        account_id="ACCT",
        symbol="BTC-USD",
        features=[0.1, 0.2],
        book_snapshot={"mid_price": 10_000.0, "spread_bps": 5.0, "imbalance": 0.1},
    )

    assert intent.approved is True
    assert intent.metadata is not None
    assert intent.metadata["model_version"] == "7"
    assert intent.metadata["feature_signature"] == cached_model.signature_hash
    assert intent.metadata["run_id"] == "abc123"

    assert registry.calls == [
        model_server._model_name("ACCT", "BTC-USD")
    ]

    assert stub_model.calls, "Model was not invoked"
    frame, params = stub_model.calls[0]
    assert list(frame[0].keys()) == ["alpha", "beta"]
    assert params["book_snapshot"]["mid_price"] == 10_000.0


def test_predict_intent_missing_model(monkeypatch):
    registry = _RegistryStub(model_server.ModelResolutionError("no production model"))
    monkeypatch.setattr(model_server, "_MODEL_REGISTRY", registry)

    intent = model_server.predict_intent(
        account_id="ACCT",
        symbol="BTC-USD",
        features=[0.1, 0.2],
        book_snapshot={"mid_price": 10_000.0, "spread_bps": 5.0, "imbalance": 0.1},
    )

    assert intent.is_null
    assert intent.reason == "model_unavailable"
    assert intent.metadata is not None
    assert "error" in intent.metadata
    assert registry.calls == [
        model_server._model_name("ACCT", "BTC-USD")
    ]


def test_predict_intent_schema_mismatch(monkeypatch, signature):
    cached_model, stub_model = _cached_model(signature)
    registry = _RegistryStub(cached_model)
    monkeypatch.setattr(model_server, "_MODEL_REGISTRY", registry)

    intent = model_server.predict_intent(
        account_id="ACCT",
        symbol="BTC-USD",
        features=[0.1],
        book_snapshot={"mid_price": 10_000.0, "spread_bps": 5.0, "imbalance": 0.1},
    )

    assert intent.is_null
    assert intent.reason == "feature_validation_error"
    assert intent.metadata is not None
    assert intent.metadata["model_version"] == "7"
    assert "error" in intent.metadata
    assert not stub_model.calls
