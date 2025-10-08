"""Lightweight Prometheus client compatibility layer."""

from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Tuple
import sys

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"

__all__ = [
    "CONTENT_TYPE_LATEST",
    "CollectorRegistry",
    "Counter",
    "Gauge",
    "Histogram",
    "REGISTRY",
    "Summary",
    "generate_latest",
    "start_http_server",
]


@dataclass(frozen=True)
class Sample:
    """Representation of a Prometheus sample."""

    name: str
    labels: Dict[str, str]
    value: float


class CollectorRegistry:
    """Register and query metric collectors."""

    def __init__(self) -> None:
        self._collectors: List[_MetricBase] = []

    def register(self, metric: "_MetricBase") -> None:
        if metric not in self._collectors:
            self._collectors.append(metric)

    def collect(self) -> List["_MetricBase"]:
        return list(self._collectors)

    def get_sample_value(
        self, name: str, labels: Mapping[str, str] | None = None
    ) -> float | None:
        label_map = {k: str(v) for k, v in (labels or {}).items()}
        for collector in self._collectors:
            value = collector.get_sample_value(name, label_map)
            if value is not None:
                return value
        return None


class _MetricBase:
    """Shared scaffolding for metric helpers."""

    _type: str = "untyped"
    _child_class: type["_MetricChildBase"]

    def __init__(
        self,
        name: str,
        documentation: str = "",
        labelnames: Iterable[str] | None = None,
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.name = name
        self.documentation = documentation
        self.labelnames = tuple(labelnames or ())
        self._children: MutableMapping[Tuple[str, ...], _MetricChildBase] = {}
        self._registry = registry or REGISTRY
        self._registry.register(self)
        if not self.labelnames:
            self._get_child(())

    def labels(self, *labelvalues: str, **labels: str) -> "_MetricChildBase":
        if labelvalues and labels:
            raise TypeError("pass label values positionally or via keywords, not both")
        if labelvalues:
            if len(labelvalues) != len(self.labelnames):
                raise ValueError("incorrect number of label values supplied")
            values = tuple(str(value) for value in labelvalues)
        else:
            values = tuple(str(labels.get(name, "")) for name in self.labelnames)
        return self._get_child(values)

    def _get_child(self, labelvalues: Tuple[str, ...]) -> "_MetricChildBase":
        child = self._children.get(labelvalues)
        if child is None:
            child = self._child_class(self, labelvalues)
            self._children[labelvalues] = child
        return child

    def _unlabeled_child(self) -> "_MetricChildBase":
        if self.labelnames:
            raise ValueError("Metric with labels requires .labels(...) before recording values")
        return self._get_child(())

    def collect(self) -> Iterator[Sample]:
        for child in self._children.values():
            yield from child.samples()

    def get_sample_value(self, name: str, labels: Mapping[str, str]) -> float | None:
        for child in self._children.values():
            value = child.get_sample_value(name, labels)
            if value is not None:
                return value
        return None


class _MetricChildBase:
    def __init__(self, parent: _MetricBase, labelvalues: Tuple[str, ...]) -> None:
        self._parent = parent
        self._labelvalues = labelvalues

    def _label_dict(self) -> Dict[str, str]:
        return {
            name: value
            for name, value in zip(self._parent.labelnames, self._labelvalues)
        }

    def samples(self) -> Iterator[Sample]:  # pragma: no cover - interface contract
        raise NotImplementedError

    def get_sample_value(
        self, name: str, labels: Mapping[str, str]
    ) -> float | None:  # pragma: no cover - interface contract
        raise NotImplementedError


class _CounterChild(_MetricChildBase):
    def __init__(self, parent: _MetricBase, labelvalues: Tuple[str, ...]) -> None:
        super().__init__(parent, labelvalues)
        self._value = 0.0

    def inc(self, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("Counter cannot be decremented")
        self._value += float(amount)

    def samples(self) -> Iterator[Sample]:
        yield Sample(self._parent.name, self._label_dict(), self._value)

    def get_sample_value(self, name: str, labels: Mapping[str, str]) -> float | None:
        if name != self._parent.name:
            return None
        if labels and labels != self._label_dict():
            return None
        return self._value


class Counter(_MetricBase):
    _type = "counter"
    _child_class = _CounterChild

    def inc(self, amount: float = 1.0) -> None:
        self._unlabeled_child().inc(amount)

    def labels(self, *labelvalues: str, **labels: str) -> _CounterChild:  # type: ignore[override]
        return super().labels(*labelvalues, **labels)  # type: ignore[return-value]


class _GaugeChild(_MetricChildBase):
    def __init__(self, parent: _MetricBase, labelvalues: Tuple[str, ...]) -> None:
        super().__init__(parent, labelvalues)
        self._value = 0.0

    def set(self, value: float) -> None:
        self._value = float(value)

    def inc(self, amount: float = 1.0) -> None:
        self._value += float(amount)

    def dec(self, amount: float = 1.0) -> None:
        self._value -= float(amount)

    def samples(self) -> Iterator[Sample]:
        yield Sample(self._parent.name, self._label_dict(), self._value)

    def get_sample_value(self, name: str, labels: Mapping[str, str]) -> float | None:
        if name != self._parent.name:
            return None
        if labels and labels != self._label_dict():
            return None
        return self._value


class Gauge(_MetricBase):
    _type = "gauge"
    _child_class = _GaugeChild

    def set(self, value: float) -> None:
        self._unlabeled_child().set(value)

    def inc(self, amount: float = 1.0) -> None:
        self._unlabeled_child().inc(amount)

    def dec(self, amount: float = 1.0) -> None:
        self._unlabeled_child().dec(amount)

    def labels(self, *labelvalues: str, **labels: str) -> _GaugeChild:  # type: ignore[override]
        return super().labels(*labelvalues, **labels)  # type: ignore[return-value]


_DEFAULT_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    float("inf"),
)


class _HistogramChild(_MetricChildBase):
    def __init__(self, parent: "Histogram", labelvalues: Tuple[str, ...]) -> None:
        super().__init__(parent, labelvalues)
        self._sum = 0.0
        self._count = 0.0
        self._bucket_counts: Dict[float, float] = {bound: 0.0 for bound in parent._buckets}

    def observe(self, value: float) -> None:
        numeric = float(value)
        self._sum += numeric
        self._count += 1.0
        for bound in self._bucket_counts:
            if numeric <= bound:
                self._bucket_counts[bound] += 1.0

    def samples(self) -> Iterator[Sample]:
        label_dict = self._label_dict()
        for bound, count in self._bucket_counts.items():
            bucket_labels = dict(label_dict)
            bucket_labels["le"] = "+Inf" if bound == float("inf") else str(bound)
            yield Sample(f"{self._parent.name}_bucket", bucket_labels, count)
        yield Sample(f"{self._parent.name}_count", label_dict, self._count)
        yield Sample(f"{self._parent.name}_sum", label_dict, self._sum)

    def get_sample_value(self, name: str, labels: Mapping[str, str]) -> float | None:
        label_dict = self._label_dict()
        if name == f"{self._parent.name}_sum":
            if labels and labels != label_dict:
                return None
            return self._sum
        if name == f"{self._parent.name}_count":
            if labels and labels != label_dict:
                return None
            return self._count
        if name == f"{self._parent.name}_bucket":
            expected = dict(label_dict)
            bound = labels.get("le", "")
            if bound in ("", None):
                return None
            expected["le"] = bound
            if labels != expected:
                return None
            if bound == "+Inf":
                bucket_bound = float("inf")
            else:
                try:
                    bucket_bound = float(bound)
                except ValueError:
                    return None
            return self._bucket_counts.get(bucket_bound)
        if name == self._parent.name:
            if labels and labels != label_dict:
                return None
            return self._count
        return None


class Histogram(_MetricBase):
    _type = "histogram"
    _child_class = _HistogramChild

    def __init__(
        self,
        name: str,
        documentation: str = "",
        labelnames: Iterable[str] | None = None,
        registry: CollectorRegistry | None = None,
        buckets: Iterable[float] | None = None,
    ) -> None:
        provided = tuple(float(bound) for bound in (buckets or _DEFAULT_BUCKETS))
        if not provided:
            raise ValueError("buckets cannot be empty")
        if provided[-1] != float("inf"):
            provided = provided + (float("inf"),)
        self._buckets = provided
        super().__init__(name, documentation, labelnames, registry)

    def observe(self, value: float) -> None:
        self._unlabeled_child().observe(value)

    def labels(self, *labelvalues: str, **labels: str) -> _HistogramChild:  # type: ignore[override]
        return super().labels(*labelvalues, **labels)  # type: ignore[return-value]


class Summary(Gauge):
    _type = "summary"


REGISTRY = CollectorRegistry()


def generate_latest(registry: CollectorRegistry | None = None) -> bytes:
    registry = registry or REGISTRY
    lines: List[str] = []
    for metric in registry.collect():
        lines.append(f"# HELP {metric.name} {metric.documentation}")
        lines.append(f"# TYPE {metric.name} {metric._type}")
        for sample in metric.collect():
            if sample.labels:
                label_str = ",".join(
                    f"{key}=\"{value}\"" for key, value in sorted(sample.labels.items())
                )
                lines.append(f"{sample.name}{{{label_str}}} {sample.value}")
            else:
                lines.append(f"{sample.name} {sample.value}")
    return "\n".join(lines).encode("utf-8")


def start_http_server(_port: int, addr: str = "0.0.0.0") -> None:  # pragma: no cover
    del _port, addr
    return None


_metrics_module = ModuleType("prometheus_client.metrics")
_metrics_module.Counter = Counter
_metrics_module.Gauge = Gauge
_metrics_module.Histogram = Histogram
_metrics_module.Summary = Summary
_metrics_module.generate_latest = generate_latest
_metrics_module.CONTENT_TYPE_LATEST = CONTENT_TYPE_LATEST
_metrics_module.REGISTRY = REGISTRY
_metrics_module.CollectorRegistry = CollectorRegistry

_registry_module = ModuleType("prometheus_client.registry")
_registry_module.CollectorRegistry = CollectorRegistry
_registry_module.REGISTRY = REGISTRY

sys.modules.setdefault("prometheus_client.metrics", _metrics_module)
sys.modules.setdefault("prometheus_client.registry", _registry_module)
