"""Drift monitoring utilities calculating PSI and KS statistics."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple, TYPE_CHECKING, Any, Sequence

try:  # pragma: no cover - optional scientific stack dependency
    import numpy as np
except Exception:  # pragma: no cover - executed when numpy is unavailable
    np = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in lightweight environments
    import pandas as pd
except Exception:  # pragma: no cover - executed when pandas is unavailable
    pd = None  # type: ignore[assignment]

try:  # pragma: no cover - SciPy is optional for drift calculations
    from scipy.stats import ks_2samp
except Exception:  # pragma: no cover - executed when scipy is unavailable
    ks_2samp = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    import numpy
    import pandas

LOGGER = logging.getLogger(__name__)


class MissingDependencyError(RuntimeError):
    """Raised when drift calculations require optional scientific packages."""


def _require_numpy() -> "numpy":
    if np is None:
        raise MissingDependencyError(
            "numpy is required for drift calculations. Install numpy to enable PSI and KS metrics."
        )
    return np


def _require_pandas() -> "pandas":
    if pd is None:
        raise MissingDependencyError(
            "pandas is required for drift calculations. Install pandas to enable PSI and KS metrics."
        )
    return pd


def _require_ks_test():
    if ks_2samp is None:
        raise MissingDependencyError(
            "scipy is required for KS drift statistics. Install scipy to evaluate drift."
        )
    return ks_2samp


def _to_numpy_array(series: Sequence[float] | Any) -> "numpy.ndarray":
    numpy = _require_numpy()
    pandas = pd

    if pandas is not None and isinstance(series, pandas.Series):
        values = series.dropna().to_numpy(dtype=float)
        return numpy.asarray(values, dtype=float)

    if isinstance(series, (list, tuple)):
        iterable = series
    else:
        try:
            iterable = list(series)  # type: ignore[arg-type]
        except TypeError:
            iterable = [series]

    raw = [value for value in iterable if value is not None]
    return numpy.asarray(raw, dtype=float)


@dataclass
class DriftReport:
    feature: str
    population_stability_index: float
    kolmogorov_smirnov: float
    psi_alert: bool
    ks_alert: bool
    alert: bool
    severity: float

    def to_dict(self) -> Dict[str, float | str | bool]:
        return {
            "feature": self.feature,
            "population_stability_index": self.population_stability_index,
            "kolmogorov_smirnov": self.kolmogorov_smirnov,
            "psi_alert": self.psi_alert,
            "ks_alert": self.ks_alert,
            "alert": self.alert,
            "severity": self.severity,
        }


def population_stability_index(expected: "pandas.Series", actual: "pandas.Series", bins: int = 10) -> float:
    numpy = _require_numpy()
    expected_values = _to_numpy_array(expected)
    actual_values = _to_numpy_array(actual)

    if expected_values.size == 0:
        return 0.0

    expected_counts, bin_edges = numpy.histogram(expected_values, bins=bins)
    actual_counts, _ = numpy.histogram(actual_values, bins=bin_edges)
    expected_total = max(expected_counts.sum(), 1)
    actual_total = max(actual_counts.sum(), 1)
    expected_perc = expected_counts / expected_total
    actual_perc = actual_counts / actual_total
    stability = numpy.sum(
        (actual_perc - expected_perc)
        * numpy.log((actual_perc + 1e-8) / (expected_perc + 1e-8))
    )
    return float(stability)


def evaluate_feature_drift(
    feature: str,
    baseline: "pandas.Series",
    production: "pandas.Series",
    psi_threshold: float = 0.2,
    ks_threshold: float = 0.1,
) -> DriftReport:
    ks_test = _require_ks_test()
    psi_value = population_stability_index(baseline, production)
    ks_stat, _ = ks_test(baseline, production)
    psi_alert = psi_value > psi_threshold
    ks_alert = ks_stat > ks_threshold
    alert = psi_alert or ks_alert
    psi_ratio = (psi_value / psi_threshold) if psi_threshold > 0 else float("inf")
    ks_ratio = (ks_stat / ks_threshold) if ks_threshold > 0 else float("inf")
    severity = max(psi_ratio if psi_alert else 0.0, ks_ratio if ks_alert else 0.0)
    LOGGER.debug(
        "Feature %s drift metrics: PSI=%.4f KS=%.4f", feature, psi_value, ks_stat
    )
    return DriftReport(
        feature=feature,
        population_stability_index=psi_value,
        kolmogorov_smirnov=float(ks_stat),
        psi_alert=psi_alert,
        ks_alert=ks_alert,
        alert=alert,
        severity=severity,
    )


def generate_drift_report(
    baseline_df: "pandas.DataFrame",
    production_df: "pandas.DataFrame",
    psi_threshold: float = 0.2,
    ks_threshold: float = 0.1,
) -> Tuple[DriftReport, ...]:
    pandas = _require_pandas()
    if not isinstance(baseline_df, pandas.DataFrame):
        baseline_df = pandas.DataFrame(baseline_df)
    if not isinstance(production_df, pandas.DataFrame):
        production_df = pandas.DataFrame(production_df)
    common_features = set(baseline_df.columns).intersection(production_df.columns)
    reports = []
    for feature in sorted(common_features):
        reports.append(
            evaluate_feature_drift(
                feature,
                baseline_df[feature].dropna(),
                production_df[feature].dropna(),
                psi_threshold=psi_threshold,
                ks_threshold=ks_threshold,
            )
        )
    return tuple(reports)


def save_report(reports: Iterable[DriftReport], output_path: Path) -> None:
    payload = [report.to_dict() for report in reports]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    LOGGER.info("Saved drift report to %s", output_path)


__all__ = [
    "DriftReport",
    "MissingDependencyError",
    "population_stability_index",
    "evaluate_feature_drift",
    "generate_drift_report",
    "save_report",
]
