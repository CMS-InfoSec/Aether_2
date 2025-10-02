"""Drift monitoring utilities calculating PSI and KS statistics."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

LOGGER = logging.getLogger(__name__)


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


def population_stability_index(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    expected_counts, bin_edges = np.histogram(expected, bins=bins)
    actual_counts, _ = np.histogram(actual, bins=bin_edges)
    expected_perc = expected_counts / expected_counts.sum()
    actual_perc = actual_counts / max(actual_counts.sum(), 1)
    stability = np.sum(
        (actual_perc - expected_perc)
        * np.log((actual_perc + 1e-8) / (expected_perc + 1e-8))
    )
    return float(stability)


def evaluate_feature_drift(
    feature: str,
    baseline: pd.Series,
    production: pd.Series,
    psi_threshold: float = 0.2,
    ks_threshold: float = 0.1,
) -> DriftReport:
    psi_value = population_stability_index(baseline, production)
    ks_stat, _ = ks_2samp(baseline, production)
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
    baseline_df: pd.DataFrame,
    production_df: pd.DataFrame,
    psi_threshold: float = 0.2,
    ks_threshold: float = 0.1,
) -> Tuple[DriftReport, ...]:
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
