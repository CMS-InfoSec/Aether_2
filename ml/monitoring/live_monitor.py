"""Runtime drift detector comparing live feature batches against training baselines.

This module provides a small wrapper around :func:`ml.monitoring.drift.generate_drift_report`
that maintains a rolling buffer of live feature observations.  The detector loads
baseline samples captured during training and evaluates drift once a sufficient
number of live points have been observed.  The resulting statistics can be fed
back into the policy service to decide whether automated retraining should be
triggered.
"""

from __future__ import annotations

import json
import math
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Mapping, Optional, Sequence

from ml.monitoring.drift import DriftReport, MissingDependencyError, generate_drift_report

LOGGER = logging.getLogger(__name__)


def _to_float(value: object) -> Optional[float]:
    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalise_sample(
    payload: Sequence[float] | Mapping[str, float],
    feature_names: Sequence[str] | None,
) -> Dict[str, float]:
    if isinstance(payload, Mapping):
        result: Dict[str, float] = {}
        for key, value in payload.items():
            numeric = _to_float(value)
            if numeric is None:
                continue
            result[str(key)] = numeric
        return result

    names = list(feature_names or [f"feature_{idx}" for idx, _ in enumerate(payload)])
    result = {}
    for name, value in zip(names, payload):
        numeric = _to_float(value)
        if numeric is None:
            continue
        result[str(name)] = numeric
    return result


@dataclass
class DriftEvaluation:
    """Container returned by :class:`FeatureDriftDetector.observe`."""

    checked_at: datetime
    reports: Sequence[DriftReport]
    sample_size: int
    psi_threshold: float
    ks_threshold: float

    @property
    def max_severity(self) -> float:
        return max((report.severity for report in self.reports), default=0.0)

    @property
    def alert(self) -> bool:
        return any(report.alert for report in self.reports)

    def to_dict(self) -> Dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "reports": [report.to_dict() for report in self.reports],
            "sample_size": self.sample_size,
            "max_severity": self.max_severity,
            "alert": self.alert,
            "psi_threshold": self.psi_threshold,
            "ks_threshold": self.ks_threshold,
        }


class FeatureDriftDetector:
    """Rolling window drift detector comparing live and baseline feature sets."""

    def __init__(
        self,
        baseline: Sequence[Mapping[str, float]],
        *,
        psi_threshold: float = 0.2,
        ks_threshold: float = 0.1,
        min_samples: int = 25,
        window_size: int = 500,
    ) -> None:
        if min_samples <= 0:
            raise ValueError("min_samples must be a positive integer")
        if window_size <= 0:
            raise ValueError("window_size must be a positive integer")
        self._baseline = list(baseline)
        self._psi_threshold = psi_threshold
        self._ks_threshold = ks_threshold
        self._min_samples = min_samples
        self._window: Deque[Dict[str, float]] = deque(maxlen=window_size)
        self._last: DriftEvaluation | None = None

    @property
    def last_evaluation(self) -> DriftEvaluation | None:
        return self._last

    def observe(
        self,
        sample: Sequence[float] | Mapping[str, float],
        *,
        feature_names: Sequence[str] | None = None,
    ) -> DriftEvaluation | None:
        """Append ``sample`` to the live buffer and evaluate drift when ready."""

        observation = _normalise_sample(sample, feature_names)
        if not observation:
            LOGGER.debug("Skipping drift observation because sample is empty")
            return None

        self._window.append(observation)
        if len(self._window) < self._min_samples:
            LOGGER.debug(
                "Collected %d/%d live samples – waiting before evaluating drift",
                len(self._window),
                self._min_samples,
            )
            return None

        try:
            reports = generate_drift_report(
                baseline_df=self._baseline,
                production_df=list(self._window),
                psi_threshold=self._psi_threshold,
                ks_threshold=self._ks_threshold,
            )
        except MissingDependencyError:
            raise
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Failed to evaluate drift: %s", exc)
            return None

        evaluation = DriftEvaluation(
            checked_at=datetime.now(timezone.utc),
            reports=reports,
            sample_size=len(self._window),
            psi_threshold=self._psi_threshold,
            ks_threshold=self._ks_threshold,
        )
        self._last = evaluation
        return evaluation

    @classmethod
    def from_json(
        cls,
        path: str | Path,
        *,
        feature_names: Sequence[str] | None = None,
    ) -> "FeatureDriftDetector":
        """Load baseline samples from ``path`` and return a configured detector."""

        file_path = Path(path)
        payload = json.loads(file_path.read_text())
        samples = payload.get("samples")
        if not isinstance(samples, list) or not samples:
            raise ValueError(f"Drift baseline at {file_path} does not contain samples")

        parsed_samples: list[Dict[str, float]] = []
        for entry in samples:
            if not isinstance(entry, Mapping):
                continue
            parsed_samples.append(
                _normalise_sample(entry, feature_names)
            )

        if not parsed_samples:
            raise ValueError(f"Drift baseline at {file_path} has no valid feature entries")

        psi = _to_float(payload.get("psi_threshold")) or 0.2
        ks = _to_float(payload.get("ks_threshold")) or 0.1
        min_samples = int(payload.get("min_samples", 25))
        window_size = int(payload.get("window_size", 500))

        return cls(
            parsed_samples,
            psi_threshold=psi,
            ks_threshold=ks,
            min_samples=max(1, min_samples),
            window_size=max(len(parsed_samples), window_size),
        )


__all__ = ["FeatureDriftDetector", "DriftEvaluation"]
