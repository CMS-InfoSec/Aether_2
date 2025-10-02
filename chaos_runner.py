#!/usr/bin/env python3
"""Chaos suite runner that evaluates declarative YAML suites and emits reports."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional dependency handling
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None


Comparator = str


def _normalize_scalar(value: Any) -> Any:
    """Convert strings such as numbers and booleans into native Python types."""
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in lowered:
                return float(lowered)
            return int(lowered)
        except ValueError:
            return value
    return value


def _compare(observed: Any, comparator: Comparator, target: Any) -> bool:
    if comparator not in {"<=", "<", ">=", ">", "==", "!="}:
        raise ValueError(f"Unsupported comparator: {comparator}")

    observed_norm = _normalize_scalar(observed)
    target_norm = _normalize_scalar(target)

    if comparator == "<":
        return observed_norm < target_norm
    if comparator == "<=":
        return observed_norm <= target_norm
    if comparator == ">":
        return observed_norm > target_norm
    if comparator == ">=":
        return observed_norm >= target_norm
    if comparator == "==":
        return observed_norm == target_norm
    if comparator == "!=":
        return observed_norm != target_norm
    raise AssertionError("unreachable")


@dataclass
class ValidationResult:
    slo_id: str
    slo_name: str
    comparator: Comparator
    observed: Any
    target: Any
    passed: bool
    note: Optional[str]
    window: Optional[str]
    stage_id: str
    stage_name: str

    def target_display(self) -> str:
        if self.comparator in {"==", "!="}:
            return f"{self.comparator} {self.target}"
        return f"{self.comparator} {self.target}"


@dataclass
class StageResult:
    stage_id: str
    stage_name: str
    description: str
    fault: Dict[str, Any]
    actions: List[str]
    monitors: Dict[str, Any]
    validations: List[ValidationResult]

    @property
    def passed(self) -> bool:
        return all(v.passed for v in self.validations)


def load_suite(path: Path) -> Dict[str, Any]:
    raw_text = path.read_text()
    if yaml is not None:  # pragma: no branch
        return yaml.safe_load(raw_text)

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise RuntimeError(
            "PyYAML is required to parse the suite file. Install dependencies or "
            "provide JSON-compatible YAML."
        ) from exc


def _resolve_target(validation: Dict[str, Any], slo_def: Dict[str, Any]) -> Any:
    if "expected" in validation:
        return validation["expected"]
    if "threshold" in validation:
        return validation["threshold"]
    if "expected" in slo_def:
        return slo_def["expected"]
    if "threshold" in slo_def:
        return slo_def["threshold"]
    raise KeyError(f"No target specified for SLO '{slo_def.get('id')}'.")


def evaluate_suite(suite_data: Dict[str, Any]) -> tuple[Dict[str, Any], List[StageResult], Dict[str, List[ValidationResult]]]:
    suite = suite_data.get("suite")
    if not suite:
        raise KeyError("Suite definition must include a top-level 'suite' key.")

    slo_definitions = {entry["id"]: entry for entry in suite.get("slos", [])}
    if not slo_definitions:
        raise KeyError("Suite must declare at least one SLO under 'slos'.")

    stage_results: List[StageResult] = []
    slo_results: Dict[str, List[ValidationResult]] = {slo_id: [] for slo_id in slo_definitions.keys()}

    for stage in suite.get("stages", []):
        stage_id = stage.get("id") or "unknown-stage"
        stage_name = stage.get("name", stage_id)
        validations: List[ValidationResult] = []
        for validation in stage.get("validations", []):
            slo_id = validation["slo"]
            if slo_id not in slo_definitions:
                raise KeyError(f"Validation references undefined SLO '{slo_id}'.")
            slo_def = slo_definitions[slo_id]
            comparator: Comparator = validation.get("comparator", slo_def.get("comparator", "=="))
            target = _resolve_target(validation, slo_def)
            observed = validation.get("observed")
            passed = _compare(observed, comparator, target)
            result = ValidationResult(
                slo_id=slo_id,
                slo_name=slo_def.get("name", slo_id),
                comparator=comparator,
                observed=observed,
                target=target,
                passed=passed,
                note=validation.get("note"),
                window=validation.get("window"),
                stage_id=stage_id,
                stage_name=stage_name,
            )
            validations.append(result)
            slo_results[slo_id].append(result)

        stage_results.append(
            StageResult(
                stage_id=stage_id,
                stage_name=stage_name,
                description=stage.get("description", ""),
                fault=stage.get("fault", {}),
                actions=stage.get("actions", []),
                monitors=stage.get("monitors", {}),
                validations=validations,
            )
        )

    return suite, stage_results, slo_results


def _format_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def generate_markdown(
    suite: Dict[str, Any],
    stage_results: List[StageResult],
    slo_results: Dict[str, List[ValidationResult]],
) -> str:
    lines: List[str] = []
    lines.append(f"# {suite.get('name', 'Chaos Suite Report')}")
    if suite.get("description"):
        lines.append("")
        lines.append(suite["description"].strip())
    lines.append("")
    meta_pairs = []
    for label in ("environment", "owner"):
        if suite.get(label):
            pretty = label.replace("_", " ").title()
            meta_pairs.append(f"**{pretty}:** {suite[label]}")
    schedule = suite.get("schedule")
    if schedule:
        schedule_details = ", ".join(f"{key}={value}" for key, value in schedule.items())
        meta_pairs.append(f"**Schedule:** {schedule_details}")
    if meta_pairs:
        lines.append(" ".join(meta_pairs))
        lines.append("")

    lines.append("## SLO Summary")
    lines.append("")
    lines.append("| SLO | Outcome | Details |")
    lines.append("| --- | --- | --- |")

    for slo_id, validations in slo_results.items():
        if not validations:
            continue
        slo_name = validations[0].slo_name
        all_passed = all(v.passed for v in validations)
        outcome = "✅ Pass" if all_passed else "❌ Fail"
        observed_values = [_normalize_scalar(v.observed) for v in validations]
        targets = {v.target for v in validations}
        detail_bits: List[str] = []
        if all(isinstance(val, (int, float)) for val in observed_values):
            maximum = max(observed_values) if observed_values else "-"
            detail_bits.append(f"max observed={maximum}")
        else:
            unique_obs = { _format_bool(val) for val in observed_values }
            detail_bits.append("observed=" + ", ".join(sorted(unique_obs)))

        target_descriptions = {f"{v.comparator} {v.target}" for v in validations}
        detail_bits.append("targets=" + ", ".join(sorted(target_descriptions)))

        failing_stages = ", ".join(v.stage_name for v in validations if not v.passed)
        if failing_stages:
            detail_bits.append(f"failing stages: {failing_stages}")

        lines.append(f"| {slo_name} | {outcome} | {'; '.join(detail_bits)} |")

    lines.append("")

    lines.append("## Stage Results")
    lines.append("")
    for stage in stage_results:
        header_status = "✅" if stage.passed else "❌"
        lines.append(f"### {header_status} {stage.stage_name} ({stage.stage_id})")
        lines.append("")
        if stage.description:
            lines.append(stage.description.strip())
            lines.append("")
        fault = stage.fault or {}
        fault_summary = ", ".join(f"{key}={value}" for key, value in fault.items())
        if fault_summary:
            lines.append(f"*Fault:* {fault_summary}")
        if stage.actions:
            actions = "; ".join(stage.actions)
            lines.append(f"*Actions:* {actions}")
        monitors = stage.monitors or {}
        monitor_bits = []
        if monitors.get("metrics"):
            monitor_bits.append("metrics: " + ", ".join(monitors["metrics"]))
        if monitors.get("logs"):
            monitor_bits.append("logs: " + ", ".join(monitors["logs"]))
        if monitor_bits:
            lines.append("*Monitors:* " + "; ".join(monitor_bits))
        lines.append("")

        lines.append("| SLO | Observed | Target | Window | Result | Notes |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for validation in stage.validations:
            result_icon = "✅" if validation.passed else "❌"
            target_text = f"{validation.comparator} {validation.target}"
            lines.append(
                "| {slo} | {observed} | {target} | {window} | {result} | {note} |".format(
                    slo=validation.slo_name,
                    observed=_format_bool(validation.observed),
                    target=target_text,
                    window=validation.window or "-",
                    result=result_icon,
                    note=validation.note or "-",
                )
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a chaos suite and emit a Markdown report.")
    parser.add_argument(
        "--suite",
        type=Path,
        default=Path("ops/chaos/chaos_suite.yml"),
        help="Path to the chaos suite YAML file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/chaos_suite_report.md"),
        help="Destination for the Markdown report.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    suite_data = load_suite(args.suite)
    suite, stage_results, slo_results = evaluate_suite(suite_data)

    report = generate_markdown(suite, stage_results, slo_results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)

    all_passed = all(stage.passed for stage in stage_results)
    status = "PASS" if all_passed else "FAIL"
    print(f"Chaos suite completed: {status}")
    print(f"Report written to {args.output}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
