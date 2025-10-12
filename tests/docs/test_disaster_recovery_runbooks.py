from __future__ import annotations

import argparse
import importlib
import re
from pathlib import Path
from typing import Dict, Iterable

import yaml

import dr_playbook

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATTERN = re.compile(
    r"<!--\s*DR-CHECK:([^>]*)-->\s*```[\w-]*\n(.*?)```",
    re.DOTALL,
)


def _load_cronjobs() -> Dict[str, Dict[str, object]]:
    cronjobs: Dict[str, Dict[str, object]] = {}
    for path in REPO_ROOT.joinpath("deploy").rglob("*.yaml"):
        try:
            docs = list(yaml.safe_load_all(path.read_text()))
        except Exception:
            continue
        for doc in docs:
            if not isinstance(doc, dict) or doc.get("kind") != "CronJob":
                continue
            metadata = doc.get("metadata") or {}
            name = metadata.get("name")
            if not name:
                continue
            spec = doc.get("spec", {})
            job_template = spec.get("jobTemplate", {})
            job_spec = job_template.get("spec", {})
            template = job_spec.get("template", {})
            pod_spec = template.get("spec", {})
            containers = pod_spec.get("containers", [])
            if not containers:
                continue
            command = containers[0].get("command", [])
            cronjobs[name] = {
                "path": path.relative_to(REPO_ROOT).as_posix(),
                "command": command,
            }
    return cronjobs


CRONJOBS = _load_cronjobs()


def _parse_params(raw: str) -> Dict[str, str]:
    params: Dict[str, str] = {}
    for token in raw.strip().split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        params[key.strip()] = value.strip()
    return params


def _normalize_cronjob_command(command: Iterable[str]) -> str:
    parts = list(command)
    if not parts:
        raise AssertionError("CronJob command is empty")
    if len(parts) >= 3 and parts[0] in {"/bin/bash", "bash"} and parts[1] == "-c":
        return parts[2].strip()
    return " ".join(parts).strip()


def _cli_commands() -> set[str]:
    parser = dr_playbook._build_cli()
    commands: set[str] = set()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            commands.update(action.choices.keys())
    return commands


def test_disaster_recovery_runbooks_are_in_sync() -> None:
    cli_commands = _cli_commands()
    doc_paths = sorted(REPO_ROOT.joinpath("docs", "runbooks").glob("*dr*.md"))
    assert doc_paths, "Expected at least one DR runbook"

    for doc_path in doc_paths:
        text = doc_path.read_text()
        matches = list(DOC_PATTERN.finditer(text))
        if not matches:
            # Some runbooks (e.g., clock drift) match the glob but do not
            # describe disaster-recovery automation. Skip validation in that
            # case because there are no CronJobs or restore scripts to verify.
            continue

        for match in matches:
            params = _parse_params(match.group(1))
            code_block = match.group(2).strip()

            cronjob_name = params.get("cronjob")
            if cronjob_name:
                assert (
                    cronjob_name in CRONJOBS
                ), f"CronJob {cronjob_name} referenced in {doc_path} does not exist"
                cronjob = CRONJOBS[cronjob_name]
                expected_path = params.get("manifest")
                if expected_path:
                    assert (
                        cronjob["path"] == expected_path
                    ), f"CronJob {cronjob_name} manifest moved: {cronjob['path']} != {expected_path}"
                expected_command = _normalize_cronjob_command(
                    cronjob["command"]  # type: ignore[arg-type]
                )
                assert (
                    expected_command == code_block
                ), f"CronJob {cronjob_name} command drift detected"

            cli_module = params.get("cli")
            cli_command = params.get("command")
            if cli_module and cli_command:
                module_name = Path(cli_module).with_suffix("").name
                if module_name != "dr_playbook":
                    importlib.import_module(module_name)
                assert (
                    cli_command in cli_commands
                ), f"CLI command {cli_command} not defined in {cli_module}"
                assert code_block.startswith(
                    f"python {cli_module} {cli_command}"
                ), f"Documented invocation for {cli_module} {cli_command} is unexpected"

            module_name = params.get("module")
            if module_name:
                module = importlib.import_module(module_name)
                callable_name = params.get("callable")
                if callable_name:
                    assert hasattr(
                        module, callable_name
                    ), f"{module_name} is missing callable {callable_name}"
                assert code_block.startswith(
                    "python -m "
                ), f"Module invocation for {module_name} should use python -m"
