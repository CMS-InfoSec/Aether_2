#!/usr/bin/env python3
"""Run production readiness audits and update documentation."""

from __future__ import annotations

import dataclasses
import datetime as dt
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = REPO_ROOT / "docs" / "production_readiness_review.md"
MARKER_START = "<!-- BEGIN CODEX AUDIT -->"
MARKER_END = "<!-- END CODEX AUDIT -->"
MAX_LOG_CHARACTERS = 4000


@dataclasses.dataclass
class AuditCommand:
    """Represents a single audit command to execute."""

    name: str
    command: List[str]
    cwd: Path | None = None
    env: dict[str, str] | None = None
    optional: bool = False


@dataclasses.dataclass
class AuditResult:
    """Captures the outcome of an executed audit."""

    command: AuditCommand
    status: str
    duration: float
    stdout: str
    stderr: str
    return_code: int | None
    error: str | None = None

    @property
    def emoji(self) -> str:
        return {
            "passed": "✅",
            "failed": "❌",
            "skipped": "⚠️",
        }.get(self.status, "⚠️")

    @property
    def label(self) -> str:
        return {
            "passed": "Passed",
            "failed": "Failed",
            "skipped": "Skipped",
        }.get(self.status, self.status.title())

    def note(self) -> str:
        if self.status == "passed":
            return f"Completed in {self.duration:.2f}s"
        if self.status == "failed":
            snippet = (self.error or self.stderr or self.stdout).strip()
            if snippet:
                snippet = _truncate(snippet.replace("\n", " "))
                return f"{snippet}"
            return "Command returned a non-zero exit status"
        if self.status == "skipped":
            return self.error or "Command skipped"
        return ""


def _truncate(value: str, max_length: int = 160) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "…"


def build_audit_plan() -> List[AuditCommand]:
    """Construct the list of audit commands to execute."""

    plan: List[AuditCommand] = [
        AuditCommand(name="Ruff", command=["ruff", "check", "."]),
        AuditCommand(name="Black", command=["black", "--check", "."]),
        AuditCommand(name="MyPy", command=["mypy", "."]),
        AuditCommand(name="Pytest", command=["pytest"]),
    ]

    requirements = REPO_ROOT / "requirements.txt"
    if requirements.exists():
        plan.append(
            AuditCommand(
                name="pip-audit",
                command=["pip-audit", "--strict", "-r", str(requirements)],
            )
        )

    policy_dir = REPO_ROOT / "deploy" / "k8s" / "policies"
    if policy_dir.exists():
        plan.append(
            AuditCommand(
                name="Conftest",
                command=["conftest", "test", "deploy", "--policy", str(policy_dir)],
                optional=True,
            )
        )

    return plan


def run_audit(command: AuditCommand) -> AuditResult:
    """Execute a single audit command."""

    start = time.monotonic()
    try:
        completed = subprocess.run(
            command.command,
            cwd=command.cwd or REPO_ROOT,
            env={**os.environ, **(command.env or {})},
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        duration = time.monotonic() - start
        return AuditResult(
            command=command,
            status="skipped" if command.optional else "failed",
            duration=duration,
            stdout="",
            stderr="",
            return_code=None,
            error=f"Command not found: {command.command[0]}",
        )

    duration = time.monotonic() - start
    status = "passed" if completed.returncode == 0 else "failed"
    return AuditResult(
        command=command,
        status=status,
        duration=duration,
        stdout=completed.stdout,
        stderr=completed.stderr,
        return_code=completed.returncode,
        error=None if status == "passed" else "",
    )


def format_markdown(results: Iterable[AuditResult], timestamp: dt.datetime) -> str:
    """Generate the markdown snippet inserted into the review document."""

    lines: List[str] = [
        "## Automated Audit Findings",
        f"_Last updated: {timestamp.replace(microsecond=0).isoformat()}Z_",
        "",
        "| Audit | Status | Notes |",
        "| --- | --- | --- |",
    ]

    details: List[str] = []
    for result in results:
        note = result.note()
        lines.append(
            f"| {result.command.name} | {result.emoji} {result.label} | {note} |"
        )

        detail_log = "\n".join(
            part for part in [result.stdout.strip(), result.stderr.strip()] if part
        ).strip()
        if not detail_log:
            detail_log = "(no output)"
        detail_log = detail_log[:MAX_LOG_CHARACTERS]
        details.append(
            "<details>\n"
            f"<summary>{result.command.name} logs</summary>\n\n"
            "```text\n"
            f"{detail_log}\n"
            "```\n"
            "</details>"
        )

    return "\n".join(lines + ["", *details])


def update_document(markdown: str) -> None:
    """Insert the markdown block into the production readiness document."""

    content = DOC_PATH.read_text(encoding="utf-8")
    block = f"{MARKER_START}\n{markdown}\n{MARKER_END}"
    pattern = re.compile(
        rf"{re.escape(MARKER_START)}.*?{re.escape(MARKER_END)}",
        re.DOTALL,
    )

    if pattern.search(content):
        updated = pattern.sub(block, content)
    else:
        separator = "\n\n" if not content.endswith("\n") else "\n"
        updated = f"{content}{separator}{block}\n"

    DOC_PATH.write_text(updated, encoding="utf-8")


def stage_commit_and_push(timestamp: dt.datetime) -> None:
    """Stage the updated files, commit, and push the changes."""

    def run_git(args: List[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )

    run_git(["config", "user.name", os.environ.get("GIT_AUTHOR_NAME", "Codex Audit Bot")])
    run_git(
        [
            "config",
            "user.email",
            os.environ.get("GIT_AUTHOR_EMAIL", "codex-audit-bot@example.com"),
        ]
    )

    run_git(["add", str(DOC_PATH.relative_to(REPO_ROOT))])

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    if not status.stdout.strip():
        return

    commit_message = (
        "Codex automated production readiness review update - "
        f"{timestamp.replace(microsecond=0).isoformat()}Z"
    )
    run_git(["commit", "-m", commit_message])

    push_result = subprocess.run(
        ["git", "push"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )
    if push_result.returncode != 0:
        raise RuntimeError(
            "Failed to push automated audit update:\n"
            f"STDOUT: {push_result.stdout}\nSTDERR: {push_result.stderr}"
        )


def main() -> None:
    audits = build_audit_plan()
    results = [run_audit(audit) for audit in audits]
    timestamp = dt.datetime.utcnow()
    markdown = format_markdown(results, timestamp)
    update_document(markdown)
    stage_commit_and_push(timestamp)


if __name__ == "__main__":
    main()
