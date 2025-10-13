"""Quarterly stateful service recovery exercise planner.

The planner rotates through predefined scenarios for TimescaleDB, Kafka, and
Redis to produce a Markdown briefing. Plans highlight backup validation,
restore, failover, and objective verification tasks to keep disaster recovery
playbooks fresh. When a Slack webhook is provided the rendered plan is delivered
straight to the incident response channel.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence
from urllib.error import URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class Scenario:
    """Single recovery drill scenario."""

    service: str
    key: str
    name: str
    drill_type: str
    focus: Sequence[str]
    runbook_refs: Sequence[str]
    failover: Sequence[str]
    verification: Mapping[str, str]
    rto_target: str
    rpo_target: str


SCENARIOS: Mapping[str, Sequence[Scenario]] = {
    "TimescaleDB": (
        Scenario(
            service="TimescaleDB",
            key="logical-restore",
            name="Logical dump restore validation",
            drill_type="simulated",
            focus=(
                "Trigger the staging restore drill to replay the latest logical dump",
                "Capture duration and validation metrics from the Slack notification",
                "Inspect backup manifest history for data staleness",
            ),
            runbook_refs=(
                "docs/runbooks/disaster-recovery.md#monthly-restore-drill-timescaledb-restore-drill",
                "docs/runbooks/stateful-service-recovery-drills.md#timescaledb-drill",
            ),
            failover=(
                "Cordon the primary TimescaleDB pod and observe replica promotion",
                "Verify API latency metrics remain within failover tolerances",
            ),
            verification={
                "RTO": "< 15 minutes from restore start until validation completes",
                "RPO": "Nightly dump timestamp no older than 24 hours",
            },
            rto_target="15 minutes",
            rpo_target="24 hours",
        ),
        Scenario(
            service="TimescaleDB",
            key="object-store-restore",
            name="Object-store snapshot validation",
            drill_type="tabletop",
            focus=(
                "Walk through `dr_playbook.py snapshot` creation including Redis and MLflow artifacts",
                "Practice restoring into a scratch namespace with `--no-clean`",
                "Review access to object storage credentials and retention policies",
            ),
            runbook_refs=(
                "docs/runbooks/disaster-recovery.md#operator-driven-restore-playbook-dr_playbookpy",
                "docs/runbooks/stateful-service-recovery-drills.md#timescaledb-drill",
            ),
            failover=(
                "Validate manual promotion procedure for the replica statefulset",
                "Cross-check that application connection strings honor the failover endpoint",
            ),
            verification={
                "RTO": "< 30 minutes for full snapshot restore during tabletop review",
                "RPO": "Snapshot manifest timestamp within policy window",
            },
            rto_target="30 minutes",
            rpo_target="24 hours",
        ),
    ),
    "Kafka": (
        Scenario(
            service="Kafka",
            key="consumer-lag-chaos",
            name="Broker throttling chaos test",
            drill_type="simulated",
            focus=(
                "Execute the `kafka_lag` stage from the chaos suite",
                "Export topic configurations and consumer offsets before and after the test",
                "Document safe-mode behaviour and backlog recovery time",
            ),
            runbook_refs=(
                "ops/chaos/chaos_suite.yml#L160",
                "docs/runbooks/stateful-service-recovery-drills.md#kafka-drill",
            ),
            failover=(
                "Promote the standby broker and validate producers repoint automatically",
                "Ensure non-critical consumers remain paused until offsets stabilise",
            ),
            verification={
                "RTO": "< 10 minutes from throttle removal until lag clears",
                "RPO": "Replicated offsets differ by < 15 minutes",
            },
            rto_target="10 minutes",
            rpo_target="15 minutes",
        ),
        Scenario(
            service="Kafka",
            key="control-plane-reseed",
            name="Control-plane reseed and replay",
            drill_type="tabletop",
            focus=(
                "Review warm start coordinator steps to reseed order books",
                "Practice KafkaNATSAdapter bootstrap scripts for control-plane events",
                "Confirm topic ACLs and schemas are documented for rapid rebuild",
            ),
            runbook_refs=(
                "services/oms/warm_start.py",
                "docs/runbooks/stateful-service-recovery-drills.md#kafka-drill",
            ),
            failover=(
                "Simulate metadata loss by rotating brokers in staging",
                "Verify standby configuration store can promote to primary",
            ),
            verification={
                "RTO": "< 20 minutes to republish control events in tabletop",
                "RPO": "Latest checkpoint file age <= 15 minutes",
            },
            rto_target="20 minutes",
            rpo_target="15 minutes",
        ),
    ),
    "Redis": (
        Scenario(
            service="Redis",
            key="encrypted-snapshot",
            name="Encrypted snapshot restore",
            drill_type="simulated",
            focus=(
                "Generate a new Feast backup archive with encryption",
                "Restore into staging Redis and validate feature availability",
                "Measure end-to-end feature serving recovery time",
            ),
            runbook_refs=(
                "ops/backup/feast_backup.py",
                "docs/runbooks/stateful-service-recovery-drills.md#redis-feast-drill",
            ),
            failover=(
                "Switch Feast online store URL to standby during the test",
                "Monitor RedisRestartLogStore metrics for automated retries",
            ),
            verification={
                "RTO": "< 10 minutes from snapshot start to features online",
                "RPO": "Manifest timestamp <= 60 minutes old",
            },
            rto_target="10 minutes",
            rpo_target="60 minutes",
        ),
        Scenario(
            service="Redis",
            key="tabletop-registry",
            name="Registry reconciliation tabletop",
            drill_type="tabletop",
            focus=(
                "Walk through restoring Feast registry YAML and secret rotation",
                "Validate operators can decrypt historical snapshots offline",
                "Confirm feature store clients reconnect to new endpoints",
            ),
            runbook_refs=(
                "docs/runbooks/stateful-service-recovery-drills.md#redis-feast-drill",
            ),
            failover=(
                "Review standby Redis deployment topology",
                "Ensure self-healer thresholds prevent restart storms",
            ),
            verification={
                "RTO": "< 20 minutes to stand up replacement Redis",
                "RPO": "Snapshot cadence <= 60 minutes",
            },
            rto_target="20 minutes",
            rpo_target="60 minutes",
        ),
    ),
}


def quarter_index(date: dt.date) -> int:
    """Return zero-based quarter index for ``date``."""

    return (date.month - 1) // 3


def select_scenarios(date: dt.date) -> List[Scenario]:
    """Select a scenario for each service based on the quarter."""

    quarter = quarter_index(date)
    selections: List[Scenario] = []
    for service, options in SCENARIOS.items():
        if not options:
            continue
        idx = quarter % len(options)
        selections.append(options[idx])
    return selections


def render_markdown(date: dt.date, scenarios: Iterable[Scenario]) -> str:
    """Render the plan into Markdown."""

    header = [
        "# Quarterly Stateful Service Recovery Plan",
        "",
        f"_Generated for {date.isoformat()} (Q{quarter_index(date)+1} {date.year})_",
        "",
    ]
    sections: List[str] = header
    for scenario in scenarios:
        sections.extend(
            [
                f"## {scenario.service}: {scenario.name}",
                "",
                f"* **Drill type:** {scenario.drill_type}",
                f"* **RTO target:** {scenario.rto_target}",
                f"* **RPO target:** {scenario.rpo_target}",
                "* **Runbook references:**",
            ]
        )
        for ref in scenario.runbook_refs:
            sections.append(f"  * {ref}")
        sections.append("* **Focus tasks:**")
        for item in scenario.focus:
            sections.append(f"  * {item}")
        sections.append("* **Failover validation:**")
        for item in scenario.failover:
            sections.append(f"  * {item}")
        sections.append("* **Objective verification:**")
        for key, value in scenario.verification.items():
            sections.append(f"  * {key}: {value}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def write_output(path: Path, content: str) -> Path:
    """Write ``content`` to ``path`` ensuring parent directories exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def post_to_slack(webhook: str, content: str) -> None:
    """Post the Markdown plan to Slack using the incoming webhook."""

    payload = json.dumps({"text": content})
    request = Request(webhook, data=payload.encode("utf-8"), headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:
        response.read()  # Ensure request completes


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        default=dt.date.today(),
        help="Reference date used to pick the quarter",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the rendered plan",
    )
    parser.add_argument(
        "--slack-webhook",
        type=str,
        default=os.environ.get("STATEFUL_DRILL_SLACK_WEBHOOK"),
        help="Slack webhook URL used to post the plan",
    )
    args = parser.parse_args(argv)

    scenarios = select_scenarios(args.date)
    if not scenarios:
        raise SystemExit("No scenarios configured")

    markdown = render_markdown(args.date, scenarios)
    sys.stdout.write(markdown)

    if args.output:
        write_output(args.output, markdown)
        print(f"Plan written to {args.output}", file=sys.stderr)

    if args.slack_webhook:
        try:
            post_to_slack(args.slack_webhook, markdown)
            print("Plan posted to Slack", file=sys.stderr)
        except URLError as exc:
            print(f"Failed to post to Slack: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI tool
    raise SystemExit(main())
