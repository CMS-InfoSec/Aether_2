"""Utilities for orchestrating chaos experiments against Kubernetes services."""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

import psycopg2

LOGGER = logging.getLogger(__name__)

DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5432/aether"
DEFAULT_DURATION_SECONDS = int(os.getenv("CHAOS_DURATION_SECONDS", "30"))
DEFAULT_INTERFACE = os.getenv("CHAOS_POD_INTERFACE", "eth0")
DEFAULT_LABEL_KEY = os.getenv("CHAOS_POD_LABEL_KEY", "app")
DEFAULT_NAMESPACE = os.getenv("CHAOS_NAMESPACE")


logging.basicConfig(
    level=os.getenv("CHAOS_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


class ChaosError(RuntimeError):
    """Raised when a chaos experiment fails."""


@contextmanager
def _db_connection():
    dsn = _normalise_db_url(
        os.getenv("CHAOS_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DB_URL
    )
    conn = psycopg2.connect(dsn, connect_timeout=int(os.getenv("CHAOS_DB_TIMEOUT", "5")))
    try:
        conn.autocommit = True
        yield conn
    finally:
        conn.close()


_TABLE_INITIALISED = False


def _ensure_log_table() -> None:
    global _TABLE_INITIALISED
    if _TABLE_INITIALISED:
        return

    try:
        with _db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chaos_log (
                        service TEXT NOT NULL,
                        action TEXT NOT NULL,
                        ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        outcome TEXT NOT NULL
                    )
                    """
                )
        _TABLE_INITIALISED = True
    except Exception:  # pragma: no cover - best effort setup.
        LOGGER.exception("Failed to ensure chaos_log table exists.")


def _log_action(service: str, action: str, outcome: str, *, ts: Optional[datetime] = None) -> None:
    _ensure_log_table()
    ts = ts or datetime.now(timezone.utc)
    try:
        with _db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO chaos_log(service, action, ts, outcome) VALUES (%s, %s, %s, %s)",
                    (service, action, ts, outcome),
                )
    except Exception:  # pragma: no cover - logging should not raise.
        LOGGER.exception("Failed to log chaos action to Postgres.")


def _normalise_db_url(url: str) -> str:
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.split("://", 1)[1]
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.split("://", 1)[1]
    if url.startswith("postgres://"):
        return "postgresql://" + url.split("://", 1)[1]
    return url


def _run_command(command: List[str]) -> subprocess.CompletedProcess:
    LOGGER.debug("Running command: %s", " ".join(command))
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        LOGGER.error("Command failed: %s", completed.stderr.strip())
        raise ChaosError(f"Command {' '.join(command)} failed: {completed.stderr.strip()}")
    return completed


def _list_pods(service: str, *, namespace: Optional[str] = None) -> List[str]:
    ns = namespace or DEFAULT_NAMESPACE or service
    selector = os.getenv("CHAOS_POD_LABEL", f"{DEFAULT_LABEL_KEY}={service}")
    command = [
        "kubectl",
        "get",
        "pods",
        "-n",
        ns,
        "-l",
        selector,
        "-o",
        "json",
    ]
    completed = _run_command(command)
    data = json.loads(completed.stdout)
    pods = [item["metadata"]["name"] for item in data.get("items", [])]
    if not pods:
        raise ChaosError(f"No pods found for selector '{selector}' in namespace '{ns}'.")
    return pods


def _select_pod(service: str, *, namespace: Optional[str] = None) -> str:
    pods = _list_pods(service, namespace=namespace)
    pod = random.choice(pods)
    LOGGER.debug("Selected pod '%s' for service '%s'.", pod, service)
    return pod


def _kubectl_exec(namespace: str, pod: str, command: Iterable[str]) -> None:
    args = ["kubectl", "exec", pod, "-n", namespace, "--", *command]
    _run_command(args)


def kill_pod(service: str, *, namespace: Optional[str] = None) -> str:
    """Delete a random pod for the given service."""
    action = "kill_pod"
    ns = namespace or DEFAULT_NAMESPACE or service
    try:
        pod = _select_pod(service, namespace=ns)
        _run_command(["kubectl", "delete", "pod", pod, "-n", ns, "--force", "--grace-period=0"])
        LOGGER.info("Deleted pod '%s' in namespace '%s' for service '%s'.", pod, ns, service)
        _log_action(service, action, f"deleted pod {pod}")
        return pod
    except Exception as exc:
        LOGGER.exception("Failed to kill pod for service '%s'.", service)
        _log_action(service, action, f"error: {exc}")
        raise


def disconnect_ws(service: str, *, namespace: Optional[str] = None, duration: Optional[int] = None) -> str:
    """Disconnect websocket traffic by dropping network packets for a duration."""
    action = "disconnect_ws"
    ns = namespace or DEFAULT_NAMESPACE or service
    duration = duration or DEFAULT_DURATION_SECONDS
    pod = _select_pod(service, namespace=ns)
    success = False
    try:
        LOGGER.info(
            "Applying 100%% packet loss to pod '%s' in namespace '%s' for %s seconds.",
            pod,
            ns,
            duration,
        )
        _kubectl_exec(ns, pod, [
            "tc",
            "qdisc",
            "replace",
            "dev",
            DEFAULT_INTERFACE,
            "root",
            "netem",
            "loss",
            "100%",
        ])
        time.sleep(duration)
        success = True
    except Exception as exc:
        _log_action(service, action, f"error: {exc}")
        LOGGER.exception("Failed to disconnect websocket for service '%s'.", service)
        raise
    finally:
        try:
            _kubectl_exec(ns, pod, ["tc", "qdisc", "del", "dev", DEFAULT_INTERFACE, "root", "netem"])
        except Exception:
            LOGGER.warning("Failed to remove netem configuration from pod '%s'.", pod)
    if success:
        _log_action(service, action, f"packet loss {duration}s applied to pod {pod}")
        LOGGER.info("Restored network for pod '%s'.", pod)
    return pod


def spike_latency(service: str, *, namespace: Optional[str] = None, duration: Optional[int] = None) -> str:
    """Introduce artificial latency on network traffic for the service."""
    action = "spike_latency"
    ns = namespace or DEFAULT_NAMESPACE or service
    duration = duration or DEFAULT_DURATION_SECONDS
    latency_ms = os.getenv("CHAOS_LATENCY_MS", "500")
    pod = _select_pod(service, namespace=ns)
    success = False
    try:
        LOGGER.info(
            "Applying %sms latency to pod '%s' in namespace '%s' for %s seconds.",
            latency_ms,
            pod,
            ns,
            duration,
        )
        _kubectl_exec(ns, pod, [
            "tc",
            "qdisc",
            "replace",
            "dev",
            DEFAULT_INTERFACE,
            "root",
            "netem",
            "delay",
            f"{latency_ms}ms",
        ])
        time.sleep(duration)
        success = True
    except Exception as exc:
        _log_action(service, action, f"error: {exc}")
        LOGGER.exception("Failed to spike latency for service '%s'.", service)
        raise
    finally:
        try:
            _kubectl_exec(ns, pod, ["tc", "qdisc", "del", "dev", DEFAULT_INTERFACE, "root", "netem"])
        except Exception:
            LOGGER.warning("Failed to remove latency rule from pod '%s'.", pod)
    if success:
        _log_action(service, action, f"latency {latency_ms}ms applied for {duration}s to pod {pod}")
        LOGGER.info("Removed latency injection from pod '%s'.", pod)
    return pod


ACTIONS: Dict[str, Callable[..., str]] = {
    "kill_pod": kill_pod,
    "disconnect_ws": disconnect_ws,
    "spike_latency": spike_latency,
}


def run_random_experiments(
    services: Iterable[str],
    *,
    actions: Optional[Iterable[str]] = None,
    count: int = 1,
    interval: float = 0.0,
    namespace: Optional[str] = None,
) -> None:
    """Run a sequence of randomly selected chaos experiments."""
    service_list = list(services)
    if not service_list:
        raise ValueError("At least one service must be provided.")

    selected_actions = list(actions or ACTIONS.keys())
    invalid = [action for action in selected_actions if action not in ACTIONS]
    if invalid:
        raise ValueError(f"Unsupported actions specified: {', '.join(invalid)}")

    LOGGER.info(
        "Starting chaos run for services=%s actions=%s count=%s interval=%ss.",
        service_list,
        selected_actions,
        count,
        interval,
    )

    for i in range(count):
        service = random.choice(service_list)
        action_name = random.choice(selected_actions)
        LOGGER.info("Run %s/%s: executing '%s' for service '%s'.", i + 1, count, action_name, service)
        action = ACTIONS[action_name]
        try:
            action(service, namespace=namespace)
        except Exception:
            LOGGER.exception("Chaos action '%s' failed for service '%s'.", action_name, service)
        if interval and i < count - 1:
            time.sleep(interval)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run chaos experiments against Kubernetes workloads.")
    parser.add_argument("services", nargs="+", help="Services to target (defaults namespace to service name).")
    parser.add_argument(
        "-a",
        "--actions",
        nargs="+",
        choices=sorted(ACTIONS.keys()),
        help="Subset of actions to choose from when running experiments.",
    )
    parser.add_argument("-c", "--count", type=int, default=1, help="Number of experiments to run.")
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=0.0,
        help="Seconds to wait between experiments.",
    )
    parser.add_argument(
        "-n",
        "--namespace",
        help="Explicit namespace to use instead of default/service name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select and log actions without executing them.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Seed for the random number generator to make runs reproducible.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)

    if args.dry_run:
        actions = args.actions or list(ACTIONS.keys())
        LOGGER.info(
            "Dry run selected. Would execute chaos actions %s against services %s.",
            actions,
            args.services,
        )
        return

    run_random_experiments(
        args.services,
        actions=args.actions,
        count=args.count,
        interval=args.interval,
        namespace=args.namespace,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
