"""Graceful shutdown orchestrator for trading control plane services.

Rolling upgrade steps:
1. Trigger a drain on the *policy* service (either via this CLI or the
   Kubernetes ``preStop`` hook which should ``POST /ops/drain/start``).
2. Wait until ``/ops/drain/status`` reports ``inflight=0`` so that no new
   intents are accepted and buffered events are flushed.
3. Roll the deployment for the drained service (for example with
   ``kubectl rollout restart deployment/policy``) and wait for the new pod to
   become Ready.
4. Repeat the process for the *risk* service and finally the *oms* service to
   avoid sending partially validated orders downstream.
5. Once all services have been restarted, re-enable traffic (service mesh
   routes, ingress, etc.) and optionally run this CLI with ``--dry-run`` to
   verify that all drain endpoints are reachable.

The CLI in this module provides a simple automation for the manual steps above
so operators can coordinate drains across services from a single command.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Dict, Iterable, List, Mapping

import httpx


DEFAULT_SERVICE_ORDER = ["policy", "risk", "oms"]
SERVICE_ENV_VARS: Mapping[str, Iterable[str]] = {
    "policy": (
        "POLICY_DRAIN_URL",
        "POLICY_SERVICE_BASE_URL",
        "POLICY_SERVICE_URL",
    ),
    "risk": (
        "RISK_DRAIN_URL",
        "RISK_SERVICE_BASE_URL",
        "RISK_SERVICE_URL",
    ),
    "oms": (
        "OMS_DRAIN_URL",
        "OMS_SERVICE_BASE_URL",
        "OMS_SERVICE_URL",
    ),
}
SERVICE_DEFAULTS: Mapping[str, str] = {
    "policy": "http://policy-service",
    "risk": "http://risk-service",
    "oms": "http://oms-service",
}


logger = logging.getLogger("ops.graceful_shutdown")


def _parse_overrides(pairs: List[str] | None) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if not pairs:
        return overrides
    for item in pairs:
        if "=" not in item:
            raise ValueError(f"Invalid override '{item}', expected name=url")
        name, url = item.split("=", 1)
        name = name.strip().lower()
        if not name:
            raise ValueError(f"Invalid override '{item}', missing service name")
        overrides[name] = url.strip()
    return overrides


def _resolve_base_url(service: str, overrides: Mapping[str, str]) -> str:
    override = overrides.get(service)
    if override:
        return override.rstrip("/")
    for env_key in SERVICE_ENV_VARS.get(service, ()):  # type: ignore[arg-type]
        value = os.getenv(env_key)
        if value:
            return value.rstrip("/")
    return SERVICE_DEFAULTS.get(service, f"http://{service}").rstrip("/")


def _drain_service(
    client: httpx.Client,
    service: str,
    base_url: str,
    timeout: float,
    poll_interval: float,
    *,
    dry_run: bool = False,
) -> Dict[str, object]:
    start_endpoint = f"{base_url}/ops/drain/start"
    status_endpoint = f"{base_url}/ops/drain/status"
    if dry_run:
        logger.info("[dry-run] would drain %s via %s", service, start_endpoint)
        return {"service": service, "dry_run": True}

    logger.info("Triggering drain for %s via %s", service, start_endpoint)
    response = client.post(start_endpoint)
    response.raise_for_status()

    deadline = time.monotonic() + timeout
    while True:
        status_response = client.get(status_endpoint)
        status_response.raise_for_status()
        payload = status_response.json()
        inflight = int(payload.get("inflight", 0))
        draining = bool(payload.get("draining", False))
        logger.debug(
            "Drain status for %s: inflight=%s draining=%s", service, inflight, draining
        )
        if draining and inflight == 0:
            logger.info("Drain complete for %s", service)
            return payload
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Timed out waiting for {service} to finish draining (last={payload})"
            )
        time.sleep(poll_interval)


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "services",
        nargs="*",
        default=DEFAULT_SERVICE_ORDER,
        help="Ordered list of services to drain (default: policy risk oms)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("GRACEFUL_DRAIN_TIMEOUT", "180.0")),
        help="Seconds to wait for each service to finish draining",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between drain status polls",
    )
    parser.add_argument(
        "--base-url",
        action="append",
        help="Override service endpoint mapping, e.g. policy=http://localhost:8000",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the drain sequence without issuing HTTP requests",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging for drain polling",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        overrides = _parse_overrides(args.base_url)
    except ValueError as exc:  # pragma: no cover - argument parsing guard
        parser.error(str(exc))
        return 2

    services = [service.lower() for service in (args.services or DEFAULT_SERVICE_ORDER)]

    timeout = max(args.timeout, 1.0)
    poll_interval = max(args.poll_interval, 0.1)

    if args.dry_run:
        logger.warning("Running in dry-run mode; no HTTP requests will be performed")

    with httpx.Client(timeout=timeout) as client:
        for service in services:
            base_url = _resolve_base_url(service, overrides)
            try:
                _drain_service(
                    client,
                    service,
                    base_url,
                    timeout,
                    poll_interval,
                    dry_run=args.dry_run,
                )
            except httpx.HTTPError as exc:
                logger.error("HTTP error while draining %s: %s", service, exc)
                return 1
            except RuntimeError as exc:
                logger.error("Drain failed for %s: %s", service, exc)
                return 1
    logger.info("Drain sequence completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    sys.exit(main())
