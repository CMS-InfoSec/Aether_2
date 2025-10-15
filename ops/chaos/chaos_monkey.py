"""Chaos monkey utility for staging environments.

This module provides a CLI, scheduler, and FastAPI status endpoint that can
inject a variety of failures in staging. It is intentionally defensive to
prevent accidental execution in production clusters.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import random
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

try:  # pragma: no cover - FastAPI optional in lightweight environments.
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
except Exception:  # pragma: no cover - fallback when FastAPI is unavailable.
    logging.getLogger("chaos_monkey").warning(
        "FastAPI dependency missing; using chaos monkey fallbacks",
        exc_info=False,
    )

    class JSONResponse(dict):  # type: ignore[assignment]
        """Minimal JSONResponse replacement used in FastAPI-optional environments."""

        def __init__(self, content: Mapping[str, Any], status_code: int = 200) -> None:
            super().__init__(content=content, status_code=status_code)
            self.content = content
            self.status_code = status_code

    class FastAPI:  # type: ignore[assignment]
        """Simplified FastAPI stand-in that records registered routes."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.routes: Dict[str, SimpleNamespace] = {}

        def get(self, path: str, **_: Any):
            def _decorator(func):
                self.routes[path] = SimpleNamespace(endpoint=func, methods=["GET"], path=path)
                return func

            return _decorator

logger = logging.getLogger("chaos_monkey")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


EVENT_LOG: "deque[Dict[str, Any]]" = deque(maxlen=50)


def chaos_log(service: str, action: str, context_json: Mapping[str, Any], ts: Optional[float] = None) -> None:
    """Record a chaos action in memory and to the logger."""
    timestamp = ts if ts is not None else time.time()
    payload = {
        "service": service,
        "action": action,
        "context": dict(context_json),
        "ts": timestamp,
    }
    EVENT_LOG.appendleft(payload)
    logger.info("CHAOS %s %s %s", service, action, json.dumps(payload["context"]))


@dataclass
class ChaosAction:
    """Represents a scheduled chaos action."""

    name: str
    weight: float
    args: MutableMapping[str, Any] = field(default_factory=dict)


@dataclass
class ChaosConfig:
    """Configuration for the chaos scheduler."""

    min_interval_seconds: float = 30.0
    max_interval_seconds: float = 180.0
    actions: Sequence[ChaosAction] = field(default_factory=list)

    def validate(self) -> None:
        if self.min_interval_seconds <= 0 or self.max_interval_seconds <= 0:
            raise ValueError("Intervals must be positive")
        if self.min_interval_seconds > self.max_interval_seconds:
            raise ValueError("min_interval_seconds cannot exceed max_interval_seconds")
        if not self.actions:
            raise ValueError("At least one action must be configured for the scheduler")
        negative = [a.name for a in self.actions if a.weight <= 0]
        if negative:
            raise ValueError(f"Actions must have positive weights: {negative}")

    def weighted_choices(self) -> List[str]:
        return [action.name for action in self.actions]

    def choose_action(self) -> ChaosAction:
        self.validate()
        total_weight = sum(action.weight for action in self.actions)
        pick = random.uniform(0, total_weight)
        cumulative = 0.0
        for action in self.actions:
            cumulative += action.weight
            if pick <= cumulative:
                return action
        return self.actions[-1]


class SafetyError(RuntimeError):
    """Raised when safety conditions are not met."""


def require_safety() -> None:
    """Ensure chaos can only run in explicitly enabled, non-production clusters."""
    enabled = os.getenv("CHAOS_ENABLED", "").lower() == "true"
    cluster_label = os.getenv("CHAOS_CLUSTER_LABEL", "")
    if not enabled:
        raise SafetyError("CHAOS_ENABLED=true is required to run chaos monkey")
    if not cluster_label:
        raise SafetyError("CHAOS_CLUSTER_LABEL must be provided")
    if cluster_label.lower() in {"prod", "production", "main"}:
        raise SafetyError(f"Chaos monkey cannot run in production cluster '{cluster_label}'")


class KubernetesController:
    """Helper for interacting with Kubernetes."""

    def __init__(self, namespace: Optional[str] = None) -> None:
        self.namespace = namespace or os.getenv("CHAOS_NAMESPACE", "default")
        self._core_api = None

    @property
    def core_api(self):
        if self._core_api is None:
            kubernetes = self._load_kubernetes()
            self._configure_kubernetes(kubernetes)
            self._core_api = kubernetes.client.CoreV1Api()
        return self._core_api

    @staticmethod
    def _load_kubernetes():
        import importlib

        spec = importlib.util.find_spec("kubernetes")
        if spec is None:
            raise RuntimeError("The 'kubernetes' package is required for pod termination actions")
        kubernetes = importlib.import_module("kubernetes")
        return kubernetes

    @staticmethod
    def _configure_kubernetes(kubernetes: Any) -> None:
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()

    def kill_pod(self, service: str) -> str:
        label_selector = f"app={service}"
        pods = self.core_api.list_namespaced_pod(self.namespace, label_selector=label_selector).items
        if not pods:
            raise RuntimeError(f"No pods found for service '{service}' in namespace '{self.namespace}'")
        victim = random.choice(pods)
        pod_name = victim.metadata.name
        self.core_api.delete_namespaced_pod(name=pod_name, namespace=self.namespace)
        return pod_name


class MockKrakenInjector:
    """Controls websocket chaos injection for the mock_kraken service."""

    def __init__(self, endpoint: Optional[str] = None) -> None:
        base = endpoint or os.getenv("MOCK_KRAKEN_ADMIN", "http://mock-kraken.admin.svc.cluster.local")
        self.endpoint = base.rstrip("/")

    def _client(self):
        import importlib

        spec = importlib.util.find_spec("httpx")
        if spec is None:
            raise RuntimeError("httpx is required for latency and disconnect injections")
        return importlib.import_module("httpx")

    def inject_latency(self, latency_ms: int) -> None:
        httpx = self._client()
        url = f"{self.endpoint}/chaos/latency"
        response = httpx.post(url, json={"latency_ms": latency_ms})
        response.raise_for_status()

    def disconnect(self) -> None:
        httpx = self._client()
        url = f"{self.endpoint}/chaos/disconnect"
        response = httpx.post(url)
        response.raise_for_status()


class KafkaProxyController:
    """Controls message dropping through a proxy shim."""

    def __init__(self, endpoint: Optional[str] = None) -> None:
        base = endpoint or os.getenv("KAFKA_PROXY_ADMIN", "http://kafka-proxy.admin.svc.cluster.local")
        self.endpoint = base.rstrip("/")

    def _client(self):
        import importlib

        spec = importlib.util.find_spec("httpx")
        if spec is None:
            raise RuntimeError("httpx is required for Kafka drop injections")
        return importlib.import_module("httpx")

    def drop_messages(self, topic: str, pct: int) -> None:
        if not 0 <= pct <= 100:
            raise ValueError("pct must be between 0 and 100")
        httpx = self._client()
        url = f"{self.endpoint}/chaos/drop"
        response = httpx.post(url, json={"topic": topic, "percentage": pct})
        response.raise_for_status()


class ChaosMonkey:
    """Main orchestration class for chaos injections."""

    def __init__(
        self,
        kube: Optional[KubernetesController] = None,
        mock_kraken: Optional[MockKrakenInjector] = None,
        kafka_proxy: Optional[KafkaProxyController] = None,
    ) -> None:
        self.kube = kube or KubernetesController()
        self.mock_kraken = mock_kraken or MockKrakenInjector()
        self.kafka_proxy = kafka_proxy or KafkaProxyController()

    def kill_service_pod(self, service: str) -> Dict[str, Any]:
        pod = self.kube.kill_pod(service)
        payload = {"service": service, "pod": pod}
        chaos_log(service, "kill_pod", payload)
        return payload

    def introduce_latency(self, latency_ms: int) -> Dict[str, Any]:
        bounded_latency = max(0, min(latency_ms, 10_000))
        self.mock_kraken.inject_latency(bounded_latency)
        payload = {"target": "mock_kraken", "latency_ms": bounded_latency}
        chaos_log("mock_kraken", "latency", payload)
        return payload

    def disconnect_ws(self) -> Dict[str, Any]:
        self.mock_kraken.disconnect()
        payload = {"target": "mock_kraken", "action": "disconnect"}
        chaos_log("mock_kraken", "disconnect", payload)
        return payload

    def drop_kafka_messages(self, topic: str, pct: int) -> Dict[str, Any]:
        self.kafka_proxy.drop_messages(topic, pct)
        payload = {"topic": topic, "pct": pct}
        chaos_log("kafka_proxy", "drop", payload)
        return payload

    async def run_schedule(self, config: ChaosConfig) -> None:
        require_safety()
        config.validate()
        logger.info(
            "Starting chaos schedule with %.1f-%.1f second interval", config.min_interval_seconds, config.max_interval_seconds
        )
        stats: Counter[str] = Counter()
        while True:
            action = config.choose_action()
            stats[action.name] += 1
            try:
                await self._execute_action(action)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Failed to execute chaos action %s: %s", action.name, exc)
            delay = random.uniform(config.min_interval_seconds, config.max_interval_seconds)
            logger.info("Next chaos action in %.1f seconds. Counts=%s", delay, dict(stats))
            await asyncio.sleep(delay)

    async def _execute_action(self, action: ChaosAction) -> None:
        if action.name == "kill":
            service = action.args.get("service", random.choice(["oms", "policy", "risk"]))
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.kill_service_pod, service)
        elif action.name == "latency":
            latency_ms = int(action.args.get("latency_ms", random.randint(200, 1000)))
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.introduce_latency, latency_ms)
        elif action.name == "disconnect":
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.disconnect_ws)
        elif action.name == "drop":
            topic = action.args.get("topic", random.choice(["intents", "fills"]))
            pct = int(action.args.get("pct", random.choice([5, 10, 15])))
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.drop_kafka_messages, topic, pct)
        else:
            raise ValueError(f"Unknown chaos action '{action.name}'")


def build_app(config: ChaosConfig) -> FastAPI:
    app = FastAPI()

    @app.get("/chaos/status")
    def status() -> JSONResponse:
        events = list(EVENT_LOG)
        response = {
            "config": {
                "min_interval_seconds": config.min_interval_seconds,
                "max_interval_seconds": config.max_interval_seconds,
                "actions": [
                    {"name": action.name, "weight": action.weight, "args": dict(action.args)}
                    for action in config.actions
                ],
            },
            "events": events,
        }
        return JSONResponse(response)

    return app


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chaos monkey CLI for staging environments")
    subparsers = parser.add_subparsers(dest="command", required=True)

    kill_parser = subparsers.add_parser("kill", help="Kill a pod for a target service")
    kill_parser.add_argument("--service", choices=["oms", "policy", "risk"], required=True)

    latency_parser = subparsers.add_parser("latency", help="Introduce websocket latency")
    latency_parser.add_argument("--target", choices=["mock_kraken"], required=True)
    latency_parser.add_argument("--ms", type=int, default=400)
    latency_parser.add_argument("--disconnect", action="store_true", help="Force a websocket disconnect")

    drop_parser = subparsers.add_parser("drop", help="Drop Kafka messages via proxy")
    drop_parser.add_argument("--topic", choices=["intents", "fills"], required=True)
    drop_parser.add_argument("--pct", type=int, required=True)

    subparsers.add_parser("serve", help="Run scheduler and status API")

    return parser.parse_args(argv)


def handle_cli(args: argparse.Namespace) -> Optional[Any]:
    monkey = ChaosMonkey()
    if args.command == "kill":
        require_safety()
        return monkey.kill_service_pod(args.service)
    if args.command == "latency":
        require_safety()
        if args.disconnect:
            return monkey.disconnect_ws()
        return monkey.introduce_latency(args.ms)
    if args.command == "drop":
        require_safety()
        return monkey.drop_kafka_messages(args.topic, args.pct)
    if args.command == "serve":
        require_safety()
        actions = [
            ChaosAction("kill", weight=3.0),
            ChaosAction("latency", weight=2.0),
            ChaosAction("drop", weight=1.5),
            ChaosAction("disconnect", weight=0.5),
        ]
        config = ChaosConfig(actions=actions)
        app = build_app(config)
        asyncio.run(run_service(monkey, config, app))
        return None
    raise ValueError(f"Unknown command {args.command}")


async def run_service(monkey: ChaosMonkey, config: ChaosConfig, app: FastAPI) -> None:
    from uvicorn import Config, Server

    async def scheduler_task() -> None:
        await monkey.run_schedule(config)

    server = Server(Config(app, host="0.0.0.0", port=int(os.getenv("CHAOS_PORT", "8080")), loop="asyncio"))
    schedule = asyncio.create_task(scheduler_task())
    try:
        await server.serve()
    finally:
        schedule.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await schedule


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    result = handle_cli(args)
    if result is not None:
        json.dump(result, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
