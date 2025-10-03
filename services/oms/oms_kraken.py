
"""Kraken OMS credential hot-reload utilities."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from services.common.adapters import KrakenSecretManager

try:  # pragma: no cover - optional dependency in some environments
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
except Exception:  # pragma: no cover - watchdog is optional during tests
    FileSystemEventHandler = object  # type: ignore[misc, assignment]
    Observer = None  # type: ignore[assignment]
    PollingObserver = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency for cluster environments
    from kubernetes import watch as kube_watch
except Exception:  # pragma: no cover - kubernetes watch may be unavailable
    kube_watch = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)

ANNOTATION_ROTATED_AT = "aether.kraken/lastRotatedAt"


def _default_secret_path(account_id: str) -> Optional[Path]:
    """Resolve the default credential mount path for an account."""

    env_key = f"AETHER_{account_id.upper()}_KRAKEN_SECRET_PATH"
    raw_path = os.getenv(env_key) or os.getenv("KRAKEN_SECRET_PATH")
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_dir():
        path = path / "kraken.json"
    return path


def _default_observer_factory() -> Optional[Any]:
    """Return the preferred watchdog observer implementation."""

    if Observer is not None:  # pragma: no branch - preference order
        return Observer()
    if PollingObserver is not None:
        return PollingObserver()
    return None


def _default_watch_factory() -> Optional[Any]:
    if kube_watch is None:
        return None
    try:
        return kube_watch.Watch()
    except Exception:  # pragma: no cover - kubernetes watch creation errors
        LOGGER.debug("Unable to create Kubernetes watch", exc_info=True)
        return None


def _sanitize_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Return a sanitized copy of metadata with credentials masked."""

    sanitized = dict(metadata)
    for key in ("api_key", "api_secret", "secret", "key"):
        if key in sanitized and sanitized[key]:
            sanitized[key] = "***"
    return sanitized


class _SecretChangeHandler(FileSystemEventHandler):
    """Watchdog handler that notifies a callback when the secret changes."""

    def __init__(self, target: Path, callback: Callable[[], None]) -> None:
        super().__init__()
        self._target = target.resolve()
        self._callback = callback

    # ``watchdog`` emits multiple event types – treat any relevant one as a
    # refresh signal. ``FileSystemEventHandler`` uses snake_case names on
    # Python, but attribute access falls back to ``__getattr__`` in tests.
    def on_any_event(self, event: Any) -> None:  # pragma: no cover - thin wrapper
        try:
            if getattr(event, "is_directory", False):
                return
            candidates: List[str] = []
            src_path = getattr(event, "src_path", None)
            if src_path:
                candidates.append(src_path)
            dest_path = getattr(event, "dest_path", None)
            if dest_path:
                candidates.append(dest_path)
            for candidate in candidates:
                if Path(candidate).resolve() == self._target:
                    self._callback()
                    return
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to handle secret change event")


def _material_changed(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
    """Return ``True`` when the credential material differs."""

    for key in ("api_key", "api_secret"):
        if old.get(key) != new.get(key):
            return True
    old_meta = old.get("metadata") or {}
    new_meta = new.get("metadata") or {}
    if isinstance(old_meta, dict) and isinstance(new_meta, dict):
        old_rotated = _extract_rotated_at(old_meta)
        new_rotated = _extract_rotated_at(new_meta)
        if old_rotated != new_rotated:
            return True
    return False


def _extract_rotated_at(metadata: Dict[str, Any]) -> Optional[str]:
    rotated = metadata.get("rotated_at") or metadata.get("last_rotated_at")
    if rotated:
        return str(rotated)
    annotations = metadata.get("annotations")
    if isinstance(annotations, dict):
        value = annotations.get(ANNOTATION_ROTATED_AT)
        if value:
            return str(value)
    return None


def _ensure_rotation_metadata(metadata: Dict[str, Any], *, source: str) -> str:
    """Ensure rotation metadata is present and normalized."""

    if not isinstance(metadata, dict):
        raise RuntimeError(f"{source} missing rotation metadata block")

    rotated_at = _extract_rotated_at(metadata)
    if rotated_at is None:
        raise RuntimeError(
            f"{source} missing rotation metadata; unable to determine credential freshness"
        )

    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    annotations = dict(annotations)
    annotations.setdefault(ANNOTATION_ROTATED_AT, rotated_at)
    metadata["annotations"] = annotations
    metadata["rotated_at"] = rotated_at
    return rotated_at


@dataclass
class _CredentialSnapshot:
    version: int
    payload: Dict[str, Any]


class KrakenCredentialWatcher:
    """Monitors Kraken credential mounts and refreshes when they change."""

    _instances: Dict[str, "KrakenCredentialWatcher"] = {}
    _instances_lock = threading.Lock()

    def __init__(
        self,
        account_id: str,
        *,
        secret_path: Optional[Path] = None,
        manager: Optional[KrakenSecretManager] = None,
        observer_factory: Optional[Callable[[], Any]] = None,
        watch_factory: Optional[Callable[[], Any]] = None,
        refresh_interval: float = 30.0,
        debounce_seconds: float = 0.5,
    ) -> None:
        self.account_id = account_id
        self._manager = manager or KrakenSecretManager(account_id)
        self._secret_path = secret_path or _default_secret_path(account_id)
        self._refresh_interval = refresh_interval
        self._debounce_seconds = debounce_seconds
        self._observer_factory = observer_factory or _default_observer_factory
        self._watch_factory = watch_factory or _default_watch_factory

        self._lock = threading.RLock()
        self._snapshot = _CredentialSnapshot(version=0, payload={})
        self._listeners: List[Callable[[Dict[str, Any], int], None]] = []
        self._stop_event = threading.Event()
        self._reload_signal = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"kraken-credential-watcher-{account_id}",
            daemon=True,
        )

        self._observer: Any | None = None
        self._watch: Any | None = None
        self._watch_thread: threading.Thread | None = None
        self._load_initial_snapshot()
        self._start_observer()
        self._start_kubernetes_watch()
        self._thread.start()

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    @classmethod
    def instance(cls, account_id: str) -> "KrakenCredentialWatcher":
        """Return a shared watcher for the provided account."""

        with cls._instances_lock:
            watcher = cls._instances.get(account_id)
            if watcher is None:
                watcher = cls(account_id)
                cls._instances[account_id] = watcher
            return watcher

    @classmethod
    def reset_instances(cls) -> None:
        """Dispose all shared watchers (used in tests)."""

        with cls._instances_lock:
            instances = list(cls._instances.values())
            cls._instances.clear()
        for watcher in instances:
            watcher.close()

    def close(self) -> None:
        """Stop the watcher and associated resources."""

        if self._stop_event.is_set():
            return
        self._stop_event.set()
        self._reload_signal.set()
        if self._observer is not None:
            try:
                self._observer.stop()
                self._observer.join(timeout=2.0)
            except Exception:  # pragma: no cover - observer specific errors
                LOGGER.exception("Failed stopping watchdog observer")
        if self._watch is not None:
            try:
                self._watch.stop()
            except Exception:  # pragma: no cover - defensive stop
                LOGGER.debug("Failed stopping kubernetes watch", exc_info=True)
        if self._watch_thread is not None and self._watch_thread.is_alive():
            self._watch_thread.join(timeout=2.0)
        self._watch_thread = None
        self._watch = None
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
        with self._instances_lock:
            for key, watcher in list(self._instances.items()):
                if watcher is self:
                    self._instances.pop(key, None)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def subscribe(self, callback: Callable[[Dict[str, Any], int], None]) -> Callable[[], None]:
        """Register a callback invoked when credentials change."""

        with self._lock:
            self._listeners.append(callback)

        def _unsubscribe() -> None:
            with self._lock:
                try:
                    self._listeners.remove(callback)
                except ValueError:
                    pass

        return _unsubscribe

    def snapshot(self) -> Tuple[Dict[str, Any], int]:
        """Return the latest credential payload and its version."""

        with self._lock:
            return dict(self._snapshot.payload), self._snapshot.version

    def trigger_refresh(self) -> None:
        """Force a credential refresh (used in tests or manual rotation)."""

        self._reload_signal.set()

    def wait_for_version(self, target_version: int, timeout: float = 5.0) -> bool:
        """Block until the watcher observes ``target_version`` or timeout."""

        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self._snapshot.version >= target_version:
                    return True
            time.sleep(0.05)
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_initial_snapshot(self) -> None:
        try:
            payload = self._load_credentials()
        except Exception as exc:  # pragma: no cover - initialization errors
            LOGGER.exception("Unable to load Kraken credentials: %s", exc)
            payload = {}
        with self._lock:
            self._snapshot = _CredentialSnapshot(version=0, payload=payload)

    def _start_observer(self) -> None:
        if self._secret_path is None:
            return
        if not self._secret_path.exists():
            return
        observer = self._observer_factory() if callable(self._observer_factory) else None
        if observer is None:
            return
        handler = _SecretChangeHandler(self._secret_path, self._reload_signal.set)
        try:
            observer.schedule(handler, str(self._secret_path.parent), recursive=False)
        except Exception:  # pragma: no cover - observer scheduling issues
            LOGGER.exception("Failed to schedule watchdog observer")
            return
        observer.daemon = True
        observer.start()
        self._observer = observer

    def _start_kubernetes_watch(self) -> None:
        if self._watch_thread is not None:
            return
        if not callable(self._watch_factory):
            return
        watch = self._watch_factory()
        if watch is None:
            return
        core_v1 = getattr(self._manager.secret_store, "core_v1", None)
        namespace = getattr(self._manager.secret_store, "namespace", None)
        if not hasattr(core_v1, "list_namespaced_secret") or namespace is None:
            return

        def _loop() -> None:
            field_selector = f"metadata.name={self._manager.secret_name}"
            while not self._stop_event.is_set():
                try:
                    stream = watch.stream(
                        core_v1.list_namespaced_secret,
                        namespace=namespace,
                        field_selector=field_selector,
                        timeout_seconds=5,
                    )
                    for event in stream:
                        if self._stop_event.is_set():
                            break
                        if isinstance(event, dict):
                            event_type = str(event.get("type", "")).upper()
                            if event_type in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK"}:
                                self._reload_signal.set()
                                continue
                        self._reload_signal.set()
                except Exception:  # pragma: no cover - kube watch failures
                    LOGGER.debug("Kubernetes watch error", exc_info=True)
                    time.sleep(1.0)
                else:
                    break
            try:
                watch.stop()
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Failed to stop kubernetes watch", exc_info=True)

        thread = threading.Thread(
            target=_loop,
            name=f"kraken-secret-watch-{self.account_id}",
            daemon=True,
        )
        thread.start()
        self._watch_thread = thread
        self._watch = watch

    def _run(self) -> None:
        while not self._stop_event.is_set():
            triggered = self._reload_signal.wait(timeout=self._refresh_interval)
            if self._stop_event.is_set():
                break
            if triggered:
                self._reload_signal.clear()
            if self._debounce_seconds:
                time.sleep(self._debounce_seconds)
            try:
                self._refresh_credentials()
            except Exception:  # pragma: no cover - handled with logging
                LOGGER.exception("Failed refreshing Kraken credentials")

    def _refresh_credentials(self) -> None:
        if self._stop_event.is_set():
            return
        if (
            self._observer is None
            and self._secret_path is not None
            and self._secret_path.exists()
        ):
            self._start_observer()
        payload = self._load_credentials()
        with self._lock:
            if not self._snapshot.payload or _material_changed(self._snapshot.payload, payload):
                version = self._snapshot.version + 1
                self._snapshot = _CredentialSnapshot(version=version, payload=payload)
                listeners = list(self._listeners)
            else:
                listeners = []
                version = self._snapshot.version
        for callback in listeners:
            try:
                callback(dict(payload), version)
            except Exception:  # pragma: no cover - listeners handle their own errors
                LOGGER.exception("Credential listener failed for account %s", self.account_id)

    def _load_credentials(self) -> Dict[str, Any]:
        if self._secret_path and self._secret_path.exists():
            return self._load_from_file(self._secret_path)
        # Fallback to secret manager lookup
        payload = self._manager.get_credentials()
        payload.setdefault("metadata", {})
        payload["metadata"] = _sanitize_metadata(dict(payload["metadata"]))
        _ensure_rotation_metadata(
            payload["metadata"],
            source=f"Kraken credentials for account '{self.account_id}'",
        )
        return payload

    def _load_from_file(self, path: Path) -> Dict[str, Any]:
        data = json.loads(path.read_text())
        key = data.get("api_key") or data.get("key")
        secret = data.get("api_secret") or data.get("secret")
        if not key or not secret:
            raise ValueError(f"Credential file at {path} missing key/secret")
        raw_metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        metadata = dict(raw_metadata)
        metadata.setdefault("secret_path", str(path))
        metadata["material_present"] = True
        metadata["api_key"] = key
        metadata["api_secret"] = secret
        rotated_at = _ensure_rotation_metadata(
            metadata,
            source=f"Credential file at {path}",
        )
        sanitized = _sanitize_metadata(metadata)
        sanitized["annotations"] = dict(metadata.get("annotations", {}))
        sanitized["rotated_at"] = rotated_at
        return {"api_key": key, "api_secret": secret, "metadata": sanitized}


__all__ = ["KrakenCredentialWatcher"]


