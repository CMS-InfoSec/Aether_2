# Production Readiness Review

## Summary

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ❌ | FastAPI deployments reference `fastapi-credentials`/`fastapi-secrets` secrets that are not defined, so pods will crash on startup. |
| Reliability & Observability | ❌ | Prometheus alerts depend on `kill_switch_response_seconds`, but the kill-switch service never emits that metric, leaving the SLO blind. |
| Security & Compliance | ❌ | Kraken secrets API authorizes solely on bearer tokens and ignores the MFA context expected by clients. |
| Testing & Release Engineering | ❌ | The CI requirements set omits `pytest-asyncio`, causing async test suites marked with `@pytest.mark.asyncio` to error in minimal installs. |
| Data Integrity & Backup | ❌ | Disaster-recovery tooling logs every action but never provisions the target table, so the very first snapshot/restore aborts with an undefined-table error. |
| API & Integration Consistency | ⚠️ | Binance and Coinbase adapters are stubs that raise `NotImplementedError`, blocking multi-exchange routing until completed. |
| ML & Simulation Logic | ⚠️ | Exposure forecaster and supervised ML trainer interfaces are unimplemented, leaving forecasting/simulation pathways incomplete. |
| Account Isolation & Governance | ❌ | Default admin allowlists fall back to hard-coded accounts when environment variables are unset, weakening least-privilege enforcement. |
| UI Integration & Frontend Connectivity | ❌ | The React API key manager calls `/secrets/status`/`/secrets/audit`, but the backend exposes only `/secrets/kraken/*`, so the UI cannot load or rotate credentials. |

## New Findings

- Missing Kubernetes secret manifests (`fastapi-credentials`, `fastapi-secrets`) referenced by FastAPI deployments cause configuration load failures during pod startup.
- Kill-switch observability gap: the alert rules expect `kill_switch_response_seconds`, yet the kill-switch service never records or exports that metric.
- Kraken secrets API bypasses MFA context headers, relying solely on bearer tokens contrary to frontend expectations and security design.
- `requirements-ci.txt` excludes `pytest-asyncio`, so async tests fail in minimal CI environments where only the CI requirements are installed.
- Disaster recovery playbook writes to a `dr_log` table without ever creating it, preventing the first snapshot/restore from completing.
- Exchange adapters for Binance and Coinbase raise `NotImplementedError`, leaving those integrations non-functional.
- Exposure forecasting and supervised ML trainer abstractions remain abstract-only, blocking downstream simulation workflows.
- Admin/Director allowlists silently default to baked-in identities (`company`, `director-1`, `director-2`) when env overrides are missing, undermining account isolation.
- Secrets manager frontend routes do not match backend endpoints, so credential status/audit calls fail and rotation actions cannot complete from the UI.
