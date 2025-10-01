# On-Call Readiness Checklist

Use this checklist during weekly reviews to verify operational readiness and document sign-off.

## Documentation Verification
- [ ] Confirm all runbooks in `docs/runbooks/` are up to date and include current contacts.
- [ ] Review [`docs/slo.md`](../slo.md) and ensure alert thresholds match Prometheus configuration.
- [ ] Validate the incident response escalation paths are recorded in the internal directory.

## Monitoring & Alerting
- [ ] Check Prometheus alerts `oms_latency_slo_breach`, `ws_latency_slo_breach`, `kill_switch_slo_warning`, and `model_canary_promotion_slow` fired within test windows or have recent successful test annotations.
- [ ] Ensure Grafana dashboards display the correct SLI panels for OMS latency, WebSocket latency, kill-switch response, and model canary duration.
- [ ] Verify synthetic checks simulate order placement and WebSocket subscription at least once per day.

## Automation & Tooling
- [ ] Run dry-run execution of `services.oms.tools kill-switch --status` to ensure connectivity.
- [ ] Confirm Argo workflows `ml-canary-deployment`, `secret-rotation`, and `kraken-ingest` have no suspended steps and latest runs succeeded.
- [ ] Validate Vault secret rotation pipeline has access tokens expiring at least 7 days in the future.

## Communication
- [ ] Review #ops-trading, #ops-realtime, and #security-ops channels for unresolved incident follow-ups.
- [ ] Update the on-call schedule and escalation matrix if personnel changes occurred.
- [ ] Record checklist completion in the operations log with timestamp and reviewer signature.

---

**Reviewer Name:** ____________________  
**Date:** ____________________
