# Policy Model Server Runbook

## Authentication Requirements

All requests to the Policy Model Server must include administrative session
credentials. Operators and automated systems should provide both of the
following headers on every call:

- `Authorization: Bearer <session-token>` — a valid admin session token issued
  by the authentication service. Tokens are tied to a specific administrator
  account and expire per the auth service configuration.
- `X-Account-ID: <admin-account>` — the administrator account identifier that
  matches the authenticated session. The service rejects mismatched or missing
  identifiers with `401`/`403` responses.

Use the same credential pair for both `/models/predict` and `/models/active`
requests. When troubleshooting, confirm upstream callers are forwarding these
headers. Session tokens can be generated through the operations console or the
`auth` CLI. Rotate tokens if you suspect compromise or authorization failures
persist after refresh.
