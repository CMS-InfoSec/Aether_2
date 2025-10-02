# Explainability Service Runbook

## Overview

The explainability service backs the UI endpoint that surfaces trade-level
feature attributions. Operators can use it to understand what signals were most
influential when a model recommended an execution.

## Authentication

`GET /explain/trade` is **admin only**. Requests must include:

* `Authorization: Bearer <admin session token>` – the token must correspond to a
  verified administrator session (for example the `company` operations account).
* Optional `X-Account-ID` header when impersonating another admin account. If
  provided, it must match the authenticated session or the request will be
  rejected.

Requests missing the authorization header receive `401 Unauthorized`. Sessions
that are valid but not associated with an administrator account are rejected
with `403 Forbidden`.

## Troubleshooting

* **401 errors** – verify the caller is including the `Authorization` header and
  that the token has not expired.
* **403 errors** – confirm the session belongs to an admin account or adjust the
  requested `X-Account-ID` header to match the session.

Successful responses log the requesting admin account for auditability. Use the
central logging system to trace requests when investigating anomalies.
