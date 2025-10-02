# Scenario Simulator Service

The Scenario Simulator exposes a `/scenario/run` endpoint that projects portfolio risk metrics under configurable shock parameters. The service now enforces administrative authentication using the shared `require_admin_account` dependency:

* Requests **must** include a valid admin session bearer token in the `Authorization` header. Optionally, callers can include an `X-Account-ID` header when acting on behalf of another admin account.
* Calls without credentials or with non-admin sessions will be rejected with `401 Unauthorized` or `403 Forbidden` responses before any portfolio calculations execute.

When authenticated, the verified actor account is recorded alongside each scenario run in the audit log. This ensures audit trails reflect the actual caller instead of trusting request payloads.
