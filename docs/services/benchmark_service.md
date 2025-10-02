# Benchmark Service Access Control

Benchmark curve management is limited to authenticated administrator sessions.  The
service now enforces the standard `require_admin_account` dependency for both
write and read operations:

* `POST /benchmark/curves` – callers must supply an administrator session token
  via the `Authorization: Bearer <session>` header.  The resolved admin identity
  is recorded with each upsert request for auditability.
* `GET /benchmark/compare` – access requires the same administrator session
  credentials.  Comparison requests are logged against the authenticated admin
  identity.

Benchmark operators should ensure that automation and manual tooling propagate a
valid admin session token when invoking these endpoints.  Requests without an
authorized admin identity are rejected with `403 Forbidden`.
