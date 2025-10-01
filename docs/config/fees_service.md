# Fee Schedule Service API

The Fee Schedule Service exposes read-only endpoints for retrieving tier data, effective fees, and recent trading volume. All requests must include an administrative account identifier header:

- `X-Account-ID`: Must be set to one of the accounts listed in `services/common/security.py`. Requests missing this header or providing an unknown account will be rejected with HTTP 403.

## Endpoints

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/fees/effective` | Returns the effective fee rate, fee amount, and tier metadata for the supplied notional and liquidity side. |
| `GET` | `/fees/tiers` | Provides the full schedule of configured fee tiers. |
| `GET` | `/fees/volume30d` | Retrieves the 30-day rolling notional volume for the requesting account. |

Include the header alongside the existing query parameters when invoking these routes from internal services or integration tests.
