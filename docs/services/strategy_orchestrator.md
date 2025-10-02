# Strategy Orchestrator API

The strategy orchestrator coordinates strategy registration, state management, and risk routing.
All endpoints are protected and require an authenticated administrator session token.

## Authentication
- Supply a valid administrator token from the authentication service using the
  `Authorization: Bearer <token>` header on every request.
- The orchestrator validates the token against the admin allow-list. Requests made with tokens for
  non-admin users are rejected with `403 Forbidden`.
- The `X-Account-ID` header is no longer required; the orchestrator logs the authenticated actor
  derived from the token for auditing.

## Endpoints
| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/strategy/register` | Register or update a strategy allocation. |
| `POST` | `/strategy/toggle` | Enable or disable an existing strategy. |
| `GET` | `/strategy/status` | Retrieve the current strategy allocation snapshot. |
| `POST` | `/strategy/intent` | Forward a trade intent to the risk engine with strategy context. |
| `GET` | `/strategy/signals` | List the available strategy-aligned signal channels. |

All endpoints return `401 Unauthorized` if the `Authorization` header is missing or invalid, and
`403 Forbidden` if the session resolves to a caller without admin privileges.
