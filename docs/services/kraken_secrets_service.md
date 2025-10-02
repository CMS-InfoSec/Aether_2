# Kraken Secrets Service

The Kraken Secrets Service manages encryption, validation, and rotation of exchange API credentials.

## Actor Attribution

* Rotation requests require a bearer token configured via `SECRETS_SERVICE_AUTH_TOKENS` and labelled in `KRAKEN_SECRETS_AUTH_TOKENS`.
* The actor recorded in metadata, logs, and audit entries is derived from the authenticated token's label. The optional `actor` field in the request body is ignored unless it matches the authenticated identity.
* If the supplied payload actor conflicts with the authenticated actor label, the service rejects the request with `400 Actor identity does not match provided credentials` to prevent spoofed attribution.

API consumers no longer need to send an `actor` value when rotating a secret—the service will attribute the rotation automatically based on the caller's credentials.
