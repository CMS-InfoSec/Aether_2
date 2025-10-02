# Authentication Service JWT Contract

The authentication service issues JSON Web Tokens (JWTs) that encode the
caller identity and authorization context.  Integrators consuming the service
should expect the following claim schema:

| Claim       | Type          | Description |
| ----------- | ------------- | ----------- |
| `sub`       | string        | Stable subject identifier (typically the admin's email). |
| `role`      | string        | Authorization role resolved during login.  Current roles include `admin` and `auditor`. |
| `iat`       | integer (sec) | Issued-at timestamp in UTC seconds. |
| `exp`       | integer (sec) | Expiration timestamp in UTC seconds. |
| `permissions` | array[string] | Optional list of fine-grained permissions for rich clients. |
| `read_only` | boolean       | Convenience flag indicating whether the role is read-only. |

Additional custom claims can be added through the authentication pipeline, but
these core fields are always present.  Downstream services **must** validate the
`role` claim before honouring privileged requests.

## Role resolution

The service determines the caller's role by inspecting the identity provider
profile (preferred usernames, explicit role claims) and server-side account
configuration.  When calling `services.auth.jwt_tokens.create_jwt` directly you
must now provide either:

* the explicit `role` argument (e.g. `role="admin"`), or
* a claims mapping that already contains a `role` entry.

If neither is supplied the helper will raise a `ValueError` to prevent issuing
role-less tokens.  This ensures integrators request the appropriate role for the
context they are operating in and makes subsequent validation deterministic.

## Validating roles in downstream services

Consumers should enforce authorization by:

1. Verifying the token signature using the shared `AUTH_JWT_SECRET`.
2. Confirming the token is not expired (`exp`).
3. Inspecting the `role` claim and rejecting any unexpected values.

This applies to internal services (e.g. OMS) and external tooling alike.  The
`role` should be treated as authoritative—if a user requires elevated
privileges they must complete the relevant onboarding process so the
authentication service issues a token with the desired role.
