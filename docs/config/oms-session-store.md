# OMS Session Store Configuration

The OMS FastAPI service requires a shared session backend so that
administrator logins remain valid when requests are routed to different
pods. Set the `SESSION_REDIS_URL` environment variable to the connection
string of the Redis (or Redis-compatible) instance that should back OMS
sessions. A typical cluster configuration points at the shared Redis
service deployed alongside the platform:

```shell
SESSION_REDIS_URL=redis://redis:6379/0
```

At runtime the OMS will refuse to start unless `SESSION_REDIS_URL` is
defined. The `SESSION_TTL_MINUTES` variable still controls how long each
session remains valid (default: 60 minutes) and must be configured as a
strictly positive integer so that administrator sessions do not expire
immediately due to misconfiguration.

For local testing you may use an in-memory store by explicitly setting
`SESSION_REDIS_URL=memory://<name>`, but this configuration does **not**
persist sessions across pods and should never be used in production.
