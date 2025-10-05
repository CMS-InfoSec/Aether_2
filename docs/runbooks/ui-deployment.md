# UI Deployment Runbook

This runbook documents how to build and deploy the [Aether-2-UI](https://github.com/CMS-InfoSec/Aether-2-UI)
frontend so that it targets the production backend services that ship with this
repository.

## 1. Build the UI container

1. Clone the UI repository and install dependencies:
   ```bash
   git clone git@github.com:CMS-InfoSec/Aether-2-UI.git
   cd Aether-2-UI
   pnpm install
   ```
2. Set the public API base URL so the compiled assets point at the production
   ingress. The backend risk API is exposed at
   `https://risk.aether.example.com` by default.
   ```bash
   export NEXT_PUBLIC_API_BASE_URL="https://risk.aether.example.com"
   export VITE_API_BASE_URL="https://risk.aether.example.com"
   ```
3. Build and publish the container image:
   ```bash
   pnpm run build
   pnpm run docker:build -- --tag ghcr.io/aether/aether-2-ui:<release>
   docker push ghcr.io/aether/aether-2-ui:<release>
   ```

## 2. Update the Helm release

1. In this repository, bump the UI image tag and, if required, override the API
   base URL in `deploy/helm/aether-platform/values.yaml`:
   ```yaml
   ui:
     image:
       tag: <release>
     env:
       - name: NEXT_PUBLIC_API_BASE_URL
         value: https://risk.aether.example.com
       - name: VITE_API_BASE_URL
         value: https://risk.aether.example.com
   ```
2. Deploy the update via Helm/ArgoCD:
   ```bash
   helm upgrade aether-platform deploy/helm/aether-platform -n aether-prod \
     -f deploy/helm/aether-platform/values.yaml
   ```
   or sync the `aether-platform` application in ArgoCD if using GitOps.

## 3. Post-deployment validation

* Browse to `https://ui.aether.example.com` and confirm the UI loads without
  console errors.
* Exercise authenticated workflows that call the backend (e.g. loading account
  summaries, OMS screens) and verify the network requests target
  `https://risk.aether.example.com`.
* Inspect the UI deployment logs for configuration issues:
  ```bash
  kubectl logs deploy/aether-2-ui -n aether-prod
  ```

Document any deviations (different ingress host names, alternative registries,
etc.) alongside the release manifest so the build pipeline stays reproducible.

