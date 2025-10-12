# Security CI Scanning Runbook

## Overview
The CI workflow builds the `risk-api` Docker image on every push and pull request and immediately scans the resulting image with [Trivy](https://github.com/aquasecurity/trivy). The scan runs as part of the `verify` job in `.github/workflows/ci.yaml`.

* Scan scope: the locally built `risk-api:ci` image.
* Severity gate: the pipeline fails when Trivy reports any **HIGH** or **CRITICAL** vulnerabilities.
* False positive handling: `ignore-unfixed: true` suppresses findings without upstream fixes. All other findings block the workflow until resolved or explicitly ignored in Trivy policy files.

## Responding to Failures
1. Inspect the failing GitHub Actions log for the "Scan risk API image with Trivy" step.
2. Identify affected packages and versions in the scan table output.
3. Remediate by:
   * Updating package bases in `deploy/docker/risk-api/Dockerfile`.
   * Adding or upgrading Python dependencies in `requirements*.txt` or `pyproject.toml`.
   * Pulling patched base images if the vulnerability originates upstream.
4. Re-run the CI workflow to confirm the scan passes.

## Running the Scan Locally
```
# Build the image just like CI
DOCKER_BUILDKIT=1 docker build -t risk-api:ci -f deploy/docker/risk-api/Dockerfile .

# Scan the local image (requires Trivy CLI)
trivy image --severity HIGH,CRITICAL --ignore-unfixed risk-api:ci
```

## Change Log
* 2025-10-12 — Added automated Trivy image scanning to CI with blocking HIGH/CRITICAL gates.
