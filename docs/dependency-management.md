# Dependency Management and Dual-Control Process

This repository now uses [Poetry](https://python-poetry.org/) to resolve and lock Python dependencies. The following workflow keeps runtime and tooling dependencies reproducible while satisfying dual-control requirements for production changes.

## Update cadence and review requirements

- **Cadence:** schedule dependency reviews at least once per month, or immediately when a high/critical CVE is disclosed against a locked package. The monthly review **must** include a `pip-compile` refresh of `pyproject.lock` to keep the pinned constraints aligned with `pyproject.toml`.
- **Dual control:** dependency lockfile updates must be raised in a pull request that is reviewed and approved by a second engineer. Automated bots **may not** self-approve Poetry/lockfile refreshes.
- **Security checks:** pull requests that modify dependencies must link to the `pip-audit` CI job run and highlight any suppressions or exceptions that were required. As part of the monthly cadence, capture scan artifacts from Trivy, Dependabot, or Snyk (whichever detected issues) and attach the remediation plan to the pull request description.

## Standard operating procedure

1. **Create a feature branch** for the dependency refresh.
2. **Install Poetry** (if not already installed): `python -m pip install poetry poetry-plugin-export`.
3. **Update dependencies** as needed, then refresh the lockfiles:
   ```bash
   poetry lock
   ```
   ```bash
   pip-compile --resolver=backtracking pyproject.toml -o pyproject.lock
   ```
4. **Regenerate `requirements.txt`** from the lock to keep legacy tooling in sync:
   ```bash
   poetry export --without-hashes --extras test --extras dev --format requirements.txt --output requirements.txt
   ```
5. **Run local validation and security scans:**
   ```bash
   poetry install
   python -m pip_audit --strict -r requirements.txt
   trivy fs --exit-code 1 --scanners vuln .
   poetry run pytest
   ```
6. **Open a pull request** summarising notable upgrades, attach the CI results, and request review from a second engineer. Include links to any Dependabot or Snyk advisories that informed the refresh and record the target remediation SLA.
7. **Merge only after approval** and once the security pipeline is green. If `pip-audit`, Trivy, Dependabot, or Snyk flag an issue, document the mitigation (upgrade, vendor patch, or approved exception) before merging and note the planned remediation date in the operations checklist.

By following this checklist we ensure each dependency change is reproducible, peer-reviewed, and automatically scanned for known vulnerabilities.
