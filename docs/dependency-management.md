# Dependency Management and Dual-Control Process

This repository now uses [Poetry](https://python-poetry.org/) to resolve and lock Python dependencies. The following workflow keeps runtime and tooling dependencies reproducible while satisfying dual-control requirements for production changes.

## Update cadence and review requirements

- **Cadence:** schedule dependency reviews at least once per month, or immediately when a high/critical CVE is disclosed against a locked package.
- **Dual control:** dependency lockfile updates must be raised in a pull request that is reviewed and approved by a second engineer. Automated bots **may not** self-approve Poetry/lockfile refreshes.
- **Security checks:** pull requests that modify dependencies must link to the `pip-audit` CI job run and highlight any suppressions or exceptions that were required.

## Standard operating procedure

1. **Create a feature branch** for the dependency refresh.
2. **Install Poetry** (if not already installed): `pip install poetry poetry-plugin-export`.
3. **Update dependencies** as needed, then refresh the lockfile:
   ```bash
   poetry lock
   ```
4. **Regenerate `requirements.txt`** from the lock to keep legacy tooling in sync:
   ```bash
   poetry export --without-hashes --extras test --extras dev --format requirements.txt --output requirements.txt
   ```
5. **Run local validation:**
   ```bash
   poetry install
   pip-audit --strict -r requirements.txt
   poetry run pytest
   ```
6. **Open a pull request** summarising notable upgrades, attach the CI results, and request review from a second engineer.
7. **Merge only after approval** and once the security pipeline is green. If `pip-audit` flags an issue, document the mitigation (upgrade, vendor patch, or approved exception) before merging.

By following this checklist we ensure each dependency change is reproducible, peer-reviewed, and automatically scanned for known vulnerabilities.
