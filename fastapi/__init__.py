"""Compatibility bridge that delegates to the real FastAPI distribution.

Historically the repository shipped a lightweight FastAPI stub so modules could
be imported without installing the framework.  The project now requires the
actual FastAPI package; this module simply loads it (bypassing the legacy stub
paths) and re-exports its namespace.  When FastAPI is missing we raise an
informative error instructing operators to install the dependency instead of
silently activating insecure fallbacks.
"""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_fastapi = load_dependency("fastapi", install_hint="pip install fastapi")

globals().update(vars(_real_fastapi))
