"""Compatibility loader for :mod:`fastapi.testclient`.

The real FastAPI package now ships with the repository.  This module proxies the
import to the upstream implementation while providing a clear error if the
optional dependency has not been installed.
"""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_testclient = load_dependency(
    "fastapi.testclient", install_hint="pip install fastapi"
)

globals().update(vars(_real_testclient))
