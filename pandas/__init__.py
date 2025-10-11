"""Proxy to the real :mod:`pandas` package."""

from __future__ import annotations

from shared.dependency_loader import load_dependency

_real_pandas = load_dependency("pandas", install_hint="pip install pandas")

globals().update(vars(_real_pandas))
