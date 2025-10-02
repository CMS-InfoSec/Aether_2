"""Integration tests for the OMS simulation mode endpoints.

These are currently marked as skipped until the simulation endpoints
are implemented in the service layer.  The module level skip allows the
suite to compile while documenting the intended coverage.
"""

import pytest

pytest.skip("Simulation mode endpoints not yet available", allow_module_level=True)
