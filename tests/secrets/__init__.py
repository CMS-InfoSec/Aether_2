
from __future__ import annotations

import os
from typing import Optional


def token_hex(nbytes: Optional[int] = None) -> str:
    """Compatibility shim for stdlib secrets.token_hex used in tests."""

    if nbytes is None:
        nbytes = 32
    return os.urandom(nbytes).hex()

