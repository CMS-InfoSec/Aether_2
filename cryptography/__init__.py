"""Lightweight cryptography compatibility package for dependency-light tests."""

from .fernet import Fernet, InvalidToken

__all__ = ["Fernet", "InvalidToken"]
