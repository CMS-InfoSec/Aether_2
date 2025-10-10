"""Error types mirrored from :mod:`aiokafka.errors` for test environments."""

class KafkaError(Exception):
    """Base error raised by the aiokafka compatibility layer."""


__all__ = ["KafkaError"]
