import logging
import os
from functools import wraps
from typing import Optional, Tuple

logger = logging.getLogger("tracing")

def swallow_exceptions(method):
    """Decorator that logs exceptions and prevents them from propagating up."""

    @wraps(method)
    def _wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception:
            logger.exception("%s failed", getattr(method, "__qualname__", repr(method)))
            return None

    return _wrapper

def rand_16_hex() -> str:
    """Return 16 random bytes as a 32-char hex string (trace_id size)."""
    return os.urandom(16).hex()


def rand_8_hex() -> str:
    """Return 8 random bytes as a 16-char hex string (span_id size)."""
    return os.urandom(8).hex()


def format_traceparent(trace_id: str, span_id: str, flags: str = "01") -> str:
    """Build a W3C traceparent header (version 00)."""
    return f"00-{trace_id}-{span_id}-{flags}"


def parse_traceparent(header: str) -> Optional[Tuple[str, str, str]]:
    """Parse W3C traceparent and return (trace_id, span_id, flags) or None."""
    try:
        parts = header.strip().split("-")
        if len(parts) != 4 or parts[0] != "00":
            logger.error("Invalid traceparent: %s", header)
            return None
        trace_id, span_id, flags = parts[1], parts[2], parts[3]
        if len(trace_id) != 32 or len(span_id) != 16 or len(flags) != 2:
            return None
        # W3C: all zero trace_id/span_id are invalid
        if set(trace_id) == {"0"} or set(span_id) == {"0"}:
            return None
        return trace_id, span_id, flags
    except Exception:
        logger.exception("Failed to parse traceparent: %s", header)
        return None
