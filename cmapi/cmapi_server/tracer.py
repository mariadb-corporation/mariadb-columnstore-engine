"""Support distributed request tracing via W3C Trace Context.
See https://www.w3.org/TR/trace-context/ for the official spec.

There are 3 and a half main components:
- trace_id: a unique identifier for a trace.
  It is a 32-hex string, passed in the outbound HTTP requests headers, so that we can
  trace the request chain through the system.
- span_id: a unique identifier for a span (the current operation within a trace chain).
  It is a 16-hex string. For example, when we we receive a request to add a host, the addition
  of the host is a separate span within the request chain.
- parent_span_id: a unique identifier for the parent span of the current span.
  Continuing the example above, when we add a host, first we start a transaction,
    then we add the host. If we are already adding a host, then creation of the transaction
    is the parent span of the current span.
- traceparent: a header that combines trace_id and span_id in one value.
  It has the format "00-<trace_id>-<span_id>-<flags>".

A system that calls CMAPI can pass the traceparent header in the request, so that we can pass
  the trace_id through the system, changing span_id as we enter new sub-operations.

We can reconstruct the trace tree from the set of the logged traceparent attributes,
  representing how the request was processed, which nodes were involved,
  how much time did each op take, etc.

This module implements a tracer class that creates spans, injects/extracts traceparent headers.
It uses contextvars to store the trace/span/parent_span ids and start time for each context.
"""

from __future__ import annotations

import contextvars
import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Contextvars containing trace/span/parent_span ids and start time for this thread
# (contextvars are something like TLS, they contain variables that are local to the context)
_current_trace_id = contextvars.ContextVar[str]("trace_id", default="")
_current_span_id = contextvars.ContextVar[str]("span_id", default="")
_current_parent_span_id = contextvars.ContextVar[str]("parent_span_id", default="")
_current_span_start_ns = contextvars.ContextVar[int]("span_start_ns", default=0)


def _rand_16_hex() -> str:
    # 16 hex bytes (32 hex chars)
    return os.urandom(16).hex()

def _rand_8_hex() -> str:
    # 8 hex bytes (16 hex chars)
    return os.urandom(8).hex()

def format_traceparent(trace_id: str, span_id: str, flags: str = "01") -> str:
    """W3C traceparent: version 00"""
    # version(2)-trace_id(32)-span_id(16)-flags(2)
    return f"00-{trace_id}-{span_id}-{flags}"

def parse_traceparent(header: str) -> Optional[tuple[str, str, str]]:
    """Return (trace_id, span_id, flags) or None if invalid."""
    try:
        parts = header.strip().split("-")
        if len(parts) != 4 or parts[0] != "00":
            logger.error(f"Invalid traceparent: {header}")
            return None
        trace_id, span_id, flags = parts[1], parts[2], parts[3]
        if len(trace_id) != 32 or len(span_id) != 16 or len(flags) != 2:
            return None
        # W3C: all zero trace_id/span_id are invalid
        if set(trace_id) == {"0"} or set(span_id) == {"0"}:
            return None
        return trace_id, span_id, flags
    except Exception:
        logger.error(f"Failed to parse traceparent: {header}")
        return None


@dataclass
class TraceSpan:
    """Lightweight span handle; keeps attributes in memory (for logging only)."""
    name: str
    kind: str  # "SERVER" | "CLIENT" | "INTERNAL"
    start_ns: int
    trace_id: str
    span_id: str
    parent_span_id: str
    attributes: Dict[str, Any]

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, **attrs: Any) -> None:
        # For simplicity we just log events immediately
        logger.info(
            "event name=%s trace_id=%s span_id=%s attrs=%s",
            name, self.trace_id, self.span_id, attrs
        )

    def set_status(self, code: str, description: str = "") -> None:
        self.attributes["status.code"] = code
        if description:
            self.attributes["status.description"] = description

    def record_exception(self, exc: BaseException) -> None:
        self.add_event("exception", type=type(exc).__name__, msg=str(exc))


class Tracer:
    """Encapsulates everything related to tracing (span creation, logging, etc)"""
    @contextmanager
    def start_as_current_span(self, name: str, kind: str = "INTERNAL") -> Iterator[TraceSpan]:
        trace_id = _current_trace_id.get() or _rand_16_hex()
        parent_span = _current_span_id.get()
        new_span_id = _rand_8_hex()

        # Push new context
        tok_tid = _current_trace_id.set(trace_id)
        tok_sid = _current_span_id.set(new_span_id)
        tok_pid = _current_parent_span_id.set(parent_span)
        tok_start = _current_span_start_ns.set(time.time_ns())

        span = TraceSpan(
            name=name,
            kind=kind,
            start_ns=_current_span_start_ns.get(),
            trace_id=trace_id,
            span_id=new_span_id,
            parent_span_id=parent_span,
            attributes={"span.kind": kind, "span.name": name},
        )

        try:
            logger.info(
                "span_begin name=%s kind=%s trace_id=%s span_id=%s parent_span_id=%s attrs=%s",
                span.name, span.kind, span.trace_id, span.span_id, span.parent_span_id, span.attributes
            )
            yield span
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status("ERROR", str(exc))
            raise
        finally:
            # Pop the span from the context (restore parent context)
            duration_ms = (time.time_ns() - span.start_ns) / 1_000_000
            logger.info(
                "span_end name=%s kind=%s trace_id=%s span_id=%s parent_span_id=%s duration_ms=%.3f attrs=%s",
                span.name, span.kind, span.trace_id, span.span_id, span.parent_span_id, duration_ms, span.attributes
            )
            # Restore previous context
            _current_span_start_ns.reset(tok_start)
            _current_parent_span_id.reset(tok_pid)
            _current_span_id.reset(tok_sid)
            _current_trace_id.reset(tok_tid)

    def set_incoming_context(
        self,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
    ) -> None:
        """Seed current context with incoming W3C traceparent values.

        Only non-empty values are applied.
        """
        if trace_id:
            _current_trace_id.set(trace_id)
        if parent_span_id:
            _current_parent_span_id.set(parent_span_id)

    def current_trace_ids(self) -> tuple[str, str, str]:
        return _current_trace_id.get(), _current_span_id.get(), _current_parent_span_id.get()

    def inject_traceparent(self, headers: Dict[str, str]) -> None:
        """Inject W3C traceparent into outbound headers."""
        trace_id, span_id, _ = self.current_trace_ids()
        if not trace_id or not span_id:
            # If called outside of a span, create a short-lived span just to carry IDs
            trace_id = trace_id or _rand_16_hex()
            span_id = span_id or _rand_8_hex()
        headers["traceparent"] = format_traceparent(trace_id, span_id)

    def extract_traceparent(self, headers: Dict[str, str]) -> tuple[str, str]:
        """Extract parent trace/span; returns (trace_id, parent_span_id)."""
        raw_traceparent = (headers.get("traceparent")
                            or headers.get("Traceparent")
                            or headers.get("TRACEPARENT"))
        if not raw_traceparent:
            return "", ""
        parsed = parse_traceparent(raw_traceparent)
        if not parsed:
            return "", ""
        return parsed[0], parsed[1]
        # No incoming context
        return "", ""

# Tracer singleton for the process (not thread)
_tracer = Tracer()

def get_tracer() -> Tracer:
    return _tracer


class TraceLogFilter(logging.Filter):
    """Inject trace/span ids into LogRecord for formatting."""
    def filter(self, record: logging.LogRecord) -> bool:
        record.traceID, record.spanID, record.parentSpanID = get_tracer().current_trace_ids()
        return True
