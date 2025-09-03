"""This module implements a tracer facade that creates spans, injects/extracts traceparent headers,
and delegates span lifecycle and enrichment to pluggable backends (e.g., Traceparent and Sentry).
It uses contextvars to store the trace/span/parent_span ids and start time for each context.
"""

import contextvars
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from tracing.backend import TracerBackend
from tracing.span import TraceSpan
from tracing.utils import (
    rand_16_hex,
    rand_8_hex,
    format_traceparent,
    parse_traceparent,
)

logger = logging.getLogger(__name__)


# Context vars are something like thread-local storage, they are context-local variables
_current_trace_id = contextvars.ContextVar[str]("trace_id", default="")
_current_span_id = contextvars.ContextVar[str]("span_id", default="")
_current_parent_span_id = contextvars.ContextVar[str]("parent_span_id", default="")
_current_span_start_ns = contextvars.ContextVar[int]("span_start_ns", default=0)


class Tracer:
    def __init__(self) -> None:
        self._backends: List[TracerBackend] = []

    def register_backend(self, backend: TracerBackend) -> None:
        try:
            self._backends.append(backend)
            logger.info(
                "Tracing backend registered: %s", backend.__class__.__name__
            )
        except Exception:
            logger.exception("Failed to register tracing backend")

    def clear_backends(self) -> None:
        self._backends.clear()

    @contextmanager
    def start_as_current_span(self, name: str, kind: str = "INTERNAL") -> Iterator[TraceSpan]:
        trace_id = _current_trace_id.get() or rand_16_hex()
        parent_span = _current_span_id.get()
        new_span_id = rand_8_hex()

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
            tracer=self,
        )

        caught_exc: Optional[BaseException] = None
        try:
            for backend in list(self._backends):
                backend.on_span_start(span)
            yield span
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status("ERROR", str(exc))
            caught_exc = exc
            raise
        finally:
            for backend in list(self._backends):
                backend.on_span_end(span, caught_exc)
            _current_span_start_ns.reset(tok_start)
            _current_parent_span_id.reset(tok_pid)
            _current_span_id.reset(tok_sid)
            _current_trace_id.reset(tok_tid)

    def set_incoming_context(
        self,
        trace_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
    ) -> None:
        if trace_id:
            _current_trace_id.set(trace_id)
        if parent_span_id:
            _current_parent_span_id.set(parent_span_id)

    def current_trace_ids(self) -> Tuple[str, str, str]:
        return _current_trace_id.get(), _current_span_id.get(), _current_parent_span_id.get()

    def inject_traceparent(self, headers: Dict[str, str]) -> None:
        trace_id, span_id, _ = self.current_trace_ids()
        if not trace_id or not span_id:
            trace_id = trace_id or rand_16_hex()
            span_id = span_id or rand_8_hex()
        headers["traceparent"] = format_traceparent(trace_id, span_id)

    def inject_outbound_headers(self, headers: Dict[str, str]) -> None:
        self.inject_traceparent(headers)
        for backend in list(self._backends):
            backend.on_inject_headers(headers)

    def notify_incoming_request(self, headers: Dict[str, str], method: str, path: str) -> None:
        for backend in list(self._backends):
            backend.on_incoming_request(headers, method, path)

    def notify_request_finished(self, status_code: Optional[int]) -> None:
        for backend in list(self._backends):
            backend.on_request_finished(status_code)

    def extract_traceparent(self, headers: Dict[str, str]) -> Tuple[str, str]:
        raw_traceparent = (headers.get("traceparent")
                            or headers.get("Traceparent")
                            or headers.get("TRACEPARENT"))
        if not raw_traceparent:
            return "", ""
        parsed = parse_traceparent(raw_traceparent)
        if not parsed:
            return "", ""
        return parsed[0], parsed[1]

    def _notify_event(self, span: TraceSpan, name: str, attrs: Dict[str, Any]) -> None:
        for backend in list(self._backends):
            backend.on_span_event(span, name, attrs)

    def _notify_status(self, span: TraceSpan, code: str, description: str) -> None:
        for backend in list(self._backends):
            backend.on_span_status(span, code, description)

    def _notify_exception(self, span: TraceSpan, exc: BaseException) -> None:
        for backend in list(self._backends):
            backend.on_span_exception(span, exc)

    def _notify_attribute(self, span: TraceSpan, key: str, value: Any) -> None:
        for backend in list(self._backends):
            backend.on_span_attribute(span, key, value)


_tracer = Tracer()


def get_tracer() -> Tracer:
    return _tracer


