import logging
import contextvars
from typing import Any, Dict, Optional

import sentry_sdk
from sentry_sdk.tracing import Transaction

from tracing.tracer import TraceSpan, TracerBackend
from tracing.utils import swallow_exceptions


logger = logging.getLogger(__name__)


class SentryBackend(TracerBackend):
    """Mirror spans and events from our Tracer into Sentry SDK."""

    def __init__(self) -> None:
        self._active_spans: Dict[str, Any] = {}
        self._current_transaction = contextvars.ContextVar[Optional[Transaction]]("sentry_transaction", default=None)

    @swallow_exceptions
    def on_span_start(self, span: TraceSpan) -> None:
        kind_to_op = {
            'SERVER': 'http.server',
            'CLIENT': 'http.client',
            'INTERNAL': 'internal',
        }
        op = kind_to_op.get(span.kind.upper(), 'internal')
        sdk_span = sentry_sdk.start_span(op=op, description=span.name)
        sdk_span.set_tag('w3c.trace_id', span.trace_id)
        sdk_span.set_tag('w3c.span_id', span.span_id)
        if span.parent_span_id:
            sdk_span.set_tag('w3c.parent_span_id', span.parent_span_id)
        if span.attributes:
            sdk_span.set_data('cmapi.span_attributes', dict(span.attributes))
        sdk_span.__enter__()
        self._active_spans[span.span_id] = sdk_span

    @swallow_exceptions
    def on_span_end(self, span: TraceSpan, exc: Optional[BaseException]) -> None:
        sdk_span = self._active_spans.pop(span.span_id, None)
        if sdk_span is None:
            return
        if exc is not None:
            sdk_span.set_status('internal_error')
        sdk_span.__exit__(
            type(exc) if exc else None,
            exc,
            exc.__traceback__ if exc else None
        )

    @swallow_exceptions
    def on_span_event(self, span: TraceSpan, name: str, attrs: Dict[str, Any]) -> None:
        sentry_sdk.add_breadcrumb(category='event', message=name, data=dict(attrs))

    @swallow_exceptions
    def on_span_status(self, span: TraceSpan, code: str, description: str) -> None:
        sdk_span = self._active_spans.get(span.span_id)
        if sdk_span is not None:
            sdk_span.set_status(code)

    @swallow_exceptions
    def on_span_exception(self, span: TraceSpan, exc: BaseException) -> None:
        sentry_sdk.capture_exception(exc)

    @swallow_exceptions
    def on_span_attribute(self, span: TraceSpan, key: str, value: Any) -> None:
        sdk_span = self._active_spans.get(span.span_id)
        if sdk_span is not None:
            sdk_span.set_data(f'attr.{key}', value)

    @swallow_exceptions
    def on_inject_headers(self, headers: Dict[str, str]) -> None:
        traceparent = sentry_sdk.get_traceparent()
        baggage = sentry_sdk.get_baggage()
        if traceparent:
            headers['sentry-trace'] = traceparent
        if baggage:
            headers['baggage'] = baggage

    @swallow_exceptions
    def on_incoming_request(self, headers: Dict[str, str], method: str, path: str) -> None:
        name = f"{method} {path}" if method or path else "http.server"
        # Continue from incoming headers, then START the transaction per SDK v2
        continued = sentry_sdk.continue_trace(headers or {}, op='http.server', name=name)
        transaction = sentry_sdk.start_transaction(transaction=continued)
        # Store started transaction in context var and make current (enter)
        self._current_transaction.set(transaction)
        transaction.__enter__()
        scope = sentry_sdk.Hub.current.scope
        if method:
            scope.set_tag('http.method', method)
        if path:
            scope.set_tag('http.path', path)
            # TODO: remove this
            logger.info(
                "Sentry transaction started name=%s sampled=%s",
                name, getattr(transaction, 'sampled', None)
            )

    @swallow_exceptions
    def on_request_finished(self, status_code: Optional[int]) -> None:
        transaction = self._current_transaction.get()
        if transaction is None:
            return
        if status_code is not None:
            transaction.set_http_status(status_code)
        # Exit to restore parent and finish the transaction
        transaction.__exit__(None, None, None)
        # Clear transaction in this context
        self._current_transaction.set(None)
        # TODO: remove this
        logger.info("Sentry transaction finished status=%s", status_code)


