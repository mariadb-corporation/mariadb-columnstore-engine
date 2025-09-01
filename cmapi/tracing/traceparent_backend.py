import logging
import time
from typing import Any, Dict, Optional

from tracing.tracer import TracerBackend, TraceSpan
from tracing.utils import swallow_exceptions

logger = logging.getLogger("tracing")


class TraceparentBackend(TracerBackend):
    """Default backend that logs span lifecycle and mirrors events/status."""

    @swallow_exceptions
    def on_span_start(self, span: TraceSpan) -> None:
        logger.info(
            "span_begin name=%s kind=%s trace_id=%s span_id=%s parent=%s attrs=%s",
            span.name, span.kind, span.trace_id, span.span_id,
            span.parent_span_id, span.attributes,
        )

    @swallow_exceptions
    def on_span_end(self, span: TraceSpan, exc: Optional[BaseException]) -> None:
        duration_ms = (time.time_ns() - span.start_ns) / 1_000_000
        logger.info(
            "span_end name=%s kind=%s trace_id=%s span_id=%s parent=%s duration_ms=%.3f attrs=%s",
            span.name, span.kind, span.trace_id, span.span_id,
            span.parent_span_id, duration_ms, span.attributes,
        )

    @swallow_exceptions
    def on_span_event(self, span: TraceSpan, name: str, attrs: Dict[str, Any]) -> None:
        logger.info(
            "span_event name=%s trace_id=%s span_id=%s attrs=%s",
            name, span.trace_id, span.span_id, attrs,
        )


