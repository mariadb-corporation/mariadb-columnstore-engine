import logging
import time
from typing import Any, Optional

import cherrypy

from mcs_node_control.models.node_config import NodeConfig
from tracing.tracer import TracerBackend, TraceSpan
from tracing.utils import swallow_exceptions

logger = logging.getLogger('tracer')
json_logger = logging.getLogger('json_trace')


class TraceparentBackend(TracerBackend):
    """Default backend that logs span lifecycle and mirrors events/status."""
    def __init__(self):
        my_addresses = list(NodeConfig().get_network_addresses())
        logger.info('My addresses: %s', my_addresses)
        json_logger.info('my_addresses', extra={'my_addresses': my_addresses})

    @swallow_exceptions
    def on_span_start(self, span: TraceSpan) -> None:
        json_logger.info('span_begin', extra=span.to_flat_dict())

    @swallow_exceptions
    def on_span_end(self, span: TraceSpan, exc: Optional[BaseException]) -> None:
        end_ns = time.time_ns()
        duration_ms = (end_ns - span.start_ns) / 1_000_000
        span.set_attribute('duration_ms', duration_ms)
        span.set_attribute('end_ns', end_ns)

        # Try to set status code if not already set
        if span.kind == 'SERVER' and 'http.status_code' not in span.attributes:
            try:
                status_str = str(cherrypy.response.status)
                code = int(status_str.split()[0])
                span.set_attribute('http.status_code', code)
            except Exception:
                pass

        json_logger.info('span_end', extra=span.to_flat_dict())

    @swallow_exceptions
    def on_span_event(self, span: TraceSpan, name: str, attrs: dict[str, Any]) -> None:
        json_logger.info('span_event', extra=span.to_flat_dict())


