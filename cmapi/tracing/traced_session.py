"""Customized requests.Session that automatically traces outbound HTTP calls."""
from typing import Any, Optional

import logging
import time
import requests

from tracing.tracer import get_tracer, TraceSpan

# Limit for raw JSON string preview (in characters)
_PREVIEW_MAX_CHARS = 512

logger = logging.getLogger("tracer")


class TracedSession(requests.Session):
    def request(self, method: str, url: str, *args: Any, **kwargs: Any) -> requests.Response:
        tracer = get_tracer()

        headers = kwargs.get("headers") or {}
        if headers is None:
            headers = {}
        kwargs["headers"] = headers

        span_name = f"HTTP {method} {url}"
        with tracer.start_as_current_span(span_name, kind="CLIENT") as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.url", url)
            span.set_attribute("request_is_sync", True)

            tracer.inject_outbound_headers(headers)
            try:
                response = super().request(method, url, *args, **kwargs)
            except Exception as exc:
                span.set_status("ERROR", str(exc))
                raise
            else:
                span.set_attribute("http.status_code", response.status_code)
                _record_outbound_json_preview(response, span)
                return response
            finally:
                duration_ms = (time.time_ns() - span.start_ns) / 1_000_000.0
                span.set_attribute("request_duration_ms", duration_ms)


_default_session: Optional[TracedSession] = None


def get_traced_session() -> TracedSession:
    global _default_session
    if _default_session is None:
        _default_session = TracedSession()
    return _default_session


def _record_outbound_json_preview(response: requests.Response, span: TraceSpan) -> None:
    """If response is JSON, attach small part of it to span"""
    try:
        content_type = str(response.headers.get('Content-Type', '')).lower()
        if 'application/json' not in content_type:
            return
        text = response.text  # requests will decode using inferred/declared encoding
        span.set_attribute('http.response.body.size', len(text))
        span.set_attribute('http.response.json', text[:_PREVIEW_MAX_CHARS])
    except Exception:
        logger.exception("Could not extract JSON response body")
