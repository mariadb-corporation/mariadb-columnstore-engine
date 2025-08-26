"""Our own customized requests.Session that automatically traces outbound HTTP calls.

Creates a CLIENT span per outbound HTTP request, injects traceparent,
records method/url/status, and closes the span when the request finishes.
"""

from typing import Any, Optional

import requests

from cmapi_server.tracer import get_tracer


class TracedSession(requests.Session):
    """requests.Session that automatically traces outbound HTTP calls."""

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

            tracer.inject_traceparent(headers)
            try:
                response = super().request(method, url, *args, **kwargs)
            except Exception as exc:
                span.set_status("ERROR", str(exc))
                raise
            else:
                # Record status code
                span.set_attribute("http.status_code", response.status_code)
                return response


_default_session: Optional[TracedSession] = None


def get_traced_session() -> TracedSession:
    """Return a process-wide TracedSession singleton."""
    global _default_session
    if _default_session is None:
        _default_session = TracedSession()
    return _default_session


