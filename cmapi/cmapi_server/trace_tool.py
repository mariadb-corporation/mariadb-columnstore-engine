"""
CherryPy tool that uses the tracer to start a span for each request.

If traceparent header is present in the request, the tool will continue this trace chain.
Otherwise, it will start a new trace (with a new trace_id).
"""
from typing import Dict

import cherrypy

from cmapi_server.tracer import get_tracer


def _on_request_start() -> None:
    """CherryPy tool hook: extract incoming context and start a SERVER span."""
    req = cherrypy.request
    tracer = get_tracer()

    headers: Dict[str, str] = dict(req.headers or {})
    trace_id, parent_span_id = tracer.extract_traceparent(headers)
    tracer.set_incoming_context(trace_id, parent_span_id)

    span_name = f"{getattr(req, 'method', 'HTTP')} {getattr(req, 'path_info', '/')}"

    ctx = tracer.start_as_current_span(span_name, kind="SERVER")
    span = ctx.__enter__()
    setattr(req, "_trace_span_ctx", ctx)
    setattr(req, "_trace_span", span)

    # Echo current traceparent to the client
    tracer.inject_traceparent(cherrypy.response.headers)  # type: ignore[arg-type]


def _on_request_end() -> None:
    """CherryPy tool hook: end the SERVER span started at request start."""
    req = cherrypy.request
    ctx = getattr(req, "_trace_span_ctx", None)
    if ctx is not None:
        try:
            ctx.__exit__(None, None, None)
        finally:
            setattr(req, "_trace_span_ctx", None)
            setattr(req, "_trace_span", None)


def register_tracing_tools() -> None:
    """Register CherryPy tools for request tracing."""
    cherrypy.tools.trace = cherrypy.Tool("on_start_resource", _on_request_start, priority=10)
    cherrypy.tools.trace_end = cherrypy.Tool("on_end_resource", _on_request_end, priority=80)


