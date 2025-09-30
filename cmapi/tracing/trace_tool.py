"""
CherryPy tool that uses the tracer to start a span for each request.
"""
import socket
from typing import Dict

import cherrypy

from tracing.tracer import get_tracer

def _on_request_start() -> None:
    req = cherrypy.request
    tracer = get_tracer()

    headers: Dict[str, str] = dict(req.headers or {})
    tracer.notify_incoming_request(
        headers=headers,
        method=getattr(req, 'method', ''),
        path=getattr(req, 'path_info', '')
    )
    trace_id, parent_span_id = tracer.extract_traceparent(headers)
    tracer.set_incoming_context(trace_id, parent_span_id)

    requester_host = getattr(getattr(req, 'remote', None), 'ip', '')

    method = getattr(req, 'method', 'HTTP')
    path = getattr(req, 'path_info', '/')
    if requester_host:
        span_name = f"{requester_host} --> {method} {path}"
    else:
        span_name = f"{method} {path}"

    ctx = tracer.start_as_current_span(span_name, kind="SERVER")
    span = ctx.__enter__()
    span.set_attribute('http.method', getattr(req, 'method', ''))
    span.set_attribute('http.path', getattr(req, 'path_info', ''))
    span.set_attribute('client.ip', requester_host)
    span.set_attribute('instance.hostname', socket.gethostname())
    safe_headers = {k: v for k, v in headers.items() if k.lower() not in {'authorization', 'x-api-key'}}
    span.set_attribute('sentry.incoming_headers', safe_headers)
    req._trace_span_ctx = ctx
    req._trace_span = span

    tracer.inject_traceparent(cherrypy.response.headers)


def _on_request_end() -> None:
    req = cherrypy.request
    try:
        status_str = str(cherrypy.response.status)
        status_code = int(status_str.split()[0])
    except Exception:
        status_code = None
    tracer = get_tracer()
    tracer.notify_request_finished(status_code)
    span = getattr(req, "_trace_span", None)
    if span is not None and status_code is not None:
        span.set_attribute('http.status_code', status_code)
    ctx = getattr(req, "_trace_span_ctx", None)
    if ctx is not None:
        try:
            ctx.__exit__(None, None, None)
        finally:
            req._trace_span_ctx = None
            req._trace_span = None


def register_tracing_tools() -> None:
    cherrypy.tools.trace = cherrypy.Tool("on_start_resource", _on_request_start, priority=10)
    cherrypy.tools.trace_end = cherrypy.Tool("on_end_resource", _on_request_end, priority=80)



