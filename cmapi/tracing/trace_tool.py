"""
CherryPy tool that uses the tracer to start a span for each request.
"""
import socket
import json
import logging
from typing import Dict, Any

import cherrypy

from tracing.utils import swallow_exceptions
from tracing.tracer import get_tracer

logger = logging.getLogger("tracer")

# Limit for raw JSON string preview (in characters)
_PREVIEW_MAX_CHARS = 512

@swallow_exceptions
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

    attrs = {
        'http.method': getattr(req, 'method', ''),
        'http.path': getattr(req, 'path_info', ''),
        'client.ip': requester_host,
        'instance.hostname': socket.gethostname(),
    }
    attrs.update(_record_incoming_json_preview(req))

    ctx = tracer.start_as_current_span(span_name, kind="SERVER", attributes=attrs)
    span = ctx.__enter__()
    safe_headers = {k: v for k, v in headers.items() if k.lower() not in {'authorization', 'x-api-key'}}
    span.set_attribute('sentry.incoming_headers', safe_headers)
    req._trace_span_ctx = ctx
    req._trace_span = span

    tracer.inject_traceparent(cherrypy.response.headers)

@swallow_exceptions
def _on_request_end() -> None:
    req = cherrypy.request
    try:
        status_str = str(cherrypy.response.status)
        status_code = int(status_str.split()[0])
    except Exception:
        status_code = None

    span = getattr(req, "_trace_span", None)
    if span is not None and status_code is not None:
        span.set_attribute('http.status_code', status_code)

    tracer = get_tracer()
    tracer.notify_request_finished(status_code)

    ctx = getattr(req, "_trace_span_ctx", None)
    if ctx is not None:
        try:
            ctx.__exit__(None, None, None)
        finally:
            req._trace_span_ctx = None
            req._trace_span = None


def register_tracing_tools() -> None:
    cherrypy.tools.trace = cherrypy.Tool("on_start_resource", _on_request_start, priority=70)
    cherrypy.tools.trace_end = cherrypy.Tool("on_end_resource", _on_request_end, priority=80)

@swallow_exceptions
def _record_incoming_json_preview(req) -> dict[str, Any]:
    """If request Content-Type is JSON, attach a compact preview to span.
    Returns dict of span attributes to set
    """
    attrs = {}
    content_type = str(getattr(req, 'headers', {}).get('Content-Type', '') or '')
    attrs['http.request.content_type'] = content_type
    if 'json' not in content_type.lower():
        return attrs

    parsed_json = getattr(req, 'json', None)
    if parsed_json is None:
        return attrs
    normalized = json.dumps(parsed_json, ensure_ascii=False, sort_keys=True)
    if len(normalized) > _PREVIEW_MAX_CHARS:
        normalized = normalized[:_PREVIEW_MAX_CHARS] + '...<truncated>'
    attrs['http.request.json'] = normalized
    attrs['http.request.body.size'] = len(normalized)
    return attrs
