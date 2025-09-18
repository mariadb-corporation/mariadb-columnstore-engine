"""
CherryPy tool that uses the tracer to start a span for each request.
"""
import socket
import json
import logging
from typing import Dict

import cherrypy

from tracing.tracer import get_tracer

logger = logging.getLogger("tracer")

# Limit for raw JSON string preview (in characters)
_PREVIEW_MAX_CHARS = 512


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
    _record_incoming_json_preview(req, span)
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


def _record_incoming_json_preview(req, span) -> None:
    """If request Content-Type is JSON, attach a compact preview to span.

    Attempts to read the body and then restore the stream position so request handling isn't affected.
    """
    try:
        content_type = str(getattr(req, 'headers', {}).get('Content-Type', '') or '')
        span.set_attribute('http.request.content_type', content_type)
        if 'json' not in content_type.lower():
            return
        try:
            parsed_json = getattr(req, 'json', None)
        except Exception:
            parsed_json = None
        if parsed_json is None:
            logger.info("Skipping JSON preview: request.json is not available")
            return
        try:
            normalized = json.dumps(parsed_json, ensure_ascii=False, sort_keys=True)
            if len(normalized) > _PREVIEW_MAX_CHARS:
                normalized = normalized[:_PREVIEW_MAX_CHARS] + '...<truncated>'
            span.set_attribute('http.request.json', normalized)
            span.set_attribute('http.request.body.size', len(normalized))
        except Exception:
            logger.exception("Failed to serialize request.json for preview")
    except Exception:
        logger.exception("Failed to record incoming JSON preview")
