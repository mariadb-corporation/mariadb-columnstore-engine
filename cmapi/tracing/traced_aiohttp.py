"""Async sibling of TracedSession."""
from typing import Any

import logging
import time
import aiohttp

from tracing.tracer import get_tracer

# Limit for raw JSON string preview (in characters)
_PREVIEW_MAX_CHARS = 512

logger = logging.getLogger("tracer")


class TracedAsyncSession(aiohttp.ClientSession):
    async def _request(
        self, method: str, str_or_url: Any, *args: Any, **kwargs: Any
    ) -> aiohttp.ClientResponse:
        tracer = get_tracer()

        headers = kwargs.get("headers") or {}
        if headers is None:
            headers = {}
        kwargs["headers"] = headers

        url_text = str(str_or_url)
        span_name = f"HTTP {method} {url_text}"
        with tracer.start_as_current_span(span_name, kind="CLIENT") as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.url", url_text)
            span.set_attribute("request_is_sync", False)
            tracer.inject_outbound_headers(headers)
            try:
                response = await super()._request(method, str_or_url, *args, **kwargs)
            except Exception as exc:
                span.set_status("ERROR", str(exc))
                raise
            else:
                span.set_attribute("http.status_code", response.status)
                await _record_outbound_json_preview(response, span)
                return response
            finally:
                duration_ms = (time.time_ns() - span.start_ns) / 1_000_000.0
                span.set_attribute("request_duration_ms", duration_ms)


def create_traced_async_session(**kwargs: Any) -> TracedAsyncSession:
    return TracedAsyncSession(**kwargs)



async def _record_outbound_json_preview(response: aiohttp.ClientResponse, span) -> None:
    """If response is JSON, attach small part of it to span

    We don't use streaming in aiohttp, so reading text is safe here.
    """
    try:
        content_type = str(response.headers.get('Content-Type', '')).lower()
        if 'application/json' not in content_type:
            return
        text = await response.text()
        if text is None:
            text = ""
        span.set_attribute('http.response.body.size', len(text))
        span.set_attribute('http.response.json', text[:_PREVIEW_MAX_CHARS])
    except Exception:
        logger.exception("Could not extract JSON response body")
        return None
