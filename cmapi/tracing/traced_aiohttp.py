"""Async sibling of TracedSession."""
from typing import Any

import aiohttp

from tracing.tracer import get_tracer


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
            tracer.inject_outbound_headers(headers)
            try:
                response = await super()._request(method, str_or_url, *args, **kwargs)
            except Exception as exc:
                span.set_status("ERROR", str(exc))
                raise
            else:
                span.set_attribute("http.status_code", response.status)
                return response


def create_traced_async_session(**kwargs: Any) -> TracedAsyncSession:
    return TracedAsyncSession(**kwargs)



