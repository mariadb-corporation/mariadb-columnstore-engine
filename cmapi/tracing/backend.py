from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from tracing.span import TraceSpan


class TracerBackend(ABC):
    @abstractmethod
    def on_span_start(self, span: TraceSpan) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_span_end(self, span: TraceSpan, exc: Optional[BaseException]) -> None:
        raise NotImplementedError

    def on_span_event(self, span: TraceSpan, name: str, attrs: Dict[str, Any]) -> None:
        return

    def on_span_status(self, span: TraceSpan, code: str, description: str) -> None:
        return

    def on_span_exception(self, span: TraceSpan, exc: BaseException) -> None:
        return

    def on_span_attribute(self, span: TraceSpan, key: str, value: Any) -> None:
        return

    def on_inject_headers(self, headers: Dict[str, str]) -> None:
        return

    def on_incoming_request(self, headers: Dict[str, str], method: str, path: str) -> None:
        return

    def on_request_finished(self, status_code: Optional[int]) -> None:
        return


