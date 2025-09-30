from typing import TYPE_CHECKING
from dataclasses import dataclass
from typing import Any, Dict
from tracing.utils import swallow_exceptions

if TYPE_CHECKING:
    from tracing.tracer import Tracer

@dataclass
class TraceSpan:
    """Span handle bound to a tracer.

    Provides helpers to add attributes/events/status/exception that
    will never propagate exceptions.
    """

    name: str
    kind: str  # "SERVER" | "CLIENT" | "INTERNAL"
    start_ns: int
    trace_id: str
    span_id: str
    parent_span_id: str
    attributes: Dict[str, Any]
    tracer: "Tracer"

    @swallow_exceptions
    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value
        self.tracer._notify_attribute(self, key, value)

    @swallow_exceptions
    def add_event(self, name: str, **attrs: Any) -> None:
        self.tracer._notify_event(self, name, attrs)

    @swallow_exceptions
    def set_status(self, code: str, description: str = "") -> None:
        self.attributes["status.code"] = code
        if description:
            self.attributes["status.description"] = description
        self.tracer._notify_status(self, code, description)

    @swallow_exceptions
    def record_exception(self, exc: BaseException) -> None:
        self.tracer._notify_exception(self, exc)

    def to_flat_dict(self) -> Dict[str, Any]:
        fd = self.__dict__.copy()
        fd['span_name'] = fd.pop('name')  # name field is reserved in log records
        # Remove non-serializable references
        fd.pop('tracer', None)
        attributes = fd.pop("attributes")
        fd.update(attributes)
        return fd

