"""Tracing support for CMAPI

Despite having many files, the idea of this package is simple: MCS is a distributed system,
  and we need to be able to trace requests across the system.
We need to understand:
* how one incoming request caused many others
* how long each request took
* which request each log line corresponds to
etc

The basic high-level mechanism is this:
1. Each incoming request is assigned a trace ID (or it may already have one, see point 2).
2. This trace ID is propagated to all other outbound requests that are caused by this request.
3. Each sub-operation is assigned a span ID. Request ID stays the same, but the span ID changes.
4. Each span can have a parent span ID, which is the span ID of the request that caused this span.
5. So basically, we have a tree of spans, and the trace ID identifies the root of the tree.

TraceID/SpanID/ParentSpanID are added to each log line, so we can identify which request each log line corresponds to.

Trace attributes are passed through the system via request headers, and here it becomes a bit more complicated.
There are two technologies that we use to pass these ids:
1. W3C TraceContext. This is a standard, it has a fixed header and its format.
   The header is called `traceparent`. It encapsulates trace id and span id.
2. Sentry. For historical reasons, it has different headers. And in our setup it is optional.
   But Sentry is very useful, we also use it to monitor the errors, and it has a powerful UI, so we support it too.

How is it implemented?
1. We have a global tracer object, that is used to create spans and pass them through the system.
2. It is a facade that hides two tracing backends with the same interface: TraceparentBackend and SentryBackend.
3. We have CherryPy tool that processes incoming requests, extracts traceparent header (or generates its parts),
   creates a span for each request, injects traceparent header into the response.
4. For each outcoming request, we ask tracer to create a new span and to inject tracing headers into the request.
   To avoid boilerplate, there is a TracedSession, an extension to requests that does all that.
   For async requests, there is a TracedAsyncSession, that does the same.
5. When the request is finished, we ask tracer to finish/pop the current span.

Logging:
There is a trace record factory, that adds a new field trace_params to each log record.
trace_params is a string representation of trace id, span id and parent span id.
If in current context they are empty (like in MainThread that doesn't process requests), trace_params is an empty string.

Sentry reporting:
If Sentry is enabled, we send info about errors and exceptions into it. We also send logs that preceded the problem
 as breadcrumbs to understand context of the error.
As we keep Sentry updated about the current trace and span, when an error happens, info about the trace will be sent to Sentry.
So we will know which chain of requests caused the error.
"""


