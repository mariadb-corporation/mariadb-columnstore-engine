import logging
import socket

import cherrypy
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH

SENTRY_ACTIVE = False

logger = logging.getLogger(__name__)

def maybe_init_sentry() -> bool:
    """Initialize Sentry from CMAPI configuration.

    Reads config and initializes Sentry only if dsn parameter is present in corresponding section.
    The initialization enables the following integrations:
    - LoggingIntegration: capture warning-level logs as Sentry events and use
      lower-level logs as breadcrumbs.
    - AioHttpIntegration: propagate trace headers for outbound requests made
      with `aiohttp`.

    The function is a no-op if the DSN is missing.

    Returns: True if Sentry is initialized, False otherwise.
    """
    global SENTRY_ACTIVE
    try:
        cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
        dsn = helpers.dequote(
            cfg_parser.get('Sentry', 'dsn', fallback='').strip()
        )
        if not dsn:
            return False

        environment = helpers.dequote(
            cfg_parser.get('Sentry', 'environment', fallback='development').strip()
        )
        traces_sample_rate_str = helpers.dequote(
            cfg_parser.get('Sentry', 'traces_sample_rate', fallback='1.0').strip()
        )
    except Exception:
        logger.exception('Failed to initialize Sentry.')
        return False

    try:
        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.WARNING,
        )

        try:
            traces_sample_rate = float(traces_sample_rate_str)
        except ValueError:
            logger.error('Invalid traces_sample_rate: %s', traces_sample_rate_str)
            traces_sample_rate = 1.0

        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            traces_sample_rate=traces_sample_rate,
            integrations=[sentry_logging, AioHttpIntegration()],
        )
        SENTRY_ACTIVE = True
        logger.info('Sentry initialized for CMAPI via config.')
    except Exception:
        logger.exception('Failed to initialize Sentry.')
        return False

    logger.info('Sentry successfully initialized.')
    return True

def _sentry_on_start_resource():
    """Start or continue a Sentry transaction for the current CherryPy request.

    - Continues an incoming distributed trace using Sentry trace headers if
      present; otherwise starts a new transaction with `op='http.server'`.
    - Pushes the transaction into the current Sentry scope and attaches useful
      request metadata as tags and context (HTTP method, path, client IP,
      hostname, request ID, and a filtered subset of headers).
    - Stores the transaction on the CherryPy request object for later finishing
      in `_sentry_on_end_request`.
    """
    if not SENTRY_ACTIVE:
        return
    try:
        request = cherrypy.request
        headers = dict(getattr(request, 'headers', {}) or {})
        name = f"{request.method} {request.path_info}"
        transaction = sentry_sdk.start_transaction(
            op='http.server', name=name, continue_from_headers=headers
        )
        sentry_sdk.Hub.current.scope.set_span(transaction)

        # Add request-level context/tags
        scope = sentry_sdk.Hub.current.scope
        scope.set_tag('http.method', request.method)
        scope.set_tag('http.path', request.path_info)
        scope.set_tag('client.ip', getattr(request.remote, 'ip', ''))
        scope.set_tag('instance.hostname', socket.gethostname())
        request_id = getattr(request, 'unique_id', None)
        if request_id:
            scope.set_tag('request.id', request_id)
        # Optionally add headers as context without sensitive values
        safe_headers = {k: v for k, v in headers.items()
                        if k.lower() not in {'authorization', 'x-api-key'}}
        scope.set_context('headers', safe_headers)

        request.sentry_transaction = transaction
    except Exception:
        logger.exception('Failed to start Sentry transaction.')


def _sentry_before_error_response():
    """Capture the current exception (if any) to Sentry before error response.

    This hook runs when CherryPy prepares an error response. If an exception is
    available in the current context, it will be sent to Sentry.
    """
    if not SENTRY_ACTIVE:
        return
    try:
        sentry_sdk.capture_exception()
    except Exception:
        logger.exception('Failed to capture exception to Sentry.')


def _sentry_on_end_request():
    """Finish the Sentry transaction for the current CherryPy request.

    Attempts to set the HTTP status code on the active transaction and then
    finishes it. If no transaction was started on this request, the function is
    a no-op.
    """
    if not SENTRY_ACTIVE:
        return
    try:
        request = cherrypy.request
        transaction = getattr(request, 'sentry_transaction', None)
        if transaction is None:
            return
        status = cherrypy.response.status
        try:
            status_code = int(str(status).split()[0])
        except Exception:
            status_code = None
        try:
            if status_code is not None and hasattr(transaction, 'set_http_status'):
                transaction.set_http_status(status_code)
        except Exception:
            logger.exception('Failed to set HTTP status code on Sentry transaction.')
        transaction.finish()
    except Exception:
        logger.exception('Failed to finish Sentry transaction.')


class SentryTool(cherrypy.Tool):
    """CherryPy Tool that wires Sentry request lifecycle hooks.

    The tool attaches handlers for `on_start_resource`, `before_error_response`,
    and `on_end_request` in order to manage Sentry transactions and error
    capture across the request lifecycle.
    """
    def __init__(self):
        cherrypy.Tool.__init__(self, 'on_start_resource', self._tool_callback, priority=50)

    @staticmethod
    def _tool_callback():
        """Attach Sentry lifecycle callbacks to the current CherryPy request."""
        cherrypy.request.hooks.attach(
            'on_start_resource', _sentry_on_start_resource, priority=50
        )
        cherrypy.request.hooks.attach(
            'before_error_response', _sentry_before_error_response, priority=60
        )
        cherrypy.request.hooks.attach(
            'on_end_request', _sentry_on_end_request, priority=70
        )


def register_sentry_cherrypy_tool() -> None:
    """Register the Sentry CherryPy tool under `tools.sentry`.

    This function is safe to call multiple times; failures are silently ignored
    to avoid impacting the application startup.
    """
    if not SENTRY_ACTIVE:
        return

    try:
        cherrypy.tools.sentry = SentryTool()
    except Exception:
        logger.exception('Failed to register Sentry CherryPy tool.')

