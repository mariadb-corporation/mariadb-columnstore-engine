import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH
from tracing.sentry_backend import SentryBackend
from tracing.tracer import get_tracer

SENTRY_ACTIVE = False

logger = logging.getLogger(__name__)

def maybe_init_sentry() -> bool:
    """Initialize Sentry from CMAPI configuration.

    Reads config and initializes Sentry only if dsn parameter is present
      in corresponding section of cmapi config.
    The initialization enables LoggingIntegration: it captures error-level logs as Sentry events
      and uses lower-level logs as breadcrumbs.

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
    except Exception:
        logger.exception('Failed to initialize Sentry.')
        return False

    try:
        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR,
        )

        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            sample_rate=1.0,
            traces_sample_rate=1.0,
            integrations=[sentry_logging],
            debug=True,
        )
        SENTRY_ACTIVE = True
        # Register backend to mirror our internal spans into Sentry
        get_tracer().register_backend(SentryBackend())
        logger.info('Sentry initialized for CMAPI via config.')
    except Exception:
        logger.exception('Failed to initialize Sentry.')
        return False

    logger.info('Sentry successfully initialized.')
    return True

