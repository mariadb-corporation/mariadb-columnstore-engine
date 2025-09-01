import json
import logging
import logging.config
from functools import partial, partialmethod

import cherrypy
from cherrypy import _cperror

from cmapi_server.constants import CMAPI_LOG_CONF_PATH
from tracing.tracer import get_tracer


class AddIpFilter(logging.Filter):
    """Filter to add IP address to logging record."""
    def filter(self, record):
        record.ip = cherrypy.request.remote.name or cherrypy.request.remote.ip
        return True


def install_trace_record_factory() -> None:
    """Install a LogRecord factory that adds 'trace_params' field.
    'trace_params' will be an empty string if there is no active trace/span
      (like in MainThread, where there is no incoming requests).
    Otherwise it will contain trace parameters.
    """
    current_factory = logging.getLogRecordFactory()

    def factory(*args, **kwargs):  # type: ignore[no-untyped-def]
        record = current_factory(*args, **kwargs)
        trace_id, span_id, parent_span_id = get_tracer().current_trace_ids()
        if trace_id and span_id:
            record.trace_params = f'rid={trace_id} sid={span_id}'
            if parent_span_id:
                record.trace_params += f' psid={parent_span_id}'
        else:
            record.trace_params = ""
        return record

    logging.setLogRecordFactory(factory)


def custom_cherrypy_error(
        self, msg='', context='', severity=logging.INFO, traceback=False
    ):
    """Write the given ``msg`` to the error log. [now without hardcoded time]

    This is not just for errors! [looks awful, but cherrypy realisation as is]
    Applications may call this at any time to log application-specific
    information.

    If ``traceback`` is True, the traceback of the current exception
    (if any) will be appended to ``msg``.

    ..Note:
        All informatio
    """
    exc_info = None
    if traceback:
        exc_info = _cperror._exc_info()

    self.error_log.log(severity, ' '.join((context, msg)), exc_info=exc_info)


def dict_config(config_filepath: str):
    with open(config_filepath, 'r', encoding='utf-8') as json_config:
        config_dict = json.load(json_config)
    logging.config.dictConfig(config_dict)


def add_logging_level(level_name, level_num, method_name=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `level_name` becomes an attribute of the `logging` module with the value
    `level_num`.
    `methodName` becomes a convenience method for both `logging` itself
    and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`).
    If `methodName` is not specified, `levelName.lower()` is used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel('TRACE')
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError(f'{level_name} already defined in logging module')
    if hasattr(logging, method_name):
        raise AttributeError(
            f'{method_name} already defined in logging module'
        )
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError(f'{method_name} already defined in logger class')

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # https://stackoverflow.com/a/35804945
    # https://stackoverflow.com/a/55276759
    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(
        logging.getLoggerClass(), method_name,
        partialmethod(logging.getLoggerClass().log, level_num)
    )
    setattr(logging, method_name, partial(logging.log, level_num))


def enable_console_logging(logger: logging.Logger) -> None:
    """Enable logging to console for passed logger by adding a StreamHandler to it"""
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)


def config_cmapi_server_logging():
    # add custom level TRACE only for develop purposes
    # could be activated using API endpoints or cli tool without relaunching
    add_logging_level('TRACE', 5)
    cherrypy._cplogging.LogManager.error = custom_cherrypy_error
    # reconfigure cherrypy.access log message format
    # Default access_log_format '{h} {l} {u} {t} "{r}" {s} {b} "{f}" "{a}"'
    # h - remote.name or remote.ip, l - "-",
    # u - getattr(request, 'login', None) or '-', t - self.time(),
    # r - request.request_line, s - status,
    # b - dict.get(outheaders, 'Content-Length', '') or '-',
    # f - dict.get(inheaders, 'Referer', ''),
    # a - dict.get(inheaders, 'User-Agent', ''),
    # o - dict.get(inheaders, 'Host', '-'),
    # i - request.unique_id, z - LazyRfc3339UtcTime()
    cherrypy._cplogging.LogManager.access_log_format = (
        '{h} ACCESS "{r}" code {s}, bytes {b}, user-agent "{a}"'
    )
    # Ensure trace_params is available on every record
    install_trace_record_factory()
    dict_config(CMAPI_LOG_CONF_PATH)
    disable_unwanted_loggers()


def change_loggers_level(level: str):
    """Set level for each custom logger except cherrypy library.

    :param level: logging level to set
    :type level: str
    """
    loggers = [
        logging.getLogger(name) for name in logging.root.manager.loggerDict
        if 'cherrypy' not in name
    ]
    loggers.append(logging.getLogger())  # add RootLogger
    for logger in loggers:
        logger.setLevel(level)

def disable_unwanted_loggers():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
