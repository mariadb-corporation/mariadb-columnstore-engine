"""Module contains custom exceptions."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Optional

from cmapi_server.controllers.error import APIError


class CMAPIBasicError(Exception):
    """Basic exception raised for CMAPI related processes.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self) -> str:
        return self.message


class CEJError(CMAPIBasicError):
    """Exception raised for CEJ related processes.

    Attributes:
        message -- explanation of the error
    """


@contextmanager
def exc_to_cmapi_error(prefix: Optional[str] = None) -> Iterator[None]:
    """Context manager to standardize error wrapping into CMAPIBasicError.

    Re-raises existing CMAPIBasicError untouched (to preserve detailed
    messages). Any other exception type is wrapped into CMAPIBasicError with an
    optional prefix and the original exception string appended as details.

    :param prefix: Optional message prefix for wrapped errors
    :raises CMAPIBasicError: for any wrapped non-CMAPIBasicError exceptions
    """
    try:
        yield
    except CMAPIBasicError:
        # Preserve detailed messages from deeper layers (e.g., validation)
        raise
    except Exception as err:
        msg = f"{prefix}. Details: {err}" if prefix else str(err)
        raise CMAPIBasicError(msg) from err


@contextmanager
def cmapi_error_to_422(logger, func_name: str) -> Iterator[None]:
    """Convert CMAPIBasicError to HTTP 422 APIError."""
    try:
        yield
    except CMAPIBasicError as err:
        # mirror raise_422_error behavior locally to avoid circular imports
        logger.error(f'{func_name} {err.message}', exc_info=False)
        raise APIError(422, err.message) from err
