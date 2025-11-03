"""Module contains custom exceptions."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Optional, TypeVar, Any

from pydantic import BaseModel, ValidationError

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


class ResolutionError(CMAPIBasicError):
    """Errors related to DNS resolution"""


class ResolutionPolicyViolationError(CMAPIBasicError):
    """Errors where results are rejected by the current resolving policy."""


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


T = TypeVar('T', bound=BaseModel)

def validate_or_422(
    model: type[T], payload: Any, logger, func_name: str,
    prefix: Optional[str] = 'Invalid request body',
) -> T:
    """Validate payload with Pydantic model or raise HTTP 422 APIError."""
    try:
        return model.model_validate(payload)
    except ValidationError as exp:
        msg = f"{prefix}: {exp.errors()}" if prefix else str(exp.errors())
        logger.error(f"{func_name} {msg}", exc_info=False)
        raise APIError(422, msg) from exp


@contextmanager
def exc_to_422(logger, func_name: str, prefix: Optional[str] = None) -> Iterator[None]:
    """Convert any exception into HTTP 422"""
    with cmapi_error_to_422(logger, func_name):
        with exc_to_cmapi_error(prefix=prefix):
            yield
