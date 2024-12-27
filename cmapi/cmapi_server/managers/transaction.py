"""Module related to CMAPI transaction management logic."""
import logging
from contextlib import ContextDecorator
from signal import (
    SIGINT, SIGTERM, SIGHUP, SIG_DFL, signal, default_int_handler
)
from typing import Optional, Type

from cmapi_server.constants import DEFAULT_MCS_CONF_PATH, TRANSACTION_TIMEOUT
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.helpers import (
    get_id, commit_transaction, rollback_transaction, start_transaction
)


class TransactionManager(ContextDecorator):
    """Context manager and decorator to put any code inside CMAPI transaction.

    :param timeout: time in sec after transaction will be autocommitted,
                    defaults to 300.0 (TRANSACTION_TIMEOUT)

    :param timeout: _description_, defaults to 300
    :type timeout: float, optional
    :param txn_id: custom transaction id, defaults to None
    :type txn_id: Optional[int], optional
    :param handle_signals: handle specific signals or not, defaults to False
    :type handle_signals: bool, optional
    """

    def __init__(
            self, timeout: float = TRANSACTION_TIMEOUT,
            txn_id: Optional[int] = None, handle_signals: bool = False
    ):
        self.timeout = timeout
        self.txn_id = txn_id or get_id()
        self.handle_signals = handle_signals
        self.active_transaction = False

    def _handle_exception(
            self, exc: Optional[Type[Exception]] = None,
            signum: Optional[int] = None
    ) -> None:
        """Handle raised exceptions.

        We need to rollback transaction in some cases and return back default
        signal handlers.

        :param exc: exception passed, defaults to None
        :type exc: Optional[Type[Exception]], optional
        :param signum: signal if it cause exception, defaults to None
        :type signum: Optional[int], optional
        :raises exc: raises passed exception
        """
        # message = 'Got exception in transaction manager'
        if (exc or signum) and self.active_transaction:
            self.rollback_transaction()
        self.set_default_signals()
        raise exc

    def _handle_signal(self, signum, frame) -> None:
        """Handler for signals.

        :param signum: signal number
        :type signum: int
        """
        logging.error(f'Caught signal "{signum}" in transaction manager.')
        self._handle_exception(signum=signum)

    def set_custom_signals(self) -> None:
        """Set handlers for several signals."""
        # register handler for signals for proper handling them
        for sig in SIGINT, SIGTERM, SIGHUP:
            signal(sig, self._handle_signal)

    def set_default_signals(self) -> None:
        """Return defalt handlers for specific signals."""
        if self.handle_signals:
            signal(SIGINT, default_int_handler)
            signal(SIGTERM, SIG_DFL)
            signal(SIGHUP, SIG_DFL)

    def rollback_transaction(self) -> None:
        """Rollback transaction."""
        try:
            rollback_transaction(self.txn_id)
            self.active_transaction = False
            logging.debug(f'Success rollback of transaction "{self.txn_id}".')
        except Exception:
            logging.error(
                f'Error while rollback transaction "{self.txn_id}"',
                exc_info=True
            )

    def commit_transaction(self):
        """Commit transaction."""
        try:
            commit_transaction(
                self.txn_id, cs_config_filename=DEFAULT_MCS_CONF_PATH
            )
        except Exception:
            logging.error(f'Error while committing transaction {self.txn_id}')
            self.rollback_transaction()
            self.set_default_signals()
            raise

    def __enter__(self):
        if self.handle_signals:
            self.set_custom_signals()
        try:
            suceeded, _transaction_id, successes = start_transaction(
                cs_config_filename=DEFAULT_MCS_CONF_PATH,
                txn_id=self.txn_id, timeout=self.timeout
            )
        except Exception as exc:
            logging.error('Error while starting the transaction.')
            self._handle_exception(exc=exc)
        if not suceeded:
            self._handle_exception(
                exc=CMAPIBasicError('Starting transaction isn\'t succesful.')
            )
        if suceeded and len(successes) == 0:
            self._handle_exception(
                exc=CMAPIBasicError('There are no nodes in the cluster.')
            )
        self.active_transaction = True
        return self

    def __exit__(self, *exc):
        if exc[0] and self.active_transaction:
            self.rollback_transaction()
            self.set_default_signals()
            return False
        if self.active_transaction:
            self.commit_transaction()
            self.set_default_signals()
        return True
