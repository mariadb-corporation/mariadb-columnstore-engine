"""Module related to CMAPI transaction management logic."""
import logging
from contextlib import ContextDecorator
from signal import (
    SIGINT, SIGTERM, SIGHUP, SIG_DFL, signal, default_int_handler
)
from typing import Optional

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
    :param extra_nodes: extra nodes to start transaction at, defaults to None
    :type extra_nodes: Optional[list], optional
    :param remove_nodes: nodes to remove from transaction, defaults to None
    :type remove_nodes: Optional[list], optional
    :param optional_nodes: nodes to add to transaction, defaults to None
    :type optional_nodes: Optional[list], optional
    :raises CMAPIBasicError: if there are no nodes in the cluster
    :raises CMAPIBasicError: if starting transaction isn't succesful
    :raises Exception: if error while starting the transaction
    :raises Exception: if error while committing transaction
    :raises Exception: if error while rollback transaction
    """

    def __init__(
            self, timeout: float = TRANSACTION_TIMEOUT,
            txn_id: Optional[int] = None, handle_signals: bool = False,
            extra_nodes: Optional[list] = None,
            remove_nodes: Optional[list] = None,
            optional_nodes: Optional[list] = None,
    ):
        self.timeout = timeout
        self.txn_id = txn_id or get_id()
        self.handle_signals = handle_signals
        self.active_transaction = False
        self.extra_nodes = extra_nodes
        self.remove_nodes = remove_nodes
        self.optional_nodes = optional_nodes
        self.success_txn_nodes = None

    def _handle_exception(
            self, exc: Optional[Exception] = None,
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
            self.rollback_transaction(nodes=self.success_txn_nodes)
        self.set_default_signals()
        if exc is not None:
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

    def rollback_transaction(self, nodes: Optional[list] = None) -> None:
        """Rollback transaction.

        :param nodes: nodes to rollback transaction, defaults to None
        :type nodes: Optional[list], optional
        """
        try:
            rollback_transaction(self.txn_id, nodes=nodes)
            self.active_transaction = False
            logging.debug(f'Success rollback of transaction "{self.txn_id}".')
        except Exception:
            logging.error(
                f'Error while rollback transaction "{self.txn_id}"',
                exc_info=True
            )

    def commit_transaction(self, nodes: Optional[list] = None) -> None:
        """Commit transaction.

        :param nodes: nodes to commit transaction, defaults to None
        :type nodes: Optional[list], optional
        """
        try:
            commit_transaction(
                self.txn_id, cs_config_filename=DEFAULT_MCS_CONF_PATH,
                nodes=nodes
            )
        except Exception:
            logging.error(f'Error while committing transaction {self.txn_id}')
            self.rollback_transaction(nodes=self.success_txn_nodes)
            self.set_default_signals()
            raise

    def __enter__(self):
        if self.handle_signals:
            self.set_custom_signals()
        suceeded, success_txn_nodes = False, []
        try:
            suceeded, _, success_txn_nodes = start_transaction(
                cs_config_filename=DEFAULT_MCS_CONF_PATH,
                extra_nodes=self.extra_nodes, remove_nodes=self.remove_nodes,
                optional_nodes=self.optional_nodes,
                txn_id=self.txn_id, timeout=self.timeout,
            )
        except Exception as exc:
            logging.error('Error while starting the transaction.')
            self._handle_exception(exc=exc)
        if not suceeded:
            self._handle_exception(
                exc=CMAPIBasicError('Starting transaction isn\'t succesful.')
            )
        if suceeded and len(success_txn_nodes) == 0:
            # corner case when deleting last node in the cluster
            # TODO: remove node mechanics potentially has a vulnerability
            #       because no transaction started for removing node.
            #       Probably in some cases rollback never works for removing
            #       node, because it never exist in success_txn_nodes.
            if not self.remove_nodes:
                self._handle_exception(
                    exc=CMAPIBasicError('There are no nodes in the cluster.')
                )
        self.active_transaction = True
        self.success_txn_nodes = success_txn_nodes
        return self

    def __exit__(self, *exc):
        if exc[0] and self.active_transaction:
            self.rollback_transaction(nodes=self.success_txn_nodes)
            self.set_default_signals()
            return False
        if self.active_transaction:
            self.commit_transaction(nodes=self.success_txn_nodes)
            self.set_default_signals()
        return True
