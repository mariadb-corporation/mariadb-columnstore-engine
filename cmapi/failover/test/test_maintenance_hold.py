"""MCOL-6610: a cluster stop must not turn into a failover.

Covers the pieces that broke the stop on a 3-node cluster: the failover
agent waiting forever for a transaction the stop holds, NodeMonitor.stop()
joining that wait, and membership changes queued while nodes were being
stopped on purpose.
"""
import logging
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, call, patch

# pylint: disable=protected-access
from lxml import etree

from cmapi_server import failover_agent
from cmapi_server.failover_agent import FailoverAgent
from cmapi_server.logging_management import add_logging_level
from failover import agent_comm, node_monitor
from failover.agent_comm import AgentBase, AgentComm
from failover.config import Config
from failover.heartbeat_history import HBHistory
from failover.node_monitor import NodeMonitor
from mcs_node_control.models.node_config import NodeConfig

# the failover code logs at TRACE, which CMAPI registers at start-up
if not hasattr(logging, 'TRACE'):
    add_logging_level('TRACE', 5)


class RecordingAgent(AgentBase):
    """Never gets a transaction unless told to; records what ran."""

    def __init__(self, txn_result=None, ignore_stop=False):
        self.txn_result = txn_result
        self.ignore_stop = ignore_stop
        self.deactivated = []
        self.txn_calls = 0

    def deactivateNodes(self, nodes):
        self.deactivated.append(nodes)

    def startTransaction(self, extra_nodes=None, remove_nodes=None, should_stop=None):
        self.txn_calls += 1
        if self.ignore_stop:
            time.sleep(3)
            return self.txn_result
        while self.txn_result is None and not should_stop():
            time.sleep(0.05)
        return self.txn_result


def wait_for(cond, secs=10):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


class FailoverAgentTxnWaitTest(unittest.TestCase):

    @patch.object(failover_agent.helpers, 'start_transaction',
                  return_value=(False, None, None))
    def test_gives_up_when_asked(self, _start):
        stop = threading.Event()
        threading.Timer(1.5, stop.set).start()
        began = time.monotonic()
        self.assertIsNone(FailoverAgent().startTransaction(should_stop=stop.is_set))
        self.assertLess(time.monotonic() - began, 4)

    @patch.object(failover_agent, 'TXN_WAIT_DEADLINE_SECS', 1)
    @patch.object(failover_agent.helpers, 'start_transaction',
                  return_value=(False, None, None))
    def test_gives_up_at_deadline(self, _start):
        self.assertIsNone(FailoverAgent().startTransaction())


class AgentCommHoldTest(unittest.TestCase):

    def test_hold_drops_queued_membership_events(self):
        ac = AgentComm(RecordingAgent())
        ac.deactivateNodes(['node2'])
        self.assertEqual((1, 1), ac.getQueueSize())
        ac.holdMembershipChanges(True)
        self.assertEqual((0, 0), ac.getQueueSize())
        # the monitor can queue it again once the hold is lifted
        ac.holdMembershipChanges(False)
        ac.deactivateNodes(['node2'])
        self.assertEqual((1, 1), ac.getQueueSize())

    def test_hold_ends_transaction_wait_without_running_events(self):
        agent = RecordingAgent()
        ac = AgentComm(agent)
        ac.start()
        try:
            ac.deactivateNodes(['node2'])
            self.assertTrue(wait_for(lambda: agent.txn_calls == 1))
            ac.holdMembershipChanges(True)
            self.assertTrue(wait_for(lambda: ac.getQueueSize() == (0, 0)))
            self.assertEqual([], agent.deactivated)
        finally:
            # the idle runner polls every 5 s
            began = time.monotonic()
            ac.die()
            self.assertLess(time.monotonic() - began, 7)

    def test_start_forgets_events_from_before_the_stop(self):
        ac = AgentComm(RecordingAgent())
        ac.deactivateNodes(['node2'])
        ac.start()
        ac.die()
        self.assertEqual((0, 0), ac.getQueueSize())


class AgentCommDieTest(unittest.TestCase):

    @patch.object(agent_comm, 'DIE_JOIN_TIMEOUT_SECS', 0.5)
    def test_die_does_not_wait_forever(self):
        agent = RecordingAgent(ignore_stop=True)
        ac = AgentComm(agent)
        ac.start()
        ac.deactivateNodes(['node2'])
        self.assertTrue(wait_for(lambda: agent.txn_calls == 1))
        began = time.monotonic()
        ac.die()
        self.assertLess(time.monotonic() - began, 2)
        ac._thread.join(10)

    def test_die_from_the_runner_thread_returns(self):
        ac = AgentComm(RecordingAgent())
        done = threading.Event()

        def runner():
            ac.die()
            done.set()

        ac._thread = threading.Thread(target=runner)
        ac._thread.start()
        self.assertTrue(done.wait(2))


class TxnRecordingAgent(RecordingAgent):
    """Gets transaction 7 and records what is committed or rolled back.
    on_txn runs just before the transaction is handed back."""

    def __init__(self, ignore_stop=False, on_txn=None):
        super().__init__(txn_result=(7, ['node2']), ignore_stop=ignore_stop)
        self.on_txn = on_txn
        self.committed = []
        self.rolled_back = []

    def startTransaction(self, extra_nodes=None, remove_nodes=None, should_stop=None):
        txn = super().startTransaction(extra_nodes, remove_nodes, should_stop)
        if self.on_txn:
            self.on_txn()
        return txn

    def commitTransaction(self, txnid, nodes):
        self.committed.append((txnid, nodes))

    def rollbackTransaction(self, txnid, nodes):
        self.rolled_back.append((txnid, nodes))


class AgentCommTxnAfterStopTest(unittest.TestCase):
    """A transaction that arrives once the runner should no longer act."""

    def test_hold_after_txn_acquired_rolls_back(self):
        agent = TxnRecordingAgent()
        ac = AgentComm(agent)
        agent.on_txn = lambda: ac.holdMembershipChanges(True)
        ac.start()
        try:
            ac.deactivateNodes(['node2'])
            self.assertTrue(wait_for(lambda: agent.rolled_back))
            self.assertEqual([(7, ['node2'])], agent.rolled_back)
            self.assertEqual([], agent.committed)
            self.assertEqual([], agent.deactivated)
            self.assertEqual((0, 0), ac.getQueueSize())
        finally:
            ac.die()

    @patch.object(agent_comm, 'DIE_JOIN_TIMEOUT_SECS', 0.5)
    def test_superseded_runner_exits_without_acting(self):
        # die() gives up on a runner stuck in startTransaction, and start()
        # makes a new one; the old one gets its transaction afterwards.
        agent = TxnRecordingAgent(ignore_stop=True)
        ac = AgentComm(agent)
        ac.start()
        old = ac._thread
        try:
            ac.deactivateNodes(['node2'])
            self.assertTrue(wait_for(lambda: agent.txn_calls == 1))
            ac.die()
            self.assertTrue(old.is_alive())
            ac.start()
            self.assertIsNot(old, ac._thread)
            old.join(10)
            self.assertFalse(old.is_alive())
            self.assertEqual([(7, ['node2'])], agent.rolled_back)
            self.assertEqual([], agent.committed)
            self.assertEqual([], agent.deactivated)
            self.assertTrue(ac._thread.is_alive())
        finally:
            ac.die()


class HeartbeatHistoryClearTest(unittest.TestCase):

    def test_clear_forgets_missed_responses(self):
        hb = HBHistory(tickWindow=10)
        hb.getNodeHistory('node2', 5)
        for tick in range(1, 11):
            hb.setCurrentTick(tick)
        self.assertIn(HBHistory.NoResponse, hb.getNodeHistory('node2', 5))
        hb.clear()
        history = hb.getNodeHistory('node2', 5)
        self.assertTrue(all(x == HBHistory.GoodResponse for x in history))


class ConfigMaintenanceTest(unittest.TestCase):
    XML = (
        '<Columnstore><DesiredNodes><Node>node1</Node></DesiredNodes>'
        '<ActiveNodes><Node>node1</Node></ActiveNodes><InactiveNodes/>'
        '<PrimaryNode>node1</PrimaryNode>{}</Columnstore>'
    )

    def _write(self, path, maintenance, mtime):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.XML.format(maintenance))
        os.utime(path, (mtime, mtime))

    @patch.object(NodeConfig, 'get_network_addresses_and_names', return_value=[])
    @patch.object(NodeConfig, 'get_current_config_root',
                  lambda self, path: etree.parse(path).getroot())
    def test_reads_the_maintenance_flag(self, _names):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'Columnstore.xml')
            self._write(path, '<Maintenance>true</Maintenance>', 1000)
            config = Config(config_file=path)
            self.assertTrue(config.inMaintenance())
            self._write(path, '<Maintenance>false</Maintenance>', 2000)
            self.assertFalse(config.inMaintenance())
            self._write(path, '', 3000)
            self.assertFalse(config.inMaintenance())


class FakeConfig:
    """Three nodes; inMaintenance() answers from a script, then stops the
    monitor at the end of that loop pass."""

    def __init__(self, maintenance):
        self.maintenance = list(maintenance)
        self.monitor = None

    def getAllNodes(self):
        nodes = ['node1', 'node2', 'node3']
        return nodes, list(nodes), []

    def who_am_I(self):
        return 'node1'

    def getPrimaryNode(self):
        return 'node1'

    def inMaintenance(self):
        if len(self.maintenance) == 1:
            self.monitor._die = True
        return self.maintenance.pop(0)


class NodeMonitorMaintenanceTest(unittest.TestCase):

    def run_monitor(self, maintenance):
        config = FakeConfig(maintenance)
        with patch.object(node_monitor, 'HeartBeater'), \
                patch.object(node_monitor, 'SharedStorageMonitor'):
            monitor = NodeMonitor(agent=RecordingAgent(), config=config)
        monitor.stop = lambda: None  # nothing was started; __del__ calls it
        config.monitor = monitor
        monitor._agentComm = MagicMock()
        clear = patch.object(monitor._hbHistory, 'clear',
                             wraps=monitor._hbHistory.clear).start()
        self.addCleanup(patch.stopall)
        with patch.object(node_monitor.time, 'sleep'), \
                patch.object(node_monitor.AppStatefulConfig, 'is_shared_storage',
                             return_value=False) as shared:
            monitor._monitor()
        return monitor, clear, shared

    def test_maintenance_holds_then_resumes_with_fresh_history(self):
        monitor, clear, shared = self.run_monitor([True, True, False])
        self.assertEqual(
            [call.holdMembershipChanges(True), call.holdMembershipChanges(False)],
            monitor._agentComm.mock_calls,
        )
        clear.assert_called_once_with()
        # the passes in maintenance skip the rest of the loop
        self.assertEqual(1, shared.call_count)

    def test_no_maintenance_no_hold(self):
        monitor, clear, shared = self.run_monitor([False, False])
        self.assertEqual([], monitor._agentComm.mock_calls)
        clear.assert_not_called()
        self.assertEqual(2, shared.call_count)


if __name__ == '__main__':
    unittest.main()
