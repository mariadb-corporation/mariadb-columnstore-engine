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
from unittest.mock import patch

# pylint: disable=protected-access
from lxml import etree

from cmapi_server import failover_agent
from cmapi_server.failover_agent import FailoverAgent
from cmapi_server.logging_management import add_logging_level
from failover import agent_comm
from failover.agent_comm import AgentBase, AgentComm
from failover.config import Config
from failover.heartbeat_history import HBHistory
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


if __name__ == '__main__':
    unittest.main()
