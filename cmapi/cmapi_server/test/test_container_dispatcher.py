import unittest
from unittest import mock

from cmapi_server.process_dispatchers.container import ContainerDispatcher


class TestContainerDispatcherLoadBRM(unittest.TestCase):
    """MCOL-6606: workernode must not start after a failed BRM load."""

    def setUp(self):
        patches = [
            mock.patch.object(
                ContainerDispatcher, 'is_service_running', return_value=False
            ),
            mock.patch.object(
                ContainerDispatcher, 'get_libjemalloc_path',
                return_value='/usr/lib64/libjemalloc.so.2'
            ),
            mock.patch.object(
                ContainerDispatcher, '_create_mcs_process_logfile',
                return_value='/dev/null'
            ),
            mock.patch('cmapi_server.process_dispatchers.container.sleep'),
        ]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _exec_results(self, loadbrm_success):
        def fake_exec(command, *args, **kwargs):
            if 'mcs-loadbrm.py' in command:
                return loadbrm_success, ''
            return True, ''
        return fake_exec

    def _started_commands(self, exec_mock):
        return [c.args[0] for c in exec_mock.call_args_list]

    def test_workernode_not_started_when_loadbrm_fails(self):
        with mock.patch.object(
            ContainerDispatcher, 'exec_command',
            side_effect=self._exec_results(loadbrm_success=False)
        ) as exec_mock:
            result = ContainerDispatcher.start('workernode', is_primary=True)

        self.assertFalse(result)
        commands = self._started_commands(exec_mock)
        self.assertEqual(len(commands), 1)
        self.assertIn('mcs-loadbrm.py', commands[0])

    def test_workernode_started_when_loadbrm_succeeds(self):
        with mock.patch.object(
            ContainerDispatcher, 'exec_command',
            side_effect=self._exec_results(loadbrm_success=True)
        ) as exec_mock:
            ContainerDispatcher.start('workernode', is_primary=True)

        commands = self._started_commands(exec_mock)
        self.assertEqual(len(commands), 2)
        self.assertIn('mcs-loadbrm.py', commands[0])
        self.assertIn('workernode', commands[1])
        self.assertIn('DBRM_Worker1', commands[1])


if __name__ == '__main__':
    unittest.main()
