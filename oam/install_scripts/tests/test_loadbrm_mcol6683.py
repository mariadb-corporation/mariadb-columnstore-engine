"""MCOL-6683: loadbrm and BRM shared memory that a worker stop kept.

run_load_brm() tells a failure that clearing the shared memory fixes from
any other, live_brm_readers() names the processes still mapping it, and the
atexit hook clears it when the load did not complete.

mcs-loadbrm.py.in is loaded as is: its @VAR@ placeholders are only string
literals, and nothing here uses them. It must stay Python 3.6-compatible, and
so must this test: run_load_brm() runs a real shell command so that the
subprocess.run() arguments are checked by the interpreter loadbrm runs on.
"""
import importlib.machinery
import importlib.util
import io
import os
import subprocess
import unittest
from unittest.mock import patch

LOADBRM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'mcs-loadbrm.py.in'
)


def load_loadbrm():
    loader = importlib.machinery.SourceFileLoader('mcs_loadbrm', LOADBRM_PATH)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


loadbrm = load_loadbrm()


class RunLoadBrmTest(unittest.TestCase):
    def run_load_brm(self, cmd):
        out = io.StringIO()
        with patch.object(loadbrm.sys, 'stdout', out):
            try:
                return loadbrm.run_load_brm(cmd)
            finally:
                self.output = out.getvalue()

    def test_success_is_true_and_output_is_echoed(self):
        self.assertIs(self.run_load_brm('echo loaded'), True)
        self.assertEqual(self.output, 'loaded\n')

    def test_has_to_be_cleared_is_false(self):
        cmd = 'echo "the shared memory segment has to be cleared" >&2; exit 1'
        self.assertIs(self.run_load_brm(cmd), False)
        self.assertIn('has to be cleared', self.output)

    def test_other_failure_raises(self):
        with self.assertRaises(subprocess.CalledProcessError) as ctx:
            self.run_load_brm('echo "no such file"; exit 3')
        self.assertEqual(ctx.exception.returncode, 3)
        self.assertEqual(self.output, 'no such file\n')


class LiveBrmReadersTest(unittest.TestCase):
    COMM = {
        '/proc/123/comm': 'PrimProc\n',
        '/proc/7/comm': 'bash\n',
        # comm is cut to 15 characters
        '/proc/99/comm': 'WriteEngineServ\n',
        # /proc/42 has exited between glob and open
    }

    def fake_open(self, path):
        if path not in self.COMM:
            raise FileNotFoundError(path)
        return io.StringIO(self.COMM[path])

    def test_names_running_readers_with_their_pid(self):
        paths = sorted(self.COMM) + ['/proc/42/comm']
        with patch.object(loadbrm.glob, 'glob', return_value=paths) as glob, \
                patch.object(loadbrm, 'open', self.fake_open, create=True):
            readers = loadbrm.live_brm_readers()
        glob.assert_called_once_with('/proc/[0-9]*/comm')
        self.assertEqual(readers, ['PrimProc[123]', 'WriteEngineServer[99]'])


class ClearShmTest(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        self.run = patch.object(loadbrm.subprocess, 'run').start()

    def test_not_loaded_clears(self):
        with patch.object(loadbrm, 'loaded', False, create=True), \
                patch.object(loadbrm, 'clear_shm') as clear_shm:
            loadbrm.clear_shm_unless_loaded()
        clear_shm.assert_called_once_with('the BRM load did not complete')

    def test_loaded_keeps_it(self):
        with patch.object(loadbrm, 'loaded', True, create=True), \
                patch.object(loadbrm, 'clear_shm') as clear_shm:
            loadbrm.clear_shm_unless_loaded()
        clear_shm.assert_not_called()

    def test_clear_names_readers_that_keep_the_old_segments(self):
        with patch.object(
            loadbrm, 'live_brm_readers', return_value=['PrimProc[123]']
        ), self.assertLogs(level='ERROR') as logs:
            loadbrm.clear_shm('why')
        self.assertEqual(self.run.call_args[0][0], loadbrm.CLEARSHM)
        self.assertIn('PrimProc[123]', logs.output[0])
        self.assertIn('restarted', logs.output[0])

    def test_clear_with_no_readers_is_info(self):
        with patch.object(loadbrm, 'live_brm_readers', return_value=[]), \
                self.assertLogs(level='INFO') as logs:
            loadbrm.clear_shm('why')
        self.assertEqual(logs.records[0].levelname, 'INFO')
        self.run.assert_called_once()


if __name__ == '__main__':
    unittest.main()
