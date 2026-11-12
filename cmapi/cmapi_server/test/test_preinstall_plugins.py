import unittest
from unittest.mock import patch

import typer
from rich.console import Console

from cmapi_server.exceptions import CEJError, CMAPIBasicError
from cmapi_server.managers.upgrade import preinstall
from cmapi_server.managers.upgrade.preinstall import PreInstallManager
from mcs_cluster_tool import install_es_helpers as h


CEJ_INFO = ('127.0.0.1', '3306', 'cej', 's3cr3t pass')


class TestCheckInstalledPlugins(unittest.TestCase):
    """Tests for PreInstallManager.check_installed_plugins."""

    def setUp(self):
        patcher_nc = patch.object(preinstall, 'NodeConfig')
        patcher_cej = patch.object(
            preinstall, 'get_cej_info', return_value=CEJ_INFO
        )
        patcher_exec = patch.object(
            preinstall.BaseDispatcher, 'exec_command'
        )
        self.addCleanup(patcher_nc.stop)
        self.addCleanup(patcher_cej.stop)
        self.addCleanup(patcher_exec.stop)
        patcher_nc.start()
        self.get_cej_info = patcher_cej.start()
        self.exec_command = patcher_exec.start()

    def test_no_unsupported_plugins(self):
        self.exec_command.return_value = (
            True,
            'InnoDB\tSTORAGE ENGINE\nColumnstore\tSTORAGE ENGINE\n'
            'Mroonga_stats\tINFORMATION SCHEMA\n',
        )
        PreInstallManager.check_installed_plugins()

    def test_unsupported_plugins_reported(self):
        self.exec_command.return_value = (
            True,
            'InnoDB\tSTORAGE ENGINE\nSPIDER\tSTORAGE ENGINE\n'
            'Mroonga\tSTORAGE ENGINE\n',
        )
        with self.assertRaises(CMAPIBasicError) as ctx:
            PreInstallManager.check_installed_plugins()
        msg = ctx.exception.message
        self.assertIn('SPIDER (STORAGE ENGINE)', msg)
        self.assertIn('mariadb-plugin-spider', msg)
        self.assertIn("'ha_mroonga.so'", msg)
        self.assertNotIn('InnoDB', msg)

    def test_password_not_in_command_line(self):
        self.exec_command.return_value = (True, '')
        PreInstallManager.check_installed_plugins()
        args, kwargs = self.exec_command.call_args
        self.assertNotIn(CEJ_INFO[3], args[0])
        self.assertEqual(kwargs['env']['MYSQL_PWD'], CEJ_INFO[3])
        self.assertTrue(kwargs['silent'])

    def test_query_failure_raises(self):
        self.exec_command.return_value = (False, 'ERROR 2002 (HY000)')
        with self.assertRaises(CMAPIBasicError):
            PreInstallManager.check_installed_plugins()

    def test_cej_error_raises(self):
        self.get_cej_info.side_effect = CEJError('no user')
        with self.assertRaises(CMAPIBasicError):
            PreInstallManager.check_installed_plugins()
        self.exec_command.assert_not_called()


class TestRunPrechecks(unittest.TestCase):
    """Tests for install_es_helpers.run_prechecks."""

    def setUp(self):
        self.console = Console(quiet=True)

    def test_all_pass_returns_results_in_order(self):
        checks = [
            h.Precheck('a', 'a ok', lambda: 1),
            h.Precheck('b', 'b ok', lambda: None),
            h.Precheck('c', 'c ok', lambda: 'x'),
        ]
        self.assertEqual(h.run_prechecks(self.console, checks), [1, None, 'x'])

    def test_cmapi_error_exits_and_stops(self):
        called = []

        def fail():
            raise CMAPIBasicError('boom [not markup]')

        checks = [
            h.Precheck('a', 'a ok', fail),
            h.Precheck('b', 'b ok', lambda: called.append('b')),
        ]
        with self.assertRaises(typer.Exit) as ctx:
            h.run_prechecks(self.console, checks)
        self.assertEqual(ctx.exception.exit_code, 1)
        self.assertEqual(called, [])

    def test_typer_exit_is_propagated(self):
        def fail():
            raise typer.Exit(code=3)

        with self.assertRaises(typer.Exit) as ctx:
            h.run_prechecks(self.console, [h.Precheck('a', 'a ok', fail)])
        self.assertEqual(ctx.exception.exit_code, 3)


if __name__ == '__main__':
    unittest.main()
