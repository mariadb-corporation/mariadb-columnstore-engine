"""Tests for optional MariaDB packages handling in install_es upgrade."""
import unittest
from unittest.mock import MagicMock, patch

from cmapi_server.constants import PkgType
from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.managers.upgrade import packages, repo
from cmapi_server.managers.upgrade.packages import PackagesManager
from cmapi_server.managers.upgrade.repo import MariaDBESRepoManager


DEB_VER = '1:10.6.22.18+maria~ubu2204'
RPM_VER = '10.6.22_18'


def fake_exec(installed: dict, available: dict):
    """Build an exec_command fake.

    :param installed: package name -> True if installed
    :param available: package name -> list of repo versions
    """
    def _exec(cmd, *args, **kwargs):
        name = cmd.split()[-1]
        if cmd.startswith('dpkg-query'):
            if name in installed:
                return True, 'ii ' if installed[name] else 'rc '
            return False, f'dpkg-query: no packages found matching {name}'
        if cmd.startswith('rpm -q'):
            return (True, name) if installed.get(name) else (False, '')
        if cmd.startswith('apt-cache madison'):
            out = ''.join(
                f' {name} | {ver} | https://dlm.mariadb.com/repo x Packages\n'
                for ver in available.get(name, [])
            )
            return True, out
        if cmd.startswith('yum info'):
            vers = available.get(name, [])
            if not vers:
                return False, 'Error: No matching Packages to list'
            return True, ''.join(
                f'Name : {name}\nVersion : {ver}\n\n' for ver in vers
            )
        # install / remove / autoremove
        return True, ''
    return _exec


class TestExtraPackagesDeb(unittest.TestCase):

    def setUp(self):
        patcher = patch.object(packages.BaseDispatcher, 'exec_command')
        self.addCleanup(patcher.stop)
        self.exec_command = patcher.start()
        self.mgr = PackagesManager('ubuntu', DEB_VER, '23.10.5')

    def _cmds(self):
        return [c.args[0] for c in self.exec_command.call_args_list]

    def test_detects_only_installed(self):
        self.exec_command.side_effect = fake_exec(
            installed={'mariadb-backup': True, 'mariadb-plugin-s3': False},
            available={},
        )
        self.assertEqual(
            self.mgr.get_installed_extra_packages(), ['mariadb-backup']
        )

    def test_check_extra_packages_reports_unavailable(self):
        self.exec_command.side_effect = fake_exec(
            installed={'mariadb-backup': True, 'mariadb-test': True},
            available={'mariadb-backup': [DEB_VER, '1:10.6.21.17+maria~ubu2204']},
        )
        self.assertEqual(
            self.mgr.check_extra_packages(),
            {
                'installed': ['mariadb-backup', 'mariadb-test'],
                'unavailable': ['mariadb-test'],
            },
        )

    def test_latest_resolves_to_server_version(self):
        mgr = PackagesManager('ubuntu', 'latest', 'latest')
        self.exec_command.side_effect = fake_exec(
            installed={'mariadb-backup': True},
            available={
                'mariadb-server': [DEB_VER, '1:10.6.21.17+maria~ubu2204'],
                'mariadb-backup': ['1:10.6.21.17+maria~ubu2204'],
            },
        )
        self.assertEqual(
            mgr.check_extra_packages()['unavailable'], ['mariadb-backup']
        )

    def test_upgrade_reinstalls_extras_after_server(self):
        self.exec_command.side_effect = fake_exec(
            installed={
                'mariadb-backup': True, 'mariadb-plugin-s3': True,
            },
            available={
                'mariadb-backup': [DEB_VER], 'mariadb-plugin-s3': [DEB_VER],
            },
        )
        with patch.object(PackagesManager, '_backup_columnstore_cnf'):
            result = self.mgr.upgrade_mdb_and_mcs()
        self.assertEqual(
            result,
            {
                'extra_packages': ['mariadb-backup', 'mariadb-plugin-s3'],
                'skipped_extra_packages': [],
            },
        )
        installs = [c for c in self._cmds() if 'apt-get install' in c]
        real = [c for c in installs if ' -s ' not in c]
        self.assertEqual(len(real), 3)
        self.assertIn('mariadb-server=', real[0])
        self.assertIn(f'mariadb-backup={DEB_VER}', real[1])
        self.assertIn(f'mariadb-plugin-s3={DEB_VER}', real[1])
        self.assertIn('--allow-downgrades', real[1])
        self.assertIn('mariadb-plugin-columnstore=', real[2])

    def test_upgrade_fails_before_removal_if_unavailable(self):
        self.exec_command.side_effect = fake_exec(
            installed={'mariadb-test': True}, available={},
        )
        with self.assertRaises(CMAPIBasicError) as ctx:
            self.mgr.upgrade_mdb_and_mcs()
        self.assertIn('mariadb-test', ctx.exception.message)
        self.assertFalse(
            [c for c in self._cmds() if 'remove' in c or 'install' in c]
        )

    def test_upgrade_skips_unavailable_when_ignored(self):
        self.exec_command.side_effect = fake_exec(
            installed={'mariadb-test': True, 'mariadb-backup': True},
            available={'mariadb-backup': [DEB_VER]},
        )
        with patch.object(PackagesManager, '_backup_columnstore_cnf'):
            result = self.mgr.upgrade_mdb_and_mcs(
                ignore_unavailable_extras=True
            )
        self.assertEqual(result['extra_packages'], ['mariadb-backup'])
        self.assertEqual(result['skipped_extra_packages'], ['mariadb-test'])
        self.assertFalse(
            [c for c in self._cmds() if 'install' in c and 'mariadb-test' in c]
        )

    def test_no_extras_keeps_old_flow(self):
        self.exec_command.side_effect = fake_exec(installed={}, available={})
        with patch.object(PackagesManager, '_backup_columnstore_cnf'):
            result = self.mgr.upgrade_mdb_and_mcs()
        self.assertEqual(result['extra_packages'], [])
        real = [
            c for c in self._cmds()
            if 'apt-get install' in c and ' -s ' not in c
        ]
        self.assertEqual(len(real), 2)


class TestExtraPackagesRpm(unittest.TestCase):

    def setUp(self):
        patcher = patch.object(packages.BaseDispatcher, 'exec_command')
        self.addCleanup(patcher.stop)
        self.exec_command = patcher.start()
        self.mgr = PackagesManager('rocky', RPM_VER, '23.10.5')

    def test_upgrade_reinstalls_extras_rpm(self):
        self.exec_command.side_effect = fake_exec(
            installed={'MariaDB-backup': True, 'MariaDB-test': True},
            available={
                'MariaDB-backup': [RPM_VER], 'MariaDB-test': [RPM_VER],
            },
        )
        with patch.object(PackagesManager, '_backup_columnstore_cnf'):
            result = self.mgr.upgrade_mdb_and_mcs()
        self.assertEqual(
            result['extra_packages'], ['MariaDB-backup', 'MariaDB-test']
        )
        cmds = [
            ' '.join(c.args[0].split())
            for c in self.exec_command.call_args_list
        ]
        self.assertIn(
            f'yum install -y MariaDB-backup-{RPM_VER} MariaDB-test-{RPM_VER}',
            cmds,
        )

    def test_unavailable_rpm(self):
        self.exec_command.side_effect = fake_exec(
            installed={'MariaDB-s3-engine': True}, available={},
        )
        self.assertEqual(
            self.mgr.check_extra_packages()['unavailable'],
            ['MariaDB-s3-engine'],
        )


class TestUnsupportedRepo(unittest.TestCase):

    def _mgr(self, os_type='ubuntu', os_version='jammy'):
        return MariaDBESRepoManager(
            token='t', arch='x86_64', os_type=os_type,
            os_version=os_version, mariadb_version='10.6.22-18',
        )

    @patch.object(repo, 'get_installed_packages', return_value=[])
    @patch.object(repo.requests, 'get')
    def test_not_needed_without_test_packages(self, get, _):
        self.assertFalse(self._mgr()._need_unsupported_repo())
        get.assert_not_called()

    @patch.object(repo, 'get_installed_packages', return_value=['mariadb-test'])
    @patch.object(repo.requests, 'get')
    def test_needed_when_repo_exists(self, get, _):
        get.return_value = MagicMock(ok=True, status_code=200)
        self.assertTrue(self._mgr()._need_unsupported_repo())
        self.assertIn(
            'mariadb-enterprise-unsupported/10.6.22-18/deb/dists/jammy',
            get.call_args.args[0],
        )

    @patch.object(repo, 'get_installed_packages', return_value=['MariaDB-test'])
    @patch.object(repo.requests, 'get')
    def test_skipped_when_repo_missing(self, get, _):
        get.return_value = MagicMock(ok=False, status_code=404)
        self.assertFalse(
            self._mgr('rocky', '9.4')._need_unsupported_repo()
        )
        self.assertIn('/rpm/rhel/9/x86_64/', get.call_args.args[0])


if __name__ == '__main__':
    unittest.main()
