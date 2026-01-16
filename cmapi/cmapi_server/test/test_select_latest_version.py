import unittest

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.managers.upgrade.repo import MariaDBESRepoManager


class TestSelectLatestVersion(unittest.TestCase):
    """Tests for MariaDBESRepoManager.select_latest_version."""
    versions = ['10.6.25-22', '11.4.10-8', '11.8.6-4']

    def test_no_prefix_returns_latest(self):
        result = MariaDBESRepoManager.select_latest_version(self.versions)
        self.assertEqual(result, '11.8.6-4')

    def test_prefix_major_only(self):
        result = MariaDBESRepoManager.select_latest_version(self.versions, '10')
        self.assertEqual(result, '10.6.25-22')

    def test_prefix_major_minor(self):
        result = MariaDBESRepoManager.select_latest_version(self.versions, '11.4')
        self.assertEqual(result, '11.4.10-8')

    def test_prefix_no_match_raises(self):
        with self.assertRaises(CMAPIBasicError) as ctx:
            MariaDBESRepoManager.select_latest_version(self.versions, '16')
        self.assertIn('No MDB version matched prefix', str(ctx.exception))

    def test_invalid_prefix_raises(self):
        with self.assertRaises(CMAPIBasicError) as ctx:
            MariaDBESRepoManager.select_latest_version(self.versions, 'abc')
        self.assertIn('Invalid version prefix', str(ctx.exception))

    def test_prefix_with_underscore(self):
        result = MariaDBESRepoManager.select_latest_version(
            self.versions, '10_6'
        )
        self.assertEqual(result, '10.6.25-22')

    def test_empty_prefix_same_as_no_prefix(self):
        result = MariaDBESRepoManager.select_latest_version(self.versions, '')
        self.assertEqual(result, '11.8.6-4')


if __name__ == '__main__':
    unittest.main()
