import unittest

from cmapi_server.managers.upgrade.packages import PackagesManager


class TestExtractDebMajorMinor(unittest.TestCase):

    _CASES = [
        ('1:10.6.12.7+maria~ubu2204', '10.6'),
        ('10.11.3+maria~ubu2204', '10.11'),
        ('11.4', '11.4'),
        ('10.6.12', '10.6'),
        ('2:23.1.0', '23.1'),
        ('', ''),
        ('foobar', ''),
        ('10', ''),
        ('1:', ''),
        ('abc10.6', ''),
    ]

    def test_extract_deb_major_minor(self):
        for version, expected in self._CASES:
            with self.subTest(version=version):
                self.assertEqual(
                    PackagesManager._extract_deb_major_minor(version),
                    expected,
                )


if __name__ == '__main__':
    unittest.main()
