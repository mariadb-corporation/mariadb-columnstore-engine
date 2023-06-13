import datetime
import logging
import sys
import unittest
from cmapi_server.logging_management import add_logging_level


class DatedTextTestResult(unittest.TextTestResult):
    def startTest(self, test: unittest.case.TestCase):
        self.stream.write('\n')
        self.stream.write(
            datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]: ")
        )
        return super().startTest(test)


def run_tests_from_package(p_name: str):
    logging.info(f'Starting tests from package {p_name}')
    loader = unittest.TestLoader()
    testsuite = loader.discover(
        pattern='test_*.py', start_dir=p_name, top_level_dir='./'
    )
    runner = unittest.runner.TextTestRunner(
        verbosity=3, failfast=True, resultclass=DatedTextTestResult
    )
    result = runner.run(testsuite)
    failed = False
    if not result.wasSuccessful():
        failed = True
        sys.exit(failed)
    logging.info(f'Finished tests from package {p_name}')


if __name__ == "__main__":
    add_logging_level('TRACE', 5)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] (%(name)s) %(message)s'
    )
    run_tests_from_package('cmapi_server')
    run_tests_from_package('mcs_node_control')
