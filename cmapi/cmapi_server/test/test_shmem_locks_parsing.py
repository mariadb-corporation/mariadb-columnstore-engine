import unittest
import logging

from cmapi_server.process_dispatchers.utils import (
    parse_locks_state,
    sum_active_locks,
)

logger = logging.getLogger(__name__)

class TestShmemLocksParsing(unittest.TestCase):
    def test_parse_locks_state_basic(self):
        sample_output = """
        VSS RWLock
           readers = 2
           writers = 1
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        ExtentMap RWLock
           readers = 0
           writers = 0
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        """
        state = parse_locks_state(sample_output, logger)
        # Two sections => IDs 1 and 2
        self.assertEqual(state, [(1, 2, 1), (2, 0, 0)])

    def test_parse_locks_state_malformed_values(self):
        sample_output = """
        VSS RWLock
           readers = blorg
           writers = 1
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        ExtentMap RWLock
           readers = 3
           writers = one
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        FreeList RWLock
           readers = 1
           writers = 0
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        """
        state = parse_locks_state(sample_output, logger)
        self.assertEqual(state, [(3, 1, 0)])

    def test_parse_locks_state_partial_section_ignored(self):
        sample_output = """
        VSS RWLock
           readers = 4
           writers = 0
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        ExtentMap RWLock
           readers = 1
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        """
        state = parse_locks_state(sample_output, logger)
        # Second section missing writers, so we skip it
        self.assertEqual(state, [(1, 4, 0)])


if __name__ == "__main__":
    unittest.main()
