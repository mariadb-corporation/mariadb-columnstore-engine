import unittest
import logging

from cmapi_server.process_dispatchers.locks import (
    parse_locks_detail,
    sum_active_from_states,
    sum_waiting_from_states,
)

logger = logging.getLogger(__name__)


class TestShmemLocksParsing(unittest.TestCase):
    def test_parse_locks_detail_basic(self):
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
           readers waiting = 1
           writers waiting = 2
           mutex locked = 1
        """
        states = parse_locks_detail(sample_output, logger)
        self.assertEqual(len(states), 2)

        # Check names
        self.assertEqual(states[0].name, 'VSS')
        self.assertEqual(states[1].name, 'ExtentMap')

        self.assertEqual(states[0].id, 1)
        self.assertEqual(states[0].readers, 2)
        self.assertEqual(states[0].writers, 1)
        self.assertEqual(states[0].readers_waiting, 0)
        self.assertEqual(states[0].writers_waiting, 0)
        self.assertFalse(states[0].mutex_locked)

        self.assertEqual(states[1].id, 2)
        self.assertEqual(states[1].readers, 0)
        self.assertEqual(states[1].writers, 0)
        self.assertEqual(states[1].readers_waiting, 1)
        self.assertEqual(states[1].writers_waiting, 2)
        self.assertTrue(states[1].mutex_locked)

        self.assertEqual(sum_active_from_states(states), 3)
        self.assertEqual(sum_waiting_from_states(states), 3)

    def test_parse_locks_detail_malformed_values(self):
        sample_output = """
        VSS RWLock
           readers = blorg
           writers = 1
           readers waiting = nope
           writers waiting = 0
           mutex locked = 0
        ExtentMap RWLock
           readers = 3
           writers = one
           readers waiting = 0
           writers waiting = two
           mutex locked = 1
        FreeList RWLock
           readers = 1
           writers = 0
           readers waiting = 0
           writers waiting = 0
           mutex locked = 0
        """
        states = parse_locks_detail(sample_output, logger)
        # Malformed numeric fields are treated as 0, sections are retained
        self.assertEqual(len(states), 3)
        self.assertEqual([s.name for s in states], ['VSS', 'ExtentMap', 'FreeList'])
        # Lock 1: readers -> 0, writers -> 1, readers_waiting -> 0
        self.assertEqual((states[0].readers, states[0].writers, states[0].readers_waiting), (0, 1, 0))
        # Lock 2: writers -> 0, writers_waiting -> 0 due to malformed
        self.assertEqual((states[1].readers, states[1].writers, states[1].writers_waiting), (3, 0, 0))
        # Lock 3 intact
        self.assertEqual((states[2].readers, states[2].writers), (1, 0))

    def test_parse_locks_detail_mutex_absent_defaults_false(self):
        sample_output = """
        VSS RWLock
           readers = 1
           writers = 0
        ExtentMap RWLock
           readers = 0
           writers = 0
        """
        states = parse_locks_detail(sample_output, logger)
        self.assertEqual(len(states), 2)
        self.assertFalse(states[0].mutex_locked)
        self.assertFalse(states[1].mutex_locked)


if __name__ == "__main__":
    unittest.main()
