import logging
import re
from time import sleep
from typing import Optional, List, Tuple
from cmapi_server.constants import SHMEM_LOCKS_PATH
from cmapi_server.process_dispatchers.base import BaseDispatcher


def parse_locks_state(cmd_output: str, logger: logging.Logger) -> List[Tuple[int, int, int]]:
    """Parse per-lock state from mcs-shmem-locks output.

    Returns a list of tuples: (lock_id, readers, writers)
    """
    locks: List[Tuple[int, int, int]] = []
    current_id = 0
    readers = None
    writers = None

    for line in cmd_output.splitlines():
        if line.strip().endswith('RWLock'):
            # flush previous section counts (if we have both)
            if current_id > 0 and readers is not None and writers is not None:
                locks.append((current_id, readers, writers))
            current_id += 1
            readers = None
            writers = None
            continue

        m = re.search(r'^\s*readers\s*=\s*(\d+)', line)
        if m:
            try:
                readers = int(m.group(1))
            except ValueError:
                logger.warning('Failed to parse readers count from line: %s', line)
                readers = 0
            continue

        m = re.search(r'^\s*writers\s*=\s*(\d+)', line)
        if m:
            try:
                writers = int(m.group(1))
            except ValueError:
                logger.warning('Failed to parse writers count from line: %s', line)
                writers = 0
            continue

    # flush the last parsed lock
    if current_id > 0 and readers is not None and writers is not None:
        locks.append((current_id, readers, writers))

    return locks


def release_shmem_locks(logger: logging.Logger, max_iterations: int = 5) -> bool:
    """Attempt to release active shmem locks.

    - Inspect all locks.
    - Unlock writer lock (there can be only one)
    - Unlock each reader lock sequentially
    - Re-check and repeat up to max_iterations.

    Returns True on success (no active readers/writers remain), False otherwise.
    """
    for attempt in range(1, max_iterations + 1):
        success, out = BaseDispatcher.exec_command(f'{SHMEM_LOCKS_PATH} --lock-id 0')
        if not success or not out:
            logger.error('Failed to inspect shmem locks during unlock (attempt %d)', attempt)
            return False

        locks = parse_locks_state(out, logger=logger)

        total_active = sum_active_locks(locks)
        if total_active == 0:
            logger.debug('Unlock attempt %d: no active locks', attempt)
            return True
        logger.debug('Unlock attempt %d: active total=%d; detail=%s', attempt, total_active, locks)

        # Issue unlocks per lock
        for lock_id, readers, writers in locks:
            # Unlock writer
            if writers > 0:
                cmd = f'{SHMEM_LOCKS_PATH} -i {lock_id} -w -u'
                ok, _ = BaseDispatcher.exec_command(cmd)
                if not ok:
                    logger.warning('Failed to unlock writer for lock-id=%d', lock_id)

            # Unlock all readers
            if readers > 0:
                for _ in range(readers):
                    cmd = f'{SHMEM_LOCKS_PATH} -i {lock_id} -r -u'
                    ok, _ = BaseDispatcher.exec_command(cmd)
                    if not ok:
                        logger.warning('Failed to unlock a reader for lock-id=%d', lock_id)
                        break

        # Wait some time for state to settle
        sleep(1)

    logger.error('Failed to fully release shmem locks using mcs-shmem-locks after %d attempts', max_iterations)
    return False


def sum_active_locks(locks: List[Tuple[int, int, int]]) -> int:
    return sum(r + w for _, r, w in locks)
