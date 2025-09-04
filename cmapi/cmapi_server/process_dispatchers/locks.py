import logging
import re
from dataclasses import dataclass
from time import sleep
from typing import Optional, List
from cmapi_server.constants import SHMEM_LOCKS_PATH
from cmapi_server.process_dispatchers.base import BaseDispatcher


@dataclass
class LocksState:
    id: int
    name: Optional[str]
    readers: int = 0
    writers: int = 0
    readers_waiting: int = 0
    writers_waiting: int = 0
    mutex_locked: bool = False

    def __str__(self):
        name = f"({self.name})" if self.name else ""
        return (f"LS {self.id}{name}: {self.readers}r/{self.writers}w" +
                f" {self.readers_waiting}rw/{self.writers_waiting}ww m={int(self.mutex_locked)}")


def parse_locks_detail(cmd_output: str, logger: logging.Logger) -> List[LocksState]:
    """Parse detailed per-lock state from mcs-shmem-locks output.

    Missing or malformed numeric fields are treated as 0. A logger must be provided
    and will be used to emit warnings for malformed lines.
    """
    states: List[LocksState] = []
    current: Optional[LocksState] = None
    current_id = 0

    for raw in cmd_output.splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.endswith('RWLock'):
            # push previous
            if current is not None:
                states.append(current)
            current_id += 1
            name = line[:-len('RWLock')].strip() or None
            current = LocksState(id=current_id, name=name)
            continue

        if current is None:
            continue

        field_specs = [
            (r'^readers\s*=\s*(\d+)$', 'readers'),
            (r'^writers\s*=\s*(\d+)$', 'writers'),
            (r'^readers waiting\s*=\s*(\d+)$', 'readers_waiting'),
            (r'^writers waiting\s*=\s*(\d+)$', 'writers_waiting'),
        ]

        matched = False
        for pattern, attr in field_specs:
            m = re.search(pattern, line)
            if m:
                try:
                    setattr(current, attr, int(m.group(1)))
                except ValueError:
                    logger.warning('Failed to parse %s from line: %s', attr, raw)
                    setattr(current, attr, 0)
                matched = True
                break
        if matched:
            continue

        m = re.search(r'^mutex locked\s*=\s*(\d+)$', line)
        if m:
            try:
                current.mutex_locked = int(m.group(1)) != 0
            except ValueError:
                current.mutex_locked = False
            continue

    # push the last one
    if current is not None:
        states.append(current)

    return states


def release_shmem_locks(logger: logging.Logger, max_iterations: int = 5) -> bool:
    """Attempt to release active shmem locks.

    - Inspect all locks.
    - Unlock writer lock (there can be only one active, but there can be multiple waiting)
    - Unlock each reader lock sequentially
    - Re-check and repeat up to max_iterations.

    Returns True on success (no active readers/writers remain), False otherwise.
    """
    # We adapt attempts/sleep when there are waiting locks to allow promotions
    attempt = 0
    while True:
        attempt += 1
        success, out = BaseDispatcher.exec_command(f'{SHMEM_LOCKS_PATH} --lock-id 0')
        if not success or not out:
            logger.error('Failed to inspect shmem locks during unlock (attempt %d)', attempt)
            return False

        states = parse_locks_detail(out, logger=logger)
        active_total = sum(s.readers + s.writers for s in states)
        waiting_total = sum(s.readers_waiting + s.writers_waiting for s in states)
        if active_total == 0 and waiting_total == 0:
            return True

        logger.debug(
            'Lock release attempt %d: active=%d waiting=%d; detailed=%s',
            attempt, active_total, waiting_total, states
        )

        for st in states:
            if st.writers > 0:
                cmd = f'{SHMEM_LOCKS_PATH} -i {st.id} -w -u'
                ok, _ = BaseDispatcher.exec_command(cmd)
                if not ok:
                    logger.warning('Failed to unlock writer for lock-id=%d', st.id)

            if st.readers > 0:
                for _ in range(st.readers):
                    cmd = f'{SHMEM_LOCKS_PATH} -i {st.id} -r -u'
                    ok, _ = BaseDispatcher.exec_command(cmd)
                    if not ok:
                        logger.warning('Failed to unlock a reader for lock-id=%d', st.id)
                        break

        # Wait for state to settle; longer if we have waiting locks
        sleep(2 if waiting_total > 0 else 1)

        # Allow more attempts when there are waiting locks
        effective_max = max_iterations if waiting_total == 0 else max(max_iterations, 15)
        if attempt >= effective_max:
            break

    logger.error('Failed to fully release shmem locks using mcs-shmem-locks after %d attempts (active/waiting remain)', attempt)
    return False


def sum_active_from_states(states: List[LocksState]) -> int:
    return sum(s.readers + s.writers for s in states)


def sum_waiting_from_states(states: List[LocksState]) -> int:
    return sum(s.readers_waiting + s.writers_waiting for s in states)
