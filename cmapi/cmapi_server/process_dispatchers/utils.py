import logging
import re
from time import sleep
from typing import Optional
from cmapi_server.constants import SHMEM_LOCKS_PATH, RESET_LOCKS_PATH
from cmapi_server.process_dispatchers.base import BaseDispatcher


def parse_locks_num(cmd_output: str) -> int:
    """Parse output of mcs-shmem-locks command."""
    active_total = 0
    for line in cmd_output.splitlines():
        m = re.search(r'^\s*(readers|writers)\s*=\s*(\d+)', line)
        if m:
            try:
                active_total += int(m.group(2))
            except ValueError:
                pass
    return active_total


def get_active_shmem_locks_num(logger: logging.Logger) -> Optional[int]:
    """Get number of active shmem locks."""
    cmd = f'{SHMEM_LOCKS_PATH} --lock-id 0'
    success, out = BaseDispatcher.exec_command(cmd)
    if not success:
        logger.error('Failed to inspect shmem locks (command failed)')
        return None
    if not out:
        logger.error('Failed to inspect shmem locks (empty output)')
        return None

    logger.debug('Current lock state:\n%s', (out or '').strip())

    return parse_locks_num(out)


def reset_shmem_locks(logger: logging.Logger) -> None:
    """Inspect and reset BRM shmem locks"""
    logger.debug('Inspecting and resetting shmem locks.')

    # Get current lock state
    active_locks_num = get_active_shmem_locks_num(logger)
    if active_locks_num is None:
        return

    # Reset if any read/write locks are active
    if active_locks_num > 0:
        logger.info('Detected active shmem locks (sum readers+writers=%d). Attempting reset.', active_locks_num)

        # Reset locks
        success, out = BaseDispatcher.exec_command(f'{RESET_LOCKS_PATH} -s')
        if not success:
            logger.error('Failed to reset shmem locks (command failed)')
            return

        # Check that locks were reset
        sleep(1)
        active_locks_num = get_active_shmem_locks_num(logger)
        if active_locks_num is not None and active_locks_num > 0:
            logger.error('Failed to reset shmem locks (locks are still active)')
            return
    else:
        logger.info('No active shmem locks detected.')
