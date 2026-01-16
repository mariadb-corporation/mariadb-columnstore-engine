import logging
import os
from typing import List, Optional, Tuple

from mr_kot import Runner, Status, any_of, check, check_all, fact, parametrize
from mr_kot_fs_validators import Exists, GroupIs, IsDir, IsExecutable, IsReadable, IsWritable, OwnerIs

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH, MCS_DATA_PATH
from mcs_node_control.models.node_config import NodeConfig

logger = logging.getLogger(__name__)


def resolve_symlinks_in_path(base_path: str, subdirs: List[str]) -> List[str]:
    """Build required directories list, resolving symlinks at each level.

    If base_path or any intermediate directory is a symlink, resolve it
    before appending subsequent path components. This ensures we check
    the actual target directories rather than potentially broken paths.

    :param base_path: The root path (e.g., MCS_DATA_PATH).
    :param subdirs: List of subdirectory components to append sequentially.
    :return: List of resolved directory paths to check.
    """
    result: List[str] = []
    current: str = base_path

    # Resolve base path if it's a symlink
    if os.path.islink(current):
        current = os.path.realpath(current)
        logger.debug('Base path %s is a symlink, resolved to %s', base_path, current)

    result.append(current)

    for subdir in subdirs:
        current = os.path.join(current, subdir)
        # Resolve if this path component is a symlink
        if os.path.islink(current):
            resolved: str = os.path.realpath(current)
            logger.debug('Path %s is a symlink, resolved to %s', current, resolved)
            current = resolved
        result.append(current)

    return result


def is_invariant_checks_enabled() -> bool:
    """Check if invariant checks are enabled in CMAPI config.

    :return: True if invariant checks are enabled (default), False otherwise.
    """
    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
    return cfg_parser.getboolean('application', 'invariant_checks', fallback=True)


def run_invariant_checks() -> Optional[str]:
    """Run invariant checks, log results, and return a formatted string with problems, if any.

    If invariant checks are disabled in CMAPI config file (invariant_checks = false),
    this function returns None without running any checks.

    :return: Formatted string with problems if checks fail, None otherwise.
    """
    if not is_invariant_checks_enabled():
        logger.info('Invariant checks are turned OFF in CMAPI config file.')
        return None

    logger.info('Starting invariant checks')
    runner = Runner()
    result = runner.run()
    problems = result.problems()

    # Log each fail/error/warning for diagnostics
    diag: str = ''
    for problem in problems:
        fn = logger.warning if problem.status == Status.WARN else logger.error
        fn(
            'Invariant check with id=%s produced %s: %r',
            problem.id, problem.status, problem.evidence,
        )
        diag += f'{problem.id}: {problem.evidence}\n'

    logger.info(
        'Stats: overall=%s counts=%s',
        result.overall.value,
        {k.value: v for k, v in result.counts.items() if v != 0}
    )
    if result.overall in (Status.FAIL, Status.ERROR):
        logger.error('Invariant checks failed')
        return diag
    else:
        logger.info('Invariant checks passed')
        return None


### Facts
@fact
def storage_type() -> str:
    """Provides storage type: shared_fs or s3."""
    return 's3' if NodeConfig().s3_enabled() else 'shared_fs'

@fact
def is_shared_fs(storage_type: str) -> bool:
    return storage_type == 'shared_fs'

@fact
def dispatcher_name() -> str:
    """Provides environment dispatcher name: systemd or container"""
    cfg = helpers.get_config_parser()
    name, _ = helpers.get_dispatcher_name_and_path(cfg)
    return name

@fact
def is_systemd_disp(dispatcher_name: str) -> bool:
    return dispatcher_name == 'systemd'


### Checks
# Build required directories list, resolving symlinks at each level.
# This handles cases where MCS_DATA_PATH or 'data1' is a symlink.
REQUIRED_LOCAL_DIRS = resolve_symlinks_in_path(
    MCS_DATA_PATH,
    ['data1', 'systemFiles', 'dbrm']
)

@check(selector='is_shared_fs')
@parametrize('dir', values=REQUIRED_LOCAL_DIRS, fail_fast=True)
def required_dirs_perms(dir: str) -> Tuple[Status, str]:
    status, ev = check_all(
        dir,
        Exists(),
        IsDir(),
        IsReadable(),
        IsWritable(),
        IsExecutable(),
    )
    return (status, ev)

@check(selector='is_shared_fs, is_systemd_disp')
@parametrize('dir', values=REQUIRED_LOCAL_DIRS, fail_fast=True)
def required_dirs_ownership(dir: str) -> Tuple[Status, str]:
    # Check ownership only when not in containers
    status, ev = check_all(
        dir,
        Exists(),
        # The correct owner is mysql, but i've seen mariadb as owner of the mountpoint,
        #   so we allow both
        any_of(OwnerIs('mysql'), OwnerIs('mariadb')),
        any_of(GroupIs('mysql'), GroupIs('mariadb')),
    )
    return (status, ev)
