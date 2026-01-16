import logging
import os
from typing import List, Optional, Tuple

from mr_kot import Runner, Status, any_of, check, check_all, fact, parametrize
from mr_kot_fs_validators import Exists, GroupIs, IsDir, IsExecutable, IsReadable, IsWritable, OwnerIs

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH, MCS_DATA_PATH
from mcs_node_control.models.node_config import NodeConfig

logger = logging.getLogger(__name__)


# If true, invariant check failures cause CMAPI to abort (exit / HTTP 422).
# If false (default), failures are logged as warnings and execution continues.
_cache = {'enforce': False}


def resolve_symlinks_in_path(base_path: str, subdirs: List[str]) -> List[str]:
    """Build required directories list, resolving symlinks at each level.

    If *base_path* or any intermediate directory is a symlink, resolve it
    before appending subsequent path components.  This ensures we check
    the actual target directories rather than potentially broken paths.

    :param base_path: The root path (e.g., ``MCS_DATA_PATH``).
    :param subdirs: Subdirectory components to append sequentially.
    :returns: Resolved directory paths to check.
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


def init_invariant_checks_enforce() -> None:
    """Read ``invariant_checks_enforce`` from CMAPI config and cache it.

    Must be called once at application startup.  Subsequent calls to
    :func:`is_invariant_checks_enforce` return the cached value without
    re-reading the config file.

    Reads ``[application] invariant_checks_enforce`` from the CMAPI
    config file.  Default is ``false`` (log warnings only).
    """
    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
    _cache['enforce'] = cfg_parser.getboolean(
        'application', 'invariant_checks_enforce', fallback=False,
    )
    logger.info('Invariant checks enforce: %s', _cache['enforce'])


def is_invariant_checks_enforce() -> bool:
    """Return the cached ``invariant_checks_enforce`` flag.

    :returns: ``True`` if failures should abort, ``False`` if they
        should only be logged as warnings.
    """
    return _cache['enforce']


def run_invariant_checks() -> Tuple[Optional[str], bool]:
    """Run invariant checks and return diagnostics.

    Checks always run.  The second element of the returned tuple
    indicates whether the caller should treat failures as non-fatal
    warnings (see :func:`is_invariant_checks_enforce`).

    :returns: ``(diag, warn_only)`` where *diag* is a formatted string
        with problems (or ``None`` when all checks pass) and
        *warn_only* is ``True`` when the caller should log a warning
        instead of raising / exiting.
    """
    enforce = is_invariant_checks_enforce()
    warn_only = not enforce
    logger.info(
        'Starting invariant checks (enforce=%s)', enforce,
    )
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
        msg = f'Invariant checks failed. Details:\n{diag.strip() if diag else ""}'
        if warn_only:
            logger.warning('%s (warning mode, not aborting)', msg)
        else:
            logger.error(msg)
        return diag, warn_only
    else:
        logger.info('Invariant checks passed')
        return None, warn_only


### Facts
@fact
def storage_type() -> str:
    """Provides storage type: shared_fs or s3."""
    try:
        return 's3' if NodeConfig().s3_enabled() else 'shared_fs'
    except Exception:
        logger.exception('Failed to detect storage type, defaulting to shared_fs')
        return 'shared_fs'

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
