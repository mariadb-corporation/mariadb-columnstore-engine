import logging
import os
from typing import List, Optional, Tuple

from mr_kot import Runner, Status, any_of, check, check_all, fact, parametrize
from mr_kot_fs_validators import Exists, GroupIs, IsDir, IsExecutable, IsReadable, IsWritable, OwnerIs

from cmapi_server import helpers
from cmapi_server.constants import CMAPI_CONF_PATH, MCS_DATA_PATH
from mcs_node_control.models.node_config import NodeConfig

logger = logging.getLogger(__name__)


# Supported modes for the 'invariant_checks' config option:
#   enforce  – run checks and fail on problems (default)
#   warning  – run checks but only log warnings, never fail
#   disabled – skip checks entirely
# Boolean values (true/false) are also accepted:
#   true  -> enforce
#   false -> disabled
_BOOL_TO_MODE = {'true': 'enforce', '1': 'enforce', 'yes': 'enforce',
                 'false': 'disabled', '0': 'disabled', 'no': 'disabled'}
VALID_MODES = ('enforce', 'warning', 'disabled')

_cache = {'mode': 'enforce'}


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


def init_invariant_checks_mode() -> None:
    """Read the invariant-checks mode from CMAPI config and cache it.

    Must be called once at application startup.  Subsequent calls to
    :func:`get_invariant_checks_mode` return the cached value without
    re-reading the config file.

    Reads ``[application] invariant_checks`` from the CMAPI config file.
    Accepted values: ``enforce`` (default), ``warning``, ``disabled``.
    Boolean literals (``true``/``false``) are accepted for backward
    compatibility and mapped to ``enforce``/``disabled``.
    """
    cfg_parser = helpers.get_config_parser(CMAPI_CONF_PATH)
    raw = cfg_parser.get(
        'application', 'invariant_checks', fallback='enforce',
    ).strip().lower()
    mode = _BOOL_TO_MODE.get(raw, raw)
    if mode not in VALID_MODES:
        logger.warning(
            'Unknown invariant_checks value %r in config, '
            'falling back to "enforce".',
            raw,
        )
        mode = 'enforce'
    _cache['mode'] = mode
    logger.info('Invariant checks mode: %s', mode)


def get_invariant_checks_mode() -> str:
    """Return the cached invariant-checks mode.

    :returns: One of ``'enforce'``, ``'warning'``, or ``'disabled'``.
    :rtype: str
    """
    return _cache['mode']


def run_invariant_checks() -> Tuple[Optional[str], bool]:
    """Run invariant checks and return diagnostics together with the mode.

    Behaviour depends on the cached ``invariant_checks`` mode
    (see :func:`init_invariant_checks_mode`):

    * ``enforce`` (default) – run checks; return diagnostics on failure.
    * ``warning`` – run checks; return diagnostics on failure but signal
      that callers should only log a warning, not abort.
    * ``disabled`` – skip checks entirely.

    :returns: ``(diag, warn_only)`` where *diag* is a formatted string
        with problems (or ``None`` when checks pass / are disabled) and
        *warn_only* is ``True`` when the caller should log a warning
        instead of raising / exiting.
    :rtype: tuple[str | None, bool]
    """
    mode = get_invariant_checks_mode()

    if mode == 'disabled':
        logger.info('Invariant checks are disabled in CMAPI config file.')
        return None, False

    warn_only = mode == 'warning'
    logger.info('Starting invariant checks (mode=%s)', mode)
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
        if warn_only:
            logger.warning(
                'Invariant checks failed (warning mode, not aborting)',
            )
        else:
            logger.error('Invariant checks failed')
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
