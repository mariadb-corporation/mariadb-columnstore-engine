import logging
import os
import sys

from mr_kot import Runner, RunResult, Status, check, check_all, fact, parametrize
from mr_kot_fs_validators import GroupIs, HasMode, IsDir, OwnerIs

from cmapi_server import helpers
from cmapi_server.constants import MCS_DATA_PATH
from mcs_node_control.models.node_config import NodeConfig

logger = logging.getLogger(__name__)


def run_invariant_checks() -> RunResult:
    """Run invariant checks, log warnings/errors/failures, and exit the process on failure/error.
    """
    logger.info('Starting invariant checks')
    try:
        runner = Runner()
        result = runner.run()
    except Exception:
        logger.exception('Got unexpected exception while checking invariants')
        sys.exit(1)

    # Log each fail/error/warning for diagnostics
    for item in result.items:
        if item.status in (Status.FAIL, Status.ERROR, Status.WARN):
            fn = logger.warning if item.status == Status.WARN else logger.error
            fn(
                'Invariant check with id=%s produced %s: %r',
                item.id, item.status, item.evidence,
            )

    logger.info('Stats: overall=%s counts=%s', result.overall, {k.value: v for k, v in result.counts.items() if v != 0})
    if result.overall in (Status.FAIL, Status.ERROR):
        logger.error('Invariant checks failed, exiting')
        sys.exit(1)
    else:
        logger.info('Invariant checks passed')

    return result


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

REQUIRED_LOCAL_DIRS = [
    os.path.join(MCS_DATA_PATH, 'data1'),
    os.path.join(MCS_DATA_PATH, 'data1', 'systemFiles'),
    os.path.join(MCS_DATA_PATH, 'data1', 'systemFiles', 'dbrm'),
]

@check(selector='is_shared_fs')
@parametrize('dir', values=REQUIRED_LOCAL_DIRS, fail_fast=True)
def required_dirs_perms(dir: str) -> tuple[Status, str]:
    status, ev = check_all(
        dir,
        IsDir(),
        HasMode('1755'),
    )
    return (status, ev)

@check(selector='is_shared_fs, is_systemd_disp')
@parametrize('dir', values=REQUIRED_LOCAL_DIRS, fail_fast=True)
def required_dirs_ownership(dir: str) -> tuple[Status, str]:
    # Check ownership only when not in containers
    status, ev = check_all(
        dir,
        OwnerIs('mysql'),
        GroupIs('mysql'),
    )
    return (status, ev)
