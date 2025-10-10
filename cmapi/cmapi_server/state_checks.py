from __future__ import annotations

import logging
import os
import sys

from mcs_node_control.models.node_config import NodeConfig
from mr_kot import Status, check, check_all, fact, parametrize
from mr_kot.runner import Runner, RunResult
from mr_kot_fs_validators import GroupIs, HasMode, IsDir, OwnerIs

from cmapi_server.constants import MCS_DATA_PATH

logger = logging.getLogger(__name__)


def run_state_checks(*, verbose: bool = False) -> RunResult:
    """Run state checks, log warnings/errors/failures, and exit the process on failure/error.
    """
    logger.info("Starting invariant checks")
    try:
        runner = Runner(verbose=verbose, include_tags=True)
        result = runner.run()
    except Exception:
        logger.exception('Unexpected exception during state checks')
        sys.exit(1)

    # Log each fail/error/warning for diagnostics
    for item in result.items:
        if item.status in (Status.FAIL, Status.ERROR, Status.WARN):
            logger.error(
                'State check with id=%s produced %s: %r',
                item.id, item.status, item.evidence,
            )

    logger.info('Stats: overall=%s counts=%s', result.overall, {k.value: v for k, v in result.counts.items() if v != 0})
    if result.overall in (Status.FAIL, Status.ERROR):
        logger.error('State check failed, exiting')
        sys.exit(1)

    return result

@fact
def storage_type() -> str:
    """Provides storage type: shared_fs or s3."""
    return 's3' if NodeConfig().s3_enabled() else 'shared_fs'

@fact
def is_shared_fs(storage_type: str) -> bool:
    return storage_type == 'shared_fs'

@check(selector='is_shared_fs')
@parametrize(
    'required_local_dir',
    values=[
        os.path.join(MCS_DATA_PATH, 'data1'),
        os.path.join(MCS_DATA_PATH, 'data1', 'systemFiles'),
        os.path.join(MCS_DATA_PATH, 'data1', 'systemFiles', 'dbrm'),
    ],
    fail_fast=True,
)
def required_dirs_fs_state(required_local_dir: str) -> tuple[Status, str]:
    status, ev = check_all(
        required_local_dir,
        IsDir(),
        HasMode('1755'),
        OwnerIs('mysql'),
        GroupIs('mysql'),
    )
    return (status, ev)
