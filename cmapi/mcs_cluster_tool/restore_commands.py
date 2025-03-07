"""Typer application for restore Columnstore data."""
import logging
import sys
from typing_extensions import Annotated

import typer

from cmapi_server.process_dispatchers.base import BaseDispatcher
from mcs_cluster_tool.constants import MCS_BACKUP_MANAGER_SH
from mcs_cluster_tool.decorators import handle_output
from mcs_cluster_tool.helpers import cook_sh_arg


logger = logging.getLogger('mcs_cli')
# pylint: disable=unused-argument, too-many-arguments, too-many-locals
# pylint: disable=invalid-name, line-too-long


@handle_output
def restore(
    l: Annotated[
        str,
        typer.Option(
            '-l', '--load',
            help='What date folder to load from the backup_location.'
        )
    ] = '',
    bl: Annotated[
        str,
        typer.Option(
            '-bl', '--backup-location',
            help=(
                'Where the backup to load is found.\n'
                'Example: /mnt/backups/'
            )
        )
    ] = '/tmp/backups/',
    bd: Annotated[
        str,
        typer.Option(
            '-bd', '--backup_destination',
            help=(
                'Is this backup on the same or remote server compared to '
                'where this script is running.\n'
                'Options: "Local" or "Remote"'
            )
        )
    ] = 'Local',
    scp: Annotated[
        str,
        typer.Option(
            '-scp', '--secure-copy-protocol',
            help=(
                'Used only if --backup-destination=Remote'
                'The user/credentials that will be used to scp the backup files.'
                'Example: "centos@10.14.51.62"'
            )
        )
    ] = '',
    bb: Annotated[
        str,
        typer.Option(
            '-bb', '--backup-bucket',
            help=(
                'Only used if --storage=S3\n'
                'Name of the bucket to store the columnstore backups.\n'
                'Example: "s3://my-cs-backups"'
            )
        )
    ] = '',
    url: Annotated[
        str,
        typer.Option(
            '-url', '--endpoint-url',
            help=(
                'Used by on premise S3 vendors.\n'
                'Example: "http://127.0.0.1:8000"'
            )
        )
    ] = '',
    s: Annotated[
        str,
        typer.Option(
            '-s', '--storage',
            help=(
                'What storage topogoly is being used by Columnstore - found '
                'in /etc/columnstore/storagemanager.cnf.\n'
                'Options: "LocalStorage" or "S3"'
            )
        )
    ] = 'LocalStorage',
    dbs: Annotated[
        int,
        typer.Option(
            '-dbs', '--dbroots',
            help='Number of database roots in the backup.'
        )
    ] = 1,
    pm: Annotated[
        str,
        typer.Option(
            '-pm', '--nodeid',
            help=(
                'Forces the handling of the restore as this node as opposed '
                'to whats detected on disk.'
            )
        )
    ] = '',
    nb: Annotated[
        str,
        typer.Option(
            '-nb', '--new-bucket',
            help=(
                'Defines the new bucket to copy the s3 data to from the '
                'backup bucket. Use -nb if the new restored cluster should '
                'use a different bucket than the backup bucket itself.'
            )
        )
    ] = '',
    nr: Annotated[
        str,
        typer.Option(
            '-nr', '--new-region',
            help=(
                'Defines the region of the new bucket to copy the s3 data to '
                'from the backup bucket.'
            )
        )
    ] = '',
    nk: Annotated[
        str,
        typer.Option(
            '-nk', '--new-key',
            help='Defines the aws key to connect to the new_bucket.'
        )
    ] = '',
    ns: Annotated[
        str,
        typer.Option(
            '-ns', '--new-secret',
            help=(
                'Defines the aws secret of the aws key to connect to the '
                'new_bucket.'
            )
        )
    ] = '',
    ha: Annotated[
        bool,
        typer.Option(
            '-ha/-no-ha', '--highavilability/--no-highavilability',
            help=(
                'Flag for high available systems (meaning shared storage '
                'exists supporting the topology so that each node sees '
                'all data)'
            )
        )
    ] = False,
    cont: Annotated[
        bool,
        typer.Option(
            '-cont/-no-cont', '--continue/--no-continue',
            help=(
                'This acknowledges data in your --new_bucket is ok to delete '
                'when restoring S3. When set to true skips the enforcement '
                'that new_bucket should be empty prior to starting a restore.'
            )
        )
    ] = False,
    f: Annotated[
        str,
        typer.Option(
            '-f', '--config-file',
            help=(
                'Path to backup configuration file to load variables from - '
                'relative or full path accepted.'
            ),
            show_default=False
        )
    ] = '',
    smdb: Annotated[
        bool,
        typer.Option(
            '-smdb/-no-smdb', '--skip-mariadb-backup/--no-skip-mariadb-backup',
            help=(
               'Skip restoring mariadb server via mariadb-backup - ideal for '
               'only restoring columnstore.'
            )
        )
    ] = False,
    sb: Annotated[
        bool,
        typer.Option(
            '-sb/-no-sb', '--skip-bucket-data/--no-skip-bucket-data',
            help=(
                'Skip restoring columnstore data in the bucket - ideal if '
                'looking to only restore mariadb server.'
            )
        )
    ] = False,
    m: Annotated[
        str,
        typer.Option(
            '-m', '--mode',
            help=(
                'Modes ["direct","indirect"] - direct backups run on the '
                'columnstore nodes themselves. indirect run on another '
                'machine that has read-only mounts associated with '
                'columnstore/mariadb\n'
            ),
            hidden=True
        )
    ] = 'direct',
    c: Annotated[
        str,
        typer.Option(
            '-c', '--compress',
            help=(
                'Hint that the backup is compressed in X format. '
                'Options: [ pigz ].'
            ),
            show_default=False
        )
    ] = '',
    P: Annotated[
        int,
        typer.Option(
            '-P', '--parallel',
            help=(
                'Determines number of decompression and mdbstream threads. '
                'Ignored if "-c/--compress" argument not set.'
            )
        )
    ] = 4,
    q: Annotated[
        bool,
        typer.Option(
            '-q/-no-q', '--quiet/--no-quiet',
            help='Silence verbose copy command outputs.'
        )
    ] = False,
    nv_ssl: Annotated[
        bool,
        typer.Option(
            '-nv-ssl/-v-ssl','--no-verify-ssl/--verify-ssl',
            help='Skips verifying ssl certs, useful for onpremise s3 storage.'
        )
    ] = False,
    list: Annotated[
        bool,
        typer.Option(
            'list',
            help='List backups.'
        )
    ] = False
):
    """Restore Columnstore (and/or MariaDB) data."""

    # Local Storage Examples:
    #     ./$0 restore -s LocalStorage -bl /tmp/backups/ -bd Local -l 12-29-2021
    #     ./$0 restore -s LocalStorage -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -l 12-29-2021

    # S3 Storage Examples:
    #     ./$0 restore -s S3 -bb s3://my-cs-backups  -l 12-29-2021
    #     ./$0 restore -s S3 -bb gs://on-premise-bucket -l 12-29-2021 -url http://127.0.0.1:8000
    #     ./$0 restore -s S3 -bb s3://my-cs-backups  -l 08-16-2022 -nb s3://new-data-bucket -nr us-east-1 -nk AKIAxxxxxxx3FHCADF -ns GGGuxxxxxxxxxxnqa72csk5 -ha
    arguments = []
    for arg_name, value in locals().items():
        sh_arg = cook_sh_arg(arg_name, value)
        if sh_arg is None:
            continue
        arguments.append(sh_arg)
    cmd = f'{MCS_BACKUP_MANAGER_SH} restore {" ".join(arguments)}'
    success, _ = BaseDispatcher.exec_command(cmd, stdout=sys.stdout)
    return {'success': success}


@handle_output
def dbrm_restore(
    bl: Annotated[
        str,
        typer.Option(
            '-bl', '--backup-location',
            help='Path of where dbrm backups exist on disk.'
        )
    ] = '/tmp/dbrm_backups',
    l: Annotated[
        str,
        typer.Option(
            '-l', '--load',
            help='Name of the directory to restore from -bl'
        )
    ] = '',
    ns: Annotated[
        bool,
        typer.Option(
            '-ns', '--no-start',
            help=(
                'Do not attempt columnstore startup post dbrm_restore.'
            )
        )
    ] = False,
    sdbk: Annotated[
        bool,
        typer.Option(
            '-sdbk/-no-sdbk', '--skip-dbrm-backup/--no-skip-dbrm-backup',
            help=(
                'Skip backing up dbrms before restoring.'
            )
        )
    ] = True,
    ssm: Annotated[
        bool,
        typer.Option(
            '-ssm/-no-ssm', '--skip-storage-manager/--no-skip-storage-manager',
            help='Skip backing up storagemanager directory.'
        )
    ] = True,
    list: Annotated[
        bool,
        typer.Option(
            'list',
            help='List backups.'
        )
    ] = False
):
    """Restore Columnstore DBRM data."""

    # Default: ./$0 dbrm_restore --backup-location /tmp/dbrm_backups

    # Examples:
    #       ./$0 dbrm_restore --backup-location /tmp/dbrm_backups --load dbrm_backup_20240318_172842
    #       ./$0 dbrm_restore --backup-location /tmp/dbrm_backups --load dbrm_backup_20240318_172842 --no-startdbrm_restore --path /tmp/dbrm_backups --directory dbrm_backup_20240318_172842 --no-start
    arguments = []
    for arg_name, value in locals().items():
        sh_arg = cook_sh_arg(arg_name, value)
        if sh_arg is None:
            continue
        arguments.append(sh_arg)
    cmd = f'{MCS_BACKUP_MANAGER_SH} dbrm_restore {" ".join(arguments)}'
    success, _ = BaseDispatcher.exec_command(cmd, stdout=sys.stdout)
    return {'success': success}
