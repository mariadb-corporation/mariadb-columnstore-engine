"""Typer application for backup Columnstore data."""
import logging
import sys
from datetime import datetime
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
def backup(
    bl: Annotated[
        str,
        typer.Option(
            '-bl', '--backup-location',
            help=(
                'What directory to store the backups on this machine or the target machine.\n'
                'Consider write permissions of the scp user and the user running this script.\n'
                'Mariadb-backup will use this location as a tmp dir for S3 and remote backups temporarily.\n'
                'Example: /mnt/backups/'
            )
        )
    ] = '/tmp/backups/',
    bd: Annotated[
        str,
        typer.Option(
            '-bd', '--backup-destination',
            help=(
                'Are the backups going to be stored on the same machine this '
                'script is running on or another server - if Remote you need '
                'to setup scp='
                'Options: "Local" or "Remote"'
            )
        )
    ] = 'Local',
    scp: Annotated[
        str,
        typer.Option(
            '-scp',
            help=(
                'Used only if --backup-destination="Remote".\n'
                'The user/credentials that will be used to scp the backup '
                'files\n'
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
    nv_ssl: Annotated[
        bool,
        typer.Option(
            '-nv-ssl/-v-ssl','--no-verify-ssl/--verify-ssl',
            help='Skips verifying ssl certs, useful for onpremise s3 storage.'
        )
    ] = False,
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
    i: Annotated[
        str,
        typer.Option(
            '-i', '--incremental',
            help=(
                'Adds columnstore deltas to an existing full backup. '
                'Backup folder to apply increment could be a value or '
                '"auto_most_recent" - the incremental backup applies to '
                'last full backup.'
            ),
            show_default=False
        )
    ] = '',
    ha: Annotated[
        bool,
        typer.Option(
            '-ha/-no-ha', '--highavilability/--no-highavilability',
            help=(
                'Hint wether shared storage is attached @ below on all nodes '
                'to see all data\n'
                '   HA LocalStorage ( /var/lib/columnstore/dataX/ )\n'
                '   HA S3           ( /var/lib/columnstore/storagemanager/ )'
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
    sbrm: Annotated[
        bool,
        typer.Option(
            '-sbrm/-no-sbrm', '--skip-save-brm/--no-skip-save-brm',
            help=(
                'Skip saving brm prior to running a backup - '
                'ideal for dirty backups.'
            )
        )
    ] = False,
    spoll: Annotated[
        bool,
        typer.Option(
            '-spoll/-no-spoll', '--skip-polls/--no-skip-polls',
            help='Skip sql checks confirming no write/cpimports running.'
        )
    ] = False,
    slock: Annotated[
        bool,
        typer.Option(
            '-slock/-no-slock', '--skip-locks/--no-skip-locks',
            help='Skip issuing write locks - ideal for dirty backups.'
        )
    ] = False,
    smdb: Annotated[
        bool,
        typer.Option(
            '-smdb/-no-smdb', '--skip-mariadb-backup/--no-skip-mariadb-backup',
            help=(
               'Skip running a mariadb-backup for innodb data - ideal for '
               'incremental dirty backups.'
            )
        )
    ] = False,
    sb: Annotated[
        bool,
        typer.Option(
            '-sb/-no-sb', '--skip-bucket-data/--no-skip-bucket-data',
            help='Skip taking a copy of the columnstore data in the bucket.'
        )
    ] = False,
    pi: Annotated[
        int,
        typer.Option(
            '-pi', '--poll-interval',
            help=(
                'Number of seconds between poll checks for active writes & '
                'cpimports.'
            )
        )
    ] = 5,
    pmw: Annotated[
        int,
        typer.Option(
            '-pmw', '--poll-max-wait',
            help=(
                'Max number of minutes for polling checks for writes to wait '
                'before exiting as a failed backup attempt.'
            )
        )
    ] = 60,
    q: Annotated[
        bool,
        typer.Option(
            '-q/-no-q', '--quiet/--no-quiet',
            help='Silence verbose copy command outputs.'
        )
    ] = False,
    c: Annotated[
        str,
        typer.Option(
            '-c', '--compress',
            help='Compress backup in X format - Options: [ pigz ].',
            show_default=False
        )
    ] = '',
    P: Annotated[
        int,
        typer.Option(
            '-P', '--parallel',
            help=(
                'Determines if columnstore data directories will have '
                'multiple rsync running at the same time for different '
                'subfolders to parallelize writes. '
                'Ignored if "-c/--compress" argument not set.'
            )
        )
    ] = 4,
    nb: Annotated[
        str,
        typer.Option(
            '-nb', '--name-backup',
            help='Define the name of the backup - default: $(date +%m-%d-%Y)'
        )
    ] = datetime.now().strftime('%m-%d-%Y'),
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
    r: Annotated[
        int,
        typer.Option(
            '-r', '--retention-days',
            help=(
                'Retain backups created within the last X days, '
                'default 0 == keep all backups.'
            )
        )
    ] = 0,
    list: Annotated[
        bool,
        typer.Option(
            'list',
            help='List backups.'
        )
    ] = False
):
    """Backup Columnstore and/or MariDB data."""

    # Local Storage Examples:
    #     ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage
    #     ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage -P 8
    #     ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage --incremental 02-18-2022
    #     ./$0 backup -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage

    # S3 Examples:
    #     ./$0 backup -bb s3://my-cs-backups -s S3
    #     ./$0 backup -bb s3://my-cs-backups -c pigz --quiet -sb
    #     ./$0 backup -bb gs://my-cs-backups -s S3 --incremental 02-18-2022
    #     ./$0 backup -bb s3://my-onpremise-bucket -s S3 -url http://127.0.0.1:8000

    # Cron Example:
    #     */60 */24 * * *  root  bash /root/$0 -bb s3://my-cs-backups -s S3  >> /root/csBackup.log  2>&1

    arguments = []
    for arg_name, value in locals().items():
        sh_arg = cook_sh_arg(arg_name, value)
        if sh_arg is None:
            continue
        arguments.append(sh_arg)
    cmd = f'{MCS_BACKUP_MANAGER_SH} backup {" ".join(arguments)}'
    success, _ = BaseDispatcher.exec_command(cmd, stdout=sys.stdout)
    return {'success': success}


@handle_output
def dbrm_backup(
    m: Annotated[
        str,
        typer.Option(
            '-m', '--mode',
            help=(
                '"loop" or "once" ; Determines if this script runs in a '
                'forever loop sleeping -i minutes or just once.'
            ),
        )
    ] = 'once',
    i: Annotated[
        int,
        typer.Option(
            '-i', '--interval',
            help='Number of minutes to sleep when --mode=loop.'
        )
    ] = 90,
    r: Annotated[
        int,
        typer.Option(
            '-r', '--retention-days',
            help=(
                'Retain dbrm backups created within the last X days, '
                'the rest are deleted'
            )
        )
    ] = 7,
    bl: Annotated[
        str,
        typer.Option(
            '-bl', '--backup-location',
            help='Path of where to save the dbrm backups on disk.'
        )
    ] = '/tmp/dbrm_backups',
    nb: Annotated[
        str,
        typer.Option(
            '-nb', '--name-backup',
            help=(
                'Define the prefix of the backup - '
                'default: dbrm_backup+date +%Y%m%d_%H%M%S'
            )
        )
    ] = 'dbrm_backup',
    q: Annotated[
        bool,
        typer.Option(
            '-q/-no-q', '--quiet/--no-quiet',
            help='Silence verbose copy command outputs.'
        )
    ] = False,
    ssm: Annotated[
        bool,
        typer.Option(
            '-ssm/-no-ssm', '--skip-storage-manager/--no-skip-storage-manager',
            help='Skip backing up storagemanager directory.'
        )
    ] = False,
):
    """Columnstore DBRM Backup."""

    # Default: ./$0 dbrm_backup -m once --retention-days 0 --backup-location /tmp/dbrm_backups

    #     Examples:
    #         ./$0 dbrm_backup --backup-location /mnt/columnstore/dbrm_backups
    #         ./$0 dbrm_backup --retention-days 7 --backup-location /mnt/dbrm_backups --mode once -nb my-one-off-backup-before-upgrade
    #         ./$0 dbrm_backup --retention-days 7 --backup-location /mnt/dbrm_backups --mode loop --interval 90brm_backup --mode once --retention-days 7 --path /mnt/dbrm_backups -nb my-one-off-backup

    #     Cron Example:
    #         */60 */3 * * * root bash /root/$0 dbrm_backup -m once --retention-days 7 --backup-location /tmp/dbrm_backups >> /tmp/dbrm_backups/cs_backup.log 2>&1
    arguments = []
    for arg_name, value in locals().items():
        sh_arg = cook_sh_arg(arg_name, value)
        if sh_arg is None:
            continue
        arguments.append(sh_arg)
    cmd = f'{MCS_BACKUP_MANAGER_SH} dbrm_backup {" ".join(arguments)}'
    success, _ = BaseDispatcher.exec_command(cmd, stdout=sys.stdout)
    return {'success': success}
