import logging
import sys

import typer

from cmapi_server.logging_management import dict_config, add_logging_level
from mcs_cluster_tool import (
    cluster_app, cmapi_app, backup_commands, restore_commands
)
from mcs_cluster_tool.constants import MCS_CLI_LOG_CONF_PATH


# don't show --install-completion and --show-completion options in help message
app = typer.Typer(
    add_completion=False,
    help=(
        'The  MCS  Command  Line  Interface is a unified tool to manage your '
        'MCS services'
    ),
)
app.add_typer(cluster_app.app, name='cluster')
app.add_typer(cmapi_app.app, name='cmapi')
app.command()(backup_commands.backup)
app.command('backup-dbrm')(backup_commands.backup_dbrm)
app.command()(restore_commands.restore)
app.command('restore-dbrm')(restore_commands.restore_dbrm)


if __name__ == '__main__':
    add_logging_level('TRACE', 5)  #TODO: remove when stadalone mode added.
    dict_config(MCS_CLI_LOG_CONF_PATH)
    logger = logging.getLogger('mcs_cli')
    # add separator between cli commands logging
    logger.debug(f'{"-":-^80}')
    cl_args_line = ' '.join(sys.argv[1:])
    logger.debug(f'Called "mcs {cl_args_line}"')
    app(prog_name='mcs')
