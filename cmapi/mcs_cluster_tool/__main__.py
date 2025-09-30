import logging
import subprocess
import sys

import typer

from cmapi_server.logging_management import dict_config, add_logging_level, enable_console_logging
from mcs_cluster_tool import (
    cluster_app, cmapi_app, backup_commands, restore_commands, tools_commands
)
from mcs_cluster_tool.constants import MCS_CLI_LOG_CONF_PATH


# don't show --install-completion and --show-completion options in help message
app = typer.Typer(
    add_completion=False,
    help=(
        'The  MCS  Command  Line  Interface is a unified tool to manage your '
        'MCS services'
    ),
    rich_markup_mode='rich',
)
app.add_typer(cluster_app.app)
# keep this only for potential backward compatibility and userfriendliness
app.add_typer(cluster_app.app, name='cluster', hidden=True)
app.add_typer(cmapi_app.app, name='cmapi')
app.command(
    'backup', rich_help_panel='Tools commands'
)(backup_commands.backup)
app.command(
    'dbrm_backup', rich_help_panel='Tools commands'
)(backup_commands.dbrm_backup)
app.command(
    'restore', rich_help_panel='Tools commands'
)(restore_commands.restore)
app.command(
    'dbrm_restore', rich_help_panel='Tools commands'
)(restore_commands.dbrm_restore)
app.command(
    'cskeys', rich_help_panel='Tools commands',
    short_help=(
        'Generate a random AES encryption key and init vector and write '
        'them to disk.'
    )
)(tools_commands.cskeys)
app.command(
    'cspasswd', rich_help_panel='Tools commands',
    short_help='Encrypt a Columnstore plaintext password.'
)(tools_commands.cspasswd)
app.command(
    'bootstrap-single-node', rich_help_panel='Tools commands',
)(tools_commands.bootstrap_single_node)
app.command(
    'review', rich_help_panel='Tools commands',
    short_help=(
        'Provides useful functions to review and troubleshoot the MCS cluster.'
    )
)(tools_commands.review)
app.add_typer(
    tools_commands.sentry_app, name='sentry', rich_help_panel='Tools commands', hidden=True
)
@app.command(
        name='help-all', help='Show help for all commands in man page style.',
        add_help_option=False
)
def help_all():
    # Open the man page in interactive mode
    subprocess.run(['man', 'mcs'])

@app.callback()
def main(verbose: bool = typer.Option(False, '--verbose', '-v', help='Enable verbose logging to console')):
    '''Add a -v option and setup logging in every subcommand'''
    setup_logging(verbose)


def setup_logging(verbose: bool = False) -> None:
    add_logging_level('TRACE', 5)
    dict_config(MCS_CLI_LOG_CONF_PATH)
    if verbose:
        for logger_name in ("", "mcs_cli"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.DEBUG)
            enable_console_logging(logger)


if __name__ == '__main__':
    logger = logging.getLogger('mcs_cli')
    # add separator between cli commands logging
    logger.debug(f'{"-":-^80}')
    cl_args_line = ' '.join(sys.argv[1:])
    logger.debug(f'Called "mcs {cl_args_line}"')
    app(prog_name='mcs')
