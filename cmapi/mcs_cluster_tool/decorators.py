"""Module contains decorators for typer cli commands."""
import json
import logging
from functools import wraps

import typer

from cmapi_server.exceptions import CMAPIBasicError


def handle_output(func):
    """Decorator for handling output errors and add result to log file."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger('mcs_cli')
        return_code = 1
        try:
            result = func(*args, **kwargs)
            typer.echo(json.dumps(result, indent=2))
            logger.debug(f'Command returned: {result}')
            return_code = 0
        except CMAPIBasicError as err:
            typer.echo(err.message, err=True)
            logger.error('Error during command execution', exc_info=True)
        except typer.BadParameter as err:
            logger.error('Bad command line parameter.')
            raise err
        except typer.Exit as err:  # if some command used typer.Exit
            #TODO: think about universal protocol to return json data and
            #      plain text results.
            return_code = err.exit_code
        except Exception:
            logger.error(
                'Undefined error during command execution',
                exc_info=True
            )
            typer.echo('Unknown error, check the log file.', err=True)

        raise typer.Exit(return_code)
    return wrapper
