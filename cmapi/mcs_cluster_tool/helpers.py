"""Module with helper functions for mcs cli tool."""
from typing import Union


def cook_sh_arg(arg_name: str, value:Union[str, int, bool]) -> str:
    """Convert argument and and value from function locals to bash argument.

    :param arg_name: function argument name
    :type arg_name: str
    :param value: function argument value
    :type value: Union[str, int, bool]
    :return: bash argument string
    :rtype: str
    """
    # skip "arguments" list and Typer ctx variables from local scope
    if arg_name in ('arguments', 'ctx'):
        return None
    # skip args that have empty string as value
    if value == '':
        return None
    if '_' in arg_name:
        arg_name = arg_name.replace('_', '-')
    # skip boolean args that have False value
    if isinstance(value, bool):
        if not value:
            return None
        # if True value presented just pass only arg name without value
        value = ''
    return f'-{arg_name} {value}' if value else f'-{arg_name}'
