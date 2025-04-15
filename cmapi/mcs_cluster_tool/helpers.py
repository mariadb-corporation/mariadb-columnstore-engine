"""Module with helper functions for mcs cli tool."""
from typing import Optional, Union


def cook_sh_arg(arg_name: str, value: Union[str, int, bool]) -> Optional[str]:
    """Convert argument and and value from function locals to bash argument.

    :param arg_name: function argument name
    :type arg_name: str
    :param value: function argument value
    :type value: Union[str, int, bool]
    :return: bash argument string or None
    :rtype: Optional[str]
    """
    # skip "arguments" list and Typer ctx variables from local scope
    if arg_name in ('arguments', 'ctx'):
        return None
    # skip args that have empty string or None as value
    # Condition below could be "not value", but I prefer to be explicit
    # and check for empty string and None to show that it's different cases:
    # empty string means that user passed empty string as value
    # and None means that user didn't pass anything and our internal None
    # applies
    if value == '' or value is None:
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
