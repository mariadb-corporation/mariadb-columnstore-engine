import os
from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def change_directory(new_dir: str) -> Generator[None, None, None]:
    """
    Context manager for temporarily changing the current working directory.

    :param new_dir: Directory to change to
    """
    old_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(old_dir)


def drop_timestamp(data: dict) -> dict:
    """
    Drop the timestamp from the data dictionary.
    """
    if "timestamp" in data:
        del data["timestamp"]
    return data


def assert_dict_includes(this: dict, other: dict) -> None:
    """
    Check if the current dictionary includes all keys and values from another dictionary.
    """
    for key, value in other.items():
        assert key in this
        assert this[key] == value, f"Key '{key}' does not match: {this[key]} != {value}"
