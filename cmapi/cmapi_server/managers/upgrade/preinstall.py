import logging
import re

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.process_dispatchers.base import BaseDispatcher


class PreInstallManager:

    @staticmethod
    def check_gtid_strict_mode():
        """
        Check if gtid_strict_mode is enabled in MariaDB/MySQL configuration.
        Throw an error if gtid_strict_mode=1 is found.

        TODO: seems to be useless if set dynamically using
              SET GLOBAL gtid_strict_mode = 1;
              Better solution is to use query SELECT @@global.gtid_strict_mode;
              But need to investigate how to implement it if no crossengine
              user set, may be check it and fallback or just throw an error.
        """
        cmd: str = 'my_print_defaults --mysqld'
        success, cmd_output = BaseDispatcher.exec_command(cmd)
        if not success:
            if not cmd_output:
                logging.debug(
                    'my_print_defaults not found. Ensure gtid_strict_mode=0.'
                )
            else:
                logging.debug(
                    'my_print_defaults --mysqld command call returns an '
                    f'error: {cmd_output}. Ensure gtid_strict_mode=0.'
                )
        else:
            # Search for gtid_strict_mode or gtid-strict-mode patterns
            gtid_pattern = re.compile(r"gtid[-_]strict[-_]mode")
            strict_mode_lines = [
                line for line in cmd_output.splitlines()
                if gtid_pattern.search(line)
            ]
            if strict_mode_lines:
                # Check if any line shows gtid_strict_mode=1
                for line in strict_mode_lines:
                    line = line.strip()
                    if (
                        line == '--gtid_strict_mode=1' or
                        line == '--gtid_strict_mode=ON'
                    ):
                        message = (
                            'gtid strick mode is ON, need to be off before '
                            'upgrade/downgrade.'
                        )
                        logging.error(message)
                        raise CMAPIBasicError(message)
