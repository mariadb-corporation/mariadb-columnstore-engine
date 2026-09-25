import logging
import re
import shlex
from typing import Dict, List, Tuple

from cmapi_server.constants import DEFAULT_MCS_CONF_PATH
from cmapi_server.exceptions import CEJError, CMAPIBasicError
from cmapi_server.helpers import get_cej_info
from mcs_node_control.models.node_config import NodeConfig
from cmapi_server.process_dispatchers.base import BaseDispatcher


# Registry of MariaDB plugins that the install_es auto-upgrader does NOT
# handle. If any of these are ACTIVE on the local node the precheck aborts
# the upgrade and instructs the operator how to proceed manually.
#
# Keys are compared case-insensitively against PLUGIN_NAME as reported by
# information_schema.PLUGINS. Currently focused on external storage engines
# shipped as separate mariadb-plugin-* packages; extend as needed.
UNSUPPORTED_PLUGINS_AUTOUPGRADE: Dict[str, str] = {
    'SPIDER': (
        "UNINSTALL SONAME 'ha_spider.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-spider package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_spider.so'."
    ),
    'CONNECT': (
        "UNINSTALL SONAME 'ha_connect.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-connect package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_connect.so'."
    ),
    'SPHINX': (
        "UNINSTALL SONAME 'ha_sphinx.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-sphinx package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_sphinx.so'."
    ),
    'OQGRAPH': (
        "UNINSTALL SONAME 'ha_oqgraph.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-oqgraph package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_oqgraph.so'."
    ),
    'MROONGA': (
        "UNINSTALL SONAME 'ha_mroonga.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-mroonga package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_mroonga.so'."
    ),
    'ROCKSDB': (
        "UNINSTALL SONAME 'ha_rocksdb.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-rocksdb package to a version matching the target "
        "ES release; start MariaDB; INSTALL SONAME 'ha_rocksdb.so'."
    ),
    'S3': (
        "UNINSTALL SONAME 'ha_s3.so'; stop MariaDB; upgrade the "
        "mariadb-plugin-s3 package to a version matching the target ES "
        "release; start MariaDB; INSTALL SONAME 'ha_s3.so'."
    ),
}


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

    @staticmethod
    def _get_active_plugins(
        config: str = DEFAULT_MCS_CONF_PATH,
    ) -> List[Tuple[str, str]]:
        """Return list of ACTIVE plugins on the local MariaDB node.

        Uses CrossEngineSupport credentials from Columnstore.xml (same as
        `helpers.system_ready`). Plugin registration is stored in the
        mysql.plugin system table so a single-node query is sufficient
        for a Columnstore cluster (mysql.plugin is server-local, but
        Columnstore cluster nodes share plugin state via replication or
        identical package layout by convention).

        :return: list of (PLUGIN_NAME, PLUGIN_TYPE) tuples
        :raises CMAPIBasicError: if the query cannot be executed
        """
        nc = NodeConfig()
        root = nc.get_current_config_root(config)
        try:
            host, port, username, password = get_cej_info(root)
        except CEJError as exc:
            raise CMAPIBasicError(
                'Cannot detect installed MariaDB plugins: '
                'CrossEngineSupport credentials are missing or invalid. '
                f'Details: {exc}'
            ) from exc

        query = (
            "SELECT PLUGIN_NAME, PLUGIN_TYPE "
            "FROM information_schema.PLUGINS "
            "WHERE PLUGIN_STATUS='ACTIVE';"
        )
        cmd = (
            f'/usr/bin/mariadb -h {shlex.quote(host)} '
            f'-P {shlex.quote(str(port))} '
            f'-u {shlex.quote(username)} '
            f'--password={shlex.quote(password)} '
            f'-sN -e {shlex.quote(query)}'
        )
        # silent=True: we log our own message on failure and don't want the
        # password-bearing command line dumped into logs on error.
        success, output = BaseDispatcher.exec_command(cmd, silent=True)
        if not success:
            raise CMAPIBasicError(
                'Failed to query information_schema.PLUGINS on local '
                'MariaDB node. Ensure MariaDB is running and '
                'CrossEngineSupport credentials are valid before running '
                'install_es.'
            )

        plugins: List[Tuple[str, str]] = []
        for line in output.splitlines():
            parts = line.split('\t')
            if len(parts) >= 2 and parts[0]:
                plugins.append((parts[0].strip(), parts[1].strip()))
        return plugins

    @staticmethod
    def check_installed_plugins(
        config: str = DEFAULT_MCS_CONF_PATH,
    ) -> None:
        """Detect ACTIVE plugins not supported by install_es auto-upgrade.

        Raises CMAPIBasicError listing every unsupported plugin along
        with per-plugin manual upgrade instructions. Bundled MariaDB
        engines (InnoDB, Aria, MyISAM, Columnstore, standard auth
        plugins, etc.) are ignored — only plugins present in
        UNSUPPORTED_PLUGINS_AUTOUPGRADE are reported.
        """
        active = PreInstallManager._get_active_plugins(config)
        detected: List[Tuple[str, str, str]] = []
        for name, ptype in active:
            instructions = UNSUPPORTED_PLUGINS_AUTOUPGRADE.get(name.upper())
            if instructions is not None:
                detected.append((name, ptype, instructions))

        if not detected:
            logging.info(
                'Plugin precheck: no unsupported plugins detected.'
            )
            return

        lines = [
            'Detected MariaDB plugins that install_es cannot upgrade '
            'automatically. Upgrade each of them manually, then re-run '
            'install_es:'
        ]
        for name, ptype, instructions in detected:
            lines.append(f'  - {name} ({ptype}): {instructions}')
        message = '\n'.join(lines)
        logging.error(message)
        raise CMAPIBasicError(message)
