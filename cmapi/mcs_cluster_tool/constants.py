import os

from cmapi_server.constants import MCS_INSTALL_BIN


MCS_CLI_ROOT_PATH = os.path.dirname(__file__)
MCS_CLI_LOG_CONF_PATH =  os.path.join(MCS_CLI_ROOT_PATH, 'mcs_cli_log.conf')

MCS_BACKUP_MANAGER_SH = os.path.join(MCS_INSTALL_BIN, 'mcs_backup_manager.sh')
MCS_COLUMNSTORE_REVIEW_SH = os.path.join(
    MCS_INSTALL_BIN, 'columnstore_review.sh'
)