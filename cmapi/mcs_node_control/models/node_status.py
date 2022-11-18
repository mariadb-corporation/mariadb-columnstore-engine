import logging
import socket

from cmapi_server.constants import MCS_DATA_PATH, MCS_MODULE_FILE_PATH
from mcs_node_control.models.dbrm import DBRM
from mcs_node_control.models.misc import get_dbroots_list, read_module_id
from mcs_node_control.models.process import get_host_uptime


PROC_NAMES = ['ExeMgr', 'PrimProc', 'WriteEngine', 'controllernode',
              'workernode', 'cmagent', 'DMLProc', 'DDLProc']


module_logger = logging.getLogger()


class NodeStatus:
    """Class to tell the status of the node.

    Inspects runtime of the cluster and OS and returns its observations.
    """
    def get_cluster_mode(self):
        """Reads cluster mode.

        Cluster can be in readwrite or readonly modes. It can be also ready or
        not ready but it is not important at this point. We pesume if there is
        no connection with DBRM master then the cluster is readonly.

        TODO:
            - Is it ok to have those method here in NodeStatus?
              Move to DBRM.
            - pass 'root' and config_filename arguments
              (likewise dbrm.set_cluster_mode)

        :rtype: string
        """
        try:
            with DBRM() as dbrm:
                return dbrm.get_cluster_mode()
        except (ConnectionRefusedError, RuntimeError, socket.error):
            module_logger.error(
                'Cannot establish or use DBRM connection.',
                exc_info=True
            )
            return 'readonly'


    def get_dbrm_status(self):
        """reads DBRM status

        DBRM Block Resolution Manager operates in two modes:
        master and slave. This m() returns the mode of this node
        looking for controllernode process running.

        :rtype: string
        """
        return DBRM.get_dbrm_status()

    def get_dbroots(self, path:str = MCS_DATA_PATH):
        """searches for services

        The method returns numeric ids of dbroots available.

        :rtype: generator of ints
        """
        for id in get_dbroots_list(path):
            yield id


    def get_host_uptime(self):
        """Retrieves uptime in seconds.

        :rtype: int : seconds
        """
        return get_host_uptime()


    def get_module_id(self):
        """Retrieves module ID from MCS_MODULE_FILE_PATH.

        :rtype: int : seconds
        """
        func_name = 'get_module_id'
        try:
            module_id = read_module_id()
        except FileNotFoundError:
            module_id = 0
            module_logger.error(
                f'{func_name} {MCS_MODULE_FILE_PATH} file is absent.'
            )
        return module_id
