import logging

import socket
import subprocess
import time

from copy import deepcopy
from datetime import datetime
from pathlib import Path

import cherrypy
import pyotp
import requests

from cmapi_server.exceptions import CMAPIBasicError
from cmapi_server.constants import (
    DEFAULT_MCS_CONF_PATH, DEFAULT_SM_CONF_PATH, EM_PATH_SUFFIX,
    MCS_BRM_CURRENT_PATH, MCS_EM_PATH, S3_BRM_CURRENT_PATH, SECRET_KEY,
)
from cmapi_server.controllers.error import APIError
from cmapi_server.handlers.cej import CEJError
from cmapi_server.handlers.cluster import ClusterHandler
from cmapi_server.helpers import (
    cmapi_config_check, dequote, get_active_nodes, get_config_parser,
    get_current_key, get_dbroots, in_maintenance_state, save_cmapi_conf_file,
    system_ready,
)
from cmapi_server.logging_management import change_loggers_level
from cmapi_server.managers.application import AppManager
from cmapi_server.managers.process import MCSProcessManager
from cmapi_server.managers.transaction import TransactionManager
from cmapi_server.node_manipulation import is_master, switch_node_maintenance
from mcs_node_control.models.dbrm import set_cluster_mode
from mcs_node_control.models.node_config import NodeConfig
from mcs_node_control.models.node_status import NodeStatus


# Bug in pylint https://github.com/PyCQA/pylint/issues/4584
requests.packages.urllib3.disable_warnings()  # pylint: disable=no-member


module_logger = logging.getLogger('cmapi_server')


def log_begin(logger, func_name):
    logger.debug(f"{func_name} starts")


def raise_422_error(
    logger, func_name: str = '', err_msg: str = '', exc_info: bool = True
) -> None:
    """Function to log error and raise 422 api error.

    :param logger: logger to use
    :type logger: logging.Logger
    :param func_name: function name where it called, defaults to ''
    :type func_name: str, optional
    :param err_msg: error message, defaults to ''
    :type err_msg: str, optional
    :param exc_info: write traceback to logs or not.
    :type exc_info: bool
    :raises APIError: everytime with custom error message
    """
    # TODO: change:
    #       - func name to inspect.stack(0)[1][3]
    #       - make something to logger, seems passing here is useless
    logger.error(f'{func_name} {err_msg}', exc_info=exc_info)
    raise APIError(422, err_msg)


# TODO: Move somwhere else, eg. to helpers
def get_use_sudo(app_config: dict) -> bool:
    """Get value about using superuser or not from app config.

    :param app_config: CherryPy application config
    :type app_config: dict
    :return: use_sudo config value
    :rtype: bool
    """
    privileges_section = app_config.get('Privileges', None)
    if privileges_section is not None:
        use_sudo = privileges_section.get('use_sudo', False)
    else:
        use_sudo = False
    return use_sudo


@cherrypy.tools.register('before_handler', priority=80)
def validate_api_key():
    """Validate API key.

    If no config file, create new one by coping from default. If no API key,
    set api key from request headers.
    """
    # TODO: simplify validation, using preload and may be class-controller
    req = cherrypy.request
    if 'X-Api-Key' not in req.headers:
        error_message = 'No API key provided.'
        module_logger.warning(error_message)
        raise cherrypy.HTTPError(401, error_message)

    # we thinking that api_key is the same with quoted api_key
    request_api_key = dequote(req.headers.get('X-Api-Key', ''))
    if not request_api_key:
        error_message = 'Empty API key.'
        module_logger.warning(error_message)
        raise cherrypy.HTTPError(401, error_message)

    # because of architecture of cherrypy config parser it makes from values
    # python objects it causes some non standart behaviour
    # - makes dequote of config values automatically if it is strings
    # - config objects always gives a dict object
    # - strings with only integers inside will be always converted to int type
    inmemory_api_key = str(
        req.app.config.get('Authentication', {}).get('x-api-key', '')
    )
    if not inmemory_api_key:
        module_logger.warning(
            'No API key in the configuration. Adding it into the config.'
        )
        req.app.config.update(
            {'Authentication': {'x-api-key': request_api_key}}
        )
        # update the cmapi server config file
        config_filepath = req.app.config['config']['path']
        cmapi_config_check(config_filepath)
        cfg_parser = get_config_parser(config_filepath)

        if not cfg_parser.has_section('Authentication'):
            cfg_parser.add_section('Authentication')
        # TODO: Do not store api key in cherrypy config.
        #       It causes some overhead on custom ini file and handling it.
        #       For cherrypy config file values have to be python objects.
        #       So string have to be quoted.
        cfg_parser['Authentication']['x-api-key'] = f"'{request_api_key}'"
        save_cmapi_conf_file(cfg_parser, config_filepath)

        return

    if inmemory_api_key != request_api_key:
        module_logger.warning(f'Incorrect API key [ {request_api_key} ]')
        raise cherrypy.HTTPError(401, 'Incorrect API key')


@cherrypy.tools.register("before_handler", priority=81)
def active_operation():
    app = cherrypy.request.app
    txn_section = app.config.get('txn', None)
    txn_manager_address = None
    if txn_section is not None:
        txn_manager_address = app.config['txn'].get('manager_address', None)
    if txn_manager_address is not None and len(txn_manager_address) > 0:
        raise_422_error(
            module_logger, 'active_operation', 'There is an active operation.'
        )


@cherrypy.tools.register('before_handler', priority=82)
def has_active_nodes():
    """Check if there are any active nodes in the cluster.

    TODO: Remove in next releases due to never used.
          Now TransactionManager has this check inside.
          Before removing, have to check all API endpoints without transaction
          mechanics to potential use of this handler.
    """
    active_nodes = get_active_nodes()

    if len(active_nodes) == 0:
        raise_422_error(
            module_logger, 'has_active_nodes',
            'No active nodes in the cluster.'
        )


class TimingTool(cherrypy.Tool):
    """Tool to measure imncoming requests processing time."""
    def __init__(self):
        # if before_handler used we got 500 on each error in request body
        # (eg wrong or no content in PUT requests):
        # - wrong request body
        # - never happened handler
        # - no before_handler event
        # - never add cherrypy.request._time
        # - got error at before_finalize event getting cherrypy.request._time
        # - return 500 instead of 415 error
        super().__init__('before_request_body', self.start_timer, priority=90)

    def _setup(self):
        """Method to call by CherryPy when the tool is applied."""
        super()._setup()
        cherrypy.request.hooks.attach(
            'before_finalize', self.end_timer, priority=5
        )

    def start_timer(self):
        """Save time and log information about incoming request."""
        cherrypy.request._time = time.time()
        logger = logging.getLogger('access_logger')
        request = cherrypy.request
        remote = request.remote.name or request.remote.ip
        logger.info(
            f'Got incoming {request.method} request from "{remote}" '
            f'to "{request.path_info}". uid: {request.unique_id}'
        )

    def end_timer(self):
        """Calculate request processing duration and leave a log message."""
        duration = time.time() - cherrypy.request._time
        logger = logging.getLogger('access_logger')
        request = cherrypy.request
        remote = request.remote.name or request.remote.ip
        logger.info(
            f'Finished processing incoming {request.method} '
            f'request from "{remote}" to "{request.path_info}" in '
            f'{duration:.4f} seconds. uid: {request.unique_id}'
        )


cherrypy.tools.timeit = TimingTool()


class StatusController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_status(self):
        """
        Handler for /status (GET)
        """
        func_name = 'get_status'
        log_begin(module_logger, func_name)
        node_status = NodeStatus()
        hostname = (
            cherrypy.request.headers.get('Host', '').split(':')[0] or
            socket.gethostname()
        )
        #TODO: add localhost condition check and another way to get FQDN
        node_fqdn = socket.gethostbyaddr(hostname)[0]

        status_response = {
            'timestamp': str(datetime.now()),
            'uptime': node_status.get_host_uptime(),
            'dbrm_mode': node_status.get_dbrm_status(),
            'cluster_mode': node_status.get_cluster_mode(),
            'dbroots': sorted(get_dbroots(node_fqdn)),
            'module_id': int(node_status.get_module_id()),
            'services': MCSProcessManager.get_running_mcs_procs(),
        }

        module_logger.debug(f'{func_name} returns {str(status_response)}')
        return status_response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    def get_primary(self):
        """
        Handler for /primary (GET)

        ..WARNING: do not add api key validation here, this may cause
                   mcs-loadbrm.py (in MCS engine repo) failure
        """
        func_name = 'get_primary'
        log_begin(module_logger, func_name)
        # TODO: convert this value to json bool (remove str() invoke here)
        #       to do so loadbrm and save brm have to be fixed
        #       + check other places
        get_master_response = {'is_primary': str(NodeConfig().is_primary_node())}
        module_logger.debug(f'{func_name} returns {str(get_master_response)}')

        return get_master_response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    def get_new_primary(self):
        """
        Handler for /new_primary (GET)
        """
        func_name = 'get_new_primary'
        log_begin(module_logger, func_name)
        try:
            get_master_response = {'is_primary': is_master()}
        except CEJError as cej_error:
            raise_422_error(
                module_logger, func_name, cej_error.message
            )
        module_logger.debug(f'{func_name} returns {str(get_master_response)}')

        return get_master_response


class ConfigController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_config(self):
        """
        Handler for /config (GET)
        """
        func_name = 'get_config'
        log_begin(module_logger, func_name)

        mcs_config = NodeConfig()
        config_response = {'timestamp': str(datetime.now()),
                           'config': mcs_config.get_current_config(),
                           'sm_config': mcs_config.get_current_sm_config(),
        }

        if (module_logger.isEnabledFor(logging.DEBUG)):
            dbg_config_response = deepcopy(config_response)
            dbg_config_response.pop('config')
            dbg_config_response['config'] = 'config was removed to reduce logs.'
            dbg_config_response['sm_config'] = 'config was removed to reduce logs.'
            module_logger.debug(
                f'{func_name} returns {str(dbg_config_response)}'
            )

        return config_response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_config(self):
        """
        Handler for /config (PUT)
        """

        func_name = 'put_config'
        log_begin(module_logger, func_name)

        app = cherrypy.request.app
        txn_section = app.config.get('txn', None)

        if txn_section is None:
            raise_422_error(
                module_logger, func_name,
                'PUT /config called outside of an operation.'
            )

        req = cherrypy.request
        use_sudo = get_use_sudo(req.app.config)
        request_body = cherrypy.request.json
        request_revision = request_body.get('revision', None)
        request_manager = request_body.get('manager', None)
        request_timeout = request_body.get('timeout', None)

        #TODO: remove is_test
        # is_test = True means this should not save
        # the config file or apply the changes
        is_test = request_body.get('test', False)

        mandatory = [request_revision, request_manager, request_timeout]
        if None in mandatory:
            raise_422_error(
                module_logger, func_name, 'Mandatory attribute is missing.')

        request_mode = request_body.get('cluster_mode', None)
        request_config = request_body.get('config', None)
        mcs_config_filename = request_body.get(
            'mcs_config_filename', DEFAULT_MCS_CONF_PATH
        )
        sm_config_filename = request_body.get(
            'sm_config_filename', DEFAULT_SM_CONF_PATH
        )

        if request_mode is None and request_config is None:
            raise_422_error(
                module_logger, func_name, 'Mandatory attribute is missing.'
            )

        request_headers = cherrypy.request.headers
        request_manager_address = request_headers.get('Remote-Addr', None)
        if request_manager_address is None:
            raise_422_error(
                module_logger, func_name,
                'Cannot get Cluster manager IP address.'
            )
        txn_manager_address = app.config['txn'].get('manager_address', None)
        if txn_manager_address is None or len(txn_manager_address) == 0:
            raise_422_error(
                module_logger, func_name,
                'PUT /config called outside of an operation.'
            )
        txn_manager_address = dequote(txn_manager_address).lower()
        request_manager_address = dequote(request_manager_address).lower()

        if request_manager_address in ['127.0.0.1', 'localhost', '::1']:
            request_manager_address = socket.gethostbyname(
                socket.gethostname()
            )
        request_response = {'timestamp': str(datetime.now())}

        node_config = NodeConfig()
        xml_config = request_body.get('config', None)
        sm_config = request_body.get('sm_config', None)
        if is_test:
            return request_response
        if request_mode is not None:
            current_mode = set_cluster_mode(
                request_mode, config_filename=mcs_config_filename
            )
            if current_mode == request_mode:
                # Normal exit
                module_logger.debug(
                    f'{func_name} returns {str(request_response)}'
                )
                return request_response
            else:
                raise_422_error(
                    module_logger, func_name,
                    (
                        f'Error occured setting cluster to "{request_mode}" '
                        f'mode, got "{current_mode}"'
                    )
                )
        elif xml_config is not None:
            node_config.apply_config(
                config_filename=mcs_config_filename,
                xml_string=xml_config,
                sm_config_filename=sm_config_filename,
                sm_config_string=sm_config
            )
            # TODO: change stop/start to restart option.
            try:
                MCSProcessManager.stop_node(
                    is_primary=node_config.is_primary_node(),
                    use_sudo=use_sudo,
                    timeout=request_timeout
                )
            except CMAPIBasicError as err:
                raise_422_error(
                    module_logger, func_name,
                    f'Error while stopping node. Details: {err.message}.',
                    exc_info=False
                )

            # if not in the list of active nodes,
            # then do not start the services
            new_root = node_config.get_current_config_root(
                mcs_config_filename
            )
            if in_maintenance_state():
                module_logger.info(
                    'Maintenance state is active in new config. '
                    'MCS processes should not be started.'
                )
                cherrypy.engine.publish('failover', False)
                # skip all other operations below
                return request_response
            else:
                cherrypy.engine.publish('failover', True)
            if node_config.in_active_nodes(new_root):
                try:
                    MCSProcessManager.start_node(
                        is_primary=node_config.is_primary_node(),
                        use_sudo=use_sudo,
                    )
                except CMAPIBasicError as err:
                    raise_422_error(
                        module_logger, func_name,
                        (
                            'Error while starting node. '
                            f'Details: {err.message}'
                        ),
                        exc_info=False
                    )
            else:
                module_logger.info(
                    'This node is not in the current ActiveNodes section. '
                    'Not starting Columnstore processes.'
                )

            attempts = 0
            # TODO: FIX IT. If got (False, False) result, for eg in case
            #       when there are no special CEJ user set, this check loop
            #       is useless and do nothing.
            try:
                ready, retry = system_ready(mcs_config_filename)
            except CEJError as cej_error:
                raise_422_error(
                    module_logger, func_name, cej_error.message
                )

            while not ready:
                if retry:
                    attempts +=1
                    if attempts >= 10:
                        module_logger.debug(
                            'Timed out waiting for node to be ready.'
                        )
                        break
                    time.sleep(1)
                else:
                    break
                try:
                    ready, retry = system_ready(mcs_config_filename)
                except CEJError as cej_error:
                    raise_422_error(
                        module_logger, func_name, cej_error.message
                    )
            else:
                module_logger.debug(f'Node is ready to accept queries.')

            app.config['txn']['config_changed'] = True

            # We might want to raise error
            return request_response

        # Unexpected exit
        raise_422_error(module_logger, func_name, 'Unknown error.')


class BeginController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    @cherrypy.tools.active_operation()  # pylint: disable=no-member
    def put_begin(self):
        """
        Handler for /begin (PUT)
        """
        func_name = 'put_begin'
        log_begin(module_logger, func_name)

        app = cherrypy.request.app
        request_body = cherrypy.request.json
        txn_id = request_body.get('id', None)
        txn_timeout = request_body.get('timeout', None)
        request_headers = cherrypy.request.headers
        txn_manager_address = request_headers.get('Remote-Addr', None)
        module_logger.debug(f'{func_name} JSON body {str(request_body)}')

        if txn_manager_address is None:
            raise_422_error(module_logger, func_name, "Cannot get Cluster Manager \
IP address.")
        txn_manager_address = dequote(txn_manager_address).lower()
        if txn_manager_address in ['127.0.0.1', 'localhost', '::1']:
            txn_manager_address = socket.gethostbyname(socket.gethostname())
        if txn_id is None or txn_timeout is None or txn_manager_address is None:
            raise_422_error(module_logger, func_name, "id or timeout is not set.")

        app.config.update({
            'txn': {
                'id': txn_id,
                'timeout': int(datetime.now().timestamp()) + txn_timeout,
                'manager_address': txn_manager_address,
                'config_changed': False,
            },
        })

        begin_response = {'timestamp': str(datetime.now())}

        module_logger.debug(f'{func_name} returns {str(begin_response)}')
        return begin_response


class CommitController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_commit(self):
        """
        Handler for /commit (PUT)
        """
        func_name = 'put_commit'
        log_begin(module_logger, func_name)

        commit_response = {'timestamp': str(datetime.now())}
        app = cherrypy.request.app
        txn_section = app.config.get('txn', None)

        if txn_section is None:
            raise_422_error(module_logger, func_name, "No operation to commit.")

        request_headers = cherrypy.request.headers
        request_manager_address = request_headers.get('Remote-Addr', None)
        if request_manager_address is None:
            raise_422_error(module_logger, func_name, "Cannot get Cluster\
 Manager IP address.")
        txn_manager_address = app.config['txn'].get('manager_address', None)
        if txn_manager_address is None or len(txn_manager_address) == 0:
            raise_422_error(module_logger, func_name, "No operation to commit.")
        txn_manager_address = dequote(txn_manager_address).lower()
        request_manager_address = dequote(request_manager_address).lower()
        if request_manager_address in ['127.0.0.1', 'localhost', '::1']:
            request_manager_address = socket.gethostbyname(socket.gethostname())
        # txn is active
        app.config['txn']['id'] = 0
        app.config['txn']['timeout'] = 0
        app.config['txn']['manager_address'] = ''
        app.config['txn']['config_changed'] = False

        module_logger.debug(f'{func_name} returns {str(commit_response)}')

        return commit_response


class RollbackController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_rollback(self):
        """
        Handler for /rollback (PUT)
        """
        rollback_response = {'timestamp': str(datetime.now())}
        app = cherrypy.request.app
        txn_section = app.config.get('txn', None)

        if txn_section is None:
            raise APIError(422, 'No operation to rollback.')

        request_headers = cherrypy.request.headers
        request_manager_address = request_headers.get('Remote-Addr', None)
        if request_manager_address is None:
            raise APIError(422, 'Cannot get Cluster Manager IP address.')
        txn_manager_address = app.config['txn'].get('manager_address', None)
        if txn_manager_address is None or len(txn_manager_address) == 0:
            raise APIError(422, 'No operation to rollback.')
        txn_manager_address = dequote(txn_manager_address).lower()
        request_manager_address = dequote(request_manager_address).lower()
        if request_manager_address in ['127.0.0.1', 'localhost', '::1']:
            request_manager_address = socket.gethostbyname(socket.gethostname())

        #TODO: add restart processes flag?
        # txn is active
        txn_config_changed = app.config['txn'].get('config_changed', None)
        if txn_config_changed is True:
            node_config = NodeConfig()
            node_config.rollback_config()
            # TODO: do we need to restart node here?
            node_config.apply_config(
                xml_string=node_config.get_current_config()
            )
        app.config['txn']['id'] = 0
        app.config['txn']['timeout'] = 0
        app.config['txn']['manager_address'] = ''
        app.config['txn']['config_changed'] = False

        return rollback_response


class StartController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_start(self):
        func_name = 'put_start'
        log_begin(module_logger, func_name)

        req = cherrypy.request
        use_sudo = get_use_sudo(req.app.config)
        node_config = NodeConfig()
        try:
            MCSProcessManager.start_node(
                is_primary=node_config.is_primary_node(),
                use_sudo=use_sudo
            )
        except CMAPIBasicError as err:
            raise_422_error(
                module_logger, func_name,
                f'Error while starting node processes. Details: {err.message}',
                exc_info=False
            )
        # TODO: should we change config revision here? Seem to be no.
        #       Do we need to change flag in a one node maintenance?
        switch_node_maintenance(False)
        cherrypy.engine.publish('failover', True)
        start_response = {'timestamp': str(datetime.now())}
        module_logger.debug(f'{func_name} returns {str(start_response)}')
        return start_response


class ShutdownController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_shutdown(self):
        func_name = 'put_shutdown'
        log_begin(module_logger, func_name)

        req = cherrypy.request
        use_sudo = get_use_sudo(req.app.config)
        request_body = cherrypy.request.json
        timeout = request_body.get('timeout', 0)
        node_config = NodeConfig()
        try:
            MCSProcessManager.stop_node(
                is_primary=node_config.is_primary_node(),
                use_sudo=use_sudo,
                timeout=timeout
            )
        except CMAPIBasicError as err:
            raise_422_error(
                module_logger, func_name,
                f'Error while stopping node processes. Details: {err.message}',
                exc_info=False
            )
        # TODO: should we change config revision here? Seem to be no.
        #       Do we need to change flag in a one node maintenance?
        switch_node_maintenance(True)
        cherrypy.engine.publish('failover', False)
        shutdown_response = {'timestamp': str(datetime.now())}
        module_logger.debug(f'{func_name} returns {str(shutdown_response)}')
        return shutdown_response


class ExtentMapController:
    def get_brm_bytes(self, element:str):
        func_name = 'get_brm_bytes'
        log_begin(module_logger, func_name)
        node_config = NodeConfig()
        result = b''
        # there must be sm available
        if node_config.s3_enabled():
            success = False
            retry_count = 0
            while not success and retry_count < 10:
                module_logger.debug(f'{func_name} returns {element} from S3.')

                # TODO: Remove conditional once container dispatcher
                #       uses non-root by default
                if MCSProcessManager.dispatcher_name == 'systemd':
                    args = [
                        'su', '-s', '/bin/sh', '-c',
                        f'smcat {S3_BRM_CURRENT_PATH}', 'mysql'
                    ]
                else:
                    args = ['smcat', S3_BRM_CURRENT_PATH]

                ret = subprocess.run(args, stdout=subprocess.PIPE)
                if ret.returncode != 0:
                    module_logger.warning(f"{func_name} got error code {ret.returncode} from smcat, retrying")
                    time.sleep(1)
                    retry_count += 1
                    continue
                elem_current_suffix = ret.stdout.decode("utf-8").rstrip()
                elem_current_filename = f'{EM_PATH_SUFFIX}/{elem_current_suffix}_{element}'

                # TODO: Remove conditional once container dispatcher
                #       uses non-root by default
                if MCSProcessManager.dispatcher_name == 'systemd':
                    args = [
                        'su', '-s', '/bin/sh', '-c',
                        f'smcat {elem_current_filename}', 'mysql'
                    ]
                else:
                    args = ['smcat', elem_current_filename]

                ret = subprocess.run(args, stdout=subprocess.PIPE)
                if ret.returncode != 0:
                    module_logger.warning(f"{func_name} got error code {ret.returncode} from smcat, retrying")
                    time.sleep(1)
                    retry_count += 1
                    continue
                result = ret.stdout
                success = True
        else:
            module_logger.debug(
                f'{func_name} returns {element} from local storage.'
            )
            elem_current_name = Path(MCS_BRM_CURRENT_PATH)
            elem_current_filename = elem_current_name.read_text().rstrip()
            elem_current_file = Path(
                f'{MCS_EM_PATH}/{elem_current_filename}_{element}'
            )
            result = elem_current_file.read_bytes()

        module_logger.debug(f'{func_name} returns.')
        return result

    @cherrypy.tools.timeit()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_em(self):
        return self.get_brm_bytes('em')

    @cherrypy.tools.timeit()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_journal(self):
        return self.get_brm_bytes('journal')

    @cherrypy.tools.timeit()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_vss(self):
        return self.get_brm_bytes('vss')

    @cherrypy.tools.timeit()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_vbbm(self):
        return self.get_brm_bytes('vbbm')

    @cherrypy.tools.timeit()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    @cherrypy.tools.json_out()
    def get_footprint(self):
        # Dummy footprint
        result = {'em': '00f62e18637e1708b080b076ea6aa9b0',
                  'journal': '00f62e18637e1708b080b076ea6aa9b0',
                  'vss': '00f62e18637e1708b080b076ea6aa9b0',
                  'vbbm': '00f62e18637e1708b080b076ea6aa9b0',
        }
        return result


class ClusterController:
    _cp_config = {
        "request.methods_with_bodies": ("POST", "PUT", "PATCH", "DELETE")
    }
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_start(self):
        func_name = 'put_start'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = request.json
        config = request_body.get('config', DEFAULT_MCS_CONF_PATH)
        in_transaction = request_body.get('in_transaction', False)

        try:
            if not in_transaction:
                with TransactionManager():
                    response = ClusterHandler.start(config)
            else:
                response = ClusterHandler.start(config)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_shutdown(self):
        func_name = 'put_shutdown'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = request.json
        timeout = request_body.get('timeout', None)
        force = request_body.get('force', False)
        config = request_body.get('config', DEFAULT_MCS_CONF_PATH)
        in_transaction = request_body.get('in_transaction', False)

        try:
            if not in_transaction:
                with TransactionManager():
                    response = ClusterHandler.shutdown(config, timeout)
            else:
                response = ClusterHandler.shutdown(config)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_mode_set(self):
        func_name = 'put_mode_set'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = request.json
        mode = request_body.get('mode', 'readonly')
        config = request_body.get('config', DEFAULT_MCS_CONF_PATH)
        in_transaction = request_body.get('in_transaction', False)

        try:
            if not in_transaction:
                with TransactionManager():
                    response = ClusterHandler.set_mode(mode, config=config)
            else:
                response = ClusterHandler.set_mode(mode, config=config)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_add_node(self):
        func_name = 'add_node'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = request.json
        node = request_body.get('node', None)
        config = request_body.get('config', DEFAULT_MCS_CONF_PATH)
        in_transaction = request_body.get('in_transaction', False)

        if node is None:
            raise_422_error(module_logger, func_name, 'missing node argument')

        try:
            if not in_transaction:
                with TransactionManager(extra_nodes=[node]):
                    response = ClusterHandler.add_node(node, config)
            else:
                response = ClusterHandler.add_node(node, config)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def delete_remove_node(self):
        func_name = 'remove_node'
        log_begin(module_logger, func_name)
        request = cherrypy.request
        request_body = request.json
        node = request_body.get('node', None)
        config = request_body.get('config', DEFAULT_MCS_CONF_PATH)
        in_transaction = request_body.get('in_transaction', False)

        #TODO: add arguments verification decorator
        if node is None:
            raise_422_error(module_logger, func_name, 'missing node argument')

        try:
            if not in_transaction:
                with TransactionManager(remove_nodes=[node]):
                    response = ClusterHandler.remove_node(node, config)
            else:
                response = ClusterHandler.remove_node(node, config)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_scan_for_attached_dbroots(self):
        '''TODO: Based on doc, endpoint not exposed'''
        func_name = 'put_scan_for_attached_dbroots'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = cherrypy.request.json
        node = request_body.get('node', None)
        response = {'timestamp': str(datetime.now())}

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_failover_master(self):
        '''TODO: Based on doc, endpoint not exposed'''
        func_name = 'put_failover_master'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = cherrypy.request.json
        source = request_body.get('from', None)
        dest = request_body.get('to', None)
        response = {'timestamp': str(datetime.now())}

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_move_dbroot(self):
        '''TODO: Based on doc, endpoint not exposed'''
        func_name = 'put_move_dbroot'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = cherrypy.request.json
        source = request_body.get('from', None)
        dest = request_body.get('to', None)
        response = {'timestamp': str(datetime.now())}

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_decommission_node(self):
        '''TODO: Based on doc, endpoint not exposed'''
        func_name = 'put_decommission_node'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = cherrypy.request.json
        node = request_body.get('node', None)
        response = {'timestamp': str(datetime.now())}

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_status(self):
        func_name = 'get_status'
        log_begin(module_logger, func_name)

        try:
            response = ClusterHandler.status()
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def set_api_key(self):
        """Handler for /cluster/apikey-set (PUT)

        Only for cli tool usage.
        """
        func_name = 'cluster_set_api_key'
        module_logger.debug('Start setting API key to all nodes in cluster.')
        request = cherrypy.request
        request_body = request.json
        new_api_key = dequote(request_body.get('api_key', ''))
        totp_key = request_body.get('verification_key', '')

        if not totp_key or not new_api_key:
            # not show which arguments in error message because endpoint for
            # cli tool or internal usage only
            raise_422_error(
                module_logger, func_name, 'Missing required arguments.'
            )

        totp = pyotp.TOTP(SECRET_KEY)
        if not totp.verify(totp_key):
            raise_422_error(
                module_logger, func_name, 'Wrong verification key.'
            )

        try:
            response = ClusterHandler.set_api_key(new_api_key, totp_key)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def set_log_level(self):
        """Handler for /cluster/log-level (PUT)

        Only for develop purposes.
        """
        func_name = 'cluster_set_log_level'
        module_logger.debug(
            'Start setting new log level to all nodes in cluster.'
        )
        request = cherrypy.request
        request_body = request.json
        new_level = request_body.get('level', None)
        if not new_level:
            raise_422_error(
                module_logger, func_name, 'Missing required level argument.'
            )
        module_logger.info(f'Start setting new logging level "{new_level}".')

        try:
            response = ClusterHandler.set_log_level(new_level)
        except CMAPIBasicError as err:
            raise_422_error(module_logger, func_name, err.message)

        module_logger.debug(f'{func_name} returns {str(response)}')
        return response


class ApiKeyController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def set_api_key(self):
        """Handler for /node/apikey-set (PUT)

        Only for cli tool usage.
        """
        func_name = 'node_set_api_key'
        module_logger.debug('Start setting new node API key.')
        request = cherrypy.request
        request_body = request.json
        new_api_key = dequote(request_body.get('api_key', ''))
        totp_key = request_body.get('verification_key', '')

        if not totp_key or not new_api_key:
            # not show which arguments in error message because endpoint for
            # internal usage only
            raise_422_error(
                module_logger, func_name, 'Missing required arguments.'
            )

        totp = pyotp.TOTP(SECRET_KEY)
        if not totp.verify(totp_key):
            raise_422_error(
                module_logger, func_name, 'Wrong verification key.'
            )

        config_filepath = request.app.config['config']['path']
        cmapi_config_check(config_filepath)
        cfg_parser = get_config_parser(config_filepath)
        config_api_key = get_current_key(cfg_parser)
        if config_api_key != new_api_key:
            if not cfg_parser.has_section('Authentication'):
                cfg_parser.add_section('Authentication')
            # TODO: Do not store api key in cherrypy config.
            #       It causes some overhead on custom ini file and handling it.
            #       For cherrypy config file values have to be python objects.
            #       So string have to be quoted.
            cfg_parser['Authentication']['x-api-key'] = f"'{new_api_key}'"
            save_cmapi_conf_file(cfg_parser, config_filepath)
        else:
            module_logger.info(
                'API key in config file is the same with new one.'
            )

        # anyway update inmemory api key
        request.app.config.update(
            {'Authentication': {'x-api-key': new_api_key}}
        )

        module_logger.info('API key successfully updated.')
        return {'timestamp': str(datetime.now())}


class LoggingConfigController:
    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def set_log_level(self):
        """Handler for /node/log-level (PUT)

        Only for develop purposes.
        """
        func_name = 'node_put_log_level'
        request = cherrypy.request
        request_body = request.json
        new_level = request_body.get('level', None)
        if not new_level:
            raise_422_error(
                module_logger, func_name, 'Missing required level argument.'
            )
        module_logger.info(f'Start setting new logging level "{new_level}".')
        try:
            change_loggers_level(new_level)
        except ValueError as exc:
            raise_422_error(
                module_logger, func_name, str(exc)
            )
        except Exception:
            raise_422_error(
                module_logger, func_name, 'Unknown error'
            )
        module_logger.debug(
            f'Finished setting new logging level "{new_level}".'
        )
        return {'new_level': new_level}


class AppController():

    @cherrypy.tools.json_out()
    def ready(self):
        if AppManager.started:
            return {'started': True}
        else:
            raise APIError(503, 'CMAPI not ready to handle requests.')


class NodeProcessController():

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def put_stop_dmlproc(self):
        """Handler for /node/stop_dmlproc (PUT) endpoint."""
        # TODO: make it works only from cli tool like set_api_key made
        func_name = 'put_stop_dmlproc'
        log_begin(module_logger, func_name)

        request = cherrypy.request
        request_body = request.json
        timeout = request_body.get('timeout', 10)
        force = request_body.get('force', False)

        if force:
            module_logger.debug(
                f'Calling DMLproc to force stop after timeout={timeout}.'
            )
            MCSProcessManager.stop(
                name='DMLProc', is_primary=True, use_sudo=True, timeout=timeout
            )
        else:
            module_logger.debug('Callling stop DMLproc gracefully.')
            try:
                MCSProcessManager.gracefully_stop_dmlproc()
            except (ConnectionRefusedError, RuntimeError):
                raise_422_error(
                    logger=module_logger, func_name=func_name,
                    err_msg='Couldn\'t stop DMlproc gracefully'
                )
        response = {'timestamp': str(datetime.now())}
        module_logger.debug(f'{func_name} returns {str(response)}')
        return response

    @cherrypy.tools.timeit()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def get_process_running(self, process_name):
        """Handler for /node/is_process_running (GET) endpoint."""
        func_name = 'get_process_running'
        log_begin(module_logger, func_name)

        process_running = MCSProcessManager.is_service_running(process_name)

        response = {
            'timestamp': str(datetime.now()),
            'process_name': process_name,
            'running': process_running
        }
        module_logger.debug(f'{func_name} returns {str(response)}')
        return response
