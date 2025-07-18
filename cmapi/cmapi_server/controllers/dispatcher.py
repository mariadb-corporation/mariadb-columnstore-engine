import json
import logging

import cherrypy

from cmapi_server.constants import _version
from cmapi_server.controllers.endpoints import (
    ApiKeyController, AppController, BeginController, ClusterController,
    CommitController, ConfigController, ExtentMapController,
    LoggingConfigController, NodeController, NodeProcessController,
    RollbackController, ShutdownController, StartController, StatusController,
)
from cmapi_server.controllers.s3dataload import S3DataLoadController


dispatcher = cherrypy.dispatch.RoutesDispatcher()
logger = logging.getLogger(__name__)


# /_version/status (GET)
dispatcher.connect(name = 'status',
                   route = f'/cmapi/{_version}/node/status',
                   action = 'get_status',
                   controller = StatusController(),
                   conditions = {'method': ['GET']})


# /_version/master (GET)
dispatcher.connect(name = 'get_primary',
                   route = f'/cmapi/{_version}/node/primary',
                   action = 'get_primary',
                   controller = StatusController(),
                   conditions = {'method': ['GET']})


# /_version/new_primary (GET)
dispatcher.connect(name = 'get_new_primary',
                   route = f'/cmapi/{_version}/node/new_primary',
                   action = 'get_new_primary',
                   controller = StatusController(),
                   conditions = {'method': ['GET']})


# /_version/config/ (GET)
dispatcher.connect(name = 'get_config', # what does this name is used for?
                   route = f'/cmapi/{_version}/node/config',
                   action = 'get_config',
                   controller = ConfigController(),
                   conditions = {'method': ['GET']})


# /_version/config/ (PUT)
dispatcher.connect(name = 'put_config',
                   route = f'/cmapi/{_version}/node/config',
                   action = 'put_config',
                   controller = ConfigController(),
                   conditions = {'method': ['PUT']})


# /_version/begin/ (PUT)
dispatcher.connect(name = 'put_begin',
                   route = f'/cmapi/{_version}/node/begin',
                   action = 'put_begin',
                   controller = BeginController(),
                   conditions = {'method': ['PUT']})


# /_version/rollback/ (PUT)
dispatcher.connect(name = 'put_rollback',
                   route = f'/cmapi/{_version}/node/rollback',
                   action = 'put_rollback',
                   controller = RollbackController(),
                   conditions = {'method': ['PUT']})


# /_version/commit/ (PUT)
dispatcher.connect(name = 'put_commit',
                   route = f'/cmapi/{_version}/node/commit',
                   action = 'put_commit',
                   controller = CommitController(),
                   conditions = {'method': ['PUT']})


# /_version/start/ (PUT)
dispatcher.connect(name = 'start',
                   route = f'/cmapi/{_version}/node/start',
                   action = 'put_start',
                   controller = StartController(),
                   conditions = {'method': ['PUT']})


# /_version/shutdown/ (PUT)
dispatcher.connect(name = 'shutdown',
                   route = f'/cmapi/{_version}/node/shutdown',
                   action = 'put_shutdown',
                   controller = ShutdownController(),
                   conditions = {'method': ['PUT']})


# /_version/meta/em/ (GET)
dispatcher.connect(name = 'get_em',
                   route = f'/cmapi/{_version}/node/meta/em',
                   action = 'get_em',
                   controller = ExtentMapController(),
                   conditions = {'method': ['GET']})


# /_version/meta/journal/ (GET)
dispatcher.connect(name = 'get_journal',
                   route = f'/cmapi/{_version}/node/meta/journal',
                   action = 'get_journal',
                   controller = ExtentMapController(),
                   conditions = {'method': ['GET']})


# /_version/meta/vss/ (GET)
dispatcher.connect(name = 'get_vss',
                   route = f'/cmapi/{_version}/node/meta/vss',
                   action = 'get_vss',
                   controller = ExtentMapController(),
                   conditions = {'method': ['GET']})


# /_version/meta/vbbm/ (GET)
dispatcher.connect(name = 'get_vbbm',
                   route = f'/cmapi/{_version}/node/meta/vbbm',
                   action = 'get_vbbm',
                   controller = ExtentMapController(),
                   conditions = {'method': ['GET']})


# /_version/meta/footprint/ (GET)
dispatcher.connect(name = 'get_footprint',
                   route = f'/cmapi/{_version}/node/meta/footprint',
                   action = 'get_footprint',
                   controller = ExtentMapController(),
                   conditions = {'method': ['GET']})


# /_version/cluster/start/ (PUT)
dispatcher.connect(name = 'cluster_start',
                   route = f'/cmapi/{_version}/cluster/start',
                   action = 'put_start',
                   controller = ClusterController(),
                   conditions = {'method': ['PUT']})


# /_version/cluster/shutdown/ (PUT)
dispatcher.connect(name = 'cluster_shutdown',
                   route = f'/cmapi/{_version}/cluster/shutdown',
                   action = 'put_shutdown',
                   controller = ClusterController(),
                   conditions = {'method': ['PUT']})


# /_version/cluster/mode-set/ (PUT)
dispatcher.connect(name = 'cluster_mode_set',
                   route = f'/cmapi/{_version}/cluster/mode-set',
                   action = 'put_mode_set',
                   controller = ClusterController(),
                   conditions = {'method': ['PUT']})


# /_version/cluster/node/ (POST, PUT)
dispatcher.connect(name = 'cluster_add_node',
                   route = f'/cmapi/{_version}/cluster/node',
                   action = 'put_add_node',
                   controller = ClusterController(),
                   conditions = {'method': ['POST', 'PUT']})


# /_version/cluster/node/ (DELETE)
dispatcher.connect(name = 'cluster_remove_node',
                   route = f'/cmapi/{_version}/cluster/node',
                   action = 'delete_remove_node',
                   controller = ClusterController(),
                   conditions = {'method': ['DELETE']})


# /_version/cluster/status/ (GET)
dispatcher.connect(name = 'cluster_status',
                   route = f'/cmapi/{_version}/cluster/status',
                   action = 'get_status',
                   controller = ClusterController(),
                   conditions = {'method': ['GET']})


# /_version/node/apikey-set/ (PUT)
dispatcher.connect(
    name = 'node_set_api_key',
    route = f'/cmapi/{_version}/node/apikey-set',
    action = 'set_api_key',
    controller = ApiKeyController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/apikey-set/ (PUT)
dispatcher.connect(
    name = 'cluster_set_api_key',
    route = f'/cmapi/{_version}/cluster/apikey-set',
    action = 'set_api_key',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/node/ (POST, PUT)
dispatcher.connect(name = 'cluster_load_s3data',
                   route = f'/cmapi/{_version}/cluster/load_s3data',
                   action = 'load_s3data',
                   controller = S3DataLoadController(),
                   conditions = {'method': ['POST', 'PUT']})


# /_version/node/log-config/ (PUT)
dispatcher.connect(
    name = 'node_set_log_level',
    route = f'/cmapi/{_version}/node/log-level',
    action = 'set_log_level',
    controller = LoggingConfigController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/log-config'/ (PUT)
dispatcher.connect(
    name = 'cluster_set_log_level',
    route = f'/cmapi/{_version}/cluster/log-level',
    action = 'set_log_level',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /ready (GET)
dispatcher.connect(
    name = 'app_ready',
    route = '/cmapi/ready',
    action = 'ready',
    controller = AppController(),
    conditions = {'method': ['GET']}
)


# /_version/node/stop_dmlproc/ (PUT)
dispatcher.connect(
    name = 'stop_dmlproc',
    route = f'/cmapi/{_version}/node/stop_dmlproc',
    action = 'put_stop_dmlproc',
    controller = NodeProcessController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/is_process_running/ (GET)
dispatcher.connect(
    name = 'is_process_running',
    route = f'/cmapi/{_version}/node/is_process_running',
    action = 'get_process_running',
    controller = NodeProcessController(),
    conditions = {'method': ['GET']}
)


# /_version/cluster/health/ (PUT)
dispatcher.connect(
    name = 'cluster_get_health',
    route = f'/cmapi/{_version}/cluster/health',
    action = 'get_health',
    controller = ClusterController(),
    conditions = {'method': ['GET']}
)


# /_version/node/versions (GET)
dispatcher.connect(
    name = 'node_get_versions',
    route = f'/cmapi/{_version}/node/versions',
    action = 'get_versions',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/cluster/version (GET)
dispatcher.connect(
    name = 'cluster_get_versions',
    route = f'/cmapi/{_version}/cluster/versions',
    action = 'get_versions',
    controller = ClusterController(),
    conditions = {'method': ['GET']}
)


# /_version/node/latest-mdb-version (GET)
dispatcher.connect(
    name = 'get_latest_mdb_version',
    route = f'/cmapi/{_version}/node/latest-mdb-version',
    action = 'latest_mdb_version',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/node/validate-mdb-version (GET)
dispatcher.connect(
    name = 'get_validate_mdb_version',
    route = f'/cmapi/{_version}/node/validate-mdb-version',
    action = 'validate_mdb_version',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/node/validate-es-token (GET)
dispatcher.connect(
    name = 'get_validate_es_token',
    route = f'/cmapi/{_version}/node/validate-es-token',
    action = 'validate_es_token',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/node/stop-mariadb (PUT)
dispatcher.connect(
    name = 'node_stop_mariadb',
    route = f'/cmapi/{_version}/node/stop-mariadb',
    action = 'stop_mariadb',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/stop-mariadb (PUT)
dispatcher.connect(
    name = 'cluster_stop_mariadb',
    route = f'/cmapi/{_version}/cluster/stop-mariadb',
    action = 'stop_mariadb',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/start-mariadb (PUT)
dispatcher.connect(
    name = 'node_start_mariadb',
    route = f'/cmapi/{_version}/node/start-mariadb',
    action = 'start_mariadb',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/start-mariadb (PUT)
dispatcher.connect(
    name = 'cluster_start_mariadb',
    route = f'/cmapi/{_version}/cluster/start-mariadb',
    action = 'start_mariadb',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/install-repo (PUT)
dispatcher.connect(
    name = 'node_install_repo',
    route = f'/cmapi/{_version}/node/install-repo',
    action = 'install_repo',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/install-repo (PUT)
dispatcher.connect(
    name = 'cluster_install_repo',
    route = f'/cmapi/{_version}/cluster/install-repo',
    action = 'install_repo',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/repo-pkg-versions (GET)
dispatcher.connect(
    name = 'get_repo_pkg_versions',
    route = f'/cmapi/{_version}/node/repo-pkg-versions',
    action = 'repo_pkg_versions',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/node/preupgrade-backup (PUT)
dispatcher.connect(
    name = 'node_preupgrade_backup',
    route = f'/cmapi/{_version}/node/preupgrade-backup',
    action = 'preupgrade_backup',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/preupgrade-backup (PUT)
dispatcher.connect(
    name = 'cluster_preupgrade_backup',
    route = f'/cmapi/{_version}/cluster/preupgrade-backup',
    action = 'preupgrade_backup',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/upgrade-mdb-mcs (PUT)
dispatcher.connect(
    name = 'node_upgrade_mdb_mcs',
    route = f'/cmapi/{_version}/node/upgrade-mdb-mcs',
    action = 'upgrade_mdb_mcs',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/upgrade-mdb-mcs (PUT)
dispatcher.connect(
    name = 'cluster_upgrade_mdb_mcs',
    route = f'/cmapi/{_version}/cluster/upgrade-mdb-mcs',
    action = 'upgrade_mdb_mcs',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/kick-cmapi-upgrade (PUT)
dispatcher.connect(
    name = 'node_kick_cmapi_upgrade',
    route = f'/cmapi/{_version}/node/kick-cmapi-upgrade',
    action = 'kick_cmapi_upgrade',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


# /_version/cluster/upgrade-cmapi (PUT)
dispatcher.connect(
    name = 'cluster_upgrade_cmapi',
    route = f'/cmapi/{_version}/cluster/upgrade-cmapi',
    action = 'upgrade_cmapi',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/check-shared-file/ (GET)
dispatcher.connect(
    name = 'node_check_shared_file',
    route = f'/cmapi/{_version}/node/check-shared-file',
    action = 'check_shared_file',
    controller = NodeController(),
    conditions = {'method': ['GET']}
)


# /_version/cluster/check-shared-storage/ (PUT)
dispatcher.connect(
    name = 'cluster_check_shared_storage',
    route = f'/cmapi/{_version}/cluster/check-shared-storage',
    action = 'check_shared_storage',
    controller = ClusterController(),
    conditions = {'method': ['PUT']}
)


# /_version/node/stateful-config/ (PUT)
dispatcher.connect(
    name = 'node_put_stateful_config',
    route = f'/cmapi/{_version}/node/stateful-config',
    action = 'put_stateful_config',
    controller = NodeController(),
    conditions = {'method': ['PUT']}
)


def jsonify_error(status, message, traceback, version): \
    # pylint: disable=unused-argument
    """JSONify all CherryPy error responses (created by raising the
    cherrypy.HTTPError exception)
    """

    cherrypy.response.headers['Content-Type'] = 'application/json'
    response_body = json.dumps(
        {
            'error': {
                'http_status': status,
                'message': message,
            }
        }
    )

    cherrypy.response.status = status

    return response_body


def jsonify_404(status, message, traceback, version):
    # pylint: disable=unused-argument
    """Specialized renderer for 404 Not Found that logs context, then renders JSON.
    """
    try:
        req = cherrypy.request
        method = getattr(req, 'method', '')
        path = getattr(req, 'path_info', '') or '/'
        remote_ip = getattr(getattr(req, 'remote', None), 'ip', '') or '?'
        logger.error("404 Not Found: %s %s from %s", method, path, remote_ip)
    except Exception:
        pass
    return jsonify_error(status, message, traceback, version)
