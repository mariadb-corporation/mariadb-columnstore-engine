import json

import cherrypy

from cmapi_server.controllers.endpoints import (
    StatusController, ConfigController, BeginController, CommitController,
    RollbackController, StartController, ShutdownController,
    ExtentMapController, ClusterController, ApiKeyController,
    LoggingConfigController, AppController, NodeProcessController
)

from cmapi_server.controllers.s3dataload import S3DataLoadController

_version = '0.4.0'
dispatcher = cherrypy.dispatch.RoutesDispatcher()


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


# /_version/node/is_process_running/ (PUT)
dispatcher.connect(
    name = 'is_process_running',
    route = f'/cmapi/{_version}/node/is_process_running',
    action = 'get_process_running',
    controller = NodeProcessController(),
    conditions = {'method': ['GET']}
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
