import logging
import os
import re
import selectors
import tempfile
import uuid
from subprocess import PIPE, Popen, run, CalledProcessError

import cherrypy
import furl
from cmapi_server.constants import (
    CMAPI_PYTHON_BIN,
    CMAPI_PYTHON_BINARY_DEPS_PATH,
    CMAPI_PYTHON_DEPS_PATH,
    CMAPI_PORT,
    LONG_REQUEST_TIMEOUT,
)

from cmapi_server.controllers.api_clients import NodeControllerClient
from cmapi_server.controllers.endpoints import raise_422_error
from cmapi_server.helpers import get_active_nodes


module_logger = logging.getLogger('cmapi_server')


def response_error(text):
    raise_422_error(module_logger, 'load_s3data', text)


class S3DataLoadController:
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.validate_api_key()  # pylint: disable=no-member
    def load_s3data(self):
        """
        Handler for /cluster/load_s3data (POST, PUT)
        Invokes cpimport with passed params
        This is internal columnstore engine handler
        Not targeted for manual usage

        Waits for json dictionary params in request
        bucket - S3 bucket with table data
        table - table name to load data into
        filename - name of file in S3 with table data
        key - S3 secret key
        secret - S3 secret
        region - S3 region
        database - db name to load data into
        """

        def checkShellParamsAreOK(param, paramname):
            """Check shell params for dangerous symbols.

            As this params will be passed to shell, we should check,
            there is no shell injection
            AWS Access Key ID is 20 alpha-numeric characters
            like022QF06E7MXBSH9DHM02
            AWS Secret Access Key is 40 alpha-numeric-slash-plus characters
            like kWcrlUX5JEDGM/LtmEENI/aVmYvHNif5zB+d9+ct
            AWS buckets names are alpha-numeric-dot-underscore
            like log-delivery-march-2020.com
            AWS regions names, table names, file names are also not allowed
            for dangerous symbols so just raise error for injection dangerous
            symbols in params.
            """
            dangerous_symbols = ' #&|;\n\r`$'
            for symbol in dangerous_symbols:
                if symbol in param:
                    response_error(
                        f'S3 configuration parameters wrong: {paramname}'
                        f'cannot contain "{symbol}"'
                    )

        def getKey(keyname, request_body, skip_check=False, required=True):
            value = request_body.get(keyname, None)

            if not value and required:
                response_error(
                    f'Some S3 configuration parameters missing: {keyname} '
                    'not provided'
                )

            if not skip_check:
                checkShellParamsAreOK(value, keyname)

            return value

        def prepare_aws(bucket, filename, secret, key, region):
            """Prepare aws_cli popen object.

            Invoke aws_cli download, and return proc for further
            use with cpimport.

            :param bucket: bucket name
            :type bucket: str
            :param filename: filename in bucket
            :type filename: str
            :param secret: aws secret
            :type secret: str
            :param key: aws key
            :type key: str
            :param region: aws region
            :type region: str
            :return: popen aws_cli object
            :rtype: subprocess.Popen
            """
            my_env = os.environ.copy()
            my_env['AWS_ACCESS_KEY_ID'] = key
            my_env['AWS_SECRET_ACCESS_KEY'] = secret
            my_env['PYTHONPATH'] = CMAPI_PYTHON_DEPS_PATH

            aws_cli_binary = os.path.join(CMAPI_PYTHON_BINARY_DEPS_PATH, 'aws')
            s3_url = furl.furl(bucket).add(path=filename).url
            aws_command_line = [
                CMAPI_PYTHON_BIN, aws_cli_binary,
                "s3", "cp", "--source-region", region, s3_url, "-"
            ]
            module_logger.debug(
                f'AWS commandline: {" ".join(aws_command_line)}')
            try:
                aws_proc = Popen(
                    aws_command_line, env=my_env, stdout=PIPE,
                    stderr=PIPE, shell=False, encoding='utf-8'
                )
            except CalledProcessError as exc:
                response_error(exc.stderr.split('\n')[0])

            return aws_proc

        def prepare_google_storage(
            bucket, filename, secret, key, temporary_config
        ):
            """Prepare gsutil popen object.

            Invoke gsutil download, and return proc for further use
            with cpimport.

            :param bucket: bucket name
            :type bucket: str
            :param filename: filename in bucket
            :type filename: str
            :param secret: gsutil secret
            :type secret: str
            :param key: gsutil key
            :type key: str
            :param temporary_config: temp config for gsutil
            :type temporary_config: str
            :return: popen gsutil object
            :rtype: subprocess.Popen
            """
            project_id = 'project_id'
            gs_cli_binary = os.path.join(
                CMAPI_PYTHON_BINARY_DEPS_PATH, 'gsutil'
            )

            commandline = (
                f'/usr/bin/bash -c '
                f'\'echo -e "{key}\n{secret}\n{project_id}"\' | '
                f'{CMAPI_PYTHON_BIN} {gs_cli_binary} '
                f'config -a -o {temporary_config}'
            )

            module_logger.debug(
                f'gsutil config commadline: '
                f'{commandline.encode("unicode_escape").decode("utf-8")}'
            )

            my_env = os.environ.copy()
            my_env['PYTHONPATH'] = CMAPI_PYTHON_DEPS_PATH
            my_env['BOTO_CONFIG'] = temporary_config

            try:
                p = run(
                    commandline, capture_output=True,
                    shell=True, encoding='utf-8', check=True, env=my_env
                )
            except CalledProcessError as exc:
                response_error(exc.stderr.split('\n')[0])

            try:
                check_commandline = [
                    CMAPI_PYTHON_BIN, gs_cli_binary, 'version', '-l'
                ]
                p = run(
                    check_commandline, capture_output=True,
                    shell=False, encoding='utf-8', check=True, env=my_env
                )
                module_logger.debug(
                    f'gsutil config check commandline  : '
                    f'{" ".join(check_commandline)}'
                )
                module_logger.debug(f'gsutil config : {p.stdout}')

            except CalledProcessError as exc:
                response_error(exc.stderr.split('\n')[0])

            gs_url = furl.furl(bucket).add(path=filename).url
            gs_command_line = [
                CMAPI_PYTHON_BIN, gs_cli_binary, 'cat', gs_url
            ]
            module_logger.debug(
                f'gsutil cat commandline : {" ".join(gs_command_line)}'
            )

            try:
                gs_process = Popen(
                    gs_command_line, env=my_env, stdout=PIPE, stderr=PIPE,
                    shell=False, encoding='utf-8'
                )
            except CalledProcessError as exc:
                response_error(exc.stderr.split('\n')[0])

            return gs_process

        module_logger.debug('LOAD S3 Data')
        request = cherrypy.request
        request_body = request.json

        bucket = getKey('bucket', request_body)

        storage: str = ''
        if bucket.startswith(r's3://'):
            storage = 'aws'
        elif bucket.startswith(r'gs://'):
            storage = 'gs'
        else:
            error = (
                'Incorrect bucket. Should start with s3://for AWS S3 or '
                'gs:// for Google Storage'
            )
            response_error(error)

        table = getKey('table', request_body)
        filename = getKey('filename', request_body)
        key = getKey('key', request_body)
        secret = getKey('secret', request_body)
        is_aws_storage = bool(storage=='aws')
        region = getKey('region', request_body, required=is_aws_storage)
        database = getKey('database', request_body)
        terminated_by = getKey('terminated_by', request_body, skip_check=True)
        enclosed_by = getKey(
            'enclosed_by', request_body, skip_check=True, required=False
        )
        escaped_by = getKey(
            'escaped_by', request_body, skip_check=True, required=False
        )
        mode = request_body.get('mode', 1)

        cpimport_command_line = [
            'cpimport', database, table, '-s', terminated_by
        ]
        if escaped_by:
            cpimport_command_line += ['-C', escaped_by]
        if enclosed_by:
            cpimport_command_line += ['-E', enclosed_by]
        try:
            mode_val = int(mode)
        except (ValueError, TypeError):
            response_error(f'Invalid mode value: {mode}')
        if mode_val not in (1, 2, 3):
            response_error(
                f'Invalid mode: {mode_val}. Must be 1, 2, or 3'
            )

        cpimport_command_line += ['-m', str(mode_val)]

        temporary_config = None
        download_proc = None
        temporary_load_file = None
        active_nodes = []

        if mode_val == 2:
            # Mode 2 requires a local file path (-f/-l) on every PM.
            # Tell each node in the cluster to download the file from S3
            # into the same local path.
            load_filename = 'cs_s3load_' + str(uuid.uuid4()) + '.dat'
            temporary_load_file = os.path.join(
                tempfile.gettempdir(), load_filename
            )

            active_nodes = get_active_nodes()
            if not active_nodes:
                response_error(
                    'Mode 2 requires an active cluster with nodes'
                )

            module_logger.info(
                f'Mode 2: distributing S3 file to {len(active_nodes)} '
                f'nodes at {temporary_load_file}'
            )

            for node in active_nodes:
                module_logger.debug(
                    f'Mode 2: requesting download on node {node}'
                )
                # we need to download possibly big files from s3, so longer timeout needed.
                client = NodeControllerClient(
                    request_timeout=LONG_REQUEST_TIMEOUT,
                    base_url=f'https://{node}:{CMAPI_PORT}'
                )
                try:
                    client.download_s3_file(
                        bucket=bucket,
                        filename=filename,
                        key=key,
                        secret=secret,
                        region=region,
                        storage=storage,
                        target_path=temporary_load_file
                    )
                except Exception as e:
                    response_error(
                        f'Failed to download file on node {node}: {e}'
                    )

            cpimport_command_line += [
                '-f', os.path.dirname(temporary_load_file),
                '-l', os.path.basename(temporary_load_file)
            ]
        else:
            # Modes 1/3: stream S3 data via download process stdout.
            if storage == 'aws':
                download_proc = prepare_aws(
                    bucket, filename, secret, key, region
                )
            elif storage == 'gs':
                temporary_config = os.path.join(
                    tempfile.gettempdir(), '.boto.' + str(uuid.uuid4())
                )
                download_proc = prepare_google_storage(
                    bucket, filename, secret, key, temporary_config
                )
            else:
                response_error('Unknown storage detected. Internal error')

        module_logger.debug(
            f'cpimport command line: {" ".join(cpimport_command_line)}'
        )

        if mode_val == 2:
            # Mode 2: file already on disk, no stdin piping needed.
            cpimport_proc = Popen(
                cpimport_command_line, shell=False, stdin=PIPE,
                stdout=PIPE, stderr=PIPE, encoding='utf-8'
            )
            cpimport_proc.stdin.close()

            cpimport_output, cpimport_error = cpimport_proc.communicate()
            downloader_error = ''
        else:
            # Mode 1/3: pipe S3 download stdout directly to cpimport stdin.
            cpimport_proc = Popen(
                cpimport_command_line, shell=False, stdin=download_proc.stdout,
                stdout=PIPE, stderr=PIPE, encoding='utf-8'
            )

            selector = selectors.DefaultSelector()
            for stream in [download_proc.stderr, cpimport_proc.stderr, cpimport_proc.stdout]:
                os.set_blocking(stream.fileno(), False)

            selector.register(download_proc.stderr, selectors.EVENT_READ, data='downloader_error')
            selector.register(cpimport_proc.stderr, selectors.EVENT_READ, data='cpimport_error')
            selector.register(cpimport_proc.stdout, selectors.EVENT_READ, data='cpimport_output')

            downloader_error = ''
            cpimport_error = ''
            cpimport_output = ''

            while True:
                events = selector.select()
                for key, mask in events:
                    name = key.data
                    line = key.fileobj.readline().rstrip()
                    if name == 'downloader_error' and line:
                        downloader_error += line + '\n'
                    if name == 'cpimport_error' and line:
                        cpimport_error += line + '\n'
                    if name == 'cpimport_output' and line:
                        cpimport_output += line + '\n'

                if downloader_error:
                    response_error(downloader_error)

                if cpimport_error:
                    response_error(cpimport_error)

                cpimport_status = cpimport_proc.poll()
                download_status = download_proc.poll()

                if cpimport_status is not None \
                  and download_status is not None:
                    break

        # clean after Prepare Google
        if temporary_config and os.path.exists(temporary_config):
            os.remove(temporary_config)

        # clean temp data file on all nodes for mode 2
        if temporary_load_file:
            if mode_val == 2:
                for node in active_nodes:
                    try:
                        client = NodeControllerClient(
                            request_timeout=REQUEST_TIMEOUT,
                            base_url=f'https://{node}:{CMAPI_PORT}'
                        )
                        client.delete_load_file(temporary_load_file)
                    except Exception as e:
                        module_logger.warning(
                            f'Failed to cleanup file on node {node}: {e}'
                        )
            elif os.path.exists(temporary_load_file):
                os.remove(temporary_load_file)

        if downloader_error:
            response_error(downloader_error)

        if cpimport_error:
            response_error(cpimport_error)

        module_logger.debug(f'LOAD S3 Data stdout: {cpimport_output}')

        pattern = '([0-9]+) rows processed and ([0-9]+) rows inserted'
        match = re.search(pattern, cpimport_output)

        if not match:
            return {
                'success': False,
                'inserted': 0,
                'processed': 0
            }

        return {
            'success': True,
            'inserted': match.group(2),
            'processed': match.group(1)
        }
