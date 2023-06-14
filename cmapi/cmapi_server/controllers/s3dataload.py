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
    CMAPI_PYTHON_BIN, CMAPI_PYTHON_BINARY_DEPS_PATH, CMAPI_PYTHON_DEPS_PATH
)

from cmapi_server.controllers.endpoints import raise_422_error


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

        module_logger.debug(f'LOAD S3 Data')
        request = cherrypy.request
        request_body = request.json

        bucket = getKey('bucket', request_body)

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
        region = getKey('region', request_body, required=storage=='aws')
        database = getKey('database', request_body)
        terminated_by = getKey('terminated_by', request_body, skip_check=True)
        enclosed_by = getKey(
            'enclosed_by', request_body, skip_check=True, required=False
        )
        escaped_by = getKey(
            'escaped_by', request_body, skip_check=True, required=False
        )

        if storage == 'aws':
            download_proc = prepare_aws(bucket, filename, secret, key, region)
        elif storage == 'gs':
            temporary_config = os.path.join(
                tempfile.gettempdir(), '.boto.' + str(uuid.uuid4())
            )

            download_proc = prepare_google_storage(
                bucket, filename, secret, key, temporary_config
            )
        else:
            response_error('Unknown storage detected. Internal error')

        cpimport_command_line = [
            'cpimport', database, table, '-s', terminated_by
        ]
        if escaped_by:
            cpimport_command_line += ['-C', escaped_by]
        if enclosed_by:
            cpimport_command_line += ['-E', enclosed_by]

        module_logger.debug(
            f'cpimport command line: {" ".join(cpimport_command_line)}'
        )

        cpimport_proc = Popen(
            cpimport_command_line, shell=False, stdin=download_proc.stdout,
            stdout=PIPE, stderr=PIPE, encoding='utf-8'
        )

        selector = selectors.DefaultSelector()
        for stream in [
            download_proc.stderr, cpimport_proc.stderr, cpimport_proc.stdout
        ]:
            os.set_blocking(stream.fileno(), False)

        selector.register(
            download_proc.stderr, selectors.EVENT_READ, data='downloader_error'
        )
        selector.register(
            cpimport_proc.stderr, selectors.EVENT_READ, data='cpimport_error'
        )
        selector.register(
            cpimport_proc.stdout, selectors.EVENT_READ, data='cpimport_output'
        )

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
        if storage == 'gs' and os.path.exists(temporary_config):
            os.remove(temporary_config)

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
