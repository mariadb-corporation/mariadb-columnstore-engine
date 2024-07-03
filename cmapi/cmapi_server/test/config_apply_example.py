import unittest
import requests
import configparser
from pathlib import Path
from datetime import datetime

from cmapi_server.controllers.dispatcher import _version

config_filename = './cmapi_server/cmapi_server.conf'
 
url = f"https://localhost:8640/cmapi/{_version}/node/config"
begin_url = f"https://localhost:8640/cmapi/{_version}/node/begin"
config_path = './cmapi_server/test/Columnstore_apply_config.xml'
 
# create tmp dir
tmp_prefix = '/tmp/mcs_config_test'
tmp_path = Path(tmp_prefix)
tmp_path.mkdir(parents = True, exist_ok = True)
copyfile(config_path_old, tmp_prefix + '/Columnstore.xml')


def get_current_key():
    app_config = configparser.ConfigParser()
    try:
        with open(config_filename, 'r') as _config_file:
            app_config.read_file(_config_file)
    except FileNotFoundError:
        return ''
    if 'Authentication' not in app_config.sections():
        return ''
    return app_config['Authentication'].get('x-api-key', '')

headers = {'x-api-key': get_current_key()}
body = {'id': 42, 'timeout': 120}
r = requests.put(begin_url, verify=False, headers=headers, json=body)

config_file = Path(config_path)
config = config_file.read_text()

body = {
    'revision': 42,
    'manager': '1.1.1.1',
    'timeout': 0,
    'config': config,
}

#print(config)

#r = requests.put(url, verify=False, headers=headers, json=body)

