#!/usr/bin/env python3
import subprocess
import sys
import xml.etree.ElementTree as ET
import configparser

XML_CONFIG_PATH = '/etc/columnstore/Columnstore.xml'
SM_CONFIG_PATH = '/etc/columnstore/storagemanager.cnf'
REST_REQUEST_TO = 2


def get_version():
    return '0.4.0'


def get_port():
    return '8640'


if __name__ == '__main__':
    master_addr = ''
    pm_count = 0
    try:
        cs_config = ET.parse(XML_CONFIG_PATH)
        config_root = cs_config.getroot()
        master_addr = config_root.find('./DBRM_Controller/IPAddr').text
        pm_count = int(config_root.find('./SystemModuleConfig/ModuleCount3').text)
    except (FileNotFoundError, AttributeError, ValueError) as e:
        print("Exception had been raised. Continue anyway")
        print(str(e))

    storage = 'LocalStorage'
    sm_config = configparser.ConfigParser()
    files_read = len(sm_config.read(SM_CONFIG_PATH))
    if files_read == 1:
        storage = sm_config.get('ObjectStorage', 'service')

    default_addr = '127.0.0.1'
    savebrm = 'save_brm'
    is_primary = False

    # For multi-node with local storage or default installations
    if (storage.lower() != 's3' and master_addr != default_addr) or \
master_addr == default_addr:
        is_primary = True
        print('Multi-node with local-storage detected.')
    else:
        has_requests = False
        try:
            import requests
            requests.packages.urllib3.disable_warnings()
            has_requests = True
        except ImportError as e:
            print('requests Python module does not exist. \
    Please install CMAPI first.')
        if has_requests is True:
            try:
                print('Requesting for the primary node status.')
                api_version = get_version()
                api_port = get_port()
                url = "https://{}:{}/cmapi/{}/node/primary".format(default_addr, \
    api_port, api_version)
                resp = requests.get(url,
                                 verify=False,
                                 timeout=REST_REQUEST_TO)
                if (resp.status_code != 200):
                    print("Error sending GET /node/primary.")
                else:
                    is_primary = resp.json()['is_primary'] == 'True'
            except:
                print('Failed to request.')
                print(str(e))

    if is_primary is True:
        try:
            retcode = subprocess.call(savebrm, shell=True)
            if retcode < 0:
                print('{} exits with {}.'.format(savebrm, retcode))
                sys.exit(0)
        except OSError as e:
                print(str(e))
                sys.exit(0)

    sys.exit(0)
