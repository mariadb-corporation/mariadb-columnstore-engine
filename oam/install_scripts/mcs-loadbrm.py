#!/usr/bin/env python3
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
import time
import configparser
import os

API_CONFIG_PATH = '/etc/columnstore/cmapi_server.conf'
BYPASS_SM_PATH = '/tmp/columnstore_tmp_files/rdwrscratch/BRM_saves'


def get_key():
    cmapi_config = configparser.ConfigParser()
    cmapi_config.read(API_CONFIG_PATH)
    if 'Authentication' not in cmapi_config.sections():
        return ''
    return cmapi_config['Authentication'].get('x-api-key', '')


def get_version():
    return '0.4.0'


def get_port():
    return '8640'


if __name__ == '__main__':
    # To avoid systemd in container environment
    use_systemd = True
    if len(sys.argv) > 1:
        use_systemd = not sys.argv[1] == 'no'
    sm_config = configparser.ConfigParser()

    sm_config.read('/etc/columnstore/storagemanager.cnf')
    cs_config = ET.parse('/etc/columnstore/Columnstore.xml')
    config_root = cs_config.getroot()

    storage = sm_config.get('ObjectStorage', 'service')
    if storage is None:
        storage = 'LocalStorage'
    bucket = sm_config.get('S3', 'bucket')
    if bucket is None:
        bucket = 'some_bucket'

    dbrmroot = config_root.find('./SystemConfig/DBRMRoot').text
    pmCount = int(config_root.find('./SystemModuleConfig/ModuleCount3').text)
    loadbrm = '/usr/bin/load_brm'

    brm_saves_current = ''

    if storage.lower() == 's3' and not bucket.lower() == 'some_bucket':
        # start SM using systemd
        if use_systemd is True:
            cmd = 'systemctl start mcs-storagemanager'
            retcode = subprocess.call(cmd, shell=True)
            if retcode < 0:
                print('Failed to start storagemanager. \
{} exits with {}.'.format(cmd, retcode))
                sys.exit(1)
            time.sleep(1)   # allow SM time to init

        brm = 'data1/systemFiles/dbrm/BRM_saves_current'
        config_root.find('./Installation/DBRootStorageType').text = "StorageManager"
        config_root.find('./StorageManager/Enabled').text = "Y"

        if config_root.find('./SystemConfig/DataFilePlugin') is None:
            config_root.find('./SystemConfig').append(ET.Element("DataFilePlugin"))

        config_root.find('./SystemConfig/DataFilePlugin').text = "libcloudio.so"
        
        cs_config.write('/etc/columnstore/Columnstore.xml.loadbrm')
        os.replace('/etc/columnstore/Columnstore.xml.loadbrm', '/etc/columnstore/Columnstore.xml')    # atomic replacement

    # Single-node on S3
    if storage.lower() == 's3' and not bucket.lower() == 'some_bucket' and pmCount == 1:
        try:
            brm_saves_current = subprocess.check_output(['smcat', brm])
        except subprocess.CalledProcessError as e:
            # will happen when brm file does not exist
            print('{} does not exist.'.format(brm), file=sys.stderr)
    else:
        brm = '{}_current'.format(dbrmroot)
        # Multi-node
        if pmCount > 1:
            try:
                import requests
                requests.packages.urllib3.disable_warnings()
            except ImportError as e:
                print('requests Python module does not exist. \
Please install CMAPI first.', file=sys.stderr)
                sys.exit(1)
            try:
                primary_address = config_root.find('./DBRM_Controller/IPAddr').text
                api_key = get_key()
                if len(api_key) == 0:
                    print('Failed to find API key in {}.'.format(API_CONFIG_PATH), \
file=sys.stderr)
                    sys.exit(1)
                headers = {'x-api-key': api_key}
                api_version = get_version()
                api_port = get_port()
                elems = ['em', 'journal', 'vbbm', 'vss']
                for e  in elems:
                    print("Pulling {} from the primary node.".format(e))
                    url = "https://{}:{}/cmapi/{}/node/meta/{}".format(primary_address, \
api_port, api_version, e)
                    r = requests.get(url, verify=False, headers=headers, timeout=30)
                    if (r.status_code != 200):
                        raise RuntimeError("Error requesting {} from the primary \
node.".format(e))

                    # To avoid SM storing BRM files
                    if storage.lower() == 's3' and bucket.lower() != 'some_bucket':
                        dbrmroot = BYPASS_SM_PATH

                        if not os.path.exists(dbrmroot):
                            os.makedirs(dbrmroot)

                    current_name = '{}_{}'.format(dbrmroot, e)

                    print ("Saving {} to {}".format(e, current_name))
                    path = Path(current_name)
                    path.write_bytes(r.content)
            except Exception as e:
                print(str(e))
                print('Failed to load BRM data from the primary \
node {}.'.format(primary_address), file=sys.stderr)
                sys.exit(1)

            brm_saves_current = b"BRM_saves\n"
        else:
            # load local dbrm
            try:
                brm_saves_current = subprocess.check_output(['cat', brm])
            except subprocess.CalledProcessError as e:
                # will happen when brm file does not exist
                print('{} does not exist.'.format(brm), file=sys.stderr)

    if brm_saves_current:
        cmd = '{} {}{}'.format(loadbrm, dbrmroot, \
brm_saves_current.decode("utf-8").replace("BRM_saves", ""))
        try:
            retcode = subprocess.call(cmd, shell=True)
            if retcode < 0:
                print('{} exits with {}.'.format(cmd, retcode))
                sys.exit(1)
        except OSError as e:
            sys.exit(1)
