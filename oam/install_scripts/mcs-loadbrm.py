#!/usr/bin/env python
from __future__ import absolute_import, division, print_function
import subprocess
import sys
import xml.etree.ElementTree as ET

if sys.version_info < (3,0):
    import ConfigParser
else:
    import configparser


if __name__ == '__main__':
    if sys.version_info < (3,0):
        sm_config = ConfigParser.ConfigParser()
    else:
        sm_config = configparser.ConfigParser()

    sm_config.read('/etc/columnstore/storagemanager.cnf')
    cs_config = ET.parse('/etc/columnstore/Columnstore.xml')
    config_root = cs_config.getroot()

    if sys.version_info < (3,0):
        storage = sm_config.get('ObjectStorage', 'service')
        if storage is None:
            storage = 'LocalStorage'
        bucket = sm_config.get('S3', 'bucket')
        if bucket is None:
            bucket = 'some_bucket'
    else:
        storage = sm_config.get('ObjectStorage', 'service', fallback='LocalStorage')
        bucket = sm_config.get('S3', 'bucket', fallback='some_bucket')

    dbrmroot = config_root.find('./SystemConfig/DBRMRoot').text
    loadbrm = '/usr/bin/load_brm'

    brm_saves_current = ''

    if storage.lower() == 's3' and not bucket.lower() == 'some_bucket':
        # load s3
        brm = 'data1/systemFiles/dbrm/BRM_saves_current'
        config_root.find('./Installation/DBRootStorageType').text = "StorageManager"
        config_root.find('./StorageManager/Enabled').text = "Y"

        if config_root.find('./SystemConfig/DataFilePlugin') is None:
            config_root.find('./SystemConfig').append(ET.Element("DataFilePlugin"))

        config_root.find('./SystemConfig/DataFilePlugin').text = "libcloudio.so"
        cs_config.write('/etc/columnstore/Columnstore.xml')

        try:
            brm_saves_current = subprocess.check_output(['smcat', brm])
        except subprocess.CalledProcessError as e:
                # will happen when brm file does not exist
                print('{} does not exist.'.format(brm), file=sys.stderr)
    else:
        pmCount = int(config_root.find('./SystemModuleConfig/ModuleCount3').text)
        brm = '{}_current'.format(dbrmroot)

        if pmCount > 1:
            # load multinode dbrm
            try:
                brm_saves_current = subprocess.check_output(['cat', brm])

                if not brm_saves_current:
                    # local dbrm empty, need to pull from main node
                    pass
            except subprocess.CalledProcessError as e:
                # will happen when brm file does not exist
                print('{} does not exist.'.format(brm), file=sys.stderr)
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
                pass

        except OSError as e:
            pass
