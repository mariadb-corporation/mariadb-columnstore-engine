#!/usr/bin/env python3

import configparser
import subprocess
import xml.etree.ElementTree as ET

sm_config = configparser.ConfigParser()
sm_config.read('/etc/columnstore/storagemanager.cnf')
cs_config = ET.parse('/etc/columnstore/Columnstore.xml')
config_root = cs_config.getroot()

storage = sm_config['ObjectStorage']['service']
region = sm_config['S3']['region']
bucket = sm_config['S3']['bucket']
loadbrm = '/usr/bin/load_brm /var/lib/columnstore/data1/systemFiles/dbrm/{0}'
brm_saves_current = ''

if storage.lower() == 's3' and not region.lower() == 'some_region' and not bucket.lower() == 'some_bucket':
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
            pass
else:
    pmCount = int(config_root.find('./SystemModuleConfig/ModuleCount3').text)
    brm = '/var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves_current'

    if pmCount > 1:
        # load multinode dbrm
        try:
            brm_saves_current = subprocess.check_output(['cat', brm])

            if not brm_saves_current:
                # local dbrm empty, need to pull from main node
                pass
        except subprocess.CalledProcessError as e:
            # will happen when brm file does not exist
            pass
    else:
        # load local dbrm
        try:
            brm_saves_current = subprocess.check_output(['cat', brm])
        except subprocess.CalledProcessError as e:
            # will happen when brm file does not exist
            pass

if brm_saves_current:
    cmd = loadbrm.format(brm_saves_current.decode('utf-8'))
    try:
        retcode = subprocess.call(cmd, shell=True)
        if retcode < 0:
            #print("Child was terminated by signal", -retcode, file=sys.stderr)
            pass

    except OSError as e:
        #print("Execution failed:", e, file=sys.stderr)
        pass
