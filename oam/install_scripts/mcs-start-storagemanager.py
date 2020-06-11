#!/usr/bin/env python3

import configparser
import sys

config = configparser.ConfigParser()
config.read('/etc/columnstore/storagemanager.cnf')

storage = config.get('ObjectStorage', 'service', fallback='LocalStorage')
region = config.get('S3', 'region', fallback='some_region')
bucket = config.get('S3', 'bucket', fallback='some_bucket')

if storage.lower() == 's3' and not region.lower() == 'some_region' and not bucket.lower() == 'some_bucket':
    sys.exit(0)
sys.exit(1)
