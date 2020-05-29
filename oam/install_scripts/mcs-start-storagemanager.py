#!/usr/bin/env python3

import configparser
import sys

config = configparser.ConfigParser()
config.read('/etc/columnstore/storagemanager.cnf')

storage = config['ObjectStorage']['service']
region = config['S3']['region']
bucket = config['S3']['bucket']

if storage.lower() == 's3' and not region.lower() == 'some_region' and not bucket.lower == 'some_bucket':
    sys.exit(0)
sys.exit(1)
