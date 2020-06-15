#!/usr/bin/env python

import sys

if sys.version_info < (3,0):
    import ConfigParser
else:
    import configparser


if __name__ == '__main__':
    if sys.version_info < (3,0):
        config = ConfigParser.ConfigParser()
    else:
        config = configparser.ConfigParser()

    config.read('/etc/columnstore/storagemanager.cnf')

    if sys.version_info < (3,0):
        storage = config.get('ObjectStorage', 'service')
        if storage is None:
            storage = 'LocalStorage'
        bucket = config.get('S3', 'bucket')
        if bucket is None:
            bucket = 'some_bucket'
    else:
        storage = config.get('ObjectStorage', 'service', fallback='LocalStorage')
        bucket = config.get('S3', 'bucket', fallback='some_bucket')


    if storage.lower() == 's3' and not bucket.lower() == 'some_bucket':
        sys.exit(0)
    sys.exit(1)
