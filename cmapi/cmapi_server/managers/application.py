import logging
from typing import Optional

from cmapi_server.constants import VERSION_PATH


class AppManager:
    started: bool = False
    version: Optional[str] = None

    @classmethod
    def get_version(cls) -> str:
        """Get CMAPI version.

        :return: cmapi version
        :rtype: str
        """
        if cls.version:
            return cls.version
        with open(VERSION_PATH, encoding='utf-8') as version_file:
            version = '.'.join([
                i.strip().split('=')[1]
                for i in version_file.read().splitlines() if i
            ])
            if not version:
                logging.error('Couldn\'t detect version from VERSION file!')
                version = 'Undefined'
        cls.version = version
        return cls.version
