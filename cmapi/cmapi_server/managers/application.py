import logging
from typing import Optional, Tuple, Dict

from cmapi_server.constants import VERSION_PATH


class AppManager:
    started: bool = False
    version: Optional[str] = None
    git_revision: Optional[str] = None

    @classmethod
    def get_version(cls) -> str:
        if cls.version:
            return cls.version
        version, revision = cls._read_version_file()
        cls.version = version
        cls.git_revision = revision
        return cls.version

    @classmethod
    def get_git_revision(cls) -> Optional[str]:
        if cls.git_revision is not None:
            return cls.git_revision
        _, revision = cls._read_version_file()
        cls.git_revision = revision
        return cls.git_revision

    @classmethod
    def _read_version_file(cls) -> Tuple[str, Optional[str]]:
        """Read structured values from VERSION file.

        Returns tuple: (semantic_version, git_revision or None)
        """
        values: Dict[str, str] = {}
        try:
            with open(VERSION_PATH, encoding='utf-8') as version_file:
                for line in version_file.read().splitlines():
                    if not line or '=' not in line:
                        continue
                    key, val = line.strip().split('=', 1)
                    values[key.strip()] = val.strip()
        except Exception:
            logging.exception("Failed to read VERSION file")
            return 'Undefined', None

        # Release (build) part is optional
        release = values.get('CMAPI_VERSION_RELEASE')
        revision = values.get('CMAPI_GIT_REVISION')

        required_keys = (
            'CMAPI_VERSION_MAJOR',
            'CMAPI_VERSION_MINOR',
            'CMAPI_VERSION_PATCH',
        )
        if not all(k in values and values[k] for k in required_keys):
            logging.error("Couldn't detect version from VERSION file!")
            return 'Undefined', revision

        version = '.'.join([
            values['CMAPI_VERSION_MAJOR'],
            values['CMAPI_VERSION_MINOR'],
            values['CMAPI_VERSION_PATCH'],
        ])
        if release:
            version = f"{version}.{release}"
        return version, revision
