import logging
import threading
from functools import total_ordering
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

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


@total_ordering
class StatefulVersionModel(BaseModel):
    """
    Version info inspired by Raft consensus algorithm.

    Provides two layers of ordering for distributed state updates:

    1. term = "leader epoch" or "fencing token"
       - Incremented each time a new leader is elected.
       - Ensures updates from old leaders are ignored.
       - Represents the leadership period the update belongs to.
    2. seq = "sequence number" within a term
       - Monotonically increases for every change made by the leader.
       - Ensures updates from the same leader are applied in order.
       - Represents the change number during the leader's term.
    """

    term: int = Field(ge=0)
    seq: int = Field(ge=0)

    model_config = ConfigDict(frozen=True)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StatefulVersionModel):
            return NotImplemented
        return (self.term, self.seq) == (other.term, other.seq)

    def __lt__(self, other: "StatefulVersionModel") -> bool:
        if not isinstance(other, StatefulVersionModel):
            return NotImplemented
        return (self.term, self.seq) < (other.term, other.seq)

    def next_seq(self) -> "StatefulVersionModel":
        """Return new version with incremented seq."""
        return StatefulVersionModel(term=self.term, seq=self.seq + 1)

    def next_term(self) -> "StatefulVersionModel":
        """Return new version with incremented term and reset seq."""
        return StatefulVersionModel(term=self.term + 1, seq=0)


class StatefulFlagsModel(BaseModel):
    """Flags for stateful config."""

    shared_storage_on: bool = False

    model_config = ConfigDict(frozen=True)


class StatefulConfigModel(BaseModel):
    """Stateful config model with version and flags."""

    version: StatefulVersionModel
    flags: StatefulFlagsModel


class AppStatefulConfig:
    """
    Stateful config shared by cluster nodes in memory.

    Uses a versioned config with thread-safe updates to avoid stale writes.
    Flags are stored as key-value pairs in a dictionary.
    # TODO: Change version.term after primary changes.
    """

    _lock = threading.RLock()
    _config: StatefulConfigModel = StatefulConfigModel(
        version=StatefulVersionModel(term=0, seq=0),
        flags=StatefulFlagsModel(shared_storage_on=False)
    )

    @classmethod
    def get_config_copy(cls) -> StatefulConfigModel:
        """Get the current config atomically.

        :return: Current config with flags and version.
        """
        with cls._lock:
            return cls._config.model_copy(deep=True)

    @classmethod
    def to_dict(cls) -> dict[str, Any]:
        """
        Get the current config flags and version atomically.

        :return: Dictionary with all flags and 'version' key included.
        """
        with cls._lock:
            return cls._config.model_dump(mode='json')

    @classmethod
    def apply_update(cls, new_config: StatefulConfigModel) -> bool:
        """
        Apply updates to config flags if the version is newer.

        Only updates flags present in new_flags. The entire update is applied
        atomically and only if the version is newer than the current version.

        :param new_config: New config with updated flags and version.
        :return: True if update was applied; False if update was stale or version missing.
        """

        with cls._lock:
            if new_config.version <= cls._config.version:
                return False  # stale update
            cls._config = new_config
            return True


    @classmethod
    def is_shared_storage(cls) -> bool:
        """Check if shared storage is enabled.

        :return: True if shared storage is enabled, False otherwise.
        """
        with cls._lock:
            return cls._config.flags.shared_storage_on
