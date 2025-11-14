import re
from dataclasses import dataclass
from functools import total_ordering


@total_ordering
@dataclass
class ComparableVersion:
    version: str

    def __post_init__(self):
        self.version_nums = self._split_version(self.version)

    def _split_version(self, version: str) -> list[int]:
        # Drop epoch if present (debian style)
        if ':' in version:
            version = version.split(':', 1)[1]

        # I'm not sure if ~ could be the first, but seems to be possible in some debians
        # Trim at first suffix marker (+, ~) if any
        for marker in ('+', '~'):
            if marker in version:
                version = version.split(marker, 1)[0]
                break
        # Remove leading .0s and split by . _ -
        parts = re.split(r'[._-]', version)
        return [int(p.lstrip('0') or '0') for p in parts]

    def _normalized(self, other) -> tuple[list, list]:
        """Return two zero-padded version lists of equal length."""
        v1 = self.version_nums.copy()
        v2 = other.version_nums.copy()
        max_len = max(len(v1), len(v2))
        v1 += [0] * (max_len - len(v1))
        v2 += [0] * (max_len - len(v2))
        return v1, v2

    def __eq__(self, other):
        v1, v2 = self._normalized(other)
        return v1 == v2

    def __lt__(self, other):
        v1, v2 = self._normalized(other)
        return v1 < v2
