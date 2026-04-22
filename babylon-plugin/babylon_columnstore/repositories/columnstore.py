"""ColumnStore engine source repository provider."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from babylon.providers.repositories.git_base import GitRepositoryProvider


class ColumnStoreRepositoryProvider(GitRepositoryProvider):
    kind: ClassVar[str] = "columnstore"
    repository_slug: ClassVar[str] = "mariadb-corporation/mariadb-columnstore-engine"
    local_clone_path: ClassVar[Path] = Path("/var/src/columnstore-engine")
    default_branches: ClassVar[tuple[str, ...]] = ("develop", "stable-23.10")

