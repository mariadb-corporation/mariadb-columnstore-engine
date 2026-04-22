"""Burza report presenter — tree-based HTML report viewer with comparison support."""

from __future__ import annotations

import json

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from babylon.providers.base import Presenter, PresenterCapabilities
from babylon.repositories.report_archive import ReportArchiveRepository
from babylon.repositories.report_comparison import ReportComparisonRepository
from babylon.services import archive_cache

log = structlog.get_logger()


class BurzaReportPresenter(Presenter):
    """Full-featured presenter for burza-generated reports.

    Capabilities:
    - report_viewer: browse HTML reports via navigation tree + file serving
    - comparison: create side-by-side comparisons via ``burza cmp-reports``
    """

    plugin_kind = "burza_reports"

    def __init__(self, session: AsyncSession) -> None:
        self._archive_repo = ReportArchiveRepository(session)
        self._comparison_repo = ReportComparisonRepository(session)

    def capabilities(self) -> PresenterCapabilities:
        return PresenterCapabilities(report_viewer=True, comparison=True)

    async def get_navigation(self, run_id: int) -> dict | None:
        archive = await self._archive_repo.get_by_run_id(run_id)
        if archive is None:
            return None

        # Return cached navigation tree if available
        if archive.navigation_tree_json:
            return json.loads(archive.navigation_tree_json)

        # Otherwise read it from the unpacked archive
        if not archive.s3_key:
            return None

        unpacked = archive_cache.ensure_unpacked(
            "archive",
            archive.report_archive_id,
            archive.s3_key,
        )
        nav_tree = archive_cache.find_navigation_tree(unpacked, archive.test_suite)
        if nav_tree is not None:
            # Cache for next time
            archive.navigation_tree_json = json.dumps(nav_tree)
            await self._archive_repo.save(archive)
        return nav_tree

    async def get_file(self, run_id: int, path: str) -> tuple[bytes, str] | None:
        archive = await self._archive_repo.get_by_run_id(run_id)
        if archive is None or not archive.s3_key:
            return None

        unpacked = archive_cache.ensure_unpacked(
            "archive",
            archive.report_archive_id,
            archive.s3_key,
        )
        file_path = unpacked / path
        if not file_path.exists() or file_path.is_dir():
            return None

        content = file_path.read_bytes()
        mime = archive_cache.mime_for_path(file_path)
        return content, mime

    async def create_comparison(
        self,
        reference_run_id: int,
        candidate_run_id: int,
        requested_by: str = "",
    ) -> int:
        ref = await self._archive_repo.get_by_run_id(reference_run_id)
        cand = await self._archive_repo.get_by_run_id(candidate_run_id)
        if ref is None or cand is None:
            raise ValueError("Both runs must have report archives")

        comparison = await self._comparison_repo.create(
            reference_archive_id=ref.report_archive_id,
            candidate_archive_id=cand.report_archive_id,
            requested_by=requested_by,
        )

        # Fire Celery task (lazy import to avoid circular dependency)
        from babylon.worker.tasks import compare_reports

        compare_reports.delay(comparison.comparison_id)
        return comparison.comparison_id
