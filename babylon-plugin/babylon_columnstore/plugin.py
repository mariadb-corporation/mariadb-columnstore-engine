"""ColumnStore plugin entry point.

``register(registry)`` is called by babylon's plugin loader after
importing this module. Registers every CS-specific provider class
against the babylon provider ABCs.
"""

from __future__ import annotations

from babylon.providers.registry import Registry

from babylon_columnstore.deployers.ansible import AnsibleColumnStoreDeployer
from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider
from babylon_columnstore.presenters.burza_reports import BurzaReportPresenter
from babylon_columnstore.repositories.columnstore import ColumnStoreRepositoryProvider
from babylon_columnstore.runners.burza import MCSBurzaRunner

PROVIDERS: list[tuple[str, str, type]] = [
    ("deployer",            "ansible_columnstore", AnsibleColumnStoreDeployer),
    ("host_provider",       "tf_ansible",          TerraformAnsibleHostProvider),
    ("presenter",           "burza_reports",       BurzaReportPresenter),
    ("repository_provider", "columnstore",         ColumnStoreRepositoryProvider),
    ("benchmark_runner",    "mcs_burza",           MCSBurzaRunner),
]


def register(registry: Registry) -> None:
    for role, kind, cls in PROVIDERS:
        registry.register(role, kind, cls)
