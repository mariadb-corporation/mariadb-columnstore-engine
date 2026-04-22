"""Unit tests for concrete provider implementations.

Tests pure logic (settings building, output parsing, CSV parsing, workspace setup)
without requiring external services.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import pytest

from babylon.providers.base import ClusterInfo

# ---------------------------------------------------------------------------
# TerraformAnsibleHostProvider
# ---------------------------------------------------------------------------


class TestTfAnsibleWorkspace:
    """Test workspace preparation and tfvars writing."""

    def _make_repo(self, tmp_path: Path) -> Path:
        """Create a minimal fake cs-ansible-aws repo structure."""
        repo = tmp_path / "cs-ansible-aws"
        repo.mkdir()
        (repo / "main.tf").write_text("# fake", encoding="utf-8")
        (repo / "variables.tf").write_text("# fake", encoding="utf-8")
        (repo / "outputs.tf").write_text("# fake", encoding="utf-8")
        (repo / "provision.yml").write_text("# fake", encoding="utf-8")
        includes = repo / "includes"
        includes.mkdir()
        (includes / "cluster.yml").write_text("# fake", encoding="utf-8")
        tf_inc = repo / "terraform_includes"
        tf_inc.mkdir()
        (tf_inc / "inventory.tmpl").write_text("# fake", encoding="utf-8")
        inv = repo / "inventory" / "group_vars" / "distro"
        inv.mkdir(parents=True)
        (inv / "ubuntu22.yml").write_text("pkg: apt", encoding="utf-8")
        return repo

    def test_prepare_workspace_creates_symlinks(self, tmp_path: Path) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        repo = self._make_repo(tmp_path)
        ws_root = tmp_path / "workspaces"
        provider = TerraformAnsibleHostProvider({"repo_dir": str(repo), "workspaces_dir": str(ws_root)})

        ws = provider._prepare_workspace("test-cluster-1")

        assert ws.exists()
        assert (ws / "main.tf").is_symlink()
        assert (ws / "provision.yml").is_symlink()
        assert (ws / "includes").is_symlink()
        assert (ws / "terraform_includes").is_symlink()
        # inventory structure created locally (not symlinked)
        assert (ws / "inventory").is_dir()
        assert not (ws / "inventory").is_symlink()
        assert (ws / "inventory" / "group_vars").is_dir()
        # distro dir is symlinked from source
        assert (ws / "inventory" / "group_vars" / "distro").is_symlink()

    def test_prepare_workspace_idempotent(self, tmp_path: Path) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        repo = self._make_repo(tmp_path)
        ws_root = tmp_path / "workspaces"
        provider = TerraformAnsibleHostProvider({"repo_dir": str(repo), "workspaces_dir": str(ws_root)})

        ws1 = provider._prepare_workspace("ws")
        ws2 = provider._prepare_workspace("ws")
        assert ws1 == ws2
        assert (ws1 / "main.tf").is_symlink()

    def test_write_tfvars(self, tmp_path: Path) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        ws = tmp_path / "ws"
        ws.mkdir()

        variables: dict[str, Any] = {
            "use_s3": True,
            "num_columnstore_nodes": 3,
            "mariadb_version": "11.4",
            "admin_pass": "secret123",
        }
        TerraformAnsibleHostProvider._write_tfvars(ws, variables)

        tfvars = (ws / "terraform.tfvars").read_text(encoding="utf-8")
        assert 'admin_pass = "secret123"' in tfvars
        assert "num_columnstore_nodes = 3" in tfvars
        assert "use_s3 = true" in tfvars
        assert 'mariadb_version = "11.4"' in tfvars

        # JSON sidecar also written
        json_data = json.loads((ws / ".babylon_tfvars.json").read_text(encoding="utf-8"))
        assert json_data["num_columnstore_nodes"] == 3

    def test_outputs_to_cluster_info(self) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        outputs: dict[str, Any] = {
            "columnstore_nodes": [
                {"name": "mcs1", "public_dns": "mcs1.example.com", "private_ip": "10.0.0.1", "id": "i-1"},
                {"name": "mcs2", "public_dns": "mcs2.example.com", "private_ip": "10.0.0.2", "id": "i-2"},
            ],
            "maxscale_nodes": [
                {"name": "mx1", "public_dns": "mx1.example.com", "private_ip": "10.0.1.1", "id": "i-3"},
            ],
            "ssh_key_file": "/tmp/key.pem",
        }
        variables = {
            "admin_user": "admin",
            "admin_pass": "pass123",
            "cmapi_key": "test-key",
            "deployment_prefix": "test-pfx",
        }

        info = TerraformAnsibleHostProvider._outputs_to_cluster_info(
            "ws-1", Path("/tmp/ws-1"), outputs, variables,
        )

        assert isinstance(info, ClusterInfo)
        assert len(info.hosts) == 3
        cs_hosts = [h for h in info.hosts if h.role == "columnstore"]
        mx_hosts = [h for h in info.hosts if h.role == "maxscale_cs"]
        assert len(cs_hosts) == 2
        assert len(mx_hosts) == 1
        # Primary = MaxScale when available
        assert info.primary_host == "mx1.example.com"
        assert info.primary_port == 3306
        assert info.sql_user == "admin"
        assert info.sql_password == "pass123"
        assert info.ssh_key_path == "/tmp/key.pem"
        assert info.extra["cmapi_key"] == "test-key"

    def test_outputs_no_maxscale_primary_falls_back_to_cs(self) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        outputs: dict[str, Any] = {
            "columnstore_nodes": [
                {"name": "mcs1", "public_dns": "mcs1.example.com", "private_ip": "10.0.0.1", "id": "i-1"},
            ],
            "maxscale_nodes": [],
        }
        info = TerraformAnsibleHostProvider._outputs_to_cluster_info(
            "ws-1", Path("/tmp/ws-1"), outputs, {},
        )
        assert info.primary_host == "mcs1.example.com"

    def test_constructor_raises_on_missing_repo(self, tmp_path: Path) -> None:
        from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

        with pytest.raises(ValueError, match="repo not found"):
            TerraformAnsibleHostProvider({"repo_dir": "/nonexistent/path", "workspaces_dir": str(tmp_path)})


# ---------------------------------------------------------------------------
# MCSBurzaRunner
# ---------------------------------------------------------------------------


class TestMCSBurzaRunner:
    """Test settings building and CSV result parsing."""

    def test_build_settings_defaults(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        settings = MCSBurzaRunner._build_settings({})
        assert settings["TEST_SUITE"] == "tpc_h"
        assert settings["SCALE_FACTOR"] == "100"
        assert settings["RUNS_NUM"] == "7"

    def test_build_settings_overrides(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        params = {
            "test_suite": "tpc_ds",
            "scale_factor": "10",
            "test_names": "q1,q2,q3",
            "mode": "multi",  # not a burza setting, ignored by mapping
        }
        settings = MCSBurzaRunner._build_settings(params)
        assert settings["TEST_SUITE"] == "tpc_ds"
        assert settings["SCALE_FACTOR"] == "10"
        assert settings["TEST_NAMES"] == "q1,q2,q3"

    def test_build_settings_passthrough_uppercase(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        params = {"SENTRY_DSN": "https://example.com", "CUSTOM_FLAG": "true"}
        settings = MCSBurzaRunner._build_settings(params)
        assert settings["SENTRY_DSN"] == "https://example.com"
        assert settings["CUSTOM_FLAG"] == "true"

    def test_parse_run_duration_csv(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        csv_content = textwrap.dedent("""\
            case_name,duration_secs,test_run_id
            q6,1.234,0
            q6,1.100,1
            q6,1.500,2
            q11,2.000,0
            q11,2.200,1
        """)

        metrics = MCSBurzaRunner._parse_run_duration_csv(csv_content)

        assert metrics["q6_median_secs"] == 1.234  # middle of [1.1, 1.234, 1.5]
        assert metrics["q6_min_secs"] == 1.1
        assert metrics["q6_runs"] == 3
        assert metrics["q11_median_secs"] == 2.1  # avg of [2.0, 2.2]
        assert metrics["q11_min_secs"] == 2.0
        assert metrics["q11_runs"] == 2

    def test_parse_run_duration_csv_empty(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        metrics = MCSBurzaRunner._parse_run_duration_csv("")
        assert metrics == {}

    def test_parse_run_duration_csv_bad_data(self) -> None:
        from babylon_columnstore.runners.burza import MCSBurzaRunner

        csv_content = textwrap.dedent("""\
            case_name,duration_secs
            q6,not_a_number
            q6,1.5
        """)
        metrics = MCSBurzaRunner._parse_run_duration_csv(csv_content)
        assert metrics["q6_runs"] == 1
        assert metrics["q6_median_secs"] == 1.5


# ---------------------------------------------------------------------------
# TiamatBurzaRunner — moved to babylon-tiamat plugin in phase 2.1; tests
# live in that repo under babylon-plugin/tests/.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# AnsibleColumnStoreDeployer
# ---------------------------------------------------------------------------


class TestAnsibleColumnStoreDeployer:
    def test_deploy_skip_no_hosts(self) -> None:
        """deploy() with empty hosts should not raise, just skip."""
        import asyncio

        from babylon_columnstore.deployers.ansible import AnsibleColumnStoreDeployer

        deployer = AnsibleColumnStoreDeployer()
        info = ClusterInfo(
            hosts=[], primary_host="", primary_port=3306,
            sql_user="admin", sql_password="pass",
            ssh_key_path="/tmp/key", workspace="ws",
        )
        # Should not raise
        asyncio.get_event_loop().run_until_complete(
            deployer.deploy(info, "11.4", {}),
        )

    def test_upgrade_raises_without_workspace(self) -> None:
        import asyncio

        from babylon_columnstore.deployers.ansible import AnsibleColumnStoreDeployer

        deployer = AnsibleColumnStoreDeployer()
        info = ClusterInfo(
            hosts=[], primary_host="", primary_port=3306,
            sql_user="admin", sql_password="pass",
            ssh_key_path="/tmp/key", workspace="ws",
            extra={},  # no workspace_dir
        )
        with pytest.raises(RuntimeError, match="workspace_dir missing"):
            asyncio.get_event_loop().run_until_complete(
                deployer.upgrade(info, "11.6", {}),
            )
