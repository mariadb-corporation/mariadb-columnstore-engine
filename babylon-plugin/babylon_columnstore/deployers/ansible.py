"""ColumnStore deployer — software is installed during host allocation.

Deploy is a health-check noop; upgrade re-runs ansible with a new version.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import structlog

from babylon.providers.base import ClusterInfo, Deployer

log = structlog.get_logger()


class AnsibleColumnStoreDeployer(Deployer):
    """Deploy/upgrade ColumnStore via ansible-playbook.

    On first deploy (during allocate), ansible already installs everything.
    This deployer's ``deploy()`` just verifies CMAPI health.
    ``upgrade()`` re-runs the ansible provision playbook with a new
    ``mariadb_version`` variable.
    """

    plugin_kind = "ansible_columnstore"

    def __init__(self, adapter: Any = None, config: dict[str, Any] | None = None) -> None:
        self._config = config or {}

    async def deploy(self, cluster_info: ClusterInfo, version: str, params: dict[str, Any]) -> None:
        cmapi_key = cluster_info.extra.get("cmapi_key", "")
        primary = cluster_info.hosts[0] if cluster_info.hosts else None
        if not primary or not primary.public_ip:
            log.info("deployer.columnstore.deploy.skip", reason="no primary host")
            return

        ok = await self._check_cmapi_health(primary.public_ip, cmapi_key)
        if not ok:
            raise RuntimeError("CMAPI health check failed on primary node")
        log.info("deployer.columnstore.deployed", version=version)

    async def upgrade(self, cluster_info: ClusterInfo, version: str, params: dict[str, Any]) -> None:
        ws_dir = cluster_info.extra.get("workspace_dir")
        if not ws_dir:
            raise RuntimeError("workspace_dir missing from cluster_info.extra")

        ws = Path(ws_dir)
        tfvars_json = ws / ".babylon_tfvars.json"
        if not tfvars_json.is_file():
            raise RuntimeError(f"tfvars json not found: {tfvars_json}")

        variables = json.loads(tfvars_json.read_text(encoding="utf-8"))
        variables["mariadb_version"] = version

        # Update tfvars
        tfvars_json.write_text(json.dumps(variables, indent=2), encoding="utf-8")

        # Re-run ansible with updated version
        all_yml = ws / "inventory" / "group_vars" / "all.yml"
        ansible_cfg = ws / "ansible.cfg"
        env = {
            **os.environ,
            "ANSIBLE_CONFIG": str(ansible_cfg),
            "ANSIBLE_FORCE_COLOR": "0",
        }
        cmd = ["ansible-playbook", "provision.yml"]
        if all_yml.is_file():
            cmd.extend(["--extra-vars", f"@{all_yml}"])
        cmd.extend(["--extra-vars", f'{{"mariadb_version": "{version}"}}'])

        log.info("deployer.columnstore.upgrade", version=version, cwd=str(ws))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=ws,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode().strip() or stdout.decode().strip()
            raise RuntimeError(f"ansible upgrade failed: {err[:2000]}")
        log.info("deployer.columnstore.upgraded", version=version)

    @staticmethod
    async def _check_cmapi_health(host: str, api_key: str) -> bool:
        """Verify CMAPI cluster status endpoint responds."""
        cmd = [
            "curl",
            "-sk",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            f"x-api-key: {api_key}",
            f"https://{host}:8640/cmapi/0.4.0/cluster/status",
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        code = stdout.decode().strip()
        return code == "200"

