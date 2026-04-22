"""Terraform + Ansible host provider for ColumnStore clusters.

Wraps the columnstore-ansible-aws repo:
  1. Creates an isolated workspace directory
  2. Symlinks terraform/.tf and ansible playbooks from the source repo
  3. Writes per-workspace terraform.tfvars
  4. terraform apply  → creates EC2 instances, generates inventory + group_vars
  5. ansible-playbook → installs and configures ColumnStore + MaxScale
  6. Parses terraform outputs → ClusterInfo
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any

import structlog

from babylon.providers.base import ClusterInfo, HostInfo, HostProvider

log = structlog.get_logger()

# Items that are safe to symlink from the cs-ansible-aws repo
# (terraform reads them, ansible reads them — nobody writes to them)
_SYMLINK_ITEMS = {
    "main.tf",
    "outputs.tf",
    "variables.tf",
    "provision.yml",
    "includes",
    "terraform_includes",
    "scripts",
    "templates",
}


class TerraformAnsibleHostProvider(HostProvider):
    """Provision ColumnStore clusters via columnstore-ansible-aws.

    Each babylon cluster gets an isolated workspace with its own terraform
    state and generated ansible inventory/config.  Shared code (playbooks,
    terraform modules) is symlinked from the source repo.
    """

    plugin_kind = "tf_ansible"

    def __init__(self, config: dict[str, Any]) -> None:
        self._repo = Path(config.get("repo_dir", "/opt/columnstore-ansible-aws"))
        self._ws_root = Path(config.get("workspaces_dir", "/opt/babylon/cs-workspaces"))
        self._config = config
        if not self._repo.is_dir():
            raise ValueError(f"cs-ansible-aws repo not found: {self._repo}")

    # ------------------------------------------------------------------
    # HostProvider interface
    # ------------------------------------------------------------------

    _APPLY_OK_MARKER = ".babylon_apply_ok"
    _SAVED_OUTPUTS = ".babylon_outputs.json"
    _SAVED_TFSTATE = ".babylon_tfstate.backup"

    async def allocate(
        self,
        workspace: str,
        variables: dict[str, Any],
    ) -> ClusterInfo:
        ws = self._prepare_workspace(workspace)
        self._write_tfvars(ws, variables)

        # A marker file indicates a previous terraform apply completed fully.
        # If present, skip apply entirely and just re-run ansible.
        apply_ok = (ws / self._APPLY_OK_MARKER).is_file()
        log.info("tf_ansible.state_check", workspace=workspace, apply_ok=apply_ok)

        if apply_ok:
            log.info("tf_ansible.skip_apply", workspace=workspace, reason="previous apply succeeded, reusing hosts")
            # Restore tfstate from backup if the current one was corrupted
            # by a previous failed apply attempt.
            self._restore_tfstate_if_needed(ws)
            await self._terraform(ws, "init", "-input=false")
        else:
            await self._terraform(ws, "init", "-input=false")
            await self._terraform(ws, "apply", "-auto-approve", "-input=false")
            self._save_apply_artifacts(ws)

        outputs = await self._terraform_outputs(ws)
        await self._ansible_provision(ws)

        return self._outputs_to_cluster_info(workspace, ws, outputs, variables)

    def _save_apply_artifacts(self, ws: Path) -> None:
        """Save outputs and tfstate after successful apply for retry."""
        (ws / self._APPLY_OK_MARKER).touch()
        # Backup tfstate so retries can restore it if a later failed apply
        # corrupts the state (e.g. partial resource creation).
        tfstate = ws / "terraform.tfstate"
        if tfstate.is_file():
            shutil.copy2(tfstate, ws / self._SAVED_TFSTATE)

    def _restore_tfstate_if_needed(self, ws: Path) -> None:
        """Restore tfstate from backup if the current state was corrupted."""
        backup = ws / self._SAVED_TFSTATE
        tfstate = ws / "terraform.tfstate"
        if backup.is_file():
            shutil.copy2(backup, tfstate)
            log.info("tf_ansible.tfstate_restored", workspace=ws.name)

    @staticmethod
    def _state_has_resources(ws: Path) -> bool:
        """Return True if terraform state already contains resources."""
        state_file = ws / "terraform.tfstate"
        if not state_file.is_file():
            return False
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
            resources = state.get("resources", [])
            return any(r.get("instances") for r in resources)
        except (json.JSONDecodeError, OSError):
            return False

    async def destroy(self, workspace: str) -> None:
        ws = self._ws_root / workspace
        if not ws.exists():
            return
        await self._terraform(ws, "init", "-input=false")
        await self._terraform(ws, "destroy", "-auto-approve", "-input=false")
        shutil.rmtree(ws, ignore_errors=True)
        log.info("tf_ansible.destroyed", workspace=workspace)

    async def get_cluster_info(self, workspace: str) -> ClusterInfo:
        ws = self._ws_root / workspace
        if not ws.exists():
            raise RuntimeError(f"workspace not found: {ws}")
        outputs = await self._terraform_outputs(ws)
        # Read tfvars back for credentials
        variables = self._read_tfvars_json(ws)
        return self._outputs_to_cluster_info(workspace, ws, outputs, variables)

    # ------------------------------------------------------------------
    # Workspace setup
    # ------------------------------------------------------------------

    def _prepare_workspace(self, workspace: str) -> Path:
        """Create workspace dir with symlinks to source repo."""
        ws = self._ws_root / workspace
        ws.mkdir(parents=True, exist_ok=True)

        # Symlink shared items (tf files, playbooks, static dirs)
        for name in _SYMLINK_ITEMS:
            src = self._repo / name
            dst = ws / name
            if src.exists() and not dst.exists():
                dst.symlink_to(src.resolve())

        # Create inventory structure for terraform-generated files.
        # inventory/hosts and inventory/group_vars/all.yml are written
        # by terraform local_file resources into this directory.
        inv_dir = ws / "inventory"
        inv_dir.mkdir(exist_ok=True)
        gv_dir = inv_dir / "group_vars"
        gv_dir.mkdir(exist_ok=True)

        # Symlink the static distro vars (ubuntu22.yml etc.)
        distro_src = self._repo / "inventory" / "group_vars" / "distro"
        distro_dst = gv_dir / "distro"
        if distro_src.is_dir() and not distro_dst.exists():
            distro_dst.symlink_to(distro_src.resolve())

        return ws

    @staticmethod
    def _write_tfvars(ws: Path, variables: dict[str, Any]) -> None:
        """Write terraform.tfvars in HCL format."""
        lines: list[str] = []
        for k, v in sorted(variables.items()):
            if isinstance(v, bool):
                lines.append(f"{k} = {'true' if v else 'false'}")
            elif isinstance(v, (int, float)):
                lines.append(f"{k} = {v}")
            elif isinstance(v, dict):
                pairs = ", ".join(f'"{dk}" = "{dv}"' for dk, dv in v.items())
                lines.append(f"{k} = {{{pairs}}}")
            else:
                lines.append(f'{k} = "{v}"')
        (ws / "terraform.tfvars").write_text("\n".join(lines) + "\n", encoding="utf-8")

        # Also stash as JSON for later reads
        (ws / ".babylon_tfvars.json").write_text(
            json.dumps(variables, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _read_tfvars_json(ws: Path) -> dict[str, Any]:
        p = ws / ".babylon_tfvars.json"
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
        return {}

    # ------------------------------------------------------------------
    # Terraform helpers
    # ------------------------------------------------------------------

    async def _terraform(self, ws: Path, *args: str) -> str:
        cmd = ["terraform", *args]
        log.info("tf_ansible.terraform", cmd=" ".join(cmd), cwd=str(ws))
        env = {**os.environ, "TF_IN_AUTOMATION": "1"}
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
            log.error("tf_ansible.terraform.failed", cmd=args[0], error=err[:500])
            raise RuntimeError(f"terraform {args[0]} failed: {err[:2000]}")
        return stdout.decode()

    async def _terraform_outputs(self, ws: Path) -> dict[str, Any]:
        raw = await self._terraform(ws, "output", "-json")
        parsed = json.loads(raw)
        return {k: v.get("value") for k, v in parsed.items()}

    # ------------------------------------------------------------------
    # Ansible provisioning
    # ------------------------------------------------------------------

    async def _ansible_provision(self, ws: Path) -> None:
        """Run ansible-playbook provision.yml in the workspace."""
        ansible_cfg = ws / "ansible.cfg"
        if not ansible_cfg.is_file():
            raise RuntimeError(f"ansible.cfg not found at {ansible_cfg} — terraform may have failed")

        env = {
            **os.environ,
            "ANSIBLE_CONFIG": str(ansible_cfg),
            "ANSIBLE_FORCE_COLOR": "0",
        }

        # Run from workspace dir so inventory relative paths in ansible.cfg resolve here.
        # Pass generated group_vars as extra-vars to override any stale source-repo copies.
        all_yml = ws / "inventory" / "group_vars" / "all.yml"
        cmd = ["ansible-playbook", "provision.yml", "--limit", "!devhost"]
        if all_yml.is_file():
            cmd.extend(["--extra-vars", f"@{all_yml}"])

        log.info("tf_ansible.ansible", cmd=" ".join(cmd), cwd=str(ws))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=ws,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            out = stdout.decode().strip()
            err = stderr.decode().strip()
            # stdout has actual task failures; stderr is mostly warnings.
            combined = out or err
            log.error(
                "tf_ansible.ansible.failed", returncode=proc.returncode, stdout_tail=out[-500:], stderr_tail=err[-500:]
            )
            raise RuntimeError(f"ansible-playbook provision.yml failed: {combined[-3000:]}")
        log.info("tf_ansible.ansible.ok")

    # ------------------------------------------------------------------
    # Output parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _outputs_to_cluster_info(
        workspace: str,
        ws: Path,
        outputs: dict[str, Any],
        variables: dict[str, Any],
    ) -> ClusterInfo:
        hosts: list[HostInfo] = []

        cs_nodes = outputs.get("columnstore_nodes") or []
        for i, node in enumerate(cs_nodes):
            hosts.append(
                HostInfo(
                    private_ip=node["private_ip"],
                    public_ip=node.get("public_dns"),
                    role="columnstore",
                    index=i,
                )
            )

        mx_nodes = outputs.get("maxscale_nodes") or []
        for i, node in enumerate(mx_nodes):
            hosts.append(
                HostInfo(
                    private_ip=node["private_ip"],
                    public_ip=node.get("public_dns"),
                    role="maxscale_cs",
                    index=i,
                )
            )

        # Primary SQL endpoint: MaxScale if available, else first CS node
        if mx_nodes:
            primary_host = mx_nodes[0].get("public_dns", mx_nodes[0]["private_ip"])
        elif cs_nodes:
            primary_host = cs_nodes[0].get("public_dns", cs_nodes[0]["private_ip"])
        else:
            primary_host = ""

        ssh_key = outputs.get("ssh_key_file", variables.get("ssh_key_file", ""))

        return ClusterInfo(
            hosts=hosts,
            primary_host=primary_host,
            primary_port=3306,
            sql_user=variables.get("admin_user", "admin"),
            sql_password=variables.get("admin_pass", ""),
            ssh_key_path=ssh_key,
            workspace=workspace,
            extra={
                "cmapi_key": variables.get("cmapi_key", ""),
                "deployment_prefix": variables.get("deployment_prefix", ""),
                "maxscale_user": variables.get("maxscale_user", ""),
                "maxscale_pass": variables.get("maxscale_pass", ""),
                "workspace_dir": str(ws),
            },
        )

