"""Burza benchmark runner for MariaDB ColumnStore TPC-H performance testing.

Wraps the Burza tool (https://github.com/mariadb-corporation/burza):
  1. Generates burza.yaml config
  2. Deploys burza to cluster nodes via deploy_burza.sh (passes config)
  3. Runs TPC-H tests (mono or multi-node Ray mode)
  4. Fetches CSV results from primary node
  5. Parses key metrics into BenchmarkResult

Requires:
  - burza repo cloned locally (path in config)
  - cs-ansible-aws workspace with generated inventory/ansible.cfg
    (from TerraformAnsibleHostProvider)
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog
import yaml

from babylon.providers.base import BenchmarkResult, BenchmarkRunner, ClusterInfo

log = structlog.get_logger()

BURZA_TARGET_PATH = "/opt/burza"
BURZA_RESULTS_PATH = f"{BURZA_TARGET_PATH}/results"

DEFAULT_RUN_TESTS_PLUGINS = [
    "memory_stats",
    "cpu_load",
    "host_hw",
    "tc_run_dur_dp",
    "tc_run_dur_dp_emitter",
    "mcs_logs",
    "df_csv_dumper",
    "runs_num_limiter",
    "service_rstrtr",
    "test_select",
    "run_progress",
]


class MCSBurzaRunner(BenchmarkRunner):
    """Run Burza TPC-H benchmarks on a ColumnStore cluster.

    The runner uses ``deploy_burza.sh`` from the burza repo, which:
    - Installs burza + dependencies on cluster nodes via ansible
    - Prepares TPC-H data if not already loaded
    - Starts Ray, runs tests, collects results
    """

    plugin_kind = "mcs_burza"

    def __init__(self, adapter: Any = None, config: dict[str, Any] | None = None) -> None:
        cfg = config or {}
        burza_repo_dir = cfg.get("burza_repo_dir", "/opt/burza")
        self._burza_repo = Path(burza_repo_dir)
        deploy_script = self._burza_repo / "burza" / "deploy" / "deploy_burza.sh"
        if not deploy_script.is_file():
            raise ValueError(f"deploy_burza.sh not found at {deploy_script}")

    async def run(
        self,
        cluster_info: ClusterInfo,
        params: dict[str, Any],
        event_cb: Callable[..., Any] | None = None,
    ) -> BenchmarkResult:
        ws_dir = cluster_info.extra.get("workspace_dir")
        if not ws_dir:
            return BenchmarkResult(success=False, error="workspace_dir missing from cluster_info.extra")

        ws = Path(ws_dir)
        mode = params.get("mode", "mono")

        # Generate burza.yaml config
        burza_config = self._build_config(params)
        config_path = ws / "burza.yaml"
        config_path.write_text(yaml.dump(burza_config, default_flow_style=False), encoding="utf-8")

        if event_cb:
            test_names = burza_config.get("plugin_config", {}).get("test_select", {}).get("test_names", "all")
            event_cb("info", "benchmark", f"Running burza ({mode} mode), tests: {test_names}")

        # Run deploy_burza.sh from the cs-ansible-aws workspace
        try:
            await self._run_deploy_burza(ws, mode, config_path)
        except RuntimeError as exc:
            return BenchmarkResult(success=False, error=str(exc))

        # Fetch results from primary node
        primary = self._get_primary_host(cluster_info)
        if not primary:
            return BenchmarkResult(success=False, error="no columnstore primary host found")

        try:
            metrics, artifacts = await self._fetch_results(
                primary,
                cluster_info.ssh_key_path,
            )
        except RuntimeError as exc:
            return BenchmarkResult(
                success=True,  # tests ran, but results fetch failed
                error=f"results fetch failed: {exc}",
            )

        if event_cb:
            event_cb("info", "benchmark", f"Burza completed: {len(metrics)} metrics collected")

        return BenchmarkResult(success=True, metrics=metrics, artifacts=artifacts)

    # ------------------------------------------------------------------
    # Config generation
    # ------------------------------------------------------------------

    @staticmethod
    def _build_config(params: dict[str, Any]) -> dict[str, Any]:
        plugins = list(DEFAULT_RUN_TESTS_PLUGINS)

        # Add extra plugins from params
        if "data_point_generators" in params:
            dpg = params["data_point_generators"]
            if isinstance(dpg, str):
                dpg = [x.strip() for x in dpg.split(",")]
            plugins.extend(p for p in dpg if p not in plugins)

        if "report_generators" in params:
            rg = params["report_generators"]
            if isinstance(rg, str):
                rg = [x.strip() for x in rg.split(",")]
            plugins.extend(p for p in rg if p not in plugins)

        config: dict[str, Any] = {
            "global": {
                "test_suite": params.get("test_suite", "tpc_h"),
                "output_dir": BURZA_RESULTS_PATH,
                "log_level": "DEBUG",
            },
            "run_tests": {
                "test_runner": params.get("test_runner", "seq_test_runner"),
                "plugins": plugins,
            },
            "gen_reports": {
                "plugins": [
                    "mem_usage_graph",
                    "cpu_load_graph",
                    "run_durations_graph",
                    "error_report",
                    "tc_dur_console",
                    "df_from_csv",
                ],
            },
            "plugin_config": {
                "runs_num_limiter": {
                    "runs_num": int(params.get("runs_num", 7)),
                    "runs_num_max": int(params.get("runs_num_max", 30)),
                },
                "tpc_h": {
                    "scale_factor": int(params.get("scale_factor", 100)),
                    "use_tpchgen": True,
                    "tpchgen_cli": "/usr/local/bin/tpchgen-cli",
                },
                "service_rstrtr": {
                    "restart_db_before_test_case": False,
                    "truncate_mcs_logs": True,
                    "restart_with_mcs_cluster": True,
                },
            },
        }

        # Optional: test name selection
        if "test_names" in params:
            config["plugin_config"]["test_select"] = {
                "test_names": params["test_names"],
            }

        # Optional: parallelism levels for parallel test runner
        if "parallelism_levels" in params:
            config["plugin_config"]["parall_test_runner"] = {
                "parallelism_levels": params["parallelism_levels"],
            }

        return config

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def _run_deploy_burza(
        self,
        ws: Path,
        mode: str,
        config_path: Path,
    ) -> None:
        deploy_script = self._burza_repo / "burza" / "deploy" / "deploy_burza.sh"
        ansible_cfg = ws / "ansible.cfg"

        env = {
            **os.environ,
            "ANSIBLE_CONFIG": str(ansible_cfg),
            "ANSIBLE_FORCE_COLOR": "0",
        }

        cmd = [
            str(deploy_script),
            "-r",
            str(ws),
            "-m",
            mode,
            "-c",
            str(config_path),
        ]

        log.info("burza.deploy", cmd=" ".join(cmd), cwd=str(ws))
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
            log.error("burza.deploy.failed", error=err[:1000])
            raise RuntimeError(f"deploy_burza.sh failed (rc={proc.returncode}): {err[:2000]}")
        log.info("burza.deploy.ok")

    # ------------------------------------------------------------------
    # Results collection
    # ------------------------------------------------------------------

    @staticmethod
    def _get_primary_host(cluster_info: ClusterInfo) -> str | None:
        for h in cluster_info.hosts:
            if h.role == "columnstore" and h.index == 0:
                return h.public_ip or h.private_ip
        return None

    async def _fetch_results(
        self,
        host: str,
        ssh_key_path: str,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """SCP the results CSV from the primary node and parse metrics."""
        ssh_opts = f"-o StrictHostKeyChecking=no -i {ssh_key_path}"

        # List CSV files in results dir
        ls_cmd = f"ssh {ssh_opts} root@{host} 'find {BURZA_RESULTS_PATH} -name \"*.csv\" -type f 2>/dev/null || true'"
        proc = await asyncio.create_subprocess_shell(
            ls_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        csv_files = [f.strip() for f in stdout.decode().splitlines() if f.strip()]

        if not csv_files:
            log.warning("burza.results.no_csv", host=host)
            return {}, {}

        metrics: dict[str, Any] = {}
        artifacts: dict[str, str] = {}

        for remote_path in csv_files:
            filename = Path(remote_path).name
            # Fetch file content
            cat_cmd = f"ssh {ssh_opts} root@{host} 'cat {remote_path}'"
            proc = await asyncio.create_subprocess_shell(
                cat_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                continue

            content = stdout.decode(errors="replace")
            artifacts[filename] = content[:10000]  # Truncate large CSVs for storage

            # Parse run_dur CSVs for key metrics (test case durations)
            if "run_dur" in filename:
                parsed = self._parse_run_duration_csv(content)
                metrics.update(parsed)

        return metrics, artifacts

    @staticmethod
    def _parse_run_duration_csv(content: str) -> dict[str, Any]:
        """Extract per-query median durations from burza's run_dur CSV."""
        result: dict[str, Any] = {}
        try:
            reader = csv.DictReader(io.StringIO(content))
            # Group durations by test case name
            durations: dict[str, list[float]] = {}
            for row in reader:
                name = row.get("case_name") or row.get("test_case") or row.get("name", "")
                dur_str = row.get("duration_secs") or row.get("duration") or row.get("value", "")
                if name and dur_str:
                    try:
                        durations.setdefault(name, []).append(float(dur_str))
                    except ValueError:
                        continue

            for name, durs in durations.items():
                sorted_durs = sorted(durs)
                n = len(sorted_durs)
                median = sorted_durs[n // 2] if n % 2 else (sorted_durs[n // 2 - 1] + sorted_durs[n // 2]) / 2
                result[f"{name}_median_secs"] = round(median, 3)
                result[f"{name}_min_secs"] = round(sorted_durs[0], 3)
                result[f"{name}_runs"] = n

        except Exception:
            log.warning("burza.results.parse_error", exc_info=True)

        return result

