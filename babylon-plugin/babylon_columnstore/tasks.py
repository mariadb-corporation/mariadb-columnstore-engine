"""ColumnStore-specific celery task logic.

Follows the same shape as babylon_tiamat/tasks.py — four entry
points that babylon core dispatches to via importlib based on
``cluster.cluster_template.provider == "columnstore"``.
"""

from __future__ import annotations

import json

import structlog

from babylon import provider_config
from babylon.services.pipeline import PipelineOrchestrator

from babylon_columnstore.hosts.tf_ansible import TerraformAnsibleHostProvider

log = structlog.get_logger()

# Core's _provision_run reads this tuple via getattr to decide whether to
# hand cluster off to this plugin's provision(). CS allows creating/failed
# so the same terraform state can be reused on retry.
PROVISION_ACCEPTED_STATUSES: tuple[str, ...] = (
    "requested", "ready", "busy", "creating", "failed",
)


async def provision(session, run, cluster, template, cluster_svc, run_repo) -> dict:
    """Allocate CS hosts (or reuse existing), run the pipeline, best-effort destroy on success."""
    run_id, cluster_id = run.run_id, cluster.cluster_id
    workspace = cluster.terraform_workspace or f"cs-{cluster_id}"
    cs_cfg = provider_config.load("columnstore")
    host_provider = TerraformAnsibleHostProvider(cs_cfg)
    cluster_info = None

    if not cluster.terraform_workspace:
        cluster.terraform_workspace = workspace
        await cluster_svc.repo.save(cluster)

    needs_allocate = cluster.status in ("requested", "creating", "failed")
    if needs_allocate:
        log.info("provision.allocating_hosts", run_id=run_id, cluster_id=cluster_id,
                 provider="columnstore", retry=cluster.status != "requested")
        run.stage = "allocating_hosts"
        run.status = "active"
        run.failure_reason = None
        await run_repo.save(run)
        await cluster_svc.mark_creating(cluster)

        tfvars = dict(cs_cfg.get("terraform_defaults", {}))
        if template and template.terraform_variables_json:
            tfvars.update(json.loads(template.terraform_variables_json))
        if run.cluster_variable_overrides_json:
            tfvars.update(json.loads(run.cluster_variable_overrides_json))
        tfvars.setdefault("deployment_prefix", f"babylon-c{cluster_id}")

        try:
            cluster_info = await host_provider.allocate(workspace, tfvars)
        except Exception as exc:
            reason = f"columnstore allocate failed: {exc}"
            log.error("provision.allocate_failed", run_id=run_id, cluster_id=cluster_id, error=str(exc))
            run.status = "failed"
            run.failure_reason = reason
            await run_repo.save(run)
            await cluster_svc.mark_failed(cluster, reason)
            # Terraform state preserved for retry.
            return {"run_id": run_id, "cluster_id": cluster_id, "status": "failed", "step": "allocate"}

        await cluster_svc.mark_ready(cluster)
        log.info("provision.hosts_allocated", run_id=run_id, cluster_id=cluster_id, provider="columnstore")

    if cluster_info is None:
        cluster_info = await host_provider.get_cluster_info(workspace)

    await cluster_svc.mark_busy(cluster, run.run_id)
    pipeline = PipelineOrchestrator(
        session, provider="columnstore", cluster_info=cluster_info,
    )
    success = await pipeline.execute(run, cluster)
    if success:
        await destroy_hosts_best_effort(host_provider, workspace, cluster_id)
        await cluster_svc.mark_destroyed(cluster)
    # On failure, keep hosts alive for retry.
    return {"run_id": run_id, "cluster_id": cluster_id,
            "status": "completed" if success else "failed"}


async def destroy_hosts_best_effort(host_provider, workspace: str, cluster_id: int) -> None:
    """Immediately destroy CS hosts to prevent orphans after a successful run."""
    log.info("provision.destroying_hosts", cluster_id=cluster_id, provider="columnstore")
    try:
        await host_provider.destroy(workspace)
        log.info("provision.hosts_destroyed", cluster_id=cluster_id)
    except Exception:
        log.error("provision.hosts_destroy_exception", cluster_id=cluster_id, exc_info=True)


async def cleanup_cluster(cluster, workspace: str) -> bool:
    """Periodic TTL cleanup — called by core's cleanup_clusters loop."""
    cs_cfg = provider_config.load("columnstore")
    host_provider = TerraformAnsibleHostProvider(cs_cfg)
    try:
        await host_provider.destroy(workspace)
        return True
    except Exception:
        log.error("cleanup.destroy_failed", cluster_id=cluster.cluster_id, exc_info=True)
        return False


async def delete_cluster_hosts(workspace: str) -> tuple[bool, str | None]:
    """API DELETE /clusters/id path. Returns (destroyed, reason_if_empty)."""
    cs_cfg = provider_config.load("columnstore")
    host_provider = TerraformAnsibleHostProvider(cs_cfg)
    # CS flow: always try destroy; report no-hosts if workspace absent.
    try:
        await host_provider.destroy(workspace)
        return True, None
    except FileNotFoundError as exc:
        # workspace dir missing → nothing to destroy, just delete from DB
        return False, str(exc)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc
