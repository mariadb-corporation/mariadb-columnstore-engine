import json
import subprocess
from pathlib import Path

import pytest

from integration_tests import cluster_mgmt
from integration_tests.ssh import ClusterConfig, RemoteHost, run_on_all_hosts_parallel
from integration_tests.state import activate_single_node_config
from integration_tests.utils import change_directory


def pytest_addoption(parser):
    """Add terraform directory option to pytest command line arguments."""
    parser.addoption(
        "--terraform-dir",
        action="store",
        default=None,
        required=True,
        help="Directory where terraform was run to set up the test cluster",
    )
    parser.addoption(
        "--create-cluster",
        action="store_true",
        default=False,
        help="Create the cluster before running tests",
    )
    parser.addoption(
        "--destroy-cluster",
        action="store_true",
        default=False,
        help="Destroy the cluster after running tests",
    )
    parser.addoption(
        "--ssh-user",
        default="ubuntu",
        help="SSH user to connect to the cluster hosts",
    )


@pytest.fixture(scope="session")
def terraform_dir(request) -> Path:
    """
    Fixture to provide the terraform directory path.
    Fails explicitly if --terraform-dir option is not provided.
    """
    terraform_directory = request.config.getoption("--terraform-dir")
    if not terraform_directory:
        pytest.fail("--terraform-dir option is required but not provided")
    terraform_directory = Path(terraform_directory).resolve()
    if not terraform_directory.is_dir():
        pytest.fail(f"Specified terraform directory '{terraform_directory}' does not exist")
    return terraform_directory


@pytest.fixture(scope="session")
def prepared_cluster(request, terraform_dir):
    """
    Provides existence of the cluster
    If it doesn't exist and --create-cluster is passed, it creates it.
    If it exists and --create-cluster is passed, it re-creates it.
    If --destroy-cluster is passed, it destroys the cluster when the tests finish.
    """
    create_cluster = request.config.getoption("--create-cluster")
    destroy_cluster = request.config.getoption("--destroy-cluster")

    cluster_already_exists = cluster_mgmt.cluster_exists(terraform_dir)

    if create_cluster:
        if cluster_already_exists:
            print("Cluster already exists, destroying it first...")
            cluster_mgmt.destroy_cluster(terraform_dir)
        print("Creating the cluster...")
        cluster_mgmt.create_cluster(terraform_dir)

    yield

    if destroy_cluster:
        print("Destroying the cluster...")
        cluster_mgmt.destroy_cluster(terraform_dir)


@pytest.fixture(scope="session")
def cluster_config(request, terraform_dir, prepared_cluster) -> ClusterConfig:
    """
    Fixture to provide the cluster configuration.
    """
    with change_directory(terraform_dir):
        # Read the cluster configuration from the terraform output
        outputs = json.loads(subprocess.check_output(
            ["terraform", "output", "-json"],
            text=True,
        ))

        if outputs == {}:
            pytest.fail("Terraform output is empty, looks like cluster is not yet created. Pass --create-cluster option to create it.")

        key_file_path = Path(outputs["ssh_key_file"]["value"])
        ssh_user = request.config.getoption("--ssh-user")

        mcs_nodes = []
        for host_descr in outputs["columnstore_nodes"]["value"]:
            mcs_nodes.append(
                RemoteHost(
                    name=host_descr["name"],
                    private_ip=host_descr["private_ip"],
                    public_fqdn=host_descr["public_dns"],
                    key_file_path=key_file_path,
                    ssh_user=ssh_user,
                )
            )

        maxscale_nodes = []
        for host_descr in outputs["maxscale_nodes"]["value"]:
            maxscale_nodes.append(
                RemoteHost(
                    name=host_descr["name"],
                    private_ip=host_descr["private_ip"],
                    public_fqdn=host_descr["public_dns"],
                    key_file_path=key_file_path,
                    ssh_user=ssh_user,
                )
            )

        all_hosts = mcs_nodes + maxscale_nodes
        # Check if all hosts are reachable
        for host in all_hosts:
            if not host.is_reachable():
                pytest.fail(f"Host {host.name} is not reachable via SSH")

        cluster_config = ClusterConfig(mcs_hosts=mcs_nodes, maxscale_hosts=maxscale_nodes)
        return cluster_config


@pytest.fixture
def disconnected_cluster(cluster_config):
    """
    Fixture to activate single node configuration on all hosts in the cluster,
      none of the nodes are in the cluster.
    """
    print("Activating single node configuration on all hosts...")
    run_on_all_hosts_parallel(cluster_config.mcs_hosts, activate_single_node_config)


@pytest.fixture
def only_primary_in_cluster(disconnected_cluster, cluster_config):
    """
    Fixture that provides a cluster with only the primary node added
    """
    print("Adding only primary node to the cluster...")
    cmd = f"cluster node add --node {cluster_config.primary.private_ip}"
    cluster_config.primary.exec_mcs(cmd)

    status_out = cluster_config.primary.exec_mcs("cluster status")
    assert status_out["num_nodes"] == 1
    assert str(cluster_config.primary.private_ip) in status_out


@pytest.fixture
def only_primary_and_one_replica_in_cluster(only_primary_in_cluster, cluster_config):
    """
    Fixture that provides a cluster with only the primary and one replica node added
    """
    print("Adding one replica...")
    cmd = f"cluster node add --node {cluster_config.replicas[0].private_ip}"
    cluster_config.primary.exec_mcs(cmd)

    status_out = cluster_config.primary.exec_mcs("cluster status")
    assert status_out["num_nodes"] == 2


@pytest.fixture
def complete_cluster(cluster_config, disconnected_cluster, only_primary_in_cluster):
    """
    Fixture to activate multinode configuration on all hosts in the cluster.
    """
    print("Adding replicas to the cluster...")
    cmd = "cluster node add "
    for host in cluster_config.replicas:
        cmd += f"--node {host.private_ip} "
    cluster_config.primary.exec_mcs(cmd)

    status_out = cluster_config.primary.exec_mcs("cluster status")
    assert status_out["num_nodes"] == len(cluster_config.mcs_hosts)
