from integration_tests.ssh import ClusterConfig
from integration_tests.utils import assert_dict_includes, drop_timestamp

ALL_MCS_SERVICES = {"StorageManager", "workernode", "controllernode", "PrimProc", "WriteEngineServer", "DDLProc", "DMLProc"}


class TestAddNode:
    def test_add_nodes(self, cluster_config: ClusterConfig, disconnected_cluster):
        """Test adding hosts to the cluster"""
        # Add primary host
        print(f"Adding self ({cluster_config.primary}) to the cluster")
        primary_private_ip = cluster_config.primary.private_ip
        add_out = cluster_config.primary.exec_mcs(f"cluster node add --node {primary_private_ip}")
        assert add_out[0]["node_id"] == str(primary_private_ip)

        status_out = cluster_config.primary.exec_mcs("cluster status")
        assert status_out["num_nodes"] == 1
        assert primary_private_ip in status_out
        primary_status = drop_timestamp(status_out[primary_private_ip])
        assert_dict_includes(
            primary_status,
            {
                "dbrm_mode": "master",
                "cluster_mode": "readwrite",
                "dbroots": ['1'],
                "module_id": 1,
            },
        )
        services_in_status = {service_descr["name"] for service_descr in primary_status["services"]}
        assert services_in_status == ALL_MCS_SERVICES

        # Add replicas
        print(f"Adding replicas ({cluster_config.replicas}) to the cluster")
        # Checking passing more that one node to the command
        add_cmd = "cluster node add "
        for replica in cluster_config.replicas:
            add_cmd += f"--node {replica.private_ip} "
        add_out = cluster_config.primary.exec_mcs(add_cmd)
        assert len(add_out) == len(cluster_config.replicas)
        added_node_ids = {node["node_id"] for node in add_out}
        expected_node_ids = {str(replica.private_ip) for replica in cluster_config.replicas}
        assert added_node_ids == expected_node_ids

        status_out = cluster_config.primary.exec_mcs("cluster status")
        assert status_out["num_nodes"] == len(cluster_config.mcs_hosts)
        for idx, replica in enumerate(cluster_config.replicas, start=2):
            assert str(replica.private_ip) in status_out
            replica_status = drop_timestamp(status_out[str(replica.private_ip)])
            assert_dict_includes(
                replica_status,
                {
                    "dbrm_mode": "slave",
                    "cluster_mode": "readonly",
                    "dbroots": [str(idx)],
                    "module_id": idx,
                },
            )
            replica_expected_services = ALL_MCS_SERVICES - {"controllernode", "DDLProc", "DMLProc"}
            services_in_status = {service_descr["name"] for service_descr in replica_status["services"]}
            assert services_in_status == replica_expected_services