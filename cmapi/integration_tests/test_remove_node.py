from integration_tests.ssh import ClusterConfig
from integration_tests.utils import assert_dict_includes, drop_timestamp


class TestRemoveNode:
    def test_remove_node(self, cluster_config: ClusterConfig, complete_cluster):
        """Test removing hosts from the cluster"""
        # Remove replicas
        remove_cmd = "cluster node remove "
        for replica in cluster_config.replicas:
            remove_cmd += f"--node {replica.private_ip} "
        remove_out = cluster_config.primary.exec_mcs(remove_cmd)
        assert len(remove_out) == len(cluster_config.replicas)
        removed_node_ids = {node["node_id"] for node in remove_out}
        expected_node_ids = {str(replica.private_ip) for replica in cluster_config.replicas}
        assert removed_node_ids == expected_node_ids

        status_out = cluster_config.primary.exec_mcs("cluster status")
        assert status_out["num_nodes"] == 1
        assert str(cluster_config.primary.private_ip) in status_out

        primary_status = drop_timestamp(status_out[str(cluster_config.primary.private_ip)])
        assert_dict_includes(
            primary_status,
            {
                "dbrm_mode": "master",
                "cluster_mode": "readwrite",
                "dbroots": ['1', '2', '3'],  # All the dbroots are rebalanced to the last host
                "module_id": 1,
            },
        )

        # Remove primary node
        print(f"Removing primary node ({cluster_config.primary}) from the cluster")
        remove_out = cluster_config.primary.exec_mcs(f"cluster node remove --node {cluster_config.primary.private_ip}")
        assert len(remove_out) == 1
        assert remove_out[0]["node_id"] == str(cluster_config.primary.private_ip)

        status_out = cluster_config.primary.exec_mcs("cluster status")
        print(status_out)
        assert status_out["num_nodes"] == 0
        assert str(cluster_config.primary.private_ip) not in status_out
