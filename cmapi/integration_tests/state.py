from integration_tests.ssh import RemoteHost

def activate_single_node_config(host: RemoteHost):
    """
    Stop MCS services on the host.
    """
    with host.ssh_connection() as conn:
        print(f"{host.name}: Stopping MCS services...")
        conn.sudo("systemctl stop mariadb mcs-workernode@1 mcs-workernode@2 mcs-writeengineserver mcs-storagemanager mcs-ddlproc mcs-dmlproc mcs-primproc mcs-controllernode", hide=True)
        print(f"{host.name}: Activating single node configuration...")
        conn.sudo("cp /usr/share/columnstore/cmapi/cmapi_server/SingleNode.xml /etc/columnstore/Columnstore.xml", hide=True)
        print(f"{host.name}: Truncating log files...")
        truncation_script = """
        for log in crit.log err.log info.log debug.log cmapi_server.log mcs_cli.log; do
            if [ -f /var/log/mariadb/columnstore/$log ]; then
                sudo truncate -s 0 /var/log/mariadb/columnstore/$log
            fi
        done"""
        conn.sudo(truncation_script, hide=True, shell=True)
        print(f"{host.name}: Starting cmapi service...")
        conn.sudo("systemctl start mariadb-columnstore-cmapi", hide=True)

        mcs_output = host.exec_mcs("cluster status")
        assert mcs_output.get("num_nodes") == 0
