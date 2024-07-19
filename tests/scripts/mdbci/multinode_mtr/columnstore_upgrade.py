#!/usr/bin/env python3

# pip3 install paramiko scp python-util

# DOC: https://mariadb.com/docs/server/service-management/upgrades/enterprise-columnstore/multi-node/from-6-23-02/

from argparse import ArgumentParser
import os

from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import ssh_tools
from mdb_buildbot_scripts.operations import mdbci
from mdb_buildbot_scripts.constants import products_names
from mdb_buildbot_scripts.operations import product_removing
import columnstore_operations
from columnstore_operations import IP_KEY


def main():
    script_dir = os.path.abspath(os.path.dirname(__file__))

    arguments = parseArguments()

    mdbci.setupMdbciEnvironment()
    csVM = []
    N = int(arguments.num)
    x = '{}'

    nodes = []
    for i in range(0, N):
        nodes.append(f'cs{i:03d}')
    nodes.append('max')

    logFile = open('columnstore_upgrade.log', "w")

    for i in range(0, N + 1):
        csVM.append(mdbci.Machine(arguments.machine_name, nodes[i]))

    ssh = []
    host.printInfo("Preparing nodes for upgrade")
    for i in range(0, N + 1):
        ssh.append(ssh_tools.loadSSH())
        ssh[i].connect(csVM[i].ip_address, username=csVM[i].ssh_user, key_filename=csVM[i].ssh_key)

    for i in range(1, N):
        s = []
        ssh_tools.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', ssh_tools.addTextToList, s)
        cnfPath = s[0].strip()
        ssh_tools.interactiveExec(ssh[i],
                                  f'sudo find {cnfPath} -type f -exec sed -i "s/gtid_strict_mode/#gtid_strict_mode/" {x} \;',
                                  ssh_tools.printAndSaveText, logFile)

    ssh_tools.interactiveExec(ssh[i], "sudo mcs cluster stop", ssh_tools.printAndSaveText, logFile)

    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i],
                                  "sudo systemctl stop mariadb-columnstore-cmapi;"
                                  "sudo systemctl stop mariadb-columnstore;"
                                  "sudo systemctl stop mariadb",
                                  ssh_tools.printAndSaveText, logFile)

    host.printInfo("Upgrade")
    for i in range(0, N):
        host.printInfo("Removing old version")
        product_removing.removeProduct(
            machineName=arguments.machine_name,
            machine=nodes[i],
            mdbciProduct=products_names.PRODS[arguments.product],
            useMdbci=False,
            allPackages=True,
            includeColumnstore=True,
        )
        host.printInfo("Install new versions of the server")
        mdbci.runMdbci('install_product',
                       '--product', products_names.PRODS[arguments.product],
                       '--product-version', arguments.target,
                       arguments.machine_name + f'/{nodes[i]}')
        host.printInfo("Install new versions of the server")
        mdbci.runMdbci('install_product',
                       '--product', 'plugin_columnstore',
                       '--product-version', arguments.target,
                       arguments.machine_name + f'/{nodes[i]}')
        host.printInfo("Install new versions of the backup tool")
        mdbci.runMdbci('install_product',
                       '--product', 'plugin_backup',
                       '--product-version', arguments.target,
                       arguments.machine_name + f'/{nodes[i]}')
    host.printInfo("Restoring nodes after upgrade")
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i],
                                  "sudo systemctl stop mariadb-columnstore;"
                                  "sudo systemctl stop mariadb",
                                  ssh_tools.printAndSaveText, logFile)
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i],
                                  "sudo systemctl start mariadb-columnstore-cmapi;"
                                  "sudo systemctl start mariadb",
                                  ssh_tools.printAndSaveText, logFile)
    ssh_tools.interactiveExec(ssh[i],
                              "sudo mariadb-upgrade --write-binlog;"
                              "sudo mcs cluster start",
                              ssh_tools.printAndSaveText, logFile)
    for i in range(1, N):
        s = []
        ssh_tools.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', ssh_tools.addTextToList, s)
        cnfPath = s[0].strip()
        ssh_tools.interactiveExec(
            ssh[i],
            f'sudo find {cnfPath} -type f -exec sed -i "s/#gtid_strict_mode/gtid_strict_mode/" {x} \;',
            ssh_tools.printAndSaveText, logFile)

    res = columnstore_operations.checkColumnstoreCluster(IP_KEY, N, csVM)

    if arguments.target_version:
        for i in range(0, N):
            mariadbVersiom = ssh_tools.getMariaDBVersion(ssh[i], startServer=False)
            host.printInfo(f"Server version after upgrade on the node {i + 1} is '{mariadbVersiom}'")
            if mariadbVersiom != arguments.target_version:
                host.printInfo(f"Server version on the node {i + 1} is wrong!")
                res = 1
        else:
            host.printInfo("Version is not checked")

    for i in range(0, N + 1):
        ssh[i].close()

    logFile.close()

    exit(res)



def parseArguments():
    parser = ArgumentParser()
    parser.add_argument("--machine-name", dest="machine_name",
                        help="Name of MDBCI configuration", type=host.machineName)
    parser.add_argument("--num", help="Number of Columstore nodes")
    parser.add_argument("--product", dest="product",
                        help="'MariaDBEnterprise' or 'MariaDBServerCommunity'", default="MariaDBEnterpriseProduction")
    parser.add_argument("--target", dest="target", help="Version to be installed")
    parser.add_argument("--target-version", dest="target_version", help="Expected version after upgrade")

    return parser.parse_args()


if __name__ == "__main__":
    main()
