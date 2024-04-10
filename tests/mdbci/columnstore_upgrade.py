#!/usr/bin/env python3

# pip3 install paramiko scp python-util

# DOC: https://mariadb.com/docs/server/service-management/upgrades/enterprise-columnstore/multi-node/from-6-23-02/

from argparse import ArgumentParser
import os
import requests
import json
import re

from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import ssh_tools
from mdb_buildbot_scripts.operations import mdbci
from mdb_buildbot_scripts.methods import versions_processing
from mdb_buildbot_scripts.constants import products_names
from mdb_buildbot_scripts.operations import product_removing
from mdb_buildbot_scripts.operations import install


def main():
    res = 0
    script_dir = os.path.abspath(os.path.dirname(__file__))

    arguments = parseArguments()

    mdbci.setupMdbciEnvironment()
    csVM = []
    N = int(arguments.num)
    IP_KEY = '93816fa66cc2d8c224e62275bd4f248234dd4947b68d4af2b29671dd7d5532dd'
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
        host.printInfo("Configure apt/yum/apt for repo with new version")
        mdbci.runMdbci('setup_repo',
                       '--product', products_names.PRODS[arguments.product],
                       '--product-version', arguments.target,
                       arguments.machine_name + f'/{nodes[i]}')
        host.printInfo("Installing new version")
        install.installServer(
            machineName=arguments.machine_name,
            machine=nodes[i],
            product=arguments.product,
            targetVersion=arguments.target_version,
            mariadbVersion=versions_processing.majorVersion(arguments.target_version),
            architecture=csVM[i].architecture)

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

    x = requests.get(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/status",
                     headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                     verify=False)
    j = x.json()
    host.printInfo(json.dumps(j, indent=2))

    # checking data from status JSON:
    # number of nodes
    # master is only one node
    # master is readwrite
    # slaves are readonly
    # number of slaves is N - 1
    try:
        numJ = j['num_nodes']
        host.printInfo(f"Number of nodes after restore: {numJ}")
        if N != int(numJ):
            host.printInfo(f"Wrong number of nodes after upgrade")
            res = 1

        k = list(j.keys())
        ipRegex = re.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$")
        masters = 0
        slaves = 0
        for node in k:
            if ipRegex.match(node):
                if j[node]['dbrm_mode'] == 'slave':
                    host.printInfo(f"node {node} is slave")
                    slaves = slaves + 1
                    if j[node]['cluster_mode'] != 'readonly':
                        res = 1
                        host.printInfo(f"Slave {node} is not readonly")
                if j[node]['dbrm_mode'] == 'master':
                    host.printInfo(f"node {node} is master")
                    masters = masters + 1
                    if j[node]['cluster_mode'] != 'readwrite':
                        res = 1
                        host.printInfo(f"Master {node} is not readwrite")

        if masters != 1:
            res = 1
            host.printInfo(f"Wrong number of masters - {masters}")

        if slaves != N - 1:
            res = 1
            host.printInfo(f"Wrong number of slavess - {slaves}")
    except:
        host.printInfo("Error decoding of cluster status JSON")
        res = 1

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
    parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI configuration")
    parser.add_argument("--num", help="Number of Columstore nodes")
    parser.add_argument("--product", dest="product",
                        help="'MariaDBEnterprise' or 'MariaDBServerCommunity'", default="MariaDBEnterpriseProduction")
    parser.add_argument("--target", dest="target", help="Version to be installed")
    parser.add_argument("--target-version", dest="target_version", help="Expected version after upgrade")

    return parser.parse_args()


if __name__ == "__main__":
    main()
