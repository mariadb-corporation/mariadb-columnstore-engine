#!/usr/bin/env python3

# DOC: https://mariadb.com/docs/server/service-management/upgrades/enterprise-columnstore/multi-node/from-6-23-02/

from argparse import ArgumentParser
import os
import requests
import common
import json
import re


def main():
    res = 0
    script_dir = os.path.abspath(os.path.dirname(__file__))

    arguments = parseArguments()

    common.setupMdbciEnvironment()
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
        csVM.append(common.Machine(arguments.machine_name, nodes[i]))

    ssh = []
    common.printInfo("Preparing nodes for upgrade")
    for i in range(0, N + 1):
        ssh.append(common.loadSSH())
        ssh[i].connect(csVM[i].ip_address, username=csVM[i].ssh_user, key_filename=csVM[i].ssh_key)

    for i in range(1, N):
        s = []
        common.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', common.addTextToList, s)
        cnfPath = s[0].strip()
        common.interactiveExec(ssh[i],
                               f'sudo find {cnfPath} -type f -exec sed -i "s/gtid_strict_mode/#gtid_strict_mode/" {x} \;',
                               common.printAndSaveText, logFile)

    common.interactiveExec(ssh[i], "sudo mcs cluster stop", common.printAndSaveText, logFile)

    for i in range(0, N):
        common.interactiveExec(ssh[i],
                               "sudo systemctl stop mariadb-columnstore-cmapi;"
                               "sudo systemctl stop mariadb-columnstore;"
                               "sudo systemctl stop mariadb",
                               common.printAndSaveText, logFile)

    common.printInfo("Upgrade")
    for i in range(0, N):
        common.printInfo("Removing old version")
        str = f"{os.path.dirname(os.path.realpath(__file__))}/remove_product.py " \
                  f"--machine-name {arguments.machine_name} " \
                  f"--machine {nodes[i]} " \
                  f"--mdbci-product {common.PRODS[arguments.product]} " \
                  f"--use-mdbci False --all-packages True " \
                  f"--include-columnstore True"
        common.printInfo(str)
        os.system(str)
        common.printInfo("Configure apt/yum/apt for repo with new version")
        common.runMdbci('setup_repo',
                        '--product', common.PRODS[arguments.product],
                        '--product-version', arguments.target_version,
                        arguments.machine_name + f'/{nodes[i]}')
        common.printInfo("Installing new version")
        str = f"{os.path.dirname(os.path.realpath(__file__))}/install_test_one_step.py " \
              f"--machine-name {arguments.machine_name} " \
              f"--machine {nodes[i]} " \
              f"--product {arguments.product} " \
              f"--target-version {arguments.target_version} " \
              f"--mariadb-version {common.majorVersion(arguments.target_version)} " \
              f"--architecture {csVM[i].architecture}"
        common.printInfo(str)
        os.system(str)

    common.printInfo("Restoring nodes after upgrade")
    for i in range(0, N):
        common.interactiveExec(ssh[i],
                               "sudo systemctl stop mariadb-columnstore;"
                               "sudo systemctl stop mariadb",
                               common.printAndSaveText, logFile)
    for i in range(0, N):
        common.interactiveExec(ssh[i],
                               "sudo systemctl start mariadb-columnstore-cmapi;"
                               "sudo systemctl start mariadb",
                               common.printAndSaveText, logFile)
    common.interactiveExec(ssh[i],
                           "sudo mariadb-upgrade --write-binlog;"
                           "sudo mcs cluster start",
                           common.printAndSaveText, logFile)
    for i in range(1, N):
        s = []
        common.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', common.addTextToList, s)
        cnfPath = s[0].strip()
        common.interactiveExec(ssh[i],
                               f'sudo find {cnfPath} -type f -exec sed -i "s/#gtid_strict_mode/gtid_strict_mode/" {x} \;',
                               common.printAndSaveText, logFile)

    x = requests.get(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/status",
                     headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                     verify=False)
    j = x.json()
    common.printInfo(json.dumps(j, indent=2))

    # checking data from status JSON:
    # number of nodes
    # master is only one node
    # master is readwrite
    # slaves are readonly
    # number of slaves is N - 1
    try:
        numJ = j['num_nodes']
        common.printInfo(f"Number of nodes after restore: {numJ}")
        if N != int(numJ):
            common.printInfo(f"Wrong number of nodes after upgrade")
            res = 1

        k = list(j.keys())
        ipRegex = re.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$")
        masters = 0
        slaves = 0
        for node in k:
            if ipRegex.match(node):
                if  j[node]['dbrm_mode'] == 'slave':
                    common.printInfo(f"node {node} is slave")
                    slaves = slaves + 1
                    if j[node]['cluster_mode'] != 'readonly':
                        res = 1
                        common.printInfo(f"Slave {node} is not readonly")
                if  j[node]['dbrm_mode'] == 'master':
                    common.printInfo(f"node {node} is master")
                    masters = masters + 1
                    if j[node]['cluster_mode'] != 'readwrite':
                        res = 1
                        common.printInfo(f"Master {node} is not readwrite")

        if masters != 1:
            res = 1
            common.printInfo(f"Wrong number of masters - {masters}")

        if slaves != N - 1:
            res = 1
            common.printInfo(f"Wrong number of slavess - {slaves}")
    except:
        common.printInfo("Error decoding of cluster status JSON")
        res = 1

    if arguments.target_version:
        for i in range(0, N):
            mariadbVersiom = common.getMariaDBVersion(ssh[i], startServer=False)
            common.printInfo(f"Server version after upgrade on the node {i + 1} is '{mariadbVersiom}'")
            if mariadbVersiom != arguments.target_version:
                common.printInfo(f"Server version on the node {i + 1} is wrong!")
                res = 1
        else:
            common.printInfo("Version is not checked")

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
