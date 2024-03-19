#!/usr/bin/env python3

# pip3 install paramiko scp python-util

# DOC: https://mariadb.com/docs/server/deploy/topologies/columnstore-shared-local-storage/enterprise-server-23-08/

from argparse import ArgumentParser
import os
import common
import requests
import json
import time

REPL_PASS = 'ct570c3521fCCR#'
REPL_USER = 'repl'
MAX_PASS = 'ct570c3521fCCR#ef'
MAX_USER = 'max'
UTIL_PASS = 'ect436c3rf34f5#ct570c3521fCCR'
UTIL_USER = 'util'
IP_KEY = '93816fa66cc2d8c224e62275bd4f248234dd4947b68d4af2b29671dd7d5532dd'

SERVER_CONF = """log_error                              = mariadbd.err
character_set_server                   = utf8
collation_server                       = utf8_general_ci
log_bin                                = mariadb-bin
#log_bin_index                          = mariadb-bin.index
#relay_log                              = mariadb-relay
#relay_log_index                        = mariadb-relay.index
log_slave_updates                      = ON
gtid_strict_mode                       = ON
log-slave-updates
plugin_maturity=unknown
binlog-format=row
binlog_row_image=full
"""

SYS_CONF = """# minimize swapping
vm.swappiness = 1

# Increase the TCP max buffer size
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216

# Increase the TCP buffer limits
# min, default, and max number of bytes to use
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# don't cache ssthresh from previous connection
net.ipv4.tcp_no_metrics_save = 1

# for 1 GigE, increase this to 2500
# for 10 GigE, increase this to 30000
net.core.netdev_max_backlog = 30000
"""

MAXSCALE_CONFIG = """[maxscale]
threads          = auto
admin_host       = 0.0.0.0
admin_secure_gui = false
"""

MAX_USER_GRANTS = [
    "SHOW DATABASES ON *.*",
    "SELECT ON mysql.columns_priv",
    "SELECT ON mysql.db",
    "SELECT ON mysql.procs_priv",
    "SELECT ON mysql.proxies_priv",
    "SELECT ON mysql.roles_mapping",
    "SELECT ON mysql.tables_priv",
    "SELECT ON mysql.user",
    "BINLOG ADMIN, READ_ONLY ADMIN, RELOAD, REPLICA MONITOR, REPLICATION MASTER ADMIN, "
    "REPLICATION REPLICA ADMIN, REPLICATION REPLICA, SHOW DATABASES, SELECT ON *.*",
]


def createGlusterFS(nodes, csVM, ssh, N):
    l = open('gluster_setup.log', "w")
    for i in range(0, N):
        pkgmgrOpt = ''
        if csVM[i].platform in ['centos', 'rhel', 'rocky'] and csVM[i].platform_version in ['7']:
            common.interactiveExec(ssh[i],
                                   f'{common.CMDS[common.DISTRO_CLASSES[csVM[i].platform]]} centos-release-gluster',
                                   common.printAndSaveText, l, echoCommand=True)
        if csVM[i].platform in ['centos', 'rocky', 'rhel'] and csVM[i].platform_version in ['8', '9']:
            pkgmgrOpt = "--enablerepo=powertools"

        if csVM[i].platform in ['debian']:
            common.interactiveExec(ssh[i], 'wget -O - https://download.gluster.org/pub/gluster/glusterfs/11/rsa.pub '
                                           '| sudo apt-key add -;'
                                           "DEBID=$(grep 'VERSION_ID=' /etc/os-release | cut -d '=' -f 2 | tr -d '\"');"
                                           "DEBVER=$(grep 'VERSION=' /etc/os-release | grep -Eo '[a-z]+');"
                                           "DEBARCH=$(dpkg --print-architecture);"
                                           "echo deb https://download.gluster.org/pub/gluster/glusterfs/LATEST/"
                                           "Debian/${DEBID}/${DEBARCH}/apt ${DEBVER} main | "
                                           "sudo tee /etc/apt/sources.list.d/gluster.list",
                                   common.printAndSaveText, l, echoCommand=True)

        if csVM[i].platform in ['centos', 'rocky']:
            common.interactiveExec(ssh[i], f'{common.CMDS[common.DISTRO_CLASSES[csVM[i].platform]]} {pkgmgrOpt} '
                                           f'centos-release-gluster10',
                                   common.printAndSaveText, l, echoCommand=True)
        glusterPackage = 'glusterfs-server'
        common.interactiveExec(ssh[i], f'{common.CMDS[common.DISTRO_CLASSES[csVM[i].platform]]} {pkgmgrOpt} '
                                       f'{glusterPackage}',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'sudo systemctl start glusterd',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'sudo systemctl enable glusterd',
                               common.printAndSaveText, l, echoCommand=True)

    for i in range(1, N):
        common.interactiveExec(ssh[0], f'sudo gluster peer probe {nodes[i]}', common.printAndSaveText, l,
                               echoCommand=True)

    common.interactiveExec(ssh[1], f'sudo gluster peer probe {nodes[0]}', common.printAndSaveText, l,
                           echoCommand=True)
    for i in range(0, N):
        for j in range(0, N):
            common.interactiveExec(ssh[i], f'sudo mkdir -p /brick/data{j + 1}', common.printAndSaveText, l,
                                   echoCommand=True)
    for i in range(0, N):
        str = ''
        for j in range(0, N):
            str = str + f' {nodes[j]}:/brick/data{i + 1}'
        common.interactiveExec(ssh[0], f'sudo gluster volume create data{i + 1} replica {N} {str} force',
                               common.printAndSaveText, l, echoCommand=True)
    for i in range(0, N):
        common.interactiveExec(ssh[0], f'sudo gluster volume start data{i + 1}',
                               common.printAndSaveText, l, echoCommand=True)
    for i in range(0, N):
        for j in range(0, N):
            common.interactiveExec(ssh[i], f'sudo mkdir -p /var/lib/storage/data{j + 1}', common.printAndSaveText,
                                   l, echoCommand=True)
            common.interactiveExec(ssh[i],
                                   f'echo 127.0.0.1:data{j + 1} /var/lib/storage/data{j + 1} '
                                   f'glusterfs defaults,_netdev 0 0 | '
                                   f'sudo tee -a /etc/fstab',
                                   common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'sudo mount -a', common.printAndSaveText, l, echoCommand=True)
    for i in range(0, N):
        common.interactiveExec(ssh[i], f'sudo chown mysql:mysql -R /var/lib/storage',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], f'sudo mv /var/lib/columnstore/data1/* /var/lib/storage/data1/;'
                                       f'sudo rmdir /var/lib/columnstore/data1',
                               common.printAndSaveText, l, echoCommand=True)
        for j in range(0, N):
            common.interactiveExec(ssh[i],
                                   f'sudo -u mysql ln -s /var/lib/storage/data{j + 1} /var/lib/columnstore/data{j + 1}',
                                   common.printAndSaveText, l, echoCommand=True)
    l.close()


def noStorage(nodes, csVM, ssh, N):
    l = open('nostorage_setup.log', "w")
    for i in range(0, N):
        for j in range(0, N):
            common.interactiveExec(ssh[i],
                                   f'sudo mkdir -p /var/lib/columnstore/data{j + 1}',
                                   common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i],
                               'sudo chown mysql:mysql /var/lib/columnstore -R -L;'
                               'sudo chmod 755 /var/lib/columnstore -R',
                               common.printAndSaveText, l, echoCommand=True)
        l.close()


def setupMaxscale(nodes, csVM, ssh, N):
    l = open('maxscale_setup.log', "w")
    # Put maxscale.cnf
    common.interactiveExec(ssh[N], f'sudo systemctl stop maxscale;'
                                   f'sudo rm -rf /etc/maxscale.cnf*;'
                                   f'printf "{MAXSCALE_CONFIG}" | sudo tee /etc/maxscale.cnf',
                           common.printAndSaveText, l, echoCommand=True)
    # start Maxscale
    common.interactiveExec(ssh[N], "sudo systemctl start maxscale",
                           common.printAndSaveText, l, echoCommand=True)
    # create servers
    serversStr = ""
    for i in range(0, N):
        common.interactiveExec(ssh[N], f'sudo maxctrl create server cs{i:03d} {csVM[i].ip_address}',
                               common.printAndSaveText, l, echoCommand=True)
        serversStr = f"{serversStr} cs{i:03d}"
    # create monitor
    common.interactiveExec(ssh[N], f"sudo maxctrl create monitor columnstore_monitor mariadbmon "
                                   f"user={MAX_USER} password='{MAX_PASS}' "
                                   f"replication_user={REPL_USER} replication_password='{REPL_PASS}' "
                                   f"--servers {serversStr}",
                           common.printAndSaveText, l, echoCommand=True)
    # create router
    common.interactiveExec(ssh[N], f"sudo maxctrl create service query_router_service readwritesplit  "
                                   f"user={MAX_USER} password='{MAX_PASS}' "
                                   f"--servers {serversStr}",
                           common.printAndSaveText, l, echoCommand=True)
    # create listener
    common.interactiveExec(ssh[N], f"sudo maxctrl create listener query_router_service "
                                   f"query_router_listener 4006  protocol=MariaDBClient",
                           common.printAndSaveText, l, echoCommand=True)
    # check status
    common.interactiveExec(ssh[N], f"sudo maxctrl show maxscale;"
                                   f"sudo maxctrl show servers;"
                                   f"sudo maxctrl show services;",
                           common.printAndSaveText, l, echoCommand=True)
    l.close()


def createUsers(ssh, N):
    l = open('create_users.log', "w")
    common.interactiveExec(ssh[0], f'echo "CREATE USER \'{UTIL_USER}\'@\'127.0.0.1\' IDENTIFIED BY'
                                   f' \'{UTIL_PASS}\'" | sudo mariadb;'
                                   f'echo "GRANT SELECT, PROCESS ON *.* TO \'{UTIL_USER}\'@\'127.0.0.1\'" '
                                   f'| sudo mariadb;',
                           common.printAndSaveText, l, echoCommand=True)
    common.interactiveExec(ssh[0], f'echo "CREATE USER \'{MAX_USER}\'@\'%\' IDENTIFIED BY'
                                   f' \'{MAX_PASS}\'" | sudo mariadb;',
                           common.printAndSaveText, l, echoCommand=True)
    for grant in MAX_USER_GRANTS:
        common.interactiveExec(ssh[0], f'echo "GRANT {grant} TO \'{MAX_USER}\'@\'%\'" '
                                       f'| sudo mariadb;', common.printAndSaveText, l, echoCommand=True)
    l.close()


def setupReplication(ssh, N, csVM):
    l = open('setup_replication.log', "w")
    common.interactiveExec(ssh[0], 'echo "RESET MASTER" | sudo mariadb;'
                                   'echo "SET GLOBAL gtid_slave_pos=\'0-1-0\'" | sudo mariadb',
                           common.printAndSaveText, l, echoCommand=True)
    common.interactiveExec(ssh[0], f'echo "CREATE USER \'{REPL_USER}\'@\'%\' IDENTIFIED BY \'{REPL_PASS}\'" '
                                   '| sudo mariadb;'
                                   'echo "GRANT REPLICA MONITOR, REPLICATION REPLICA, '
                                   'REPLICATION REPLICA ADMIN, REPLICATION MASTER ADMIN '
                                   f'ON *.* TO \'{REPL_USER}\'@\'%\'" | sudo mariadb',
                           common.printAndSaveText, l, echoCommand=True)
    for i in range(1, N):
        common.interactiveExec(ssh[i], "sudo systemctl start mariadb",
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'echo "STOP REPLICA" | sudo mariadb;'
                                       'echo "RESET MASTER" | sudo mariadb;'
                                       'echo "SET GLOBAL gtid_slave_pos=\'0-1-0\'" | sudo mariadb',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], f'echo "CHANGE MASTER TO MASTER_HOST=\'{csVM[0].ip_address}\', '
                                       f'MASTER_USER=\'{REPL_USER}\', MASTER_PASSWORD=\'{REPL_PASS}\', '
                                       f'MASTER_USE_GTID=slave_pos" | sudo mariadb',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'echo "START REPLICA" | sudo mariadb; '
                                       'echo "SET GLOBAL read_only=ON" | sudo mariadb',
                               common.printAndSaveText, l, echoCommand=True)
        l.close()


storageInit = {
    "nostorage": noStorage,
    "gluster": createGlusterFS,
}


def main():
    res = 0
    script_dir = os.path.abspath(os.path.dirname(__file__))

    arguments = parseArguments()

    if arguments.storage not in list(storageInit.keys()):
        common.printInfo("Unknown storage type")
        exit(1)

    common.setupMdbciEnvironment()
    csVM = []
    hosts = []
    N = int(arguments.num)

    nodes = []
    for i in range(0, N):
        nodes.append(f'cs{i:03d}')
    nodes.append('max')

    l = open('columnstore_bootstrap.log', "w")

    for i in range(0, N + 1):
        csVM.append(common.Machine(arguments.machine_name, nodes[i]))
        hosts.append(f'{csVM[i].ip_address}\t{nodes[i]}')

    hostsStr = '\n'.join(hosts)
    common.printInfo(hostsStr)

    # Open ssh connections, disable security, create /etc/hosts
    ssh = []
    for i in range(0, N + 1):
        ssh.append(common.loadSSH())
        ssh[i].connect(csVM[i].ip_address, username=csVM[i].ssh_user, key_filename=csVM[i].ssh_key)
        common.interactiveExec(ssh[i], f'printf "{hostsStr}" | sudo tee -a /etc/hosts', common.printAndSaveText, l,
                               echoCommand=True)
        common.interactiveExec(ssh[i], f'printf "{SYS_CONF}" | '
                                       f'sudo tee /etc/sysctl.d/90-mariadb-enterprise-columnstore.conf',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'sudo systemctl disable apparmor; '
                                       'sudo ufw disable; '
                                       'sudo setenforce permissive;'
                                       'sudo localedef -i en_US -f UTF-8 en_US.UTF-8;'
                                       'sudo iptables -I INPUT -j ACCEPT',
                               common.printAndSaveText, l, echoCommand=True)
        common.interactiveExec(ssh[i], 'sudo sysctl --load=/etc/sysctl.d/90-mariadb-enterprise-columnstore.conf',
                               common.printAndSaveText, l, echoCommand=True)

    storageInit[arguments.storage](nodes, csVM, ssh, N)
    common.interactiveExec(ssh[i], "ls -la /var/lib/columnstore", common.printAndSaveText, l,
                           echoCommand=True)

    # Create MariaDB .cnf files, initialize MariaDB DB
    for i in range(0, N):
        s = []
        common.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', common.addTextToList, s)
        cnfPath = s[0].strip()
        common.printInfo(f"Path to my.cnf.d '{cnfPath}'")
        common.interactiveExec(ssh[i], f'sudo systemctl stop mariadb; '
                                       f'sudo systemctl stop mariadb-columnstore;'
                                       f'sudo rm -rf /var/lib/mysql;'
                                       f'echo "[server]" | sudo tee {cnfPath}/server{i + 1}.cnf; '
                                       f'echo "bind-address  = {csVM[i].ip_address}" '
                                       f'| sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                       f'echo "server-id  = {i + 1}" '
                                       f'| sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                       f'printf "{SERVER_CONF}" | sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                       f'sudo mariadb-install-db; sudo chown mysql:mysql -R /var/lib/mysql;'
                                       f'sudo systemctl start mariadb',
                               common.printAndSaveText, l, echoCommand=True)

    for i in range(0, N):
        common.interactiveExec(ssh[i],
                               'sudo chown mysql:mysql /var/lib/columnstore -R -L; '
                               'sudo chmod 755 /var/lib/columnstore -R',
                               common.printAndSaveText, l, echoCommand=True)

    setupReplication(ssh, N, csVM)
    createUsers(ssh, N)

    # Start CMAPI
    for i in range(0, N):
        common.interactiveExec(ssh[i], 'sudo systemctl stop mariadb-columnstore;'
                                       'sudo systemctl start mariadb-columnstore-cmapi;'
                                       'sudo systemctl enable mariadb-columnstore-cmapi',
                               common.printAndSaveText, l, echoCommand=True)

    # Configure Columnstore
    for i in range(0, N):
        common.interactiveExec(ssh[i], 'sudo mcsSetConfig CrossEngineSupport Host 127.0.0.1;'
                                       'sudo mcsSetConfig CrossEngineSupport Port 3306;'
                                       f'sudo mcsSetConfig CrossEngineSupport User {UTIL_USER}',
                               common.printAndSaveText, l, echoCommand=True)

    for i in range(0, N):
        common.interactiveExec(ssh[i], f"sudo mcsSetConfig CrossEngineSupport Password '{UTIL_PASS}'",
                               common.printAndSaveText, l, echoCommand=True)

    # Start Columnstore cluster
    for i in range(0, N):
        x = requests.put(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/node",
                         headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                         json={"timeout": 120, "node": f"{csVM[i].ip_address}"}, verify=False)
        common.printInfo(json.dumps(x.json(), indent=2))
        time.sleep(10)

    x = requests.get(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/status",
                     headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                     verify=False)
    common.printInfo(json.dumps(x.json(), indent=2))

    setupMaxscale(nodes, csVM, ssh, N)

    for i in range(0, N + 1):
        ssh[i].close()

    l.close()

    exit(res)


def parseArguments():
    parser = ArgumentParser()
    parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI configuration")
    parser.add_argument("--num", help="Number of Columstore nodes")
    parser.add_argument("--storage", help="Type of storage", choices=list(storageInit.keys()), default="nostorage")

    return parser.parse_args()


if __name__ == "__main__":
    main()
