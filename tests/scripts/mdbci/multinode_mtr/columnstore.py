#!/usr/bin/env python3

# pip3 install paramiko scp python-util

# DOC: https://mariadb.com/docs/server/deploy/topologies/columnstore-shared-local-storage/enterprise-server-23-08/

from argparse import ArgumentParser
import os
import requests
import json
import time

from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import ssh_tools
from mdb_buildbot_scripts.operations import mdbci
from mdb_buildbot_scripts.constants import platform_names
from mdb_buildbot_scripts.constants import cli_commands

import columnstore_operations
from columnstore_operations import IP_KEY
from columnstore_operations import REPL_PASS
from columnstore_operations import REPL_USER
from columnstore_operations import MAX_PASS
from columnstore_operations import MAX_USER
from columnstore_operations import UTIL_PASS
from columnstore_operations import UTIL_USER



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

LISTENER = """[query_router_listener_socket]
type=listener
service=query_router_service
protocol=MariaDBClient
socket=/var/run/maxscale/maxscale.sock
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

CENTOS_GLUSTER_REPOS = {
    '7': {
        platform_names.AMD64_ARCH: "https://buildlogs.centos.org/centos/7/storage/x86_64/gluster-9",
        platform_names.AMD64_RPM_ALIAS: "https://buildlogs.centos.org/centos/7/storage/x86_64/gluster-9",
        platform_names.AARCH64_ARCH: "https://buildlogs.centos.org/centos/7/storage/aarch64/gluster-9",
        platform_names.PPC64_ARCH: "https://buildlogs.centos.org/centos/7/storage/ppc64le/gluster-9",
    },
    '8': {
        platform_names.AMD64_ARCH: "https://buildlogs.centos.org/centos/8-stream/storage/x86_64/gluster-11",
        platform_names.AMD64_RPM_ALIAS: "https://buildlogs.centos.org/centos/8-stream/storage/x86_64/gluster-11",
        platform_names.AARCH64_ARCH: "https://buildlogs.centos.org/centos/8-stream/storage/aarch64/gluster-11",
        platform_names.PPC64_ARCH: "https://buildlogs.centos.org/centos/8-stream/storage/ppc64le/gluster-11",
    },
    '9': {
        platform_names.AMD64_ARCH: "https://buildlogs.centos.org/centos/9-stream/storage/x86_64/gluster-11",
        platform_names.AMD64_RPM_ALIAS: "https://buildlogs.centos.org/centos/9-stream/storage/x86_64/gluster-11",
        platform_names.AARCH64_ARCH: "https://buildlogs.centos.org/centos/9-stream/storage/aarch64/gluster-11",
        platform_names.PPC64_ARCH: "https://buildlogs.centos.org/centos/9-stream/storage/ppc64le/gluster-11",
    },
}



def createGlusterFS(nodes, csVM, ssh, N):
    l = open('gluster_setup.log', "w")
    print("Install and configure Gluster")
    for i in range(0, N):
        glusterPackage = 'glusterfs-server'
        pkgmgrOpt = ''
        if csVM[i].platform in ['centos', 'rocky', 'alma'] and csVM[i].platform_version in ['7']:
            glusterPackage = 'gluster-server'
            ssh_tools.interactiveExec(ssh[i],
                                      f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[csVM[i].platform]]} '
                                      f'centos-release-gluster',
                                      ssh_tools.printAndSaveText, l, echoCommand=True)
        if (csVM[i].platform in ['centos', 'rocky', 'rhel', 'alma'] and csVM[i].platform_version in ['8', '9']) \
                or (csVM[i].platform in ['rhel'] and csVM[i].platform_version in ['7']):
            glusterPackage = " glusterfs glusterfs-cli glusterfs-server "
            if csVM[i].platform not in ['rhel']:
                if csVM[i].platform_version in ['8']:
                    ssh_tools.interactiveExec(ssh[i],
                                              f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[csVM[i].platform]]} '
                                              f'dnf-plugins-core',
                                              ssh_tools.printAndSaveText, l, echoCommand=True)
                    pkgmgrOpt = "--enablerepo=powertools"
                else:
                    pkgmgrOpt = "--enablerepo=crb"
            if csVM[i].platform in ['rhel'] and \
                    csVM[i].platform_version in ['8'] and \
                    csVM[i].architecture in [platform_names.AARCH64_ARCH]:
                pkgmgrOpt = "--enablerepo=codeready-builder-for-rhel-8-rhui-rpms"
            repoLink = CENTOS_GLUSTER_REPOS[csVM[i].platform_version][csVM[i].architecture]
            print(repoLink)
            repoFile = f"""[gluster]
name=Gluster
baseurl={repoLink}
enabled=1
gpgcheck=0
"""
            for line in repoFile.splitlines():
                ssh_tools.interactiveExec(ssh[i],
                                      f'echo "{line}" | sudo tee -a /etc/yum.repos.d/gluster.repo',
                                      ssh_tools.printAndSaveText, l, echoCommand=True)

        if csVM[i].platform in ['debian']:
            ssh_tools.interactiveExec(ssh[i],
                                      'wget -O - https://download.gluster.org/pub/gluster/glusterfs/11/rsa.pub '
                                      '| sudo apt-key add -;'
                                      "DEBID=$(grep 'VERSION_ID=' /etc/os-release | cut -d '=' -f 2 | tr -d '\"');"
                                      "DEBVER=$(grep 'VERSION=' /etc/os-release | grep -Eo '[a-z]+');"
                                      "DEBARCH=$(dpkg --print-architecture);"
                                      "echo deb https://download.gluster.org/pub/gluster/glusterfs/LATEST/"
                                      "Debian/${DEBID}/${DEBARCH}/apt ${DEBVER} main | "
                                      "sudo tee /etc/apt/sources.list.d/gluster.list",
                                      ssh_tools.printAndSaveText, l, echoCommand=True)

        if ssh_tools.interactiveExec(ssh[i],
                                  f'{cli_commands.CMDS[cli_commands.DISTRO_CLASSES[csVM[i].platform]]} {pkgmgrOpt} '
                                  f'{glusterPackage}',
                                  ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
            return 1
        if ssh_tools.interactiveExec(ssh[i], 'sudo systemctl start glusterd',
                                  ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
            return 1
        ssh_tools.interactiveExec(ssh[i], 'sudo systemctl enable glusterd',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    for i in range(1, N):
        if ssh_tools.interactiveExec(ssh[0], f'sudo gluster peer probe {nodes[i]}', ssh_tools.printAndSaveText,
                                  l, echoCommand=True) !=0:
            return 1

    if ssh_tools.interactiveExec(ssh[1], f'sudo gluster peer probe {nodes[0]}', ssh_tools.printAndSaveText, l,
                              echoCommand=True) !=0:
        return 1
    for i in range(0, N):
        for j in range(0, N):
            if ssh_tools.interactiveExec(ssh[i], f'sudo mkdir -p /brick/data{j + 1}', ssh_tools.printAndSaveText,
                                      l, echoCommand=True) != 0:
                return 1
    for i in range(0, N):
        str = ''
        for j in range(0, N):
            str = str + f' {nodes[j]}:/brick/data{i + 1}'
        if ssh_tools.interactiveExec(ssh[0], f'sudo gluster volume create data{i + 1} replica {N} {str} force',
                                  ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
            return 1
    for i in range(0, N):
       if  ssh_tools.interactiveExec(ssh[0], f'sudo gluster volume start data{i + 1}',
                                  ssh_tools.printAndSaveText, l, echoCommand=True) !=0 :
           return 1
    for i in range(0, N):
        for j in range(0, N):
            ssh_tools.interactiveExec(ssh[i], f'sudo mkdir -p /var/lib/storage/data{j + 1}',
                                      ssh_tools.printAndSaveText,
                                      l, echoCommand=True)
            ssh_tools.interactiveExec(ssh[i],
                                      f'echo 127.0.0.1:data{j + 1} /var/lib/storage/data{j + 1} '
                                      f'glusterfs defaults,_netdev 0 0 | '
                                      f'sudo tee -a /etc/fstab',
                                      ssh_tools.printAndSaveText, l, echoCommand=True)
        if ssh_tools.interactiveExec(ssh[i], 'sudo mount -a', ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
            return 1
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i], f'sudo chown mysql:mysql -R /var/lib/storage',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], f'sudo mv /var/lib/columnstore/data1/* /var/lib/storage/data1/;'
                                          f'sudo rmdir /var/lib/columnstore/data1',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        for j in range(0, N):
            if ssh_tools.interactiveExec(ssh[i],
                                         f'sudo -u mysql ln -s /var/lib/storage/data{j + 1} '
                                         f'/var/lib/columnstore/data{j + 1}',
                                         ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
                return 1
    l.close()
    return 0


def noStorage(nodes, csVM, ssh, N):
    l = open('nostorage_setup.log', "w")
    for i in range(0, N):
        for j in range(0, N):
            ssh_tools.interactiveExec(ssh[i],
                                      f'sudo mkdir -p /var/lib/columnstore/data{j + 1}',
                                      ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i],
                                  'sudo chown mysql:mysql /var/lib/columnstore -R -L;'
                                  'sudo chmod 755 /var/lib/columnstore -R',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
    l.close()
    return 0


def setupMaxscale(nodes, csVM, ssh, N):
    l = open('maxscale_setup.log', "w")
    # Put maxscale.cnf
    ssh_tools.interactiveExec(ssh[N], f'sudo systemctl stop maxscale;'
                                      f'sudo rm -rf /etc/maxscale.cnf*;'
                                      f'printf "{MAXSCALE_CONFIG}" | sudo tee /etc/maxscale.cnf',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    # start Maxscale
    ssh_tools.interactiveExec(ssh[N], "sudo systemctl start maxscale",
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    # create servers
    serversStr = ""
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[N], f'sudo maxctrl create server cs{i:03d} {csVM[i].ip_address}',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        serversStr = f"{serversStr} cs{i:03d}"
    # create monitor
    ssh_tools.interactiveExec(ssh[N], f"sudo maxctrl create monitor columnstore_monitor mariadbmon "
                                      f"user={MAX_USER} password='{MAX_PASS}' "
                                      f"replication_user={REPL_USER} replication_password='{REPL_PASS}' "
                                      f"--servers {serversStr}",
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    # create router
    ssh_tools.interactiveExec(ssh[N], f"sudo maxctrl create service query_router_service readwritesplit  "
                                      f"user={MAX_USER} password='{MAX_PASS}' enable_root_user=true "
                                      f"--servers {serversStr}",
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    # create listener
    ssh_tools.interactiveExec(ssh[N], f"sudo maxctrl create listener query_router_service "
                                      f"query_router_listener 4006  protocol=MariaDBClient ",
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    """
    # create one more listener with a socket
    for line in LISTENER.splitlines():
        ssh_tools.interactiveExec(ssh[N],
                                  f'echo "{line}" | sudo tee -a /etc/maxscale.cnf',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[N],
                              f'sudo systemctl restart maxscale',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    """

    # check status
    ssh_tools.interactiveExec(ssh[N], f"sudo maxctrl show maxscale;"
                                      f"sudo maxctrl show servers;"
                                      f"sudo maxctrl show services;"
                                      f"sudo maxctrl list listeners;",
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    l.close()


def createUsers(ssh, N):
    l = open('create_users.log', "w")
    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'{UTIL_USER}\'@\'127.0.0.1\' IDENTIFIED BY'
                                      f' \'{UTIL_PASS}\'" | sudo mariadb;'
                                      f'echo "GRANT SELECT, PROCESS ON *.* TO \'{UTIL_USER}\'@\'127.0.0.1\'" '
                                      f'| sudo mariadb;',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'{MAX_USER}\'@\'%\' IDENTIFIED BY'
                                      f' \'{MAX_PASS}\'" | sudo mariadb;',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'{MAX_USER}\'@\'localhost\' IDENTIFIED BY'
                                      f' \'{MAX_PASS}\'" | sudo mariadb;',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    for grant in MAX_USER_GRANTS:
        ssh_tools.interactiveExec(ssh[0], f'echo "GRANT {grant} TO \'{MAX_USER}\'@\'%\'" '
                                          f'| sudo mariadb;', ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[0], f'echo "GRANT {grant} TO \'{MAX_USER}\'@\'localhost\'" '
                                          f'| sudo mariadb;', ssh_tools.printAndSaveText, l, echoCommand=True)

    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'testUser\'@\'%\' IDENTIFIED BY'
                                      f' \'{MAX_PASS}\'" | sudo mariadb;',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'testUser\'@\'localhost\' IDENTIFIED BY'
                                      f' \'{MAX_PASS}\'" | sudo mariadb;',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "GRANT ALL PRIVILEGES   ON *.*   TO \'testUser\'@\'%\' '
                                      f'WITH GRANT OPTION" '
                                      f'| sudo mariadb;', ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "GRANT ALL PRIVILEGES   ON *.*   TO \'testUser\'@\'localhost\' '
                                      f'WITH GRANT OPTION" '
                                      f'| sudo mariadb;', ssh_tools.printAndSaveText, l, echoCommand=True)
    l.close()


def setupReplication(ssh, N, csVM):
    l = open('setup_replication.log', "w")
    ssh_tools.interactiveExec(ssh[0], 'echo "RESET MASTER" | sudo mariadb;'
                                      'echo "SET GLOBAL gtid_slave_pos=\'0-1-0\'" | sudo mariadb',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    ssh_tools.interactiveExec(ssh[0], f'echo "CREATE USER \'{REPL_USER}\'@\'%\' IDENTIFIED BY \'{REPL_PASS}\'" '
                                      '| sudo mariadb;'
                                      'echo "GRANT REPLICA MONITOR, REPLICATION REPLICA, '
                                      'REPLICATION REPLICA ADMIN, REPLICATION MASTER ADMIN '
                                      f'ON *.* TO \'{REPL_USER}\'@\'%\'" | sudo mariadb',
                              ssh_tools.printAndSaveText, l, echoCommand=True)
    for i in range(1, N):
        ssh_tools.interactiveExec(ssh[i], "sudo systemctl start mariadb",
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], 'echo "STOP REPLICA" | sudo mariadb;'
                                          'echo "RESET MASTER" | sudo mariadb;'
                                          'echo "SET GLOBAL gtid_slave_pos=\'0-1-0\'" | sudo mariadb',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], f'echo "CHANGE MASTER TO MASTER_HOST=\'{csVM[0].ip_address}\', '
                                          f'MASTER_USER=\'{REPL_USER}\', MASTER_PASSWORD=\'{REPL_PASS}\', '
                                          f'MASTER_USE_GTID=slave_pos" | sudo mariadb',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], 'echo "START REPLICA" | sudo mariadb; '
                                          'echo "SET GLOBAL read_only=ON" | sudo mariadb',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
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
        host.printInfo("Unknown storage type")
        exit(1)

    mdbci.setupMdbciEnvironment()
    csVM = []
    hosts = []
    N = int(arguments.num)

    nodes = []
    for i in range(0, N):
        nodes.append(f'cs{i:03d}')
    nodes.append('max')

    l = open('columnstore_bootstrap.log', "w")
    ssh = []
    IPs = {}
    for i in range(0, N + 1):
        csVM.append(mdbci.Machine(arguments.machine_name, nodes[i]))
        ssh.append(ssh_tools.loadSSH())
        ssh[i].connect(csVM[i].ip_address, username=csVM[i].ssh_user, key_filename=csVM[i].ssh_key)
    for i in range(0, N + 1):
        if arguments.aws:
            intIPList = []
            ssh_tools.interactiveExec(ssh[i], 'curl http://169.254.169.254/latest/meta-data/local-ipv4 --silent',
                                      ssh_tools.addTextToList, intIPList)
            hosts.append(f'{intIPList[0]}\t{nodes[i]}')
            print(intIPList)
            IPs[nodes[i]] = intIPList[0]
        else:
            hosts.append(f'{csVM[i].ip_address}\t{nodes[i]}')
            IPs[nodes[i]] = csVM[i].ip_address

    hostsStr = '\n'.join(hosts)
    host.printInfo(hostsStr)

    # Open ssh connections, disable security, create /etc/hosts

    for i in range(0, N + 1):
        ssh_tools.interactiveExec(ssh[i], f'printf "{hostsStr}" | sudo tee -a /etc/hosts',
                                  ssh_tools.printAndSaveText, l,
                                  echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], f'printf "{SYS_CONF}" | '
                                          f'sudo tee /etc/sysctl.d/90-mariadb-enterprise-columnstore.conf',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], 'sudo systemctl disable apparmor; '
                                          'sudo ufw disable; '
                                          'sudo setenforce permissive;'
                                          'sudo localedef -i en_US -f UTF-8 en_US.UTF-8;'
                                          'sudo iptables -I INPUT -j ACCEPT',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i],
                                  'sudo sysctl --load=/etc/sysctl.d/90-mariadb-enterprise-columnstore.conf',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    if storageInit[arguments.storage](nodes, csVM, ssh, N) != 0:
        exit(1)
    ssh_tools.interactiveExec(ssh[i], "ls -la /var/lib/columnstore", ssh_tools.printAndSaveText, l,
                              echoCommand=True)

    # Create MariaDB .cnf files, initialize MariaDB DB
    for i in range(0, N):
        s = []
        ssh_tools.interactiveExec(ssh[i], 'sudo find /etc/ | grep "my\.cnf\.d$"', ssh_tools.addTextToList, s)
        cnfPath = s[0].strip()
        host.printInfo(f"Path to my.cnf.d '{cnfPath}'")
        ssh_tools.interactiveExec(ssh[i], f'sudo systemctl stop mariadb; '
                                          f'sudo systemctl stop mariadb-columnstore;'
                                          #f'sudo rm -rf /var/lib/mysql;'
                                          f'echo "[server]" | sudo tee {cnfPath}/server{i + 1}.cnf; '
                                          #f'echo "bind-address  = {csVM[i].ip_address}" '
                                          #f'| sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                          f'echo "server-id  = {i + 1}" '
                                          f'| sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                          f'printf "{SERVER_CONF}" | sudo tee -a {cnfPath}/server{i + 1}.cnf;'
                                          #f'sudo mariadb-install-db; sudo chown mysql:mysql -R /var/lib/mysql;'
                                          f'sudo systemctl start mariadb',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i],
                                  'sudo chown mysql:mysql /var/lib/columnstore -R -L; '
                                  'sudo chmod 755 /var/lib/columnstore -R',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    setupReplication(ssh, N, csVM)
    createUsers(ssh, N)

    # Start CMAPI
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i], 'sudo systemctl stop mariadb-columnstore',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)
        if ssh_tools.interactiveExec(ssh[i], 'sudo systemctl start mariadb-columnstore-cmapi',
                                  ssh_tools.printAndSaveText, l, echoCommand=True) != 0:
            ssh_tools.interactiveExec(ssh[i], 'sudo journalctl -xeu mariadb-columnstore-cmapi.service',
                                      ssh_tools.printAndSaveText, l, echoCommand=True)
        ssh_tools.interactiveExec(ssh[i], 'sudo systemctl enable mariadb-columnstore-cmapi',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    # Configure Columnstore
    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i], 'sudo mcsSetConfig CrossEngineSupport Host 127.0.0.1;'
                                          'sudo mcsSetConfig CrossEngineSupport Port 3306;'
                                          f'sudo mcsSetConfig CrossEngineSupport User {UTIL_USER}',
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    for i in range(0, N):
        ssh_tools.interactiveExec(ssh[i], f"sudo mcsSetConfig CrossEngineSupport Password '{UTIL_PASS}'",
                                  ssh_tools.printAndSaveText, l, echoCommand=True)

    # Start Columnstore cluster
    for i in range(0, N):
        x = requests.put(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/node",
                         headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                         json={"timeout": 120, "node": f"{IPs[nodes[i]]}"}, verify=False)
        host.printInfo(json.dumps(x.json(), indent=2))
        time.sleep(10)

    x = requests.get(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/status",
                     headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                     verify=False)
    host.printInfo(json.dumps(x.json(), indent=2))

    if columnstore_operations.checkColumnstoreCluster(IP_KEY, N, csVM) != 0:
        res = 1

    setupMaxscale(nodes, csVM, ssh, N)

    for i in range(0, N + 1):
        ssh[i].close()

    l.close()
    exit(res)


def parseArguments():
    parser = ArgumentParser()
    parser.add_argument("--machine-name", dest="machine_name",
                        help="Name of MDBCI configuration", type=host.machineName)
    parser.add_argument("--num", help="Number of Columstore nodes")
    parser.add_argument("--storage", help="Type of storage", choices=list(storageInit.keys()), default="nostorage")
    parser.add_argument("--aws", help="Set to True if VMs are in AWS", type=host.stringArgumentToBool, default=False)

    return parser.parse_args()


if __name__ == "__main__":
    main()
