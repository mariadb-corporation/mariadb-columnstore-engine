#!/usr/bin/env python3

# To be moved to the library to share between Columnstore and BuildBot repos

from argparse import ArgumentParser
import os
import subprocess
import sys
import pathlib

sys.path.append(pathlib.Path(__file__).parent.absolute())
import common

def runMdbci(*commands):
    process = subprocess.run(
        commands, stdout=sys.stdout, stderr=sys.stderr
    )
    return process.returncode


parser = ArgumentParser(description="Tool to install MariaDB plugins to VM using MDBCI")
parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI VM", default="build_vm")
parser.add_argument("--machine", help="Name of machine inside of MDBCI config",
                    default="build")
parser.add_argument("--mdbci-product", dest="mdbci_product", help="Name of plugin",
                    default="mdbe_plugin_mariadb_test")
parser.add_argument("--use-mdbci", dest="use_mdbci",
                    type=common.stringArgumentToBool, default=True,
                    help="Use MDBCI to remove, otherwise use package manager directly")
parser.add_argument("--all-packages", dest="all_packages",
                    type=common.stringArgumentToBool, default=False,
                    help="Try to remove all MariaDB-related packages")
parser.add_argument("--include-columnstore", dest="include_columnstore",
                    type=common.stringArgumentToBool, default=False,
                    help="Try to remove Columnstore")

arguments = parser.parse_args()

MDBCI_VM_PATH = os.path.expanduser("~/vms/")
os.environ["MDBCI_VM_PATH"] = MDBCI_VM_PATH
os.environ["PATH"] = os.environ["PATH"] + os.pathsep + os.path.expanduser('~/mdbci')

if arguments.use_mdbci:
    if runMdbci('mdbci', 'remove_product', '--product', arguments.mdbci_product,
                arguments.machine_name + '/' + arguments.machine) != 0:
        common.printInfo('MDBCI failed to install')
        exit(1)
else:
    res = 0
    machine = common.Machine(arguments.machine_name, arguments.machine)
    ssh = common.createSSH(machine)
    if not os.path.exists('results'):
        os.mkdir("results")
    remove_log_file = open('results/maria_remove', 'w+', errors='ignore')
    cmd = common.RM_CMDS[common.DISTRO_CLASSES[machine.platform]]
    if common.DISTRO_CLASSES[machine.platform] in ['apt']:
        package = 'mariadb-server'
        if arguments.all_packages:
            package ='mariadb-* galera* libmariadb3 rocksdb*'
    else:
        package = 'MariaDB-server'
        if arguments.all_packages:
            package ='MariaDB-* galera* mariadb-* libmariadb3 rocksdb*'
    mariadbClient = common.getClientCmd(common.getMariaDBVersion(ssh, startServer=False))

    common.interactiveExec(ssh, f'echo "UNINSTALL SONAME \'server_audit\';" | sudo {mariadbClient}',
                           common.printAndSaveText, remove_log_file, get_pty=False)
    common.interactiveExec(ssh, f'echo "SET GLOBAL innodb_fast_shutdown = 1;" | sudo {mariadbClient}',
                           common.printAndSaveText, remove_log_file, get_pty=False)
    common.interactiveExec(ssh, f'echo "XA RECOVER;" | sudo  {mariadbClient}',
                           common.printAndSaveText, remove_log_file, get_pty=False)
    if common.interactiveExec(ssh, f'sudo systemctl stop mariadb',
                           common.printAndSaveText, remove_log_file, get_pty=False) != 0 :
        res = 1
    uninstallRes = common.interactiveExec(ssh, f"{cmd} {package}",
                                       common.printAndSaveText, remove_log_file, get_pty=False)
    # Exclude zypper error 104 (not all packages are found)
    if uninstallRes != 0 and not (common.DISTRO_CLASSES[machine.platform] in ['zypper'] and uninstallRes == 104):
        common.printInfo(f"Uninstall failed! Package manager exit code is {uninstallRes}")
        res = 1
    if arguments.include_columnstore:
        if common.DISTRO_CLASSES[machine.platform] in ['apt']:
            common.interactiveExec(ssh, f"{cmd} mariadb-columnstore*",
                           common.printAndSaveText, remove_log_file, get_pty=False)
            common.interactiveExec(ssh, f"{cmd} mariadb-plugin-columnstore*",
                           common.printAndSaveText, remove_log_file, get_pty=False)
        else:
            common.interactiveExec(ssh, f"{cmd} MariaDB-columnstore*",
                           common.printAndSaveText, remove_log_file, get_pty=False)
    remove_log_file.close()
    exit(res)
