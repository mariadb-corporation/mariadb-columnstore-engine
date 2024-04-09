#!/usr/bin/env python3

from argparse import ArgumentParser

import os
import shutil

from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import ssh_tools
from mdb_buildbot_scripts.operations import mdbci
from mdb_buildbot_scripts.operations import install
from mdb_buildbot_scripts.constants import cli_commands
from mdb_buildbot_scripts.constants import products_names

# Maxscale
MAXSCALE_PACKAGES = 'maxscale'

# Connectors
CONNECTOR_PACKAGES = {
    'ConnectorCpp': 'mariadbcpp',
    'ConnectorC': 'mariadb-connector-c',
    'ConnectorODBC': 'mariadb-connector-odbc',
}


parser = ArgumentParser(description="Install test")
parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI VM config",
                    default="build_vm")
parser.add_argument("--machine", help="Name of machine inside of MDBCI config",
                    default="build")
parser.add_argument("--product", dest="product", help="'MariaDBEnterprise' or 'MariaDBServerCommunity'",
                    default="MariaDBEnterprise")
parser.add_argument("--target-version", dest="target_version", help="Version to be installed")
parser.add_argument("--mariadb-version", dest="mariadb_version", help="Major version of the server")
parser.add_argument("--include-unsupported", dest="include_unsupported",
                    type=host.stringArgumentToBool, default=False,
                    help="Try to install all packages including packages from 'unsupported' repositories")
parser.add_argument("--remove-mariadb-libs", type=host.stringArgumentToBool, default=True,
                    help="Remove mariadb_libs before installation (default True)")
parser.add_argument("--architecture", help="Processor architecture (x86_64 / aarch64)", default="x86_64")
parser.add_argument("--commands-dir", help="Save all commands to the file")

arguments = parser.parse_args()

mdbci.setupMdbciEnvironment()

machine = mdbci.Machine(arguments.machine_name, arguments.machine)
machineSSH = mdbci.createSSH(machine)
machineCommand = ssh_tools.createRunner(machineSSH)

shutil.rmtree('results', ignore_errors=True)
os.mkdir("results")


def getMaxscalePackageFullVersion(machine, machineSSH):
    if cli_commands.PACKAGE_TYPE[cli_commands.DISTRO_CLASSES[machine.platform]] == 'DEB':
        versionList = []
        if ssh_tools.interactiveExec(machineSSH, 'dpkg -l | grep -i "maxscale"',
                                     ssh_tools.addTextToList, versionList, get_pty=True) != 0:
            return ''
        for l in versionList:
            ll = l.split()
            if ll[1] == 'maxscale':
                versionStr = ll[2]
                break
        versionNum = versionStr.split('~')[0]
        versionPostfix = versionStr.split('-')[-1]
        return f'{versionNum}-{versionPostfix}'

    if cli_commands.PACKAGE_TYPE[cli_commands.DISTRO_CLASSES[machine.platform]] == 'RPM':
        versionList = []
        if ssh_tools.interactiveExec(machineSSH, 'rpm -qa | grep -i "maxscale"',
                                     ssh_tools.addTextToList, versionList, get_pty=True) != 0:
            return ''
        for l in versionList:
            ll = l.split('-')
            if ll[0] == 'maxscale' and l[len('maxscale') + 1:len('maxscale') + 2].isdigit():
                versionStr = l[len('maxscale') + 1:]
                p = versionStr.find(next(filter(str.isalpha, versionStr)))
                break
        return versionStr[:p - 1]
    return ''


def installMaxscale():
    build_log_file = open('results/install', 'w+', errors='ignore')
    host.printInfo('Trying to install {}'.format(arguments.product))
    cmd = cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + MAXSCALE_PACKAGES
    host.printInfo(cmd)
    res = ssh_tools.interactiveExec(machineSSH, cmd,
                                    ssh_tools.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    build_log_file = open('results/version', 'w+', errors='ignore')
    host.printInfo('Get version')
    ssh_tools.interactiveExec(machineSSH, "maxscale --version",
                              ssh_tools.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    if res == 0:
        host.printInfo("install test for {} PASSED".format(arguments.product))
    else:
        host.printInfo("install test for {} FAILED".format(arguments.product))
    # Ignore installation error on SLES 15
    if machine.platform == 'sles' and machine.platform_version == '15':
        res = 0
    versionFile = open("results/version", "r")
    versionFullString = versionFile.read().split(" ")
    if len(versionFullString) > 1:
        version = versionFullString[1].strip()
    else:
        version = ''
    versionFile.close()
    packageVersion = getMaxscalePackageFullVersion(machine, machineSSH)
    host.printInfo(f'Maxscale package version {packageVersion}')
    if arguments.target_version:
        # if target_version is defined in the full package version format
        # (contains '-')
        # this script compares target_version with version from package name
        # (output of rpm or dpkg)
        if '-' in arguments.target_version:
            versionToCompare = packageVersion
            host.printInfo('Package version will be compared with target version')
        else:
            versionToCompare = version
            host.printInfo("'maxscale --version' will be compared with target version")
        if versionToCompare.split('-')[0] != arguments.target_version.split('-')[0]:
            host.printInfo('Wrong version is installed! Test FAILED!')
            host.printInfo('Installed: {}'.format(versionToCompare))
            host.printInfo('Expected: {}'.format(arguments.target_version))
            res = 1
    else:
        host.printInfo('Target version is not defined, version check is not done')
    return res


def installConnector(product):
    build_log_file = open('results/install', 'w+', errors='ignore')
    host.printInfo('Trying to install {}'.format(arguments.product))
    cmd = cli_commands.CMDS[cli_commands.DISTRO_CLASSES[machine.platform]] + CONNECTOR_PACKAGES[product]
    host.printInfo(cmd)
    res = ssh_tools.interactiveExec(machineSSH, cmd,
                                    ssh_tools.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    return res


result = 1
if arguments.product in products_names.SERVER_PRODS:
    result = install.installServer(
        machine=machine, machineSSH=machineSSH, machineName=arguments.machine_name,
        targetVersion=arguments.target_version, mariadbVersion=arguments.mariadb_version,
        product=arguments.product, commandsDir=arguments.commands_dir,
        removeMariadbLibs=arguments.remove_mariadb_libs, architecture=arguments.architecture,
        includeUnsupported=arguments.include_unsupported
    )
if arguments.product in products_names.MAXSCALE_PRODS:
    result = installMaxscale()
if arguments.product in ['ConnectorODBC', 'ConnectorCpp', 'ConnectorC']:
    result = installConnector(arguments.product)

exit(result)
