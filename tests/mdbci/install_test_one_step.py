#!/usr/bin/env python3

from argparse import ArgumentParser

import os
import shutil
import json
import subprocess

import common

# Maxscale
MAXSCALE_PACKAGES = 'maxscale'

# Connectors
CONNECTOR_PACKAGES = {
    'ConnectorCpp': 'mariadbcpp',
    'ConnectorC': 'mariadb-connector-c',
    'ConnectorODBC': 'mariadb-connector-odbc',
}

MARIADB_PASSWORD = "ct570c3521fCCR"


def getESPackages(machine, targetVersion, product, architecture, includeUnsupported):
    common.printInfo("Generate package names for installation")
    result = subprocess.Popen([f'{os.path.abspath(os.path.dirname(__file__))}/generate_plugins_list.py',
                               '--product', product,
                               '--target-version', targetVersion,
                               '--architecture', architecture,
                               '--platform', machine.platform,
                               '--platform-version', machine.platform_version,
                               '--include-unsupported', str(includeUnsupported),
                               '--use-mdbci-names', 'False'], stdout=subprocess.PIPE)
    stdout, _ = result.communicate()
    common.printInfo(stdout)
    packages = json.loads(stdout.decode("utf-8").strip())
    return " ".join(packages)


def checkPackages(machine, machineSSH, packages):
    packagesListExpected = packages.split(" ")
    cmd = common.LIST_PACKAGES_CMDS[common.PACKAGE_TYPE[common.DISTRO_CLASSES[machine.platform]]]
    allPackagesList = []
    common.interactiveExec(machineSSH, cmd,
                                     common.addTextToList, allPackagesList, get_pty=True)
    installedPackages = []
    for packageItem in allPackagesList:
        installedPackages.extend(packageItem.replace("\r\n", "\n").split("\n"))
    packagesListFile = open('results/all_packages_list', 'w+', errors='ignore')
    packagesListFile.write("\n".join(installedPackages))
    packagesListFile.close()
    res = set(packagesListExpected).issubset(set(installedPackages))
    if not res:
        common.printInfo("Wrong list of packages")
        common.printInfo(f"Expected {packagesListExpected}")
        common.printInfo(f"Pagackes sets intersection {set(installedPackages) & set(packagesListExpected)}")
        common.printInfo(f"Missing packages {set(packagesListExpected) - set(installedPackages)}")
    return res


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
                    type=common.stringArgumentToBool, default=False,
                    help="Try to install all packages including packages from 'unsupported' repositories")
parser.add_argument("--remove-mariadb-libs", type=common.stringArgumentToBool, default=True,
                    help="Remove mariadb_libs before installation (default True)")
parser.add_argument("--architecture", help="Processor architecture (x86_64 / aarch64)", default="x86_64")
parser.add_argument("--commands-dir", help="Save all commands to the file")

arguments = parser.parse_args()

common.setupMdbciEnvironment()

machine = common.Machine(arguments.machine_name, arguments.machine)
machineSSH = common.createSSH(machine)
machineCommand = common.createRunner(machineSSH)

shutil.rmtree('results', ignore_errors=True)
os.mkdir("results")


def installServer():
    commandsFile = common.createCommandsLogFile("commands.sh", arguments.commands_dir,
                                                additionalFileName="install_commands.sh",
                                                machineName=arguments.machine_name)
    build_log_file = open('results/install', 'w+', errors='ignore')
    if not arguments.target_version:
        clientVersion = arguments.mariadb_version
    else:
        clientVersion = arguments.target_version
    #Windows: attempt to install MariaDB.msi - file should be in the VM (setup_repo.py downloads it)
    if machine.platform in ['windows']:
        if arguments.product not in ['MariaDBEnterprise', 'MariaDBServerCommunity']:
            common.printInfo("Windows is not supported (yet)")
            return 0
        result = []
        execDir = "C:\\Program Files\\MariaDB"
        common.interactiveExec(machineSSH,
                               f"start /wait msiexec.exe /i MariaDB.msi PASSWORD={MARIADB_PASSWORD} "
                               f"INSTALLDIR=\"{execDir}\" "
                               f"SERVICENAME=MariaDB-Enterprise /qn /l*v .\msi-install-log.txt",
                               common.addTextToList, result, get_pty=False)
        mariadbCmd = common.getClientCmd(clientVersion, windows_extension=True)
        common.interactiveExec(machineSSH,
                               f"\"{execDir}\\bin\\{mariadbCmd}\" -uroot -p{MARIADB_PASSWORD} "
                               f"-e \"select @@version\" -B",
                               common.addTextToList, result, get_pty=False)
        machineSSH.close()
        version = common.extractVersion(result[-1].splitlines())
        if arguments.target_version:
            if version == arguments.target_version.split('-')[0]:
                return 0
            else:
                common.printInfo('Wrong version is installed! Test FAILED!')
                common.printInfo('Installed: {}'.format(version))
                common.printInfo('Expected: {}'.format(arguments.target_version))
                return 1
        else:
            common.printInfo('Target version is not defined, version check is not done')
            return 0
    # not Windows
    if arguments.remove_mariadb_libs:
        common.printInfo('Remove :mariadb-lib')
        cmd = common.RM_CMDS[common.DISTRO_CLASSES[machine.platform]] + ' mariadb-libs'
        common.printInfo(cmd)
        common.interactiveExec(machineSSH, cmd,
                                     common.printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    common.printInfo('Trying to install {}'.format(arguments.product))
    common.printInfo(f"{arguments.mariadb_version}, {arguments.product}, {arguments.architecture}, {arguments.include_unsupported}")
    packagesListString = getESPackages(machine, arguments.mariadb_version, arguments.product,
                                 arguments.architecture, arguments.include_unsupported)
    common.printInfo(packagesListString)
    if machine.platform in ['debian', 'ubuntu']:
        # TODO: check if repo is always "MariaDB Enterprise" (or try to get this repo name from somewhere)
        cmd = common.CMDS[common.DISTRO_CLASSES[machine.platform]] + ' -t "o=MariaDB Enterprise" ' + packagesListString
    else:
        cmd = common.CMDS[common.DISTRO_CLASSES[machine.platform]] + packagesListString
    common.printInfo(cmd)
    res = common.interactiveExec(machineSSH, cmd,
                                 common.printAndSaveText, build_log_file, get_pty=False, commandFile=commandsFile)
    build_log_file.close()
    # Ignore installation error on SLES 15
    if machine.platform == 'sles' and machine.platform_version == '15' and res in [104, 106]:
        res = 0
    build_log_file = open('results/maria_start', 'w+', errors='ignore')
    common.printInfo('Starting server')
    common.interactiveExec(machineSSH, common.getServerStartCmd(),
                           common.printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile)
    if common.interactiveExec(machineSSH,
                           "sudo systemctl status mariadb.service",
                           common.printAndSaveText, build_log_file, get_pty=False, commandFile=commandsFile) != 0:
        res = 1
        common.printInfo('Starting server error!')
    build_log_file.close()
    common.printInfo('Get version')
    build_log_file = open('results/version', 'w+', errors='ignore')
    if common.interactiveExec(machineSSH,
                               f"echo select @@version | sudo {common.getClientCmd(clientVersion)}",
                               common.printAndSaveText, build_log_file, get_pty=True, commandFile=commandsFile) != 0 :
        res = 1
        common.printInfo('Version retrieval error')
    build_log_file.close()
    if not checkPackages(machine, machineSSH, packagesListString):
        res = 1
    if res == 0:
        common.printInfo("install test for {} PASSED".format(arguments.product))
    else:
        common.printInfo("install test for {} FAILED".format(arguments.product))
    version = common.getMariaDBVersion(machineSSH, "results/version1", commandFile=commandsFile)
    if arguments.target_version and arguments.target_version != "distro":
        if version != arguments.target_version.split('-')[0]:
            common.printInfo('Wrong version is installed! Test FAILED!')
            common.printInfo('Installed: {}'.format(version))
            common.printInfo('Expected: {}'.format(arguments.target_version))
            res = 1
    else:
        common.printInfo('Target version is not defined, version check is not done')
    return res

def getMaxscalePackageFullVersion(machine, machineSSH):
    if common.PACKAGE_TYPE[common.DISTRO_CLASSES[machine.platform]] == 'DEB':
        versionList = []
        if common.interactiveExec(machineSSH, 'dpkg -l | grep -i "maxscale"',
                                  common.addTextToList, versionList, get_pty=True) != 0:
            return ''
        for l in versionList:
            ll = l.split()
            if ll[1] == 'maxscale':
                versionStr = ll[2]
                break;
        versionNum = versionStr.split('~')[0]
        versionPostfix = versionStr.split('-')[-1]
        return f'{versionNum}-{versionPostfix}'

    if common.PACKAGE_TYPE[common.DISTRO_CLASSES[machine.platform]] == 'RPM':
        versionList = []
        if common.interactiveExec(machineSSH, 'rpm -qa | grep -i "maxscale"',
                                 common.addTextToList, versionList, get_pty=True) != 0:
            return ''
        for l in versionList:
            ll = l.split('-')
            if ll[0] == 'maxscale' and l[len('maxscale') + 1:len('maxscale') + 2].isdigit() :
                versionStr = l[len('maxscale') + 1:]
                p = versionStr.find(next(filter(str.isalpha, versionStr)))
                break;
        return versionStr[:p - 1]
    return ''


def installMaxscale():
    build_log_file = open('results/install', 'w+', errors='ignore')
    common.printInfo('Trying to install {}'.format(arguments.product))
    cmd = common.CMDS[common.DISTRO_CLASSES[machine.platform]] + MAXSCALE_PACKAGES
    common.printInfo(cmd)
    res = common.interactiveExec(machineSSH, cmd,
                                 common.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    build_log_file = open('results/version', 'w+', errors='ignore')
    common.printInfo('Get version')
    common.interactiveExec(machineSSH, "maxscale --version",
                           common.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    if res == 0:
        common.printInfo("install test for {} PASSED".format(arguments.product))
    else:
        common.printInfo("install test for {} FAILED".format(arguments.product))
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
    common.printInfo(f'Maxscale package version {packageVersion}')
    if arguments.target_version:
        # if target_version is defined in the full package version format
        # (contains '-')
        # this script compares target_version with version from package name
        # (output of rpm or dpkg)
        if '-' in arguments.target_version:
            versionToCompare = packageVersion
            common.printInfo('Package version will be compared with target version')
        else:
            versionToCompare = version
            common.printInfo("'maxscale --version' will be compared with target version")
        if versionToCompare.split('-')[0] != arguments.target_version.split('-')[0]:
            common.printInfo('Wrong version is installed! Test FAILED!')
            common.printInfo('Installed: {}'.format(versionToCompare))
            common.printInfo('Expected: {}'.format(arguments.target_version))
            res = 1
    else:
        common.printInfo('Target version is not defined, version check is not done')
    return res


def installConnector(product):
    build_log_file = open('results/install', 'w+', errors='ignore')
    common.printInfo('Trying to install {}'.format(arguments.product))
    cmd = common.CMDS[common.DISTRO_CLASSES[machine.platform]] + CONNECTOR_PACKAGES[product]
    common.printInfo(cmd)
    res = common.interactiveExec(machineSSH, cmd,
                                 common.printAndSaveText, build_log_file, get_pty=True)
    build_log_file.close()
    return res


result = 1
if arguments.product in common.SERVER_PRODS:
    result = installServer()
if arguments.product in common.MAXSCALE_PRODS:
    result = installMaxscale()
if arguments.product in ['ConnectorODBC', 'ConnectorCpp', 'ConnectorC']:
    result = installConnector(arguments.product)

exit(result)
