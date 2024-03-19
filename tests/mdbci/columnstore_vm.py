#!/usr/bin/env python3

from argparse import ArgumentParser
import json
import os

import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).parent.absolute()))
import common


def generateColumnstoreTemplate(N, product="mdbe", maxscaleProduct="MaxscaleProduction",
                                version="23.08", maxscaleVersion="23.08",
                                box="ubuntu_jammy_gcp", maxscaleBox="ubuntu_jammy_gcp"):
    template = {
        "max":
            {
                "hostname": "max",
                "box": maxscaleBox,
                "memory_size": "8192",
                "products": [
                    {
                        "name": "core_dump"
                    },
                    {
                        "name": common.PRODS[maxscaleProduct],
                        "version": maxscaleVersion
                    }
                ]
            }
    }
    for i in range(0, N):
        name = f"cs{i:03d}"
        template[name] = {
            "hostname": name,
            "box": box,
            "memory_size": "8192",
            "products": [
                {
                    "name": "core_dump"
                },
                {
                    "name": common.PRODS[product],
                    "version": version
                },
                {
                    "name": "plugin_columnstore",
                    "version": version
                },
                {
                    "name": "plugin_backup",
                    "version": version
                }
            ]
        }
    return template


def main():
    arguments = parseArguments()

    common.setupMdbciEnvironment()

    machine_name = arguments.machine_name.replace('/', '-')

    template_path = common.MDBCI_VM_PATH + machine_name + '.json'

    if arguments.destroy:
        if arguments.destroy_all:
            commands = ['destroy', '--all', os.path.expanduser(arguments.config_dir), '--force']
        else:
            commands = ['destroy', machine_name, '--force']
        if common.runMdbci(*commands) != 0:
            common.printInfo('MDBCI failed to destroy VM')
            if os.path.exists(template_path):
                os.remove(template_path)
            exit(1)
        else:
            common.printInfo('VM killed!')
            exit(0)

    template = generateColumnstoreTemplate(int(arguments.num),
                                           box=arguments.box,
                                           product=arguments.server_product,
                                           version=arguments.target,
                                           maxscaleBox=arguments.maxscale_box,
                                           maxscaleProduct=arguments.maxscale_product,
                                           maxscaleVersion=arguments.maxscale_target)
    if not arguments.subscription:
        for node in list(template.keys()):
            template[node]["box_parameters"] = {"configure_subscription_manager": "false"}

    with open(template_path, "w") as template_file:
        templateJson = json.dumps(template, indent=2)
        template_file.write(templateJson)

    common.printInfo(template)

    forceVersion = ''
    if arguments.forceVersion == 'True':
        forceVersion = '--force-version'

    attempts = 3
    delay = 2

    for attempt in range(attempts - 1):
        if common.runMdbci('generate', machine_name, '--template', template_path, '--override', forceVersion) != 0:
            print('MDBCI failed to generate VM', flush=True)
            common.runMdbci('destroy', machine_name)
            exit(1)

        if common.runMdbci('up', machine_name) != 0:
            print('MDBCI failed to bring VM up', flush=True)
            common.runMdbci('destroy', machine_name, '--keep-template')
            if attempt != attempts:
                for waiting in range(delay - 1):
                    print('Waiting ...', flush=True)
                    time.sleep(60)
        else:
            exit(0)

    print(f'MDBCI failed to bring VM up after {attempts} attempts! Give up!', flush=True)
    common.runMdbci('destroy', machine_name)
    exit(1)


def parseArguments():
    parser = ArgumentParser(description="Tool to create and destroy VM using MDBCI")
    parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI VM", default="build_vm")
    parser.add_argument("--box", dest="box", help="Name of MDBCI box", default="centos_7_gcp")
    parser.add_argument("--target", dest="target", help="Name of CI target (or version)")
    parser.add_argument("--destroy", dest="destroy", action='store_true', help="If true VM will be destroyed")
    parser.add_argument("--destroy-all", dest="destroy_all", help="Whether to destroy all machines in the directory",
                        default=False, action='store_true')

    parser.add_argument("--num", help="Number of cluster nodes")

    parser.add_argument("--force-version", dest="forceVersion",
                        help="disable smart searching for repo and install specified version", default='False')
    parser.add_argument("--filesystem", help="Filesystem for data (if defined, block device will added, "
                                             "but mounting and formatting can be done only from run_mtr.py")
    parser.add_argument("--subscription", help="If False, RHEL or SLES system will not be registered",
                        type=common.stringArgumentToBool, default=True)
    # used only by Columstore
    parser.add_argument("--server-product", help="MariaDB product "
                                                 "(MariaDBServerCommunity, MariaDBServerCommunityProduction, "
                                                 "MariaDBEnterpriseProduction or MariaDBEnterprise)",
                        default="MariaDBEnterpriseProduction")
    parser.add_argument("--maxscale-product",
                        help="Maxscale product (Maxscale, MaxscaleProduction, MaxscaleEnterprise)",
                        default="MaxscaleProduction")
    parser.add_argument("--maxscale-box", help="Maxscale box", default="ubuntu_jammy_gcp")
    parser.add_argument("--maxscale-target", help="Name of Maxscale CI target (or version)", default="23.08")

    return parser.parse_args()


if __name__ == "__main__":
    main()
