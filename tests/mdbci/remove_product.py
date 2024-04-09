#!/usr/bin/env python3

from argparse import ArgumentParser

from mdb_buildbot_scripts.operations import host
from mdb_buildbot_scripts.operations import product_removing


def parseArguments():
    parser = ArgumentParser(description="Tool to install MariaDB plugins to VM using MDBCI")
    parser.add_argument("--machine-name", dest="machine_name", help="Name of MDBCI VM", default="build_vm")
    parser.add_argument("--machine", help="Name of machine inside of MDBCI config",
                        default="build")
    parser.add_argument("--mdbci-product", dest="mdbci_product", help="Name of plugin",
                        default="mdbe_plugin_mariadb_test")
    parser.add_argument("--use-mdbci", dest="use_mdbci",
                        type=host.stringArgumentToBool, default=True,
                        help="Use MDBCI to remove, otherwise use package manager directly")
    parser.add_argument("--all-packages", dest="all_packages",
                        type=host.stringArgumentToBool, default=False,
                        help="Try to remove all MariaDB-related packages")
    parser.add_argument("--include-columnstore", dest="include_columnstore",
                        type=host.stringArgumentToBool, default=False,
                        help="Try to remove Columnstore")

    return parser.parse_args()


if __name__ == "__main__":
    args = parseArguments()
    product_removing.removeProduct(
        machineName=args.machine_name,
        machine=args.machine,
        mdbciProduct=args.mdbci_product,
        useMdbci=args.use_mdbci,
        allPackages=args.all_packages,
        includeColumnstore=args.include_columnstore,
    )
