## Checking out the source

Since MCS is not meant to be built independently outside outside of MariaDB server, we should checkout the [server](https://github.com/MariaDB/server) code first.

Install git with your distro package manager

You can now clone from github with
	git clone git@github.com:MariaDB/server.git

or if you are not a github user,
	git clone https://github.com/MariaDB/server.git

MariaDB server contains many git submodules that need to be checked out with,

	git submodule update --init --recursive --depth=1

Then we should checkout Columnstore branch we want to build and install

	cd server/storage/columnstore/columnstore
	git checkout <columnstore-branch-you-want-to-build>
	git config --global --add safe.directory `pwd`
## Building phase

But for development convenience, we supply a script to build, install and run Mariadb server in MCS repo.

	sudo -E build/bootstrap_mcs.sh

Ubuntu:20.04/22.04, CentOS:7, Debian:10/11, RockyLinux:8 are supported with this script