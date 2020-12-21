## Checking out the source

Since MCS is not meant to be built independently outside outside of MariaDB server, we should checkout the [server](https://github.com/MariaDB/server) code first.

You can clone from github with

	git clone git@github.com:MariaDB/server.git

or if you are not a github user,

	git clone https://github.com/MariaDB/server.git

branch 10.5 was suggested despite the fact that develop branch of the engine will eventually work with 10.6

	git checkout -b 10.5 origin/10.5

MariaDB server contains many git submodules that need to be checked out with,

	git submodule update --init --recursive --depth=1

This would be automatically done when you excute cmake, but consider we are focus MCS here, so dependencies of MCS should be installed first.

## Build Prerequisites

The list of Debian or RPM packages dependencies can be installed with:

	./install-deps.sh

## Building phase

After the dependencies had been installed, just follow the normal building instrutions to build, MCS will be compiled along with mariadbd.

But for development convenience, we supply a script to build and run Mariadb server in MCS repo.

	# TODO
	
