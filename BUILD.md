## Checking out the source

Since MCS is not meant to be built independently outside outside of MariaDB server, we should checkout the [server](https://github.com/MariaDB/server) code first.

You can clone from github with

	git clone git@github.com:MariaDB/server.git

or if you are not a github user,

	git clone https://github.com/MariaDB/server.git

The MCS engine repo has a number of develop-X branches where X is a number and it equals with the last number of MariaDB server major release, e.g develop-6 must be used with 10.6 of the server. There is single exception, namely develop branch of the engine must be compile with a current develop version of the server.

	git checkout -b 10.6 origin/10.6

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
	
