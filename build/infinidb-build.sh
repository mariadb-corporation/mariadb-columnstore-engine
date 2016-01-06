#!/bin/bash

infinidbBranch=$1
mysqlBranch=$2

if [ -z "$infinidbBranch" ]; then
        echo Usage: $0 infinidb-branch mysql-branch 1>&2
        exit 1
fi

if [ "$infinidbBranch" != "current" ]; then
	if [ -z "$mysqlBranch" ]; then
	        echo Usage: $0 infinidb-branch mysql-branch  1>&2
        	exit 1
	fi
else
	mysqlBranch=$1
fi

{
if [ "$infinidbBranch" != "current" ]; then
	rm -rf InfiniDB-MySQL
	rm -rf InfiniDB
	rm -rf InfiniDB-Enterprise

	git clone https://github.com/mariadb-corporation/InfiniDB
	git clone https://github.com/mariadb-corporation/InfiniDB-MySQL
	git clone https://github.com/mariadb-corporation/InfiniDB-Enterprise
fi

echo "****** Compile InfiniDB-MySQL"
cd InfiniDB-MySQL
if [ "$infinidbBranch" != "current" ]; then
	git checkout $mysqlBranch
fi
./build-MySQL $mysqlBranch

echo "****** Compile InfiniDB"
cd ../InfiniDB
if [ "$infinidbBranch" != "current" ]; then
	git checkout $infinidbBranch
fi
./build/bootstrap
make -j4
make -j4
make install

echo "****** Compile InfiniDB-Enterprise"
cd ../InfiniDB-Enterprise
if [ "$infinidbBranch" != "current" ]; then
	git checkout $infinidbBranch
fi
make -j4
make -j4
make install

echo "****** Generate RPMS"
cd ../InfiniDB
./build/build_rpms
} > infinidb-build-$infinidbBranch.log 2>&1
 
echo "InfiniDB Build Completed for $infinidbBranch"
exit 0

