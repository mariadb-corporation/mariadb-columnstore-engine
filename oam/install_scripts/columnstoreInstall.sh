#!/usr/bin/expect
#
# $Id$
#
# Install Package on system

set timeout 30
set USERNAME "root"
set RPMVERSION " "
set PASSWORD " "
set MYSQLPASSWORD dummymysqlpw
set PACKAGE " "
set CONFIGFILE " "
set DEBUG 0
set NODEPS "-h"
set INSTALLDIR "@CMAKE_INSTALL_PREFIX@"

spawn -noecho /bin/bash

for {set i 0} {$i<[llength $argv]} {incr i} {
	set arg($i) [lindex $argv $i]
}

set i 0
while true {
	if { $i == [llength $argv] } { break }
	if { $arg($i) == "-h" } {
		send_user "\n"
		send_user "'calpontInstaller.sh' performs a system install of the Calpont InfiniDB Packages\n"
		send_user "from the /root/ directory. These Packages would have already been installed\n"
		send_user "on the local Module.\n"
		send_user "Usage: calpontInstaller.sh -v 'infinidb-version' -p 'password' -t 'package-type' -c 'config-file'-m 'mysql-password' -d\n"
		send_user "		infinidb-version - InfiniDB Version, i.e. 1.0.0-1\n"
		send_user "		password    	- root password on the servers being installed'\n"
		send_user "		package-type 	- Package Type being installed (rpm, deb, or binary)\n"
		send_user "		config-file 	- Optional: Columnstore.xml config file with directory location, i.e. /root/Columnstore.xml\n"
		send_user "			 	 	Default version is $INSTALLDIR/etc/Columnstore.xml.rpmsave\n"
		send_user "		mysql-password    - MySQL password on the servers being installed'\n"		
		send_user "		-d		- Debug flag, output verbose information\n"
		exit 0
	} elseif { $arg($i) == "-v" } {
		incr i
		set RPMVERSION $arg($i)
	} elseif { $arg($i) == "-p" } {
		incr i
		set PASSWORD $arg($i)
	} elseif { $arg($i) == "-t" } {
		incr i
		set PACKAGE $arg($i)
	} elseif { $arg($i) == "-c" } {
		incr i
		set CONFIGFILE $arg($i)
	} elseif { $arg($i) == "-d" } {
		set DEBUG 1
	} elseif { $arg($i) == "-f" } {
		set NODEPS "--nodeps"
	} elseif { $arg($i) == "-m" } {
		incr i
		set MYSQLPASSWORD $arg($i)
	} elseif { $arg($i) == "-i" } {
		incr i
		set INSTALLDIR $arg($i)
	} elseif { $arg($i) == "-u" } {
		incr i
		set USERNAME $arg($i)
	}
	incr i
}

log_user $DEBUG

set timeout 2
send "$INSTALLDIR/bin/infinidb status\n"
expect {
        "is running"	{ puts "InfiniDB is running, can't run calpontInstall.sh while InfiniDB is running. Exiting..\n";
						exit 1
					}
}

if { $CONFIGFILE == " " } {
	set CONFIGFILE $INSTALLDIR/etc/Columnstore.xml.rpmsave
}

if { [catch { open $CONFIGFILE "r"} handle ] } {
	puts "Calpont Config file not found: $CONFIGFILE"; exit 1
}

exec rm -f $INSTALLDIR/etc/Columnstore.xml.new  > /dev/null 2>&1
exec mv -f $INSTALLDIR/etc/Columnstore.xml $INSTALLDIR/etc/Columnstore.xml.new  > /dev/null 2>&1
exec /bin/cp -f $CONFIGFILE  $INSTALLDIR/etc/Columnstore.xml  > /dev/null 2>&1

set timeout 2
set INSTALL 2
send "$INSTALLDIR/bin/getConfig DBRM_Controller NumWorkers\n"
expect {
        1                         { set INSTALL 1
										set PASSWORD "dummy"
										set RPMVERSION "rpm" }
}


if { $INSTALL == "2" && $PASSWORD == " "} {puts "please enter the remote server root password, enter ./calpontInstaller.sh -h for additional info"; exit 1}

if { $INSTALL == "2" && $RPMVERSION == " " } {puts "please enter Package version, enter ./calpontInstaller.sh -h for additional info"; exit 1}

send_user "\n"

if { $INSTALL == "2" } {
	if { $PACKAGE == "rpm" } {
		set CALPONTPACKAGE1 /root/infinidb-libs-$RPMVERSION*.rpm
		set CALPONTPACKAGE2 /root/infinidb-platform-$RPMVERSION*.rpm
		set CALPONTPACKAGE3 /root/infinidb-enterprise-$RPMVERSION*.rpm
		set CONNECTORPACKAGE1 /root/infinidb-mysql-$RPMVERSION*.rpm
		set CONNECTORPACKAGE2 /root/infinidb-storage-engine-$RPMVERSION*.rpm
		send_user "Installing InfiniDB Packages: $CALPONTPACKAGE1, $CALPONTPACKAGE2, $CALPONTPACKAGE3, $CONNECTORPACKAGE1, $CONNECTORPACKAGE2\n\n"
		set EEPKG "rpm"
	} elseif { $PACKAGE == "deb" } {
		set CALPONTPACKAGE1 /root/infinidb-libs_$RPMVERSION*.deb
		set CALPONTPACKAGE2 /root/infinidb-platform_$RPMVERSION*.deb
		set CALPONTPACKAGE3 /root/infinidb-enterprise_$RPMVERSION*.deb
		set CONNECTORPACKAGE1 /root/infinidb-mysql_$RPMVERSION*.deb
		set CONNECTORPACKAGE2 /root/infinidb-storage-engine_$RPMVERSION*.deb
		send_user "Installing InfiniDB Packages: $CALPONTPACKAGE1, $CALPONTPACKAGE2, $CALPONTPACKAGE3, $CONNECTORPACKAGE1, $CONNECTORPACKAGE2\n\n"
		set EEPKG "deb"
	} elseif { $PACKAGE == "binary" } {
		set CALPONTPACKAGE /root/infinidb-ent-$RPMVERSION*bin.tar.gz
		set CONNECTORPACKAGE1 "nopkg"
		set CONNECTORPACKAGE2 "nopkg"
		send_user "Installing InfiniDB Package: $CALPONTPACKAGE\n\n"
		set EEPKG "binary"
	} else {
		puts "please enter Valid Package Type, enter ./calpontInstaller.sh -h for additional info"; exit 1
	}
} else {
	set CALPONTPACKAGE1 "dummy.rpm"
	set CALPONTPACKAGE2 "dummy.rpm"
	set CALPONTPACKAGE3 "dummy.rpm"
	set CONNECTORPACKAGE1 "dummy.rpm"
	set CONNECTORPACKAGE2 "dummy.rpm"
	set EEPKG "rpm"
}

send_user "Performing InfiniDB System Install, please wait...\n"

send "$INSTALLDIR/bin/setConfig -d Installation EEPackageType $EEPKG\n" 
expect {
	-re {[$#] }                  {  }
}

send_user "\n"
set timeout 600
#
# Run installer
#
send "$INSTALLDIR/bin/installer $CALPONTPACKAGE1 $CALPONTPACKAGE2 $CALPONTPACKAGE3 $CONNECTORPACKAGE1 $CONNECTORPACKAGE2 initial $PASSWORD n $NODEPS  $MYSQLPASSWORD $DEBUG\n"
expect {
	"InfiniDB Install Successfully Completed" 	{ }
	"ERROR"   { send_user "FAILED: error returned from installer, execute with debug mode on to determine error\n" ; exit 1 }
	"Enter MySQL password"   { send_user "FAILED: a MySQL password is set\n" ; exit 1 }	
	timeout	{ send_user "FAILED: Timeout while running installer, execute with debug mode on to determine error\n" ; exit 1 }
}

send_user "\nCalpont Package System Install Completed\n\n"

exit 0
# vim:ts=4 sw=4:

