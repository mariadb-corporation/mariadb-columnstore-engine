#!/usr/bin/expect
#
# $Id$
#
# Parent OAM Installer, copy RPM's and custom OS files from postConfigure script
# Argument 0 - Parent OAM IP address
# Argument 1 - Root Password of Parent OAM Module
# Argument 2 - Calpont Config File
# Argument 3 - Debug flag 1 for on, 0 for off

set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
#set PACKAGE [lindex $argv 2]
set PACKAGE *.rpm
set RELEASE [lindex $argv 3]
set CONFIGFILE [lindex $argv 4]
set USERNAME [lindex $argv 5]
set CEFLAG [lindex $argv 6]
set PACKAGENAME [lindex $argv 7]
set INSTALLLOCATION [lindex $argv 8]
set DEBUG [lindex $argv 9]

set INSTALLDIR "/usr/local"

set SHARED "//srvhill01/shared"

log_user $DEBUG
spawn -noecho /bin/bash

set CONFIG "Calpont"
if { $PACKAGENAME != "columnstore"} {
	set CONFIG "Calpont"
}


set timeout 10
send "ssh $USERNAME@$SERVER 'rm -f /root/$INSTALLLOCATION-*.rpm /root/infinidb*.rpm'\n"
expect {
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" }
							}
						}
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit -1 }
	-re "word: " { send "$PASSWORD\n" }
}
expect {
	-re {[$#] } 						{ }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
}
# 
# erase InfiniDB packages
#
set timeout 60
send_user "Erase Old InfiniDB Packages                   "
send "ssh $USERNAME@$SERVER 'pkill -9 mysqld'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re {[$#] }                  { }
}
send "ssh $USERNAME@$SERVER 'rpm -e --nodeps \$(rpm -qa | grep '^mariadb-columnstore') >/dev/null 2>&1; rpm -e --nodeps \$(rpm -qa | grep '^infinidb-')'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" {  }
	-re {[$#] }                  {  }
	-re "not installed"       {  }
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
sleep 10
send "ssh $USERNAME@$SERVER 'rm -f $INSTALLDIR/$INSTALLLOCATION/releasenum >/dev/null 2>&1; test -x $INSTALLDIR/$INSTALLLOCATION/bin/pre-uninstall && $INSTALLDIR/$INSTALLLOCATION/bin/pre-uninstall'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re {[$#] }                  { send_user "DONE" }
	-re "uninstall completed" { send_user "DONE" }
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
sleep 5
# 
# get the InfiniDB package
#
send_user "Get InfiniDB Packages                         "
send "smbclient $SHARED -WMARIADB -Uroot%Calpont1 -c 'cd packages/$RELEASE/;prompt OFF;mget $PACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "FAILED: $PACKAGE not found\n" ; exit -1 }
	-re "getting" 				 { send_user "DONE" }
}
send_user "\n"
# 
# send the InfiniDB package
#
set timeout 120
send_user "Copy InfiniDB Packages                        "
send "scp -q $PACKAGE $USERNAME@$SERVER:/root/.\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
expect {
	-re {[$#] } 						{ send_user "DONE" }
	-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
}
send_user "\n"
#send "rm -f $PACKAGE\n"
#
# install InfiniDB package
#
send_user "Install New InfiniDB Packages                 "
send "ssh $USERNAME@$SERVER ' rpm -iv --nodeps --force infinidb-*'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "completed" { expect {
        			  -re "completed" { expect {
        						-re "completed" { send_user "DONE" }
						}
				}
			}
	}
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; 
								send_user "\n*** Installation Failed\n" ; 
									exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
set timeout 120
expect -re {[$#] }
#
if { $CONFIGFILE != "NULL"} {
	#
	# copy over Calpont.xml file
	#
	send "scp $CONFIGFILE $USERNAME@$SERVER:/usr/local/$INSTALLLOCATION/etc/$CONFIG.xml\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{  }
		-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
		-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
		-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
	}
	send_user "Copy InfiniDB Configuration File              "
	send "scp $CONFIGFILE $USERNAME@$SERVER:/usr/local/$INSTALLLOCATION/etc/$CONFIG.xml.rpmsave\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{ send_user "DONE"}
		-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
		-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
		-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
	}
	#do a dummy scp command
	send "scp $CONFIGFILE $USERNAME@$SERVER:/tmp/$CONFIG.xml\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{ send_user " " }
	}
} else {
	#
	# rename previous installed config file
	#
	send_user "Copy RPM-saved InfiniDB Configuration File     "
	send "ssh $USERNAME@$SERVER 'cd /usr/local/$INSTALLLOCATION/etc/;mv -f Calpont.xml Calpont.xml.install;cp -v Calpont.xml.rpmsave Calpont.xml'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Calpont.xml"         { send_user "DONE" }
		-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
	}
}
send_user "\n"
#
exit

