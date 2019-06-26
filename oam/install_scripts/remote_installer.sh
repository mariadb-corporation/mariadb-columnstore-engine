#!/usr/bin/expect
#
# $Id: remote_installer.sh 2925 2012-05-24 18:58:17Z dhill $
#
# Remote Install RPM and custom OS files from postConfigure script
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Package name being installed
# Argument 5 - Debug flag 1 for on, 0 for off
set timeout 10
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set RPMPACKAGE [lindex $argv 3]
set DEBUG [lindex $argv 4]
log_user $DEBUG
spawn -noecho /bin/bash
# 
# erase package
#
send_user "Perform Old Calpont Package erased from Module"
expect -re "# "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps calpont'\n"
# accept the remote host fingerprint (assuming never logged in before)
expect -re "service not known" { send_user "           FAILED: Invalid Host\n" ; exit }
expect -re "authenticity" { send "yes\n" }
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "           DONE" }
	-re "# "                  { send_user "           DONE" }
	-re "not installed"       { send_user "           FAILED: Package not installed" }
	-re "Failed dependencies" { send_user "           FAILED: Failed dependencies\n" ; exit }
	-re "Permission denied"   { send_user "           FAILED: Invalid password\n" ; exit }
}
send_user "\n"
# 
# send the package
#
send_user "Copy New Calpont Package to Module"
send "scp $RPMPACKAGE  $USERNAME@$SERVER:$RPMPACKAGE\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	-re "100%" 				{ send_user "                       DONE" }
	-re "# " 				{ send_user "                       DONE" }
	-re "scp"  				{ send_user "                       FAILED\n" ; 
				 			send_user "\n*** Installation Failed\n" ; 
							exit }
	-re "Permission denied"         { send_user "                       FAILED: Invalid password\n" ; exit }
	-re "No such file or directory" { send_user "                       FAILED: Invalid package\n" ; exit }
}
send_user "\n"
#sleep to make sure it's finished
sleep 5
#
# install package
#
set timeout 30
send_user "Perform New Calpont Package install on Module"
send "ssh $USERNAME@$SERVER ' rpm -ivh $RPMPACKAGE'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "completed" 		  { send_user "            DONE" }
	-re "# " 				  { send_user "            DONE" }
	-re "Failed dependencies" { send_user "            FAILED: Failed dependencies\n" ; 
								send_user "\n*** Installation Failed\n" ; 
									exit }
	-re "error" 			  { send_user "            FAILED\n" ; 
							    send_user "\n*** Installation Failed\n" ; 
								exit }
	-re "Permission denied"   { send_user "            FAILED: Invalid password\n" ; exit }
}
set timeout 10
send_user "\n"
#
# copy over custom OS files
#
send_user "Copy Custom OS files to Module"
send "scp @CMAKE_INSTALL_PREFIX@/local/etc/$MODULE/*  $USERNAME@$SERVER:/etc/.\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 		  		  { send_user "            DONE" }
	-re "Permission denied"   { send_user "            FAILED: Invalid password\n" ; exit }
}
#
send_user "\n!!!Installation Successfully Completed!!!\n"
exit

