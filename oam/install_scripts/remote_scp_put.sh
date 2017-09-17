#!/usr/bin/expect
#
# $Id: remote_commend.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server root password
# Argument 3 - Command
set timeout 30
exec whoami >/tmp/whoami.tmp
set USERNAME [exec cat /tmp/whoami.tmp]
exec rm -f /tmp/whoami.tmp
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set FILE [lindex $argv 2]
set DEBUG [lindex $argv 3]
log_user $DEBUG
spawn -noecho /bin/bash

if { $PASSWORD == "ssh" } {
	set PASSWORD ""
}

# 
# send command
#
send "scp -v $FILE $USERNAME@$SERVER:$FILE\n"
expect {
	"Exit status 0" { exit 0 }
        "Exit status 1" { exit 1 }
        -re "100%"                      { send_user "DONE\n" ; sleep 2; exit 0 }
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" }
							-re "passphrase" { send "$PASSWORD\n" }
							}
						}
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	-re "Connection refused" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "Connection timed out" { send_user "FAILED: Connection timed out\n" ; exit 1 }
	-re "lost connection" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	-re "word: " { send "$PASSWORD\n" }
	-re "passphrase" { send "$PASSWORD\n" }
	-re "WARNING:" { send "rm -f /root/.ssh/known_hosts" ; exit 1 }
        -re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit 1 }
}
expect {
	"Exit status 0" { exit 0 }
        "Exit status 1" { exit 1 }
	-re "100%" 			{ send_user "DONE\n" ; sleep 2 ; exit 0 }
	-re "scp:"  					{ send_user "FAILED\n" ; exit 1 }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit 1 }
	-re "No such file or directory" { send_user "FAILED: Invalid file\n" ; exit 1 }
	-re "Connection refused" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
}
exit 0

