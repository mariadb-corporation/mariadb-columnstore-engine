#!/usr/bin/expect
#
# $Id: remote_commend.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server root password
# Argument 3 - Command
set timeout 10
exec whoami > whoami.tmp
set USERNAME [exec cat whoami.tmp]
exec rm -f whoami.tmp
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
#expect -re {[$#] }
send "scp -v $USERNAME@$SERVER:$FILE .\n"
expect {
	"Exit status 0" { exit 0 }
        "Exit status 1" { exit 1 }
        "100%"                                          { send_user "DONE\n" ; exit 0 }
	"authenticity" { send "yes\n" 
						expect {
							"word: " { send "$PASSWORD\n" }
							"passphrase" { send "$PASSWORD\n" }
							}
						}
	"service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	"Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	"Connection timed out" { send_user "FAILED: Connection timed out\n" ; exit 1 }
	"lost connection" { send_user "FAILED: Connection refused\n" ; exit 1 }
	"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	"word: " { send "$PASSWORD\n" }
	"passphrase" { send "$PASSWORD\n" }
        "scp:"   { send_user "FAILED\n" ; exit 1 }
        "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit 1 }
}
expect {
	"Exit status 0" { exit 0 }
        "Exit status 1" { exit 1 }
	"100%" 						{ send_user "DONE\n" ; exit 0 }
	"scp:"  					{ send_user "FAILED\n" ; exit 1 }
	"Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit 1 }
	"No such file or directory" { send_user "FAILED: No such file or directory\n" ; exit 1 }
	"Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
}
exit 0

