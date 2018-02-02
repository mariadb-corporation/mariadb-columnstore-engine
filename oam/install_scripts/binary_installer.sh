#!/usr/bin/expect
#
# $Id$
#
# Install RPM and custom OS files on system
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - User Password of remote server
# Argument 4 - Package name being installed
# Argument 6 - Install Type, "initial", "upgrade", "uninstall"
# Argument 7 - Server type?
# Argument 8 - Debug flag 1 for on, 0 for off
# Argument 9 - install dir (optional)
# Argument 10 - user name (optional)
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set CALPONTPKG [lindex $argv 3]
set INSTALLTYPE [lindex $argv 4]
set AMAZONINSTALL [lindex $argv 5]
set PKGTYPE "binary"
set DEBUG [lindex $argv 6]
set INSTALLDIR "/usr/local/mariadb/columnstore"
set IDIR [lindex $argv 7]
if { $IDIR != "" } {
	set INSTALLDIR $IDIR
}
set env(COLUMNSTORE_INSTALL_DIR) $INSTALLDIR
set PREFIX [file dirname $INSTALLDIR]
set PREFIX [file dirname $PREFIX]
set USERNAME $env(USER)
set UNM [lindex $argv 8]
if { $UNM != "" } {
	set USERNAME $UNM
}

log_user $DEBUG
spawn -noecho /bin/bash
#

#check and see if remote server has ssh keys setup, set PASSWORD if so
send_user " "
send "ssh -v $USERNAME@$SERVER 'time'\n"
set timeout 60
expect {
	"authenticity" { send "yes\n" 
    exp_continue
	}
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE"}	
	"Exit status 1" { send_user "FAILED: Login Failure\n" ; exit 1 }
	"Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1 }
	"service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	"Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	"No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"


send_user "Stop ColumnStore service                       "
send "ssh -v $USERNAME@$SERVER '$INSTALLDIR/bin/columnstore stop'\n"
set timeout 60
# check return
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
    	"Exit status 0" { send_user "DONE" }
	"Exit status 127" { send_user "DONE" }
	"Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	timeout { send_user "DONE" }
}
send_user "\n"

#
# remove MariaDB Columnstore files
#
send_user "Uninstall MariaDB Columnstore Package                       "
send_user " \n"
send "ssh -v $USERNAME@$SERVER '$INSTALLDIR/bin/pre-uninstall --installdir=$INSTALLDIR >/dev/null 2>&1'\n"
set timeout 60
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	"Exit status 127" { send_user "DONE" }
	timeout { send_user "DONE" }
}
send_user "\n"

if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the MariaDB Columnstore package
#
send_user "Copy New MariaDB Columnstore Package to Module              "
send_user " \n"
send "scp -v $CALPONTPKG $USERNAME@$SERVER:$CALPONTPKG\n"
set timeout 360
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	"scp :"  	{ send_user "ERROR\n" ; 
				send_user "\n*** Installation ERROR\n" ; 
				exit 1 }
	"Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	timeout { send_user "ERROR: Timeout\n" ; exit 2 }
}
send_user "\n"
#
# install package
#
send_user "Install MariaDB Columnstore Package on Module               "
send_user " \n"
send "ssh -v $USERNAME@$SERVER 'tar -C $PREFIX --exclude db -zxvf $CALPONTPKG'\n"
set timeout 360
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	"Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	timeout { send_user "ERROR: Timeout\n" ; exit 2 }
}
send_user "\n"

#
# copy over custom OS tmp files
#
send_user "Copy Custom OS files to Module                  "
send_user " \n"
send "scp -rv $INSTALLDIR/local/etc $USERNAME@$SERVER:$INSTALLDIR/local\n"
set timeout 120
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	"scp :"  	{ send_user "ERROR\n" ; 
				send_user "\n*** Installation ERROR\n" ; 
				exit 1 }
	"Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	timeout { send_user "ERROR: Timeout\n" ; exit 2 }
}
send_user "\n"

#
# copy over MariaDB Columnstore Module file
#
send_user "Copy MariaDB Columnstore Module file to Module                 "
send "scp -v $INSTALLDIR/local/etc/$MODULE/*  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
set timeout 120
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	"scp :"  	{ send_user "ERROR\n" ; 
				send_user "\n*** Installation ERROR\n" ; 
				exit 1 }	"Exit status 0" { send_user "DONE" }
	"Exit status 1" { send_user "ERROR: scp failed" ; exit 1 }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"

send_user "Run post-install script                         "
send_user " \n"
send "ssh -v $USERNAME@$SERVER '$INSTALLDIR/bin/post-install --installdir=$INSTALLDIR'\n"
set timeout 90
# check return
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 127" { send_user "ERROR: $INSTALLDIR/bin/post-install Not Found\n" ; exit 1 }
	"MariaDB Columnstore syslog logging not working" { send_user "WARNING: MariaDB Columnstore System logging not setup\n"; exp_continue }
	"Exit status 0" { send_user "DONE" }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"

send_user "Start ColumnStore service                       "
send_user " \n"
send "ssh -v $USERNAME@$SERVER '$INSTALLDIR/bin/columnstore restart'\n"
set timeout 120
# check return
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 127" { send_user "ERROR: $INSTALLDIR/bin/post-install Not Found\n" ; exit 1 }
	"Exit status 0" { send_user "DONE" }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"

send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

