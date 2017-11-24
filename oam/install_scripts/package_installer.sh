#!/usr/bin/expect
#
# $Id: package_installer.sh 1128 2009-01-05 16:36:59Z rdempsey $
#
# Install RPM and custom OS files on system
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Package name being installed
# Argument 5 - Install Type, "initial" or "upgrade"
# Argument 6 - Debug flag 1 for on, 0 for off
set timeout 30
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set VERSION [lindex $argv 3]
set INSTALLTYPE [lindex $argv 4]
set AMAZONINSTALL [lindex $argv 5]
set PKGTYPE [lindex $argv 6]
set NODEPS [lindex $argv 7]
set DEBUG [lindex $argv 8]
set INSTALLDIR "/usr/local/mariadb/columnstore"
set IDIR [lindex $argv 9]
if { $IDIR != "" } {
	set INSTALLDIR $IDIR
}
set USERNAME "root"
set UNM [lindex $argv 10]
if { $UNM != "" } {
	set USERNAME $UNM
}

set HOME "$env(HOME)"

log_user $DEBUG
spawn -noecho /bin/bash
#
if { $PKGTYPE == "rpm" } {
	set PKGERASE "rpm -e --nodeps \$(rpm -qa | grep '^mariadb-columnstore')"
	set PKGERASE1 "rpm -e --nodeps "

	set PKGINSTALL "rpm -ivh $NODEPS --force mariadb-columnstore*$VERSION*rpm"
	set PKGUPGRADE "rpm -Uvh --noscripts mariadb-columnstore*$VERSION*rpm"
} else {
	if { $PKGTYPE == "deb" } {
		set PKGERASE "dpkg -P \$(dpkg --get-selections | grep '^mariadb-columnstore')"
		set PKGERASE1 "dpkg -P "
		set PKGINSTALL "dpkg -i --force-confnew mariadb-columnstore*$VERSION*deb"
		set PKGUPGRADE "dpkg -i --force-confnew mariadb-columnstore*$VERSION*deb"
	} else {
		if { $PKGTYPE != "bin" } {
			send_user "Invalid Package Type of $PKGTYPE"
			exit 1
		}
	}
}


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
#	"No such file or directory" { send_user "DONE" }
        "Exit status 127" { send_user "DONE" }
	 "Exit status 0" { send_user "DONE" }
	"Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	timeout { send_user "DONE" }
}
send_user "\n"

# 
# erase package
#
send_user "Erase MariaDB Columnstore Packages on Module                 "
send "ssh -v $USERNAME@$SERVER '$PKGERASE '\n"
set timeout 60
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit 1 }
	"Exit status 0" { send_user "DONE" }
	"Exit status 2" { send_user "DONE" }
	timeout { send_user "DONE" }
}
send_user "\n"

if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the package
#
set timeout 60
send_user "Copy New MariaDB Columnstore Package to Module              "
send "ssh -v $USERNAME@$SERVER 'rm -f /root/mariadb-columnstore-*.$PKGTYPE'\n"
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"Exit status 0" { send_user "DONE" }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
set timeout 180
send "scp -v $HOME/mariadb-columnstore*$VERSION*$PKGTYPE $USERNAME@$SERVER:.\n"
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"scp :"  	{ send_user "ERROR\n" ; 
				send_user "\n*** Installation ERROR\n" ; 
				exit 1 }
	"Exit status 0" { send_user "DONE" }
	"Exit status 1" { send_user "ERROR: scp failed" ; exit 1 }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"

#
# install package
#
send_user "Install MariaDB Columnstore Packages on Module               "

send "ssh -v $USERNAME@$SERVER '$PKGINSTALL '\n"
set timeout 180
expect {
	"word: " { send "$PASSWORD\n"
    exp_continue
	}
	"passphrase" { send "$PASSWORD\n" 
    exp_continue
	}
	"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
									exit 1 }
	"needs"    { send_user "ERROR: disk space issue\n" ; exit 1 }
	"conflicts"	   { send_user "ERROR: File Conflict issue\n" ; exit 1 }
	"MariaDB Columnstore syslog logging not working" { send_user "WARNING: MariaDB Columnstore System logging not setup\n"; exp_continue }
	"Exit status 0" { send_user "DONE" }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
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
	"scp :"  	{ send_user "ERROR\n" ; 
				send_user "\n*** Installation ERROR\n" ; 
				exit 1 }
	"Exit status 0" { send_user "DONE" }
	"Exit status 1" { send_user "ERROR: scp failed" ; exit 1 }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}
send_user "\n"

send_user "Start ColumnStore service                       "
send_user " \n"
send "ssh -v $USERNAME@$SERVER '$INSTALLDIR/bin/columnstore restart'\n"
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
    "Exit status 127" { send_user "ERROR: $INSTALLDIR/bin/columnstore Not Found\n" ; exit 1 }
    timeout { send_user "ERROR: Timeout to host\n" ; exit 2 }
}

send_user "\n"

send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

