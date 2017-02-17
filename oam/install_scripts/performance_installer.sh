#!/usr/bin/expect
#
# $Id: performance_installer.sh 1128 2009-01-05 16:36:59Z rdempsey $
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
set PKGTYPE [lindex $argv 5]
set NODEPS [lindex $argv 6]
set DEBUG [lindex $argv 7]
set INSTALLDIR "/usr/local/mariadb/columnstore"
set IDIR [lindex $argv 8]
if { $IDIR != "" } {
	set INSTALLDIR $IDIR
}
set USERNAME "root"
set UNM [lindex $argv 13]
if { $UNM != "" } {
	set USERNAME $UNM
}
set DEPLOY_PACKAGES [lindex $argv 14]

set BASH "/bin/bash "
if { $DEBUG == "1" } {
	set BASH "/bin/bash -x "
}

set HOME "$env(HOME)"

log_user $DEBUG
spawn -noecho /bin/bash
#
if { $DEPLOY_PACKAGES } {
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

    # check and see if remote server has ssh keys setup, set PASSWORD if so
    send_user " "
    send "ssh $USERNAME@$SERVER 'time'\n"
    set timeout 60
    expect {
        "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1 }
        "service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
        "authenticity" { send "yes\n"
                            expect {
                                "word: " { send "$PASSWORD\n" }
                                "passphrase" { send "$PASSWORD\n" }
                            }
        }
        "sys" { set PASSWORD "ssh" }
        "word: " { send "$PASSWORD\n" }
        "passphrase" { send "$PASSWORD\n" }
        "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
        "Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
        "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
        "No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
        timeout { send_user "ERROR: Timeout to host\n" ; exit 1 }
    }
    set timeout 30
    expect {
        -re {[$#] }        {  }
        "sys" {  }
    }
    send_user "\n"
    #BUG 5749 - SAS: didn't work on their system until I added the sleep 60

    sleep 60

    if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
        #
        # erase package
        #
        send_user "Erase MariaDB Columnstore Packages on Module                 "
        send "ssh $USERNAME@$SERVER '$PKGERASE ;$PKGERASE1 dummy'\n"
        if { $PASSWORD != "ssh" } {
            set timeout 30
            expect {
                "word: " { send "$PASSWORD\n" }
                "passphrase" { send "$PASSWORD\n" }
            }
        }
        set timeout 120
        expect {
            "package dummy" { send_user "DONE" }
            "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit 1 }
            "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
            "Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
            "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
            "No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
        }
        send_user "\n"
    }
}
if { $INSTALLTYPE == "uninstall" } { exit 0 }

if { $DEPLOY_PACKAGES } {
    #
    # send the package
    #
    set timeout 30
    #expect -re {[$#] }
    send_user "Copy New MariaDB Columnstore Package to Module              "
    send "ssh $USERNAME@$SERVER 'rm -f /root/mariadb-columnstore-*.$PKGTYPE'\n"
    if { $PASSWORD != "ssh" } {
        set timeout 30
        expect {
            "word: " { send "$PASSWORD\n" }
            "passphrase" { send "$PASSWORD\n" }
        }
    }
    expect {
        -re {[$#] } { }
        "Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
        "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
        "No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
    }
    set timeout 30
    expect {
            -re {[$#] } { }
    }

    send "scp $HOME/mariadb-columnstore*$VERSION*$PKGTYPE $USERNAME@$SERVER:.;$PKGERASE1 dummy\n"
    if { $PASSWORD != "ssh" } {
            set timeout 30
            expect {
                    "word: " { send "$PASSWORD\n" }
                    "passphrase" { send "$PASSWORD\n" }
            }
    }
    set timeout 180
    expect {
            "package dummy"         { send_user "DONE" }
            "directory"             { send_user "ERROR\n" ;
                                            send_user "\n*** Installation ERROR\n" ;
                                            exit 1 }
            "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
    }
    send_user "\n"

    #sleep to make sure it's finished
    sleep 5

    #
    if { $INSTALLTYPE == "initial"} {
        #
        # install package
        #
        send_user "Install MariaDB Columnstore Packages on Module               "

        send "ssh $USERNAME@$SERVER '$PKGINSTALL ;$PKGERASE1 dummy'\n"
        if { $PASSWORD != "ssh" } {
            set timeout 30
            expect {
                "word: " { send "$PASSWORD\n" }
                "passphrase" { send "$PASSWORD\n" }
            }
        }
        set timeout 180
        expect {
            "package dummy" 		  { send_user "DONE" }
            "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ;
                                        send_user "\n*** Installation ERROR\n" ;
                                            exit 1 }
            "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
            "Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
            "needs"    { send_user "ERROR: disk space issue\n" ; exit 1 }
            "conflicts"	   { send_user "ERROR: File Conflict issue\n" ; exit 1 }
        }

    }
    send_user "\n"
    #sleep to make sure it's finished
    sleep 5
}
set timeout 30
#expect -re {[$#] }
if { $INSTALLTYPE == "initial"} {
	#
	# copy over MariaDB Columnstore config file
	#
	send_user "Copy MariaDB Columnstore Config file to Module              "
	send "scp $INSTALLDIR/etc/*  $USERNAME@$SERVER:$INSTALLDIR/etc/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# copy over custom OS tmp files
	#
	send_user "Copy Custom OS files to Module                  "
	send "scp -r $INSTALLDIR/local/etc  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# copy over MariaDB Columnstore OS files
	#
	send_user "Copy MariaDB Columnstore OS files to Module                 "
	send "scp $INSTALLDIR/local/etc/$MODULE/*  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	#
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# Start module installer to setup Custom OS files
	#
	send_user "Run Module Installer                            "
	send "ssh $USERNAME@$SERVER '$BASH $INSTALLDIR/bin/module_installer.sh --module=pm'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		"!!!Module" 	{ send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		"FAILED"   { send_user "ERROR: missing OS file\n" ; exit 1 }
		"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
		"No such file"   { send_user "ERROR: File Not Found\n" ; exit 1 }
        "WARNING"   { send_user "WARNING: SYSLOG setup failed\n" }
	}
	send_user "\n"
}

#
# check MariaDB Columnstore syslog functionality
#

#
send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0
# vim:ts=4 sw=4:

