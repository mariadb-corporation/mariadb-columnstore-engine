#!/bin/bash

#
# Sets up some common environment variables used by various scripts.  Also has some common functions.
#

if [ -z "$INSTALLDIR" ]; then
        INSTALLDIR="/usr/local/Calpont"
        if [ $1 ]; then
                INSTALLDIR=$1
        fi
        export INSTALLDIR
	echo INSTALLDIR $INSTALLDIR
fi

if [ -z "$QUERYTESTER_P" ]; then
        if [ -d "$INSTALLDIR/mysql" ]; then
		QUERYTESTER_P="$INSTALLDIR/mysql" # Linux
	else
		QUERYTESTER_P="$INSTALLDIR" 	  # Windows
	fi
	export QUERYTESTER_P
	echo QUERYTSTER_P $QUERYTESTER_P
fi

if [ -z "$MYSQLCNF" ]; then
        if [ -d "$INSTALLDIR/mysql" ]; then
		MYSQLCNF=$INSTALLDIR/mysql/my.cnf # Linux
	else
		MYSQLCNF=$INSTALLDIR/my.ini	  # Windows
	fi
	export MYSQLCNF
	echo MYSQLCNF $MYSQLCNF
fi

if [ -z "$MYSQLCMD" ]; then
        if [ -d "$INSTALLDIR/mysql" ]; then
		# Linux.
                MYSQLCMD="$INSTALLDIR/mysql/bin/mysql --defaults-file=$MYSQLCNF -u root"
        else
		# Windows.
                MYSQLCMD="$INSTALLDIR/bin/mysql --defaults-file=$MYSQLCNF -u root"
        fi
        export MYSQLCMD
	echo MYSQLCMD $MYSQLCMD
fi

if [ -z "$CPIMPORTCMD" ]; then
	CPIMPORTCMD="$INSTALLDIR/bin/cpimport"
	export CPIMPORTCMD
	echo CPIMPORTCMD $CPIMPORTCMD
fi

if [ -z "$WINDOWS" ]; then
	if [ -f /usr/local/Calpont/bin/calpontConsole ]; then
		WINDOWS=false
	else
		WINDOWS=true
	fi
	export WINDOWS
	echo WINDOWS $WINDOWS
fi	

if $WINDOWS; then
	if [ "`echo $INSTALLDIR | awk '{print substr($0, 2, 1)}'`" == ":" ]; then
        WINDRIVE=`echo $INSTALLDIR | awk '{print substr($0, 1, 2)}'`
	else
        WINDRIVE=""
    fi
	export WINDRIVE
	echo WINDRIVE $WINDRIVE
fi
