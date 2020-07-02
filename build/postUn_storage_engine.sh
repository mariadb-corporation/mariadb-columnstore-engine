running_systemd() {
   if  [ "$(ps --no-headers -o comm 1)" = "systemd" ]; then
       echo 0
   else
       echo 1
   fi
}

rpmmode=upgrade
if  [ "$1" -eq "$1" 2> /dev/null ]; then
	if [ $1 -ne 1 ]; then
		rpmmode=erase
	fi
else
	rpmmode=erase
fi

if [ $rpmmode = erase ]; then
    if [ ! -z "$(pgrep -x mariadbd)" ];then
        systemctl cat mariadb.service > /dev/null 2>&1
        if [ $? -eq 0 ] && [ $(running_systemd) -eq 0 ]; then
            systemctl restart mariadb.service > /dev/null 2>&1
        else
            pkill mysqld > /dev/null 2>&1
            /usr/bin/mysqld_safe &
        fi
    fi
fi

exit 0

