
if [ "$1" == "2" ]; then
    #Perform tasks to prepare for the upgrade installation
    mcsadmin shutdown y > /dev/null 2>&1
    /bin/cp -f /etc/columnstore/storagemanager.cnf /etc/columnstore/storagemanager.cnf.rpmsave > /dev/null 2>&1
    columnstore-pre-uninstall
fi
exit 0
