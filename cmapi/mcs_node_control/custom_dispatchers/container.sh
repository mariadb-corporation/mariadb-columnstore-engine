#!/bin/bash

# TODO: remove in next releases

programname=$0

function usage {
    echo "usage: $programname op [service_name] [is_primary]"
    echo "  op - operation name [start|stop]"
    echo "  service_name - [mcs-controllernode|mcs-workernode etc]"
    echo "  is_primary - [0|1]"
    exit 1
}

operation=$1
service_name=$2
is_primary=$3

if [[ -z "$operation" || -z "$service_name" || $is_primary -ne 0 && $is_primary -ne 1 ]]; then
    usage
fi

LOG_FILE=/var/log/mariadb/columnstore/container-sh.log

start_up_to_workernode() {
    # Set Variables
    IFLAG=/etc/columnstore/container-initialized
    LOG_PREFIX=/var/log/mariadb/columnstore
    MCS_INSTALL_PATH=/var/lib/columnstore
    MCS_INSTALL_BIN=/usr/bin
    PROGS='StorageManager mcs-loadbrm.py workernode'
    JEMALLOC_PATH=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')
    if [ -z "$JEMALLOC_PATH" && -f $MCS_INSTALL_PATH/libjemalloc.so.2 ]; then
        JEMALLOC_PATH=$MCS_INSTALL_PATH/libjemalloc.so.2
    fi
    export LD_PRELOAD=$JEMALLOC_PATH

    # Intialize Container If Necessary
    if [ ! -e $IFLAG ]; then
      $MCS_INSTALL_BIN/columnstore-init &>> $LOG_PREFIX/columnstore-init.log
    fi

    # Verify All Programs Are Available
    for i in $PROGS ; do
      if [ ! -x $MCS_INSTALL_BIN/$i ] ; then
        echo "$i doesn't exist."
        exit 1
      fi
    done

    # Start System
    echo `date`: start_up_to_workernode\(\)... >> $LOG_FILE

    touch $LOG_PREFIX/storagemanager.log && chmod 666 $LOG_PREFIX/storagemanager.log
    $MCS_INSTALL_BIN/StorageManager &>> $LOG_PREFIX/storagemanager.log &
    echo `date`: StorageManager PID = $! >> $LOG_FILE

    sleep 1

    echo `date`: loading BRM >> $LOG_FILE
    touch $LOG_PREFIX/mcs-loadbrm.log && chmod 666 $LOG_PREFIX/mcs-loadbrm.log
    # Argument "no" here means don't use systemd to start SM
    $MCS_INSTALL_BIN/mcs-loadbrm.py no >> $LOG_PREFIX/mcs-loadbrm.log 2>&1

    touch $LOG_PREFIX/workernode.log && chmod 666 $LOG_PREFIX/workernode.log
    $MCS_INSTALL_BIN/workernode DBRM_Worker1 &>> $LOG_PREFIX/workernode.log &
    echo `date`: workernode PID = $! >> $LOG_FILE

    exit 0
}

start_those_left_at_master() {
    # Set Variables
    LOG_PREFIX=/var/log/mariadb/columnstore
    MCS_INSTALL_PATH=/var/lib/columnstore
    MCS_INSTALL_BIN=/usr/bin
    # TODO: remove fast fix
    # skip check binary for ExeMgr
    PROGS='controllernode PrimProc WriteEngineServer DMLProc DDLProc'
    JEMALLOC_PATH=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')
    if [ -z "$JEMALLOC_PATH" && -f $MCS_INSTALL_PATH/libjemalloc.so.2 ]; then
        JEMALLOC_PATH=$MCS_INSTALL_PATH/libjemalloc.so.2
    fi
    export LD_PRELOAD=$JEMALLOC_PATH

    # Verify All Programs Are Available (except ExeMgr)
    for i in $PROGS ; do
      if [ ! -x $MCS_INSTALL_BIN/$i ] ; then
        echo "$i doesn't exist."
        exit 1
      fi
    done

    echo `date`: start_those_left_at_master\(\) >> $LOG_FILE

    if [[ $is_primary -eq 1 ]]; then
        touch $LOG_PREFIX/controllernode.log && chmod 666 $LOG_PREFIX/controllernode.log
        $MCS_INSTALL_BIN/controllernode fg &>> $LOG_PREFIX/controllernode.log &
        echo `date`: controllernode PID = $! >> $LOG_FILE
    fi

    touch $LOG_PREFIX/primproc.log && chmod 666 $LOG_PREFIX/primproc.log
    $MCS_INSTALL_BIN/PrimProc &>> $LOG_PREFIX/primproc.log &
    echo `date`: PrimProc PID = $! >> $LOG_FILE

    sleep 1

    if [ -e $MCS_INSTALL_BIN/ExeMgr ] ; then
        touch $LOG_PREFIX/exemgr.log && chmod 666 $LOG_PREFIX/exemgr.log
        $MCS_INSTALL_BIN/ExeMgr &>> $LOG_PREFIX/exemgr.log &
        echo `date`: ExeMgr PID = $! >> $LOG_FILE
    fi

    touch $LOG_PREFIX/writeengineserver.log && chmod 666 $LOG_PREFIX/writeengineserver.log
    $MCS_INSTALL_BIN/WriteEngineServer &>> $LOG_PREFIX/writeengineserver.log &
    echo `date`: WriteEngineServer PID = $! >> $LOG_FILE

    sleep 3

    touch $LOG_PREFIX/dmlproc.log && chmod 666 $LOG_PREFIX/dmlproc.log
    $MCS_INSTALL_BIN/DMLProc &>> $LOG_PREFIX/dmlproc.log &
    echo `date`: DMLProc PID = $! >> $LOG_FILE

    touch $LOG_PREFIX/ddlproc.log && chmod 666 $LOG_PREFIX/ddlproc.log
    $MCS_INSTALL_BIN/DDLProc &>> $LOG_PREFIX/ddlproc.log &
    echo `date`: DDLProc PID = $! >> $LOG_FILE

    exit 0
}



start() {
    # Set Variables
    IFLAG=/etc/columnstore/container-initialized
    LOG_PREFIX=/var/log/mariadb/columnstore
    MCS_INSTALL_PATH=/var/lib/columnstore
    MCS_INSTALL_BIN=/usr/bin
    # TODO: remove fast fix
    # skip check binary for ExeMgr
    PROGS='StorageManager load_brm workernode controllernode PrimProc WriteEngineServer DMLProc DDLProc'
    JEMALLOC_PATH=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')
    if [ -z "$JEMALLOC_PATH" && -f $MCS_INSTALL_PATH/libjemalloc.so.2 ]; then
        JEMALLOC_PATH=$MCS_INSTALL_PATH/libjemalloc.so.2
    fi
    export LD_PRELOAD=$JEMALLOC_PATH

    # Intialize Container If Necessary
    if [ ! -e $IFLAG ]; then
      $MCS_INSTALL_BIN/columnstore-init &>> $LOG_PREFIX/columnstore-init.log
    fi

    # Verify All Programs Are Available (except ExeMgr)
    for i in $PROGS ; do
      if [ ! -x $MCS_INSTALL_BIN/$i ] ; then
        echo "$i doesn't exist."
        exit 1
      fi
    done

    # Start System
    echo `date`: start\(\)... >> $LOG_FILE

    touch $LOG_PREFIX/storagemanager.log && chmod 666 $LOG_PREFIX/storagemanager.log
    $MCS_INSTALL_BIN/StorageManager &>> $LOG_PREFIX/storagemanager.log &
    echo `date`: StorageManager PID = $! >> $LOG_FILE
    sleep 1

    echo `date`: loading BRM >> $LOG_FILE
    touch $LOG_PREFIX/mcs-loadbrm.log && chmod 666 $LOG_PREFIX/mcs-loadbrm.log
    # Argument "no" here means don't use systemd to start SM
    $MCS_INSTALL_BIN/mcs-loadbrm.py no >> $LOG_PREFIX/mcs-loadbrm.log 2>&1

    touch $LOG_PREFIX/workernode.log && chmod 666 $LOG_PREFIX/workernode.log
    $MCS_INSTALL_BIN/workernode DBRM_Worker2 &>> $LOG_PREFIX/workernode.log &
    echo `date`: workernode PID = $! >> $LOG_FILE

    sleep 2

    if [[ $is_primary -eq 1 ]]; then
        touch $LOG_PREFIX/controllernode.log && chmod 666 $LOG_PREFIX/controllernode.log
        $MCS_INSTALL_BIN/controllernode fg &>> $LOG_PREFIX/controllernode.log &
        echo `date`: controllernode PID = $! >> $LOG_FILE
    fi

    touch $LOG_PREFIX/primproc.log && chmod 666 $LOG_PREFIX/primproc.log
    $MCS_INSTALL_BIN/PrimProc &>> $LOG_PREFIX/primproc.log &
    echo `date`: PrimProc PID = $! >> $LOG_FILE

    sleep 1

    if [ -e $MCS_INSTALL_BIN/ExeMgr ] ; then
        touch $LOG_PREFIX/exemgr.log && chmod 666 $LOG_PREFIX/exemgr.log
        $MCS_INSTALL_BIN/ExeMgr &>> $LOG_PREFIX/exemgr.log &
        echo `date`: ExeMgr PID = $! >> $LOG_FILE
    fi

    touch $LOG_PREFIX/writeengineserver.log && chmod 666 $LOG_PREFIX/writeengineserver.log
    $MCS_INSTALL_BIN/WriteEngineServer &>> $LOG_PREFIX/writeengineserver.log &
    echo `date`: WriteEngineServer PID = $! >> $LOG_FILE

    sleep 3

    if [[ $is_primary -eq 1 ]]; then
        touch $LOG_PREFIX/dmlproc.log && chmod 666 $LOG_PREFIX/dmlproc.log
        $MCS_INSTALL_BIN/DMLProc &>> $LOG_PREFIX/dmlproc.log &
        echo `date`: DMLProc PID = $! >> $LOG_FILE
        touch $LOG_PREFIX/ddlproc.log && chmod 666 $LOG_PREFIX/ddlproc.log
        $MCS_INSTALL_BIN/DDLProc &>> $LOG_PREFIX/ddlproc.log &
        echo `date`: DDLProc PID = $! >> $LOG_FILE
    fi

    exit 0
}

stop() {
    # TODO: remove fast fix
    # skip check binary for ExeMgr
    PROGS='DMLProc DDLProc WriteEngineServer PrimProc workernode controllernode StorageManager'
    MCS_INSTALL_BIN=/usr/bin
    LOG_PREFIX=/var/log/mariadb/columnstore

    # Stop System
    echo `date`: Stopping... >> $LOG_FILE

    if  [[ ! -z "$(pidof $PROGS)" ]]; then
        # Save BRM only on the primary node now.
        if  [[ ! -z "$(pidof controllernode)" ]]; then
            $MCS_INSTALL_BIN/mcs-savebrm.py &>> $LOG_PREFIX/savebrm.log 2>&1
        fi

        echo `date`: Sending SIGTERM >> $LOG_FILE
        kill $(pidof $PROGS) > /dev/null
        sleep 3
        # Make sure StorageManager had a chance to shutdown clean
        counter=1
        while [ -n "$(pidof StorageManager)" -a $counter -le 60 ]
        do
            sleep 1
            ((counter++))
        done
        echo `date`: Sending SIGKILL >> $LOG_FILE
        kill -9 $(pidof $PROGS) > /dev/null
    fi

    echo `date`: Clearing SHM >> $LOG_FILE
    $MCS_INSTALL_BIN/clearShm

    exit 0
}

case "$operation" in
    'start')
        # We start everything when controllernode starts at primary node and with workernode at non-primary
        if [[ $is_primary -eq 1 && "mcs-workernode" == "$service_name" ]]; then
            start_up_to_workernode $is_primary
        elif [[ $is_primary -eq 1 && "mcs-controllernode" == "$service_name" ]]; then
            start_those_left_at_master $is_primary
        elif [[ $is_primary -eq 0 && "mcs-workernode" == "$service_name" ]]; then
            start $is_primary
        fi
    ;;

    'stop')
        if [[ $is_primary -eq 1 && "mcs-controllernode" == "$service_name" || $is_primary -eq 0 && "mcs-workernode" == "$service_name" ]]; then
            stop
        fi
    ;;
esac
