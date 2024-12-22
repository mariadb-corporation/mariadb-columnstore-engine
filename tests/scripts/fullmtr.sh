SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_MTR_SOURCE=$(realpath $SCRIPT_LOCATION/../../mysql-test/columnstore)
INSTALLED_MTR_PATH='/usr/share/mysql/mysql-test'
COLUMSNTORE_MTR_INSTALLED=${INSTALLED_MTR_PATH}/suite/columnstore

CURRENT_DIR=`pwd`
mysql -e "create database if not exists test;"
SOCKET=`mysql -e "show variables like 'socket';" | grep socket | cut -f2`

export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0,print_stats=false,detect_odr_violation=0,check_initialization_order=1,detect_stack_use_after_return=1,atexit=false,log_path=/core/asan.hz


cd ${INSTALLED_MTR_PATH}

if [[ ! -d  ${COLUMSNTORE_MTR_INSTALLED} ]]; then
    echo ' ・ Adding symlink for columnstore tests to ${COLUMSNTORE_MTR_INSTALLED} from ${COLUMNSTORE_MTR_SOURCE}   '
    ln -s ${COLUMNSTORE_MTR_SOURCE} ${COLUMSNTORE_MTR_INSTALLED}
fi


if [[ ! -d  '/data/qa/source/dbt3/' || ! -d '/data/qa/source/ssb/' ]]; then
    echo ' ・ Downloading and extracting test data for full MTR to /data'
    bash -c "wget -qO- https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4 | lz4 -dc - | tar xf - -C /"
fi
run_suite()
{
    ls /core >$CURRENT_DIR/mtr.$1.cores-before
    ./mtr --force --extern=socket=/run/mysqld/mysqld.sock --max-test-fail=0 --testcase-timeout=60 --suite=columnstore/$1 $2 | tee $CURRENT_DIR/mtr.$1.log 2>&1
    # dump analyses.
    systemctl stop mariadb
    systemctl start mariadb
    ls /core >$CURRENT_DIR/mtr.$1.cores-after
    echo "reports or coredumps:"
    diff -u $CURRENT_DIR/mtr.$1.cores-before $CURRENT_DIR/mtr.$1.cores-after && echo "no new reports or coredumps"
    rm $CURRENT_DIR/mtr.$1.cores-before $CURRENT_DIR/mtr.$1.cores-after
}

if (( $# == 2 )); then
    run_suite $1 $2
    exit 1
fi

run_suite basic
run_suite bugfixes
run_suite setup
run_suite devregression
run_suite autopilot
run_suite extended
run_suite multinode
run_suite oracle
run_suite 1pmonly

cd -
