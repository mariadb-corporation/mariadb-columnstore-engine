#!/bin/bash

SCRIPT_LOCATION=$(dirname "$0")
COLUMNSTORE_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../)
MARIADB_SOURCE_PATH=$(realpath $SCRIPT_LOCATION/../../../../../)

VERSION_GREATER_THAN_10=$(mariadb -N -s -e 'SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) >= (11, 4, 0);')

SERVERNAME="mysql"
if [[ $VERSION_GREATER_THAN_10 == '1' ]]; then
    SERVERNAME="mariadb"
fi

COLUMNSTORE_MTR_SOURCE=$(realpath $COLUMNSTORE_SOURCE_PATH/mysql-test/columnstore)
INSTALLED_MTR_PATH="/usr/share/${SERVERNAME}/${SERVERNAME}-test/"
PATCHNAME=$(realpath $SCRIPT_LOCATION)/mtr_warn.patch
CURRENT_DIR=$(pwd)

source $COLUMNSTORE_SOURCE_PATH/build/utils.sh

optparse.define short=s long=suite desc="whole suite to run" variable=SUITE_NAME
optparse.define short=t long=test_full_name desc="Testname with suite as like bugfixes.mcol-4899" variable=TEST_FULL_NAME default=""
optparse.define short=f long=full desc="Run full MTR" variable=RUN_FULL default=false value=true
optparse.define short=r long=record desc="Record the result" variable=RECORD default=false value=true
optparse.define short=e long=no-extern desc="Run with --extern" variable=EXTERN default=false value=true

source $(optparse.build)

# Detect if the user explicitly specified -e/--no-extern
EXTERN_SPECIFIED=false
for arg in "$@"; do
    if [[ "$arg" == "-e" || "$arg" == "--no-extern" ]]; then
        EXTERN_SPECIFIED=true
        break
    fi
done

mariadb -e "create database if not exists test;"
SOCKET=$(mariadb -e "show variables like 'socket';" | grep socket | cut -f2)

export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0,print_stats=false,detect_odr_violation=0,check_initialization_order=1,detect_stack_use_after_return=1,atexit=false,log_path=/core/asan.hz

# needed when run MTR tests locally, see mariadb-test-run.pl:417, mtr functions
# are added to the database mtr only when --extern is not specified

add_mtr_warn_functions() {
    message "Adding mtr warnings functions..."
    cd /tmp
    mariadb -e "drop database if exists mtr"
    cp ${MARIADB_SOURCE_PATH}/mysql-test/include/mtr_warnings.sql mtr_warnings.sql
    patch -p1 <${PATCHNAME}
    mariadb -e "create database if not exists mtr;"
    mariadb mtr <mtr_warnings.sql
    rm mtr_warnings.sql
    cd -
    echo "MTR Warnings function added"
}

cd ${INSTALLED_MTR_PATH}

if [[ ! -d ${INSTALLED_MTR_PATH}/suite/columnstore ]]; then
    message " ・ Adding symlink for columnstore tests to ${INSTALLED_MTR_PATH}/suite/columnstore from ${COLUMNSTORE_MTR_SOURCE}"
    ln -s ${COLUMNSTORE_MTR_SOURCE} ${INSTALLED_MTR_PATH}/suite
fi

if [[ ! -d '/data/qa/source/dbt3/' || ! -d '/data/qa/source/ssb/' ]]; then
    message ' ・ Downloading and extracting test data for full MTR to /data'
    bash -c "wget -qO- https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4 | lz4 -dc - | tar xf - -C /"
fi

if [[ -n $TEST_FULL_NAME ]]; then
    SUITE_NAME="${TEST_FULL_NAME%%.*}"
    TEST_NAME="${TEST_FULL_NAME#*.}"
fi

run_suite() {
    ls /core >$CURRENT_DIR/mtr.$1.cores-before

    # Decide effective extern per suite:
    # - If user explicitly set --extern (-e), honor it for all suites
    # - If not specified, force no extern for basic, bugfixes, and future suites
    EXTERN_EFFECTIVE=$EXTERN
    if [[ $EXTERN_SPECIFIED == false ]]; then
        case "$1" in
        basic | bugfixes | future)
            EXTERN_EFFECTIVE=false
            ;;
        esac
    fi

    if [[ $EXTERN_EFFECTIVE == true ]]; then
        EXTERN_FLAG="--extern=socket=${SOCKET}"
    else
        EXTERN_FLAG="--mysqld=--plugin-load-add=ha_columnstore"
    fi

    if [[ $RECORD == true ]]; then
        RECORD_FLAG="--record"
        warn "Test results are RECORDED"
    else
        RECORD_FLAG=""
    fi

    message "Running suite $SUITE_NAME with test $TEST_NAME and --extern=${EXTERN_EFFECTIVE}"

    ./mtr --force $EXTERN_FLAG $RECORD_FLAG --max-test-fail=0 --testcase-timeout=60 --suite=columnstore/$1 $2 | tee $CURRENT_DIR/mtr.$1.log 2>&1
    # dump analyses.
    systemctl stop mariadb
    systemctl start mariadb
    ls /core >$CURRENT_DIR/mtr.$1.cores-after
    message "reports or coredumps:"
    diff -u $CURRENT_DIR/mtr.$1.cores-before $CURRENT_DIR/mtr.$1.cores-after && echo "no new reports or coredumps"
    rm $CURRENT_DIR/mtr.$1.cores-before $CURRENT_DIR/mtr.$1.cores-after
}

add_mtr_warn_functions

if [[ $RUN_FULL == true ]]; then
    message "Running FULL MTR"
    run_suite basic
    run_suite bugfixes
    run_suite setup
    run_suite devregression
    run_suite autopilot
    run_suite extended
    run_suite multinode
    run_suite oracle
    run_suite 1pmonly
else
    run_suite $SUITE_NAME $TEST_NAME
fi

cd -
