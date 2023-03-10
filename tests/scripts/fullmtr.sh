CURRENT_DIR=`pwd`
mysql -e "create database if not exists test;"
SOCKET=`mysql -e "show variables like 'socket';" | grep socket | cut -f2`
cd /usr/share/mysql/mysql-test

./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/setup
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/basic | tee $CURRENT_DIR/mtr.basic.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/bugfixes | tee $CURRENT_DIR/mtr.bugfixes.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/devregression | tee $CURRENT_DIR/mtr.devregression.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/extended  | tee $CURRENT_DIR/mtr.extended.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/multinode  | tee $CURRENT_DIR/mtr.multinode.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/1pmonly  | tee $CURRENT_DIR/mtr.1pmonly.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/oracle  | tee $CURRENT_DIR/mtr.multinode.log 2>&1
./mtr --force --max-test-fail=0 --testcase-timeout=60 --extern socket=$SOCKET --suite=columnstore/autopilot | tee $CURRENT_DIR/mtr.autopilot.log 2>&1

cd -
