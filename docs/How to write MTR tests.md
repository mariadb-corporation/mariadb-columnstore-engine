

MTR is a regression test framework for MariaDB/MySQL. It is written in Perl.
[Here](https://github.com/mariadb-corporation/mariadb-columnstore-engine/blob/develop/mysql-test/columnstore/basic/t/mcol271-empty-string-is-not-null.test) is an example of MTR test. When you right the test you can ask MTR to produce a golden file automatically like this.
```shell
./mtr --record --extern socket=/run/mysqld/mysqld.sock  --suite=columnstore/basic test_name
```
 
The golden file goes into mysql-test/columnstore/basic/r/test_name.result.