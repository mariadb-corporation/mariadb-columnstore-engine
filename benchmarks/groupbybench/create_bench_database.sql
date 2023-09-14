CREATE OR REPLACE DATABASE bench;
USE bench;
CREATE TABLE `bench_real` (`c` varchar(30)) ENGINE=Columnstore;
CREATE TABLE `bench_two_groups` (`c` varchar(1)) ENGINE=Columnstore;
CREATE TABLE `bench_ten_groups` (`c` varchar(1)) ENGINE=Columnstore;