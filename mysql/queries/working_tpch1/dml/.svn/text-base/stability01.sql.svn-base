drop table if exists qa_stability;
create table qa_stability (col_char char(8), col_int int, col_varchar varchar(4), col_smallint smallint) engine=infinidb;
select * from qa_stability;
insert into qa_stability values('Roberts', 1, 'MKR', 9537);
insert into qa_stability values('Kerr', 2, 'TK', 9540);
insert into qa_stability values('Lee', 3, 'DL', 9525);
insert into qa_stability values('Lowe', 4, 'JL', 9523);
insert into qa_stability values('Williams', 5, 'JW', 9515);
select * from qa_stability;
update qa_stability set col_varchar = null where col_char = 'Kerr';
update qa_stability set col_smallint = 9999 where col_varchar = 'JW';
select * from qa_stability;
delete from qa_stability where col_int = 1;
select * from qa_stability;
drop table if exists qa_stability;

