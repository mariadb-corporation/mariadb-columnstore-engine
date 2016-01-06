drop table if exists scriptTimeResults;

create table scriptTimeResults
(
	script varchar(20),
	origVersion varchar(8),
	origTime double,
	baselineTime double,
	margin int, 		/* % allowed over the baseline time before considered failed */
	description varchar(100),
	curTime double,
	resultsMatch bool,
	timeWithinMargin bool
);

insert into scriptTimeResults values
('bug4901.sql',  '3.6.1-1', 14.61, 14.00, 20, 'concat fncn',   null, null, null),
('bug4902a.sql', '3.6.1-1', 15.14,  7.54, 20, 'insert fncn',   null, null, null),
('bug4902b.sql', '3.6.1-1', 12.48,  7.97, 20, 'replace fncn',  null, null, null),
('bug4919a.sql', '3.6.1-1', 88.28,  3.34, 20, 'makedate fncn', null, null, null),
('bug4919b.sql', '3.6.1-1', 47.44,  6.70, 20, 'maketime fncn', null, null, null),
('bug4920.sql',  '3.6.1-1', 34.64,  1.70, 50, 'adddate fncn',  null, null, null),
('bug5360.sql',  '3.6.1-1', 26.08, 25.25, 10, 'from_days fncn (ostringstream)',  null, null, null)
;
