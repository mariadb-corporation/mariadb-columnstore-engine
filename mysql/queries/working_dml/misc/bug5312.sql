drop table if exists dml.sam_r368530763500;
CREATE TABLE dml. `sam_r368530763500` (   `cmp_inst_id` varchar(32) DEFAULT
NULL,   `agt_inst_id` varchar(32) DEFAULT NULL,   `output_id` varchar(10)
DEFAULT NULL,   `level_id` varchar(100) DEFAULT NULL,   `tgt_level_id`
varchar(100) DEFAULT NULL,   `row_id` int(11) DEFAULT NULL,   `rndnr`
decimal(18,0) DEFAULT NULL,   `b_cmp_inst_id` varchar(32) DEFAULT NULL,  
`b_agt_inst_id` varchar(32) DEFAULT NULL,   `b_output_id` varchar(10) DEFAULT
NULL,   `clientid` varchar(32) DEFAULT NULL,   `A_` varchar(100) DEFAULT NULL, 
 `C_` varchar(100) DEFAULT NULL,   `B_` varchar(100) DEFAULT NULL )
ENGINE=infinidb;
insert into dml.SAM_R368530763500 values('abc', 'a2cd4803c0a8641214988506a8938f5a','-2','sss','qqq',10,1.2,'lll','ppp','qw','ooo','llolo','uuu','ttt');
select * from dml.SAM_R368530763500;
update dml.SAM_R368530763500 set output_id =
'0',b_agt_inst_id=agt_inst_id,b_cmp_inst_id=cmp_inst_id,agt_inst_id = 
'a2cd4803c0a8641214988506a8938f5a' where
agt_inst_id='a2cd4803c0a8641214988506a8938f5a' and row_id in (select row_id1 
from (select row_id1 ,rndnr1 from (select min(row_id) row_id1, min(rndnr) 
rndnr1 from dml.SAM_R368530763500  where
agt_inst_id='a2cd4803c0a8641214988506a8938f5a'  and output_id = '-2' group by
level_id) x  order by rndnr1  ASC  limit 0) xy );
select * from dml.SAM_R368530763500;
update dml.SAM_R368530763500 set output_id =
'0',b_agt_inst_id=agt_inst_id,b_cmp_inst_id=cmp_inst_id,agt_inst_id = 
'a2cd4803c0a8641214988506a8938f5a' where
agt_inst_id='a2cd4803c0a8641214988506a8938f5a' and row_id in (select row_id1 
from (select row_id1 ,rndnr1 from (select min(row_id) row_id1, min(rndnr) 
rndnr1 from dml.SAM_R368530763500  where
agt_inst_id='a2cd4803c0a8641214988506a8938f5a'  and output_id = '-2' group by
level_id) x  order by rndnr1  ASC  limit 1) xy );
select * from dml.SAM_R368530763500;
drop table dml.sam_r368530763500;