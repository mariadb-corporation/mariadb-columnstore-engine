--	VIEW used in Prepare Statement
create or replace view v_temp as select * from part;
prepare st_part from "select * from v_temp where p_partkey<10";
execute st_part;
deallocate prepare st_part;
drop view v_temp;