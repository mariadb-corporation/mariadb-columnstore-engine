drop table if exists qaTestAuto;
create table qaTestAuto (r_regionkey int, r_name char (25), r_comment varchar (152)) engine=INFINIDB;
insert into qaTestAuto values(101, 'Hawaiian nation', 'Aloha');
insert into qaTestAuto values(102, 'Honolulu City Light', 'Mahalo');
insert into qaTestAuto values(103, 'Humuhumunukunukuapuaa ', 'Hawaii
state fish');
select r_regionkey from qatestauto;
update qaTestAuto set r_regionkey = (select r_regionkey from tpch1.region where r_name = 'ASIA');
select r_regionkey from qatestauto;
drop table qaTestAuto;
