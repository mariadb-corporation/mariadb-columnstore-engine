create table if not exists dim_date ( companycode integer, FinancialQuarterID integer,
FinancialQuarter integer, FinancialYear integer ) engine=infinidb;
insert into dim_date values (1,1,2,1);
insert into dim_date values (1,3,2,1);
select count(*)     from dim_date x_varDate      where      FinancialQuarterID > CASE      WHEN
x_varDate.FinancialQuarter <= 3 THEN (x_varDate.FinancialYear-1) ELSE (x_varDate.FinancialYear) END ;
select count(*)     from dim_date x_varDate      where      FinancialQuarterID BETWEEN CASE      WHEN
x_varDate.FinancialQuarter <=               3       THEN          ((x_varDate.FinancialYear  - 1) * 10)          +
(1 + x_varDate.FinancialQuarter)       ELSE          (x_varDate.FinancialYear * 10)          +
(x_varDate.FinancialQuarter - 3)         END       AND x_varDate.FinancialQuarterID;
drop table dim_date;

