

- addExpresssionStepsToBps, combineJobStepsByTable
	- if TableInfoMap fTableOid is 0

 jobInfo.keyInfo->tupleKeyMap
CrossEngineStep is created, replacing TBPS if fTableOid is 0
setTupleInfo sets 

```
mcsSetConfig CrossEngineSupport User 'cejuser'
mcsSetConfig CrossEngineSupport Password 'Vagrant1|0000001'

CREATE USER IF NOT EXISTS'cejuser'@'localhost' IDENTIFIED BY 'Vagrant1|0000001';
GRANT ALL PRIVILEGES ON *.* TO 'cejuser'@'localhost';
FLUSH PRIVILEGES;
```

```SQL
create table i1(i bigint)engine=innodb;
insert into i1 SELECT floor(rand(seq)*50001) FROM seq_1_to_1000;

analyze table i1 PERSISTENT FOR ALL;
```


```sql
MariaDB [mysql]> select * from column_stats;
+---------+------------+-------------+-----------+-----------+-------------+------------+---------------+-----------+-----------+-----------+
| db_name | table_name | column_name | min_value | max_value | nulls_ratio | avg_length | avg_frequency | hist_size | hist_type | histogram |
+---------+------------+-------------+-----------+-----------+-------------+------------+---------------+-----------+-----------+-----------+
| test    | i1         | i           | 12        | 49977     |      0.0000 |     8.0000 |        1.0183 |        10 | JSON_HB   | {
  "target_histogram_size": 10,
  "collected_at": "2025-05-29 14:54:20",
  "collected_by": "11.4.5-3-MariaDB-debug",
  "histogram_hb": [
    {
      "start": "12",
      "size": 0.100081811,
      "ndv": 1101
    },
    {
      "start": "30480",
      "size": 0.100081811,
      "ndv": 1101
    },
    {
      "start": "31581",
      "size": 0.100081811,
      "ndv": 1101
    },
    {
      "start": "32682",
      "size": 0.100081811,
      "ndv": 1074
    },
    {
      "start": "33756",
      "size": 0.100081811,
      "ndv": 1072
    },
    {
      "start": "34828",
      "size": 0.100081811,
      "ndv": 1071
    },
    {
      "start": "35899",
      "size": 0.100081811,
      "ndv": 1072
    },
    {
      "start": "36971",
      "size": 0.100081811,
      "ndv": 1072
    },
    {
      "start": "38043",
      "size": 0.100081811,
      "ndv": 1072
    },
    {
      "start": "39115",
      "end": "49977",
      "size": 0.099263703,
      "ndv": 1067
    }
  ]
} |
+---------+------------+-------------+-----------+-----------+-------------+------------+---------------+-----------+-----------+-----------+
1 row in set (0.001 sec)
```

Get buckets. Need to come up with recursive CTE that reduces buckets down to N, where N is a parallel factor for CES. 
```sql
SELECT 
  JSON_UNQUOTE(JSON_EXTRACT(hb.value, '$.start')) AS start,
  JSON_UNQUOTE(JSON_EXTRACT(hb.value, '$.size')) AS size,
  JSON_UNQUOTE(JSON_EXTRACT(hb.value, '$.ndv')) AS ndv
FROM 
  column_stats AS cs,
  JSON_TABLE(
    cs.histogram,
    '$.histogram_hb[*]' COLUMNS (
      value JSON PATH '$'
    )
  ) AS hb
WHERE 
  cs.db_name = 'test' and
  cs.table_name = 'i1' and
  cs.column_name = 'i' and
  cs.hist_type = 'JSON_HB';
```

MDB 
Histogram_json_hb 
read_statistics_for_table once per runtime
get_column_range_cardinality -> every Field::read_stats::histogram -> ? Histogram_json_hb ? 

```SQL
select * from cs1 union all select cs1.i from cs1, cs1 as cs11 where cs1.i=cs1.i;
select * from cs1 union all select cs2.i from cs2, cs3 where cs2.i=cs3.i;

select * from cs1 union all select cs1.i from cs1, cs1 as cs11 where cs1.i=cs1.i;

CREATE TABLE cs1 (i bigint) engine=columnstore;
CREATE table i1(i bigint);
select * from i1 union all select i1.i from i1, cs1 where i1.i=cs1.i;

select i1.i from i1 union all select i1.i from i1, cs1 where i1.i=cs1.i;
Нужно менять верхний уровень 

select i1.i from (select i1.i from i1 union all select i1.i from i1) sub union all select i1.i from i1, cs1 where i1.i=cs1.i;
select * from (select i1.i from i1 union all select i1.i from i1) sub union all select i1.i from i1, cs1 where i1.i=cs1.i;


select i1.i from i1, cs1 where i1.i=cs1.i union all select * from (select i1.i from i1 union all select i1.i from i1) sub;

select i1.i from i1, cs1 where i1.i=cs1.i union all select i1.i from i1;
```


- Простой вариант переписывать leaf без JOIN
- Сложный переписывать leaf с JOIN
	- обе MCS
	- одна MCS

Получается, что если в запросе UNION с UNIT из Foreign, main query будет содержать запрос из primary union unit -> нужен detect primary или нет и переставлять

select * from i1;

select i1.i from i1 rewritten plan differs in columnmap 


adjustLastStep
JobInfo:: pjColList 

When column doesn't have table name it fails to ct 
makeSubQueryStep and projectSimpleColumn
via mapping that delivers ct 
oid = 100 p jobInfo

buildSimpleColFromDerivedTable

to add derived table:
- 

so colPosition for SC is used to build a mapping of subquery columns' types and store it in JobInfo::vtableColTypes using (subQ table oid, table.alias).
В JobList-е есть два неявных инварианта, с поиском которых я провозился дня три:  

- TableAliasName для sub должен иметь пустую schema и tableName, но иметь alias
- все ReturnedColumn исходного CSEP должны иметь пустые schema и table и ReturnedColumn::colPosition() должен быть явно установлен в соответствии с порядком возвращаемых колонок - иначе возвращаемый из sub RowGroup экзотически взорвёт весь JobList  - вариантов масса


select * from (select * from i1) i;



```
#include "simplecolumn.h"
#include "execplan/calpontsystemcatalog.h"
#include "simplefilter.h"
#include "constantcolumn.h"

using namespace execplan;
const SOP opeq(new Operator("="));



int main()
{
  CalpontSelectExecutionPlan csep;

  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
  CalpontSelectExecutionPlan::ColumnMap colMap;

  string columnlength = CALPONT_SCHEMA + "." + SYSCOLUMN_TABLE + "." + COLUMNLEN_COL;

  SimpleColumn* col[1];
  col[0] = new SimpleColumn(columnlength, 22222222);

  SRCP srcp;
  srcp.reset(col[0]);
  colMap.insert({columnlength, srcp});
  csep.columnMapNonStatic(colMap);
  srcp.reset(col[0]->clone());
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  {
    SCSEP csepDerived(new CalpontSelectExecutionPlan());
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnListLocal;
    CalpontSelectExecutionPlan::ColumnMap colMapLocal;

    string columnlength = CALPONT_SCHEMA + "." + SYSCOLUMN_TABLE + "." + COLUMNLEN_COL;

    SimpleColumn* col[1];
    col[0] = new SimpleColumn(columnlength, 22222222);

    SRCP srcpLocal;
    srcpLocal.reset(col[0]);
    colMapLocal.insert({columnlength, srcpLocal});
    csepDerived->columnMapNonStatic(colMapLocal);

    srcp.reset(col[0]->clone());
    returnedColumnListLocal.push_back(srcpLocal);
    csepDerived->returnedCols(returnedColumnList);

    CalpontSelectExecutionPlan::SelectList derivedTables;
    derivedTables.push_back(csepDerived);
    csep.derivedTableList(derivedTables);
  }

  CalpontSelectExecutionPlan::TableList tableList = {execplan::CalpontSystemCatalog::TableAliasName("", "", "alias")};
  csep.tableList(tableList);

  CalpontSelectExecutionPlan::SelectList unionVec;

  for (size_t i = 0; i < 3; ++i)
  {
    SCSEP plan(new CalpontSelectExecutionPlan());
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnListLocal;
    CalpontSelectExecutionPlan::ColumnMap colMapLocal;

    SRCP srcpLocal;
    srcpLocal.reset(col[0]);
    colMapLocal.insert({columnlength, srcpLocal});
    plan->columnMapNonStatic(colMapLocal);
    srcpLocal.reset(col[0]->clone());
    returnedColumnListLocal.push_back(srcpLocal);
    plan->returnedCols(returnedColumnListLocal);

    plan->txnID(csep.txnID());
    plan->verID(csep.verID());
    plan->sessionID(csep.sessionID());
    plan->columnMapNonStatic(colMapLocal);
    plan->returnedCols(returnedColumnListLocal);
    unionVec.push_back(plan);

    // std::cout << plan->toString() << std::endl;
  }

  csep.unionVec(unionVec);
  std::cout << csep.toString() << std::endl;
}
```


set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';

Any type of columns must produce SimpleColumn that preserves ?OID?, op type, CS, timezone and position but sets alias 
sc = new SimpleColumn();
sc->columnName(tcn.column);
sc->tableName(tcn.table);
sc->schemaName(tcn.schema);
sc->oid(oidlist[j].objnum);
sc->alias(!ifp->is_explicit_name() ? tcn.column : ifp->name.str);
sc->tableAlias(gwi.tbList[i].alias);
sc->viewName(viewName, lower_case_table_names);
sc->partitions(gwi.tbList[i].partitions);
sc->resultType(ct);
sc->timeZone(gwi.timeZone);

cols = csep->returnedCols()
sc->resultType(cols[j]->resultType());

select i as c1, i as c2 from i1; doesn't work properly

	newConstantColumnNotNullUsingValNativeNoTz



```

set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';

```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      (select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem) l1,
      orders,
      nation
    where
      s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.recdate_gt_commitdate = 1
      and exists(
        select
          l2.l_orderkey
        from
          lineitem l2
        where
          l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
      and not exists (
        select
          l3.l_orderkey
        from
          lineitem l3
        where
          l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.recdate_gt_commitdate = 1
      )
      and s_nationkey = n_nationkey
      and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```

```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      lineitem l1,
      orders,
      nation
    where
      s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.recdate_gt_commitdate = 1
      and exists(
        select
          l_orderkey
        from
          lineitem l2
        where
          l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
      and not exists (
        select
          l_orderkey
        from
          lineitem l3
        where
          l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.recdate_gt_commitdate = 1
      )
      and s_nationkey = n_nationkey
      and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```

Potentially representative q
```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      lineitem l1
    where
      s_suppkey = l1.l_suppkey
      and exists(
        select
          l_orderkey
        from
          lineitem l2
        where
          l2.l_orderkey = l1.l_orderkey
      )
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```


```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      (select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey < 3000961 UNION ALL select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey >= 3000961 ) l1,
      orders,
      nation
    where
      s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.recdate_gt_commitdate = 1
      and exists(
	    select * from 
	    (
		    select
	          l_orderkey,l_suppkey
	        from
	          lineitem
	        where 
		        l_orderkey < 3000961
	        UNION ALL
	        select
	          l_orderkey,l_suppkey
	        from
	          lineitem
	        where 
		        l_orderkey >= 3000961
	    ) l2
        where
          l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
      and not exists (
        select * from 
	    (
		    select
	          l_orderkey,l_suppkey,recdate_gt_commitdate
	        from
	          lineitem l3_s
	        where 
		        l3_s.l_orderkey < 3000961
		        and l3_s.l_orderkey >= 0
		        and l3_s.recdate_gt_commitdate = 1
	        UNION ALL
	        select
	          l_orderkey,l_suppkey,recdate_gt_commitdate
	        from
	          lineitem l3_s
	        where 
		        l_orderkey >= 3000961 and l3_s.l_orderkey  < 18000000000
		        and l3_s.recdate_gt_commitdate = 1
	    ) l3
        where
          l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          
      )
      and s_nationkey = n_nationkey
      and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```

```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      (select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem) l1,
      orders,
      nation
    where
      s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.recdate_gt_commitdate = 1
      and exists(
	    select * from 
	    (
		    select
	          l_orderkey,l_suppkey
	        from
	          lineitem
	        where 
		        l_orderkey < 3000961
	        UNION ALL
	        select
	          l_orderkey,l_suppkey
	        from
	          lineitem
	        where 
		        l_orderkey >= 3000961
	    ) l2
        where
          l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
      and not exists (
        select * from 
	    (
		    select
	          l_orderkey,l_suppkey,recdate_gt_commitdate
	        from
	          lineitem
	        where 
		        l_orderkey < 3000961
		        and lineitem.recdate_gt_commitdate = 1
	        UNION ALL
	        select
	          l_orderkey,l_suppkey,recdate_gt_commitdate
	        from
	          lineitem
	        where 
		        l_orderkey >= 3000961
		        and lineitem.recdate_gt_commitdate = 1
	    ) l3
        where
          l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
      )
      and s_nationkey = n_nationkey
      and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```


```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      lineitem l1
    where
      s_suppkey = l1.l_suppkey
      and l1.recdate_gt_commitdate = 1
      and not exists (
        select
          l_orderkey
        from
          lineitem l3
        where
          l3.l_orderkey = l1.l_orderkey
      )
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```


```bash
cs_package_manager.sh install dev stable-23.10 cron/792 -dev cspkg
```


```SQL
select
  s_name,
  count(*) as numwait
    from
      supplier,
      (select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey < 300096 UNION ALL
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 3000960 and 6000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 6000960 and 9000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 9000960 and 12000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 12000960 and 15000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 15000960 and 18000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 18000960 and 21000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 21000960 and 24000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 24000960 and 27000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 27000960 and 30000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 30000960 and 33000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 33000960 and 36000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 36000960 and 39000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 39000960 and 42000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 42000960 and 45000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 45000960 and 48000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 48000960 and 52000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 52000960 and 55000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 55000960 and 57000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 57000960 and 60000960 
	) l1,
      orders,
      nation
    where
      s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.recdate_gt_commitdate = 1
      and exists(
	    select * from 
	    (
	    select l_orderkey,l_suppkey from lineitem where l_orderkey < 300096 UNION ALL
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 3000960 and 6000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 6000960 and 9000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 9000960 and 12000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 12000960 and 15000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 15000960 and 18000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 18000960 and 21000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 21000960 and 24000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 24000960 and 27000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 27000960 and 30000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 30000960 and 33000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 33000960 and 36000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 36000960 and 39000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 39000960 and 42000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 42000960 and 45000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 45000960 and 48000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 48000960 and 52000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 52000960 and 55000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 55000960 and 57000960 union all
      select l_orderkey,l_suppkey from lineitem where l_orderkey BETWEEN 57000960 and 60000960
	    ) l2
        where
          l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
      )
      and not exists (
        select * from 
	    (
	    select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem l3_s where l_orderkey < 300096 UNION ALL
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem l3_s where l_orderkey BETWEEN 3000960 and 6000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem l3_s where l_orderkey BETWEEN 6000960 and 9000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem l3_s where l_orderkey BETWEEN 9000960 and 12000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem l3_s where l_orderkey BETWEEN 12000960 and 15000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 15000960 and 18000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 18000960 and 21000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 21000960 and 24000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 24000960 and 27000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 27000960 and 30000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 30000960 and 33000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 33000960 and 36000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 36000960 and 39000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 39000960 and 42000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 42000960 and 45000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 45000960 and 48000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 48000960 and 52000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 52000960 and 55000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 55000960 and 57000960 and l3_s.recdate_gt_commitdate = 1  union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 57000960 and 60000960 and l3_s.recdate_gt_commitdate = 1
	    ) l3
        where
          l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          
      )
      and s_nationkey = n_nationkey
      and n_name = 'SAUDI ARABIA'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
```


```SQL
select
  l_suppkey,
  count(l_orderkey) as numwait
  from (
	  select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey < 300096 UNION ALL
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 3000960 and 6000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 6000960 and 9000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 9000960 and 12000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 12000960 and 15000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 15000960 and 18000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 18000960 and 21000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 21000960 and 24000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 24000960 and 27000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 27000960 and 30000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 30000960 and 33000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 33000960 and 36000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 36000960 and 39000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 39000960 and 42000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 42000960 and 45000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 45000960 and 48000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 48000960 and 52000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 52000960 and 55000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 55000960 and 57000960 union all
      select l_orderkey,l_suppkey,recdate_gt_commitdate from lineitem where l_orderkey BETWEEN 57000960 and 60000960
  ) tmp
  group by
  l_suppkey
order by
  numwait desc,
  l_suppkey
limit
  100;
  
```

```SQL
create table lineitem (
l_orderkey int,
l_partkey int,
l_suppkey int,
l_linenumber bigint,
l_quantity decimal(12,2),
l_extendedprice decimal(12,2),
l_discount decimal(12,2),
l_tax decimal(12,2),
l_returnflag char (1),
l_linestatus char (1),
l_shipdate date,
l_commitdate date,
l_receiptdate date,
l_shipinstruct char (25),
l_shipmode char (10),
l_comment varchar (44)
);

--mariadb tpc_h_i -e "load data infile '/data/columnstore-tooling/tpc-h/dbgen/lineitem.tbl' INTO TABLE lineitem FIELDS TERMINATED BY '|'";
mariadb test -e "load data infile '/data/regr/testData/tpch/1g/lineitem.tbl' INTO TABLE lineitem FIELDS TERMINATED BY '|'";


SET SESSION alter_algorithm='INSTANT';
alter table lineitem add column `recdate_gt_commitdate` tinyint(4) not null default 0;
set autocommit=0;LOCK TABLE lineitem WRITE; update lineitem set recdate_gt_commitdate = 1 where l_receiptdate > l_commitdate; commit; UNLOCK TABLES;

alter table lineitem add index(`l_orderkey`, `l_suppkey`,recdate_gt_commitdate);
```

```SQL

select l_suppkey, count(l_orderkey) as numwait from lineitem group by l_suppkey order by numwait desc, l_suppkey limit 100;

set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';
select l_suppkey, count(l_orderkey) as numwait from (select l_suppkey, l_orderkey from lineitem) s group by l_suppkey order by numwait desc, l_suppkey limit 100;


/etc/my.cnf.d/columnstore.cnf
columnstore_innodb_queries_use_mcs = on

mcsSetConfig CrossEngineSupport User 'cejuser'
mcsSetConfig CrossEngineSupport Password 'Vagrant1|0000001'

CREATE USER IF NOT EXISTS'cejuser'@'localhost' IDENTIFIED BY 'Vagrant1|0000001';
GRANT ALL PRIVILEGES ON *.* TO 'cejuser'@'localhost';
FLUSH PRIVILEGES;
create table i1(col bigint);
insert into i1 values (42),(45),(46);
analyze persistant table i1; 
create index on i1 (col);
	ANALYZE TABLE i1 PERSISTENT FOR ALL;
alter table i1 add index(col);
select col from i1;


set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';
select calsettrace(1);

select l_orderkey,l_suppkey from lineitem limit 10;
select l_suppkey,l_orderkey from (
	select l_suppkey,l_orderkey from lineitem where l_orderkey > min and l_orderkey < median_value union all
	select l_suppkey,l_orderkey from lineitem where l_orderkey >= median and l_orderkey < last_value
	 ) s1
select l_suppkey,l_orderkey from (select l_suppkey,l_orderkey from lineitem limit 10) s1


select l_suppkey,l_orderkey from (select l_suppkey,l_orderkey from lineitem limit 10) s1;


select l_suppkey+1,l_orderkey+1 from (select l_suppkey+1,l_orderkey+1 from lineitem limit 10) s1;

-- must ignore
select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_suppkey from lineitem l,lineitem_10rows nl WHERE l.l_suppkey=nl.l_suppkey limit 10;
-- test set with join
select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_orderkey from lineitem l,lineitem_10rows nl WHERE l.l_suppkey=nl.l_suppkey limit 10;

-- join with derived
select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_orderkey from (select l_orderkey,l_suppkey from lineitem) l,lineitem_10rows nl WHERE l.l_suppkey=nl.l_suppkey limit 10;
-- subquery with AC
select * from (select l_orderkey,l_suppkey+1 from lineitem limit 10) sa where sa.l_orderkey = 1999905;
-- subquery with FC

select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_orderkey from (select l_orderkey,l_suppkey from lineitem) l,(select l_orderkey,l_suppkey from lineitem_10rows) nl WHERE l.l_suppkey=nl.l_suppkey limit 10;

select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_orderkey from (select l_orderkey,l_suppkey from lineitem) l,lineitem_10rows nl WHERE l.l_suppkey=nl.l_suppkey limit 10;

-- feat
-- Запросы без ключевой колонки
select nl.l_orderkey,l.l_suppkey+1 from lineitem l,lineitem_10rows nl WHERE l.l_suppkey=nl.l_suppkey limit 10;
```

Check if the expression uses only index keys `uses_index_fields_only`

Tasks:
- expand support types adding varchar, timestamps
- add tests to see if expressions works
- search for a specific interesting column available
- replacing rule filter to work on tables also
	- alter rule to handle this case
- add support for correlated subquery

```
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: looking for l_orderkey in ctx.gwi.columnStatisticsMap  with size 1
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: key l_orderkey vector size 4
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: columnStatistics.size() 4
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: bucket.start_value 1
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: bucket.start_value 1500738
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: currentLowerBound 1 currentUpperBound 1875794
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: bucket.start_value 3000961
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: bucket.start_value 6000000
Jul 16 22:02:27 drrtuy-u24 mariadbd[727576]: currentLowerBound 3000961 currentUpperBound 6374911
Jul 16 22:02:36 drrtuy-u24 mariadbd[727576]: Adding column statistics for l_orderkey
Jul 16 22:02:36 drrtuy-u24 mariadbd[727576]: Type of histogram object: 17Histogram_json_hb
Jul 16 22:02:36 drrtuy-u24 mariadbd[727576]: gwi.columnStatisticsMap[ifp->field->field_name.str].size() 4
```

Алгоритм для derived c учётом semi-join/scalar subquery фильтров:
- пройти по таблицам и найти подходящие
	- для каждой подходящей
		- оставить фильтр, заменив обращение к локальным колонкам на true
		- создать union и добавить
			- в проекцию все колонки/выражения, содержащие только колонки таблицы для целевой таблицы
			- в фильтр все пригодные выражения из исходного фильтра
				- в фильтре найти для каждой данной таблицы эквивалент?
					- использовать исходный фильтр, заменив все предикаты с нелокальными колонками на true?
				- не должно быть выражений, содержащих несколько таблиц 
				- что делать с join правилами?
		- добавить в map с ключём (schema,table,alias) -> имя derived, если derived был создан
- пройти по проекции и заменить обращения к колонке из derived на обращение к derived

 replaceRefCol
 getSimpleCols
 pt->walk(getSimpleCols, &fSimpleColumnList);
 SC::derivedTable

Для запроса вида 

```SQL
select nl.l_orderkey,l.l_suppkey,nl.l_suppkey,l.l_orderkey from (select l_orderkey,l_suppkey from lineitem) l,(select l_orderkey,l_suppkey from lineitem_10rows) nl WHERE l.l_suppkey=nl.l_suppkey limit 10;
```


Добавить в фильтр ограничение на то, что может быть в проекции масштабируемой таблицы
Проход по SF для поиска SCs setSimpleColumnList + simpleColumnList
-DWITH_SAFEMALLOC=OFF and sql_alloc.h SqlAlloc class controls memory allocation in a thread blowing up if I want to copy Histogram_json_bb instance
Если несколько таблиц, то AC/FC -> можно выбрать и заменить данные в указателях на колонки.

1 UNION UNIT возвращает нормальное значение для второй колонки, 2ой UNION UNIT в RClist имеет две l_orderkey ! - SC::setSimpleColumnList - добавлял в vector, не очищая его.

```
set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';
select calsettrace(1);

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
16,5 secs

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from (
SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 1 AND l_orderkey < 250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 250001 AND l_orderkey < 500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 500001 AND l_orderkey < 750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 750001 AND l_orderkey < 1000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 1000001 AND l_orderkey < 1250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 1250001 AND l_orderkey < 1500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 1500001 AND l_orderkey < 1750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 1750001 AND l_orderkey < 2000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 2000001 AND l_orderkey < 2250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 2250001 AND l_orderkey < 2500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 2500001 AND l_orderkey < 2750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 2750001 AND l_orderkey < 3000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 3000001 AND l_orderkey < 3250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 3250001 AND l_orderkey < 3500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 3500001 AND l_orderkey < 3750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 3750001 AND l_orderkey < 4000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 4000001 AND l_orderkey < 4250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 4250001 AND l_orderkey < 4500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 4500001 AND l_orderkey < 4750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 4750001 AND l_orderkey < 5000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 5000001 AND l_orderkey < 5250001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 5250001 AND l_orderkey < 5500001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 5500001 AND l_orderkey < 5750001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 5750001 AND l_orderkey < 6000001

UNION ALL

SELECT l_orderkey, l_suppkey, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
FROM lineitem
WHERE l_orderkey >= 6000001 AND l_orderkey <= 6144000
  ) tmp
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;

9,5 secs
```


```SQL
set columnstore_unstable_optimizer=on;
set @@optimizer_switch='derived_merge=off';
select calsettrace(1);

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;

(45,42) 
(46,40)
(48,41),
(NULL,41)

1 - 45*42 + 46 * 40
2 - 48 * 41

? sum(c1 * c2) | range(k) = [45,46] === sum( sum(c1 * c2) | range(k) =[45,46), sum(c1 * c2) | range(k) =[46,48))

select count(col),col2 from i1 GROUP BY col2;

select count(s1.col),s1.col2,min(s1.col)+s1.col2
from
(
 select col,col2 from i1 where col >= min AND < median UNION ALL
 select col,col2 from i1 where col >= median AND <= max
) s1
GROUP BY s1.col2;



```

фичи:
- собрать первые колонки ключей, по которым есть статистика и сделать из них SC, которые можно использовать - extractColumnStatistics ходит по table, а не по ifp
- запрос `select l_suppkey from lineitem limit 10;`
- Статистика собирается из таблиц, нужно добавить SC-кандидаты в статистику


Про фильтры MCOL-6117
- Рассмотрев, что фильтр содержит выражения из:
  - колонок, прендалежищих не затронутым таблицам (гр 1)
  - выражений, содержащих колонки только незатронутых таблиц (гр 1)

  - колонок  затронутых таблиц SC (гр 2)
    - замапить на SC соответствующих derived
  - выражений, содержащих колонки только затронутых таблиц (гр 2)
    - замапить SC затронутых таблиц на SC derived
    - ОПТ замапить на SC затронутых таблиц, если нет AC в поддереве
  - выражений, содержащих колонки затронутых таблиц и колонок не затронутых таблиц (гр 2)
    - замапить SC затронутых таблиц на SC derived
- применить правила маппинга SC в выражении фильтра
- прогнать существующее правило проброса условий вниз
  - если не получится, то клонить дерево фильтра и заменять нерелевантные части на true
  - добавить правило очистки от constant true в дереве
  - правило проброса условий вниз работает для derived, но не работает для UNION
    - оптимизация добавить правило проброса условий в UNION 
