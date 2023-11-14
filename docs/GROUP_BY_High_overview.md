
Firstly, here is the context for GB in MCS in general. The whole facility is spread between multiple compilation units. Speaking in terms of classical SQL engine processing. There is a code for PLAN and PREPARE phases. It is glued together b/c now MCS doesn't have a clear distinction b/w the two. It is available in dbcon/mysql/ and dbcon/joblist. I don't think that you will need this part so I will omit it for simplicity. 
There is EXECUTE phase code that you will optimize. 
It mainly consists of symbols that defined
- [here](https://github.com/mariadb-corporation/mariadb-columnstore-engine/blob/develop/dbcon/joblist/tupleaggregatestep.cpp). SQL AST is translated into a flat program called JobList where Jobs are called Steps and closely relates to the nodes of the original AST. There is TupleAggregateStep that is an equivalent for GROUP BY, DISTINCT and derivatives. This file describes high-level control of TupleAggregateStep.
- [here](https://github.com/mariadb-corporation/mariadb-columnstore-engine/blob/develop/utils/rowgroup/rowaggregation.cpp) is the aggregate and distinct machinery that operates on rows. 
- [here](https://github.com/mariadb-corporation/mariadb-columnstore-engine/blob/develop/utils/rowgroup/rowstorage.cpp) is an abstraction of hashmap based as I prev said on RobinHood header-only hash map.

