# What is Query Accelerator

Query Accelerator is a feature that allows MariaDB to use ColumnStore to execute queries that are otherwise executed by InnoDB.
Under the hood Columnstore:
- receives a query
- searches for applicable Engine Independent statistics for InnoDB table index column
- applies RBO rule to transform its InnoDB tables into a number of UNIONs over non-overlapping ranges of a suitable InnoDB table index
- retrieves the data in parallel from MariaDB and runs it using Columnstore runtime

# How to enable Query Accelerator

- Set `columnstore_innodb_queries_use_mcs = on` in MariaDB configuration file and restart MariaDB server (my.cnf).
- Use the convenience routines in the `queryacc` schema (automatically created during ColumnStore installation):
```SQL
-- Enable Query Accelerator and save previous settings
SET @old_settings = queryacc.enable_queryacc();

-- Run your queries
SELECT ...;

-- Disable and restore previous settings
CALL queryacc.disable_queryacc(@old_settings);
```
- To run a single query with Query Accelerator without manually managing enable/disable:
```SQL
CALL queryacc.with_queryacc('SELECT ...');
```

> **Warning:** Do not leave Query Accelerator enabled for an entire session. Always call `disable_queryacc()` after your queries, or use `with_queryacc()` which handles this automatically.

# Enable ColumnStore processing for InnoDB tables
There must be Engine Independent statistics for InnoDB table index column to be used for Query Accelerator.
```SQL
analyze table <table_name> persistent for columns (<column_name>) indexes();
```

# Control client session variables and parameters

- `columnstore_unstable_optimizer`: enables unstable optimizer that is required for Query Accelerator RBO rule
- `columnstore_select_handler`: enables/disables ColumnStore processing for InnoDB tables
- `columnstore_query_accel_parallel_factor`: controls the number of parallel ranges to be used for Query Accelerator

Watch out `max_connections`. If you set `columnstore_query_accel_parallel_factor` to a high value, you may need to increase `max_connections` to avoid connection pool exhaustion.

> **Note:** `enable_queryacc()` sets `columnstore_query_accel_parallel_factor` to 5 by default. To use a different value, set it manually after calling `enable_queryacc()`.

# How to verify Query Accelerator is being used
There are two ways to verify Query Accelerator is being used:
1. Use `select mcs_get_plan('rules')` to get a list of the rules that were applied to the query.
2. Look for patterns like `derived table - $added_sub_#db_name_#table_name_X` in the optimized plan using `select mcs_get_plan('optimized')`. 
