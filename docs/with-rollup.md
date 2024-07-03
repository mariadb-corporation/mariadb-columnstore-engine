# What is WITH ROLLUP

If you are grouping and aggregating data, adding "wITH ROLLUP" after ``GROUP BY`` part would allow you to get subtotals.

Main page: https://mariadb.com/kb/en/select-with-rollup/

Subtotals are marked with NULLs instead of keys. E.g., if you ``SELECT year, SUM(sales) FROM bookstores GROUP BY year WITH ROLLUP``, the result will contain rows each year with total sales sums and also there will be a row with NULL as year's value and a whole total sales sum.

It is handy (one query instead of three) and also is required for TPC-DS.

# How it is implemented in other engines (speculation)

InnoDB outputs sorted results in ``GROUP BY`` queries. Thus, when ``GROUP BY`` prefix change, we can output one subtotal and start other. There is no storage required for all subtotals, however small it is.

# How it is implemented in Columnstore

Columnstore does not have sort-based aggregation, it has hash-based aggregation. The data can arrive in any order and come out also in any order. We have to keep all subtotals in our hash tables. We also have to distinguish between a data with a NULL value in GROUP BY key and a subtotal key.

Thus, if there is a ``WITH ROLLUP`` modifier present, we add a hidden column to the keys of ``GROUP BY`` part of a query. The column is represented with RollupMarkColumn class which is very similar to a ConstantColumn, but should be handled slightly differently. For example, it cannot be optimized away. When we process data in the aggregation, we process each row as usual, but then:

1. we increase value of the RollupMarkColumn-provided value by one to distinguish from raw data (or mark row as a subtotal row) and
2. we set a corresponding ``GROUP BY`` key to a NULL and process row again.

