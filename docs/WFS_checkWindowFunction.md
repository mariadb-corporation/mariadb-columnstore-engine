Statements:
- WFS adds extra aux columns into projection.
- WFS sorts using the outer ORDER BY key column list()

jobInfo.windowDels is used as a temp storage to pass unchanged list of jobInfo.deliveredCols(read as projection list of delivered cols) through WFS if there is no GB/DISTINCT involved.
GB/DISTINCT can clean the list up on their own.

This method might implicitly add GB column in case of aggregates over WF and w/o explicit GROUP BY. This is disabled in WFS::AddSimpleColumn
```SQL
SELECT min(fist_value(i) partition over i) from tab1; 
```



