DROP TABLE IF EXISTS smoke_table;
CREATE TABLE smoke_table(a int) ENGINE COLUMNSTORE;
INSERT INTO smoke_table VALUES(42);
SELECT * FROM smoke_table;
---DROP TABLE smoke_table;
