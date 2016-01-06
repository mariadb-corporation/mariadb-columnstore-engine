SELECT
	`LOGICAL_TABLE_1`.`p_name` AS `COL0`,
	`LOGICAL_TABLE_1`.`p_size` AS `COL1`
FROM
	(select * from part where p_partkey < 20 limit 10) `LOGICAL_TABLE_1`
ORDER BY `COL0`, `COL1`;

SELECT
	`LOGICAL_TABLE_1`.`p_name` AS `COL0`,
	`LOGICAL_TABLE_1`.`p_size` AS `COL1`
FROM
	(select * from part where p_partkey < 20 limit 10) `LOGICAL_TABLE_1`
ORDER BY COL0, COL1;

