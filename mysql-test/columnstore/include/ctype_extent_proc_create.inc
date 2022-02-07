
DELIMITER $$;
CREATE PROCEDURE show_extent_min_max(schema_arg VARCHAR(64),
                                     table_arg VARCHAR(64),
                                     column_arg VARCHAR(64))
BEGIN
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    hex(CAST(e.min_value AS SIGNED)),
    hex(CAST(e.max_value AS SIGNED))
  FROM
    information_schema.columnstore_extents e,
    information_schema.columnstore_columns c
  WHERE c.object_id=e.object_id
    AND table_schema=schema_arg
    AND table_name=table_arg
    AND column_name=column_arg;
END;
$$
DELIMITER ;$$

DELIMITER $$;
CREATE FUNCTION caltrace_extract_block_diagnostics(msg TEXT CHARACTER SET utf8mb4)
        RETURNS TEXT CHARACTER SET utf8mb4
BEGIN
  RETURN CONCAT(regexp_substr(msg, '(BlocksTouched-[^;]*);'), ' ',
                regexp_substr(msg, '(PartitionBlocksEliminated-[^;]*);'));
END;
$$
DELIMITER ;$$
