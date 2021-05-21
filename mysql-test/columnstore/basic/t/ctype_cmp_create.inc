--source default_storage_engine_by_combination.inc

--disable_query_log
--eval SET @mysqltest_file='$MYSQLTEST_FILE';
SET @mysqltest_file=regexp_replace(@mysqltest_file, '^(.*)/([a-z0-9_]*)([.]test$)','\\2');
--enable_query_log
SELECT @mysqltest_file;

SET @database=CONCAT('mcs_', @mysqltest_file);
--disable_warnings
EXECUTE IMMEDIATE CONCAT('DROP DATABASE IF EXISTS ', @database);
--enable_warnings
EXECUTE IMMEDIATE CONCAT('CREATE DATABASE ', @database);
--let database=`SELECT @database`
--disable_query_log
--eval USE $database
--enable_query_log


DELIMITER $$;
CREATE PROCEDURE exec(query TEXT)
BEGIN
  SELECT query AS '';
  EXECUTE IMMEDIATE query;
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test01_execval(op VARCHAR(3) CHARACTER SET utf8,
                                val VARCHAR(64) CHARACTER SET utf8)
BEGIN
  DECLARE query TEXT DEFAULT 'SELECT HEX(c1), QUOTE(c1) FROM t1 WHERE c1=''VAL''';
  SET query=REPLACE(query,'=',op);
  SET query=REPLACE(query,'VAL', val);
  CALL exec(query);
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test01_populate(len1 INT, len2 INT)
BEGIN

  -- Make sure the table is handled by a proper ENGINE
  SELECT (SELECT engine FROM information_schema.tables WHERE (table_schema,table_name)=(@database,'t1')) =
         @@default_storage_engine;

  FOR i IN len1..len2
  DO
    INSERT INTO t1 VALUES (RPAD('a',i)),(RPAD('A',i));
    -- LATIN SMALL LETTER A WITH TILDE
    -- LATIN CAPITAL LETTER A WITH TILDE
    INSERT INTO t1 VALUES (RPAD(_latin1 0xE3,i)),(RPAD(_latin1 0xC3,i));
    INSERT INTO t1 VALUES (RPAD('b',i)),(RPAD('B',i));
  END FOR;
  -- MCOL-4454 "ORDER BY BINARY a" is not like in InnoDB
  CALL exec('SELECT QUOTE(c1) FROM t1 ORDER BY c1, BINARY(c1) ASC');
  CALL exec('SELECT QUOTE(c1) FROM t1 ORDER BY c1, BINARY(c1) DESC');
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test01_field_literal_op(len1 INT,
                                         len2 INT,
                                         op VARCHAR(3) CHARACTER SET utf8)
BEGIN
  FOR i IN len1..len2
  DO
    CALL test01_execval(op,RPAD('a',i));
    CALL test01_execval(op,RPAD('A',i));
    CALL test01_execval(op,RPAD(_latin1 0xE3,i)); -- LATIN SMALL LETTER A WITH TILDE
    CALL test01_execval(op,RPAD(_latin1 0xC3,i)); -- LATIN CAPITAL LETTER A WITH TILDE
  END FOR;
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test01_field_literal(len1 INT,
                                      len2 INT)
BEGIN
  CALL test01_field_literal_op(len1, len2, '=');
  CALL test01_field_literal_op(len1, len2, '<=');
  CALL test01_field_literal_op(len1, len2, '<');
  CALL test01_field_literal_op(len1, len2, '>');
  CALL test01_field_literal_op(len1, len2, '>=');
  CALL test01_field_literal_op(len1, len2, '<>');
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test01_field_field()
BEGIN
  SELECT QUOTE(t1.c1), QUOTE(t2.c1) FROM t1, t1 t2 WHERE t1.c1=t2.c1 ORDER BY HEX(t1.c1), HEX(t2.c1);
END;
$$
DELIMITER ;$$


# This query:
#   SELECT DISTINCT c1 FROM t1;
# is not deterministic, because we don't know which exactly distinct value
# will get to the result set, e.g.:  'A' or 'a' or 'A ' or 'a '.
# So here we create a table t2 with distinct values of t1 and make sure
# that every record in t1 has only one matching record in t2.
#

DELIMITER $$;
CREATE PROCEDURE test01_distinct()
BEGIN
  DECLARE vrecno INT DEFAULT 1;
  CALL exec('SELECT GROUP_CONCAT(CONCAT(''['',c1,'']'') ORDER BY BINARY(c1) SEPARATOR '''') FROM t1 GROUP BY c1 ORDER BY c1');
  CALL exec('SELECT COUNT(DISTINCT c1) FROM t1');
  CREATE TABLE t2 AS SELECT DISTINCT c1 FROM t1;
  CALL exec('SELECT COUNT(*) FROM t2');

  CREATE TABLE t3 AS SELECT t1.c1, CAST(0 AS UNSIGNED) AS distinct_count FROM t1 LIMIT 0;
  FOR rec IN (SELECT c1 FROM t1)
  DO
    INSERT INTO t3 SELECT rec.c1, COUNT(*) FROM t2 WHERE t2.c1=rec.c1;
  END FOR;
  CALL exec('SELECT distinct_count, QUOTE(c1) FROM t3 ORDER BY c1, BINARY(c1)');
  DROP TABLE t3;

  CREATE TABLE t4 AS SELECT t1.c1, CAST(0 AS UNSIGNED) AS distinct_recno FROM t1 LIMIT 0;
  FOR rec IN (SELECT c1 FROM t2 ORDER BY c1)
  DO
    INSERT INTO t4 SELECT t1.c1, vrecno FROM t1 WHERE t1.c1=rec.c1 ORDER BY BINARY t1.c1;
    SET vrecno= vrecno+1;
  END FOR;
  CALL exec('SELECT distinct_recno, QUOTE(c1) FROM t4 ORDER BY c1, BINARY(c1)');
  DROP TABLE t4;

  DROP TABLE t2;
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test02_same_table_create_table(dtype VARCHAR(128))
BEGIN
  DECLARE tbl TEXT DEFAULT 'CREATE TABLE t1 (c1 TYPE, c2 TYPE)';
  SET tbl=REPLACE(tbl,'TYPE',dtype);
  CALL exec(tbl);
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test02_same_table_populate()
BEGIN
  INSERT INTO t1 VALUES ('a', 'A');
  INSERT INTO t1 VALUES ('a', 'A ');
  INSERT INTO t1 VALUES ('a ', 'A');
  INSERT INTO t1 VALUES ('a ', 'A ');
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test02_same_table_cmp_field_field_op(op VARCHAR(2))
BEGIN
  DECLARE query TEXT DEFAULT 'SELECT QUOTE(c1), QUOTE(c2) FROM t1 '
                             'WHERE c1=c2 ORDER BY HEX(c1), HEX(c2)';
  SET query=REPLACE(query,'=',op);
  CALL exec(query);
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test02_same_table_cmp_field_field()
BEGIN
  CALL test02_same_table_cmp_field_field_op('=');
  CALL test02_same_table_cmp_field_field_op('<=');
  CALL test02_same_table_cmp_field_field_op('<');
  CALL test02_same_table_cmp_field_field_op('>');
  CALL test02_same_table_cmp_field_field_op('>=');
  CALL test02_same_table_cmp_field_field_op('<>');
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test03(len INT, datatype TEXT)
BEGIN
  CALL exec(REPLACE('CREATE TABLE t1 (c1 DATATYPE, c2 DATATYPE)', 'DATATYPE', datatype));
  CALL exec(REPLACE('INSERT INTO t1 VALUES (LEFT(''Hellooooooooooooooooooo'',LEN),LEFT(''hellooooooooooooooooooo'',LEN))', 'LEN', len));
  CALL exec(REPLACE('CREATE TABLE t2 (c1 DATATYPE, c2 DATATYPE)', 'DATATYPE', datatype));
  CALL exec(REPLACE('INSERT INTO t2 VALUES (LEFT(''Hellooooooooooooooooooo'',LEN),LEFT(''hellooooooooooooooooooo'',LEN))', 'LEN', len));
  CALL exec(REPLACE('SELECT * FROM t1 WHERE c1=''VAL''', 'VAL', LEFT('Hellooooooooooooooooooo', len)));
  CALL exec(REPLACE('SELECT * FROM t1 WHERE c1=''VAL''', 'VAL', LEFT('hellooooooooooooooooooo', len)));
  CALL exec('SELECT * FROM t1 WHERE c1=c2');
  CALL exec('SELECT * FROM t1, t2 WHERE t1.c1=t2.c1');
  CALL exec('SELECT * FROM t1, t2 WHERE t1.c1=t2.c2');
  CALL exec('SELECT * FROM t1, t2 WHERE t1.c2=t2.c1');
  CALL exec('SELECT * FROM t1, t2 WHERE t1.c2=t2.c2');
  CALL exec('DROP TABLE t1');
END;
$$
DELIMITER ;$$

DELIMITER $$;
CREATE PROCEDURE test04_like(len INT, datatype TEXT)
BEGIN
  CALL exec(REPLACE('CREATE TABLE t1 (c1 DATATYPE)', 'DATATYPE', datatype));
  CALL exec(REPLACE('INSERT INTO t1 VALUES (REPEAT(''a'',LEN))','LEN',len));
  CALL exec(REPLACE('INSERT INTO t1 VALUES (REPEAT(''A'',LEN))','LEN',len));
  CALL exec('SELECT * FROM t1 WHERE c1 LIKE ''a%'' ORDER BY BINARY c1');
  CALL exec('SELECT * FROM t1 WHERE c1 LIKE ''A%'' ORDER BY BINARY c1');
  CALL exec(REPLACE('SELECT * FROM t1 WHERE c1 LIKE ''VAL%'' ORDER BY BINARY c1','VAL',REPEAT('a',len)));
  CALL exec(REPLACE('SELECT * FROM t1 WHERE c1 LIKE ''VAL%'' ORDER BY BINARY c1','VAL',REPEAT('A',len)));
  CALL exec('DROP TABLE t1');
END;
$$
DELIMITER ;$$


DELIMITER $$;
CREATE PROCEDURE test05_mcol4726(len INT, datatype TEXT)
BEGIN
  DECLARE small_a                VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT('a',len);
  DECLARE cap_a                  VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT('A',len);
  DECLARE small_a_with_tilde     VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT(_latin1 0xE3,len);
  DECLARE cap_a_with_tilde       VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT(_latin1 0xC3,len);
  DECLARE small_a_with_diaeresis VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT(_latin1 0xE4,len);
  DECLARE cap_a_with_diaeresis   VARCHAR(64) CHARACTER SET utf8 DEFAULT REPEAT(_latin1 0xC4,len);
  DECLARE ins VARCHAR(64) CHARACTER SET utf8 DEFAULT 'INSERT INTO t1 VALUES(IVAL)';
  DECLARE sel VARCHAR(64) CHARACTER SET utf8 DEFAULT 'SELECT c1 FROM t1 WHERE c1=IVAL ORDER BY BINARY c1';

  CALL exec(REPLACE('CREATE TABLE t1 (c1 DATATYPE)', 'DATATYPE', datatype));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', small_a, '''')));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', cap_a, '''')));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', small_a_with_tilde, '''')));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', cap_a_with_tilde, '''')));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', small_a_with_diaeresis, '''')));
  CALL exec(REPLACE(ins, 'IVAL', CONCAT('''', cap_a_with_diaeresis, '''')));

  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', small_a, '''')));
  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', cap_a, '''')));
  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', small_a_with_tilde, '''')));
  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', cap_a_with_tilde, '''')));
  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', small_a_with_diaeresis, '''')));
  CALL exec(REPLACE(sel, 'IVAL', CONCAT('''', cap_a_with_diaeresis, '''')));

  CALL exec('DROP TABLE t1');
END;
$$
DELIMITER ;$$

