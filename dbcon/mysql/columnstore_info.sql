CREATE DATABASE IF NOT EXISTS columnstore_info;
USE columnstore_info;

DROP FUNCTION IF EXISTS `format_filesize`;

DELIMITER //
CREATE FUNCTION `format_filesize`(filesize FLOAT) RETURNS varchar(20) CHARSET utf8 DETERMINISTIC
BEGIN

DECLARE n INT DEFAULT 1;

IF filesize IS NULL THEN
    RETURN '0 Bytes';
END IF;

LOOP
    IF filesize < 1024 THEN
        RETURN concat(round(filesize, 2), ' ', elt(n, 'Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB', 'BB'));
    END IF;
    SET filesize = filesize / 1024;
    SET n = n + 1;
END LOOP;

END //

DROP PROCEDURE IF EXISTS `total_usage` //

CREATE PROCEDURE total_usage ()
BEGIN
    SELECT
        (SELECT columnstore_info.format_filesize(sum(data_size)) TOTAL_DATA_SIZE FROM INFORMATION_SCHEMA.COLUMNSTORE_EXTENTS) TOTAL_DATA_SIZE,
        (SELECT columnstore_info.format_filesize(sum(file_size)) TOTAL_DISK_USAGE FROM INFORMATION_SCHEMA.COLUMNSTORE_FILES) TOTAL_DISK_USAGE;
END //

DROP PROCEDURE IF EXISTS `table_usage` //

CREATE PROCEDURE table_usage (IN t_schema char(64), IN t_name char(64))
`table_usage`: BEGIN

    DECLARE done INTEGER DEFAULT 0;
    DECLARE dbname VARCHAR(64);
    DECLARE tbname VARCHAR(64);
    DECLARE object_ids TEXT;
    DECLARE dictionary_object_ids TEXT;
    DECLARE `locker` TINYINT UNSIGNED DEFAULT IS_USED_LOCK('table_usage');
    DECLARE columns_list CURSOR FOR SELECT TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(object_id) OBJECT_IDS, GROUP_CONCAT(dictionary_object_id) DICT_OBJECT_IDS FROM INFORMATION_SCHEMA.COLUMNSTORE_COLUMNS WHERE table_name = t_name and table_schema = t_schema  GROUP BY table_schema, table_name;
    DECLARE columns_list_sc CURSOR FOR SELECT TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(object_id) OBJECT_IDS, GROUP_CONCAT(dictionary_object_id) DICT_OBJECT_IDS FROM INFORMATION_SCHEMA.COLUMNSTORE_COLUMNS WHERE table_schema = t_schema  GROUP BY table_schema, table_name;
    DECLARE columns_list_all CURSOR FOR SELECT TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(object_id) OBJECT_IDS, GROUP_CONCAT(dictionary_object_id) DICT_OBJECT_IDS FROM INFORMATION_SCHEMA.COLUMNSTORE_COLUMNS  GROUP BY table_schema, table_name;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    IF `locker` IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error acquiring table_usage lock';
        LEAVE `table_usage`;
    END IF;
    DO GET_LOCK('table_usage', 0);
    DROP TABLE IF EXISTS columnstore_info.columnstore_files;
    CREATE TEMPORARY TABLE columnstore_info.columnstore_files (TABLE_SCHEMA VARCHAR(64), TABLE_NAME VARCHAR(64), DATA BIGINT, DICT BIGINT);

    IF t_name IS NOT NULL THEN
        OPEN columns_list;
    ELSEIF t_schema IS NOT NULL THEN
        OPEN columns_list_sc;
    ELSE
        OPEN columns_list_all;
    END IF;

    files_table: LOOP
	IF t_name IS NOT NULL THEN
            FETCH columns_list INTO dbname, tbname, object_ids, dictionary_object_ids;
        ELSEIF t_schema IS NOT NULL THEN
            FETCH columns_list_sc INTO dbname, tbname, object_ids, dictionary_object_ids;
        ELSE
            FETCH columns_list_all INTO dbname, tbname, object_ids, dictionary_object_ids;
        END IF;
        IF done = 1 THEN LEAVE files_table;
        END IF;
        INSERT INTO columnstore_info.columnstore_files (SELECT dbname, tbname, sum(file_size), 0 FROM information_schema.columnstore_files WHERE find_in_set(object_id, object_ids));
        IF dictionary_object_ids IS NOT NULL THEN
    	UPDATE columnstore_info.columnstore_files SET DICT = (SELECT sum(file_size) FROM information_schema.columnstore_files WHERE find_in_set(object_id, dictionary_object_ids)) WHERE TABLE_SCHEMA = dbname AND TABLE_NAME = tbname;
        END IF;
    END LOOP;
    IF t_name IS NOT NULL THEN
        CLOSE columns_list;
    ELSEIF t_schema IS NOT NULL THEN
        CLOSE columns_list_sc;
    ELSE
        CLOSE columns_list_all;
    END IF;
    SELECT TABLE_SCHEMA, TABLE_NAME, columnstore_info.format_filesize(DATA) as DATA_DISK_USAGE, columnstore_info.format_filesize(DICT) as DICT_DATA_USAGE, columnstore_info.format_filesize(DATA + COALESCE(DICT, 0)) as TOTAL_USAGE FROM columnstore_info.columnstore_files;

    DROP TABLE IF EXISTS columnstore_info.columnstore_files;
    DO RELEASE_LOCK('table_usage');
END //

DROP PROCEDURE IF EXISTS `compression_ratio` //

CREATE PROCEDURE compression_ratio()
BEGIN
SELECT CONCAT((SELECT SUM(data_size) FROM information_schema.columnstore_extents ce left join information_schema.columnstore_columns cc on ce.object_id = cc.object_id where compression_type='Snappy') / (SELECT SUM(compressed_data_size) FROM information_schema.columnstore_files WHERE compressed_data_size IS NOT NULL), ':1') COMPRESSION_RATIO;
END //

DELIMITER ;
