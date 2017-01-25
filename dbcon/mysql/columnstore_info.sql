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
        (SELECT format_filesize(sum(data_size)) TOTAL_DATA_SIZE FROM INFORMATION_SCHEMA.COLUMNSTORE_EXTENTS) TOTAL_DATA_SIZE,
        (SELECT format_filesize(sum(file_size)) TOTAL_DISK_USAGE FROM INFORMATION_SCHEMA.COLUMNSTORE_FILES) TOTAL_DISK_USAGE;
END //

DROP PROCEDURE IF EXISTS `table_usage` //

CREATE PROCEDURE table_usage (IN t_schema char(64), IN t_name char(64))
`table_usage`: BEGIN

    DECLARE `locker` TINYINT UNSIGNED DEFAULT IS_USED_LOCK('table_usage');

    IF `locker` IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error acuqiring table_usage lock';
        LEAVE `table_usage`;
    END IF;
    DO GET_LOCK('table_usage', 0);
    DROP TABLE IF EXISTS columnstore_info.columnstore_columns;
    DROP TABLE IF EXISTS columnstore_info.columnstore_files;
    CREATE TABLE columnstore_info.columnstore_columns engine=myisam as (select * from information_schema.columnstore_columns);
    ALTER TABLE columnstore_info.columnstore_columns ADD INDEX `object_id` (`object_id`);
    ALTER TABLE columnstore_info.columnstore_columns ADD INDEX `dictionary_object_id` (`dictionary_object_id`);
    CREATE TABLE columnstore_info.columnstore_files engine=myisam as (select * from information_schema.columnstore_files);
    ALTER TABLE columnstore_info.columnstore_files ADD INDEX `object_id` (`object_id`);
    IF t_name IS NOT NULL THEN
SELECT TABLE_SCHEMA, TABLE_NAME, data as DATA_DISK_USAGE, dict as DICT_DISK_USAGE, data + dict as TOTAL_USAGE FROM (
SELECT TABLE_SCHEMA, TABLE_NAME, (SELECT sum(cf.file_size) as data FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema) as data, (SELECT sum(cf.file_size) as dict FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.dictionary_object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema GROUP BY table_schema, table_name) as dict
FROM
columnstore_info.columnstore_columns ics where table_name = t_name and (table_schema = t_schema or t_schema IS NULL)
group by table_schema, table_name
) q;
    ELSEIF t_schema IS NOT NULL THEN
SELECT TABLE_SCHEMA, TABLE_NAME, data as DATA_DISK_USAGE, dict as DICT_DISK_USAGE, data + dict as TOTAL_USAGE FROM (
SELECT TABLE_SCHEMA, TABLE_NAME, (SELECT sum(cf.file_size) as data FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema) as data, (SELECT sum(cf.file_size) as dict FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.dictionary_object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema GROUP BY table_schema, table_name) as dict
FROM
columnstore_info.columnstore_columns ics where table_schema = t_schema
group by table_schema, table_name
) q;
    ELSE
SELECT TABLE_SCHEMA, TABLE_NAME, data as DATA_DISK_USAGE, dict as DICT_DISK_USAGE, data + dict as TOTAL_USAGE FROM (
SELECT TABLE_SCHEMA, TABLE_NAME, (SELECT sum(cf.file_size) as data FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema) as data, (SELECT sum(cf.file_size) as dict FROM columnstore_info.columnstore_columns cc JOIN columnstore_info.columnstore_files cf ON cc.dictionary_object_id = cf.object_id WHERE table_name = ics.table_name and table_schema = ics.table_schema GROUP BY table_schema, table_name) as dict
FROM
columnstore_info.columnstore_columns ics
group by table_schema, table_name
) q;
    END IF;
    DROP TABLE IF EXISTS columnstore_info.columnstore_columns;
    DROP TABLE IF EXISTS columnstore_info.columnstore_files;
    DO RELEASE_LOCK('table_usage');
END //

DROP PROCEDURE IF EXISTS `compression_ratio` //

CREATE PROCEDURE compression_ratio()
BEGIN
SELECT CONCAT(((sum(compressed_data_size) / sum(data_size)) * 100), '%') COMPRESSION_RATIO FROM INFORMATION_SCHEMA.COLUMNSTORE_EXTENTS ce
JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES cf ON ce.object_id = cf.object_id
WHERE compressed_data_size IS NOT NULL;
END //

DELIMITER ;
