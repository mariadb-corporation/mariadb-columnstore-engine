CREATE DATABASE columnstore_info;
USE columnstore_info;

DELIMITER //
CREATE FUNCTION `format_filesize`(filesize FLOAT) RETURNS varchar(20) CHARSET utf8 DETERMINISTIC
BEGIN

DECLARE n INT DEFAULT 1;

LOOP
    IF filesize < 1024 THEN
        RETURN concat(round(filesize, 2), ' ', elt(n, 'Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB', 'BB'));
    END IF;
    SET filesize = filesize / 1024;
    SET n = n + 1;
END LOOP;

END //


CREATE PROCEDURE total_usage ()
BEGIN
    SELECT
        (SELECT format_filesize(sum(data_size)) TOTAL_DATA_SIZE FROM INFORMATION_SCHEMA.COLUMNSTORE_EXTENTS) TOTAL_DATA_SIZE,
        (SELECT format_filesize(sum(file_size)) TOTAL_DISK_USAGE FROM INFORMATION_SCHEMA.COLUMNSTORE_FILES) TOTAL_DISK_USAGE;
END //

CREATE PROCEDURE table_usage (IN t_name char(64))
BEGIN
    IF t_name IS NOT NULL THEN
    SELECT TABLE_NAME, format_filesize(sum(cf.file_size)) DATA_DISK_USAGE, format_filesize(sum(IFNULL(ccf.file_size, 0))) DICT_DISK_USAGE, format_filesize(sum(cf.file_size) + sum(IFNULL(ccf.file_size, 0))) TOTAL_USAGE FROM INFORMATION_SCHEMA.COLUMNSTORE_COLUMNS cc
JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES cf ON cc.object_id = cf.object_id
LEFT JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES ccf ON cc.dictionary_object_id = ccf.object_id
WHERE table_name = t_name GROUP BY table_name;
    ELSE
    SELECT TABLE_NAME, format_filesize(sum(cf.file_size)) DATA_DISK_USAGE, format_filesize(sum(IFNULL(ccf.file_size, 0))) DICT_DISK_USAGE, format_filesize(sum(cf.file_size) + sum(IFNULL(ccf.file_size, 0))) TOTAL_USAGE FROM INFORMATION_SCHEMA.COLUMNSTORE_COLUMNS cc
JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES cf ON cc.object_id = cf.object_id
LEFT JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES ccf ON cc.dictionary_object_id = ccf.object_id
GROUP BY table_name;
    END IF;
END //
    
CREATE PROCEDURE compression_ratio()
BEGIN
SELECT CONCAT(((sum(compressed_data_size) / sum(data_size)) * 100), '%') COMPRESSION_RATIO FROM INFORMATION_SCHEMA.COLUMNSTORE_EXTENTS ce
JOIN INFORMATION_SCHEMA.COLUMNSTORE_FILES cf ON ce.object_id = cf.object_id
WHERE compressed_data_size IS NOT NULL;
END //

DELIMITER ;
