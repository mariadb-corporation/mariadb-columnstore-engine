--disable_query_log
REVOKE ALL PRIVILEGES ON *.* FROM 'cejuser'@'localhost';
DROP USER 'cejuser'@'localhost';
--enable_query_log
