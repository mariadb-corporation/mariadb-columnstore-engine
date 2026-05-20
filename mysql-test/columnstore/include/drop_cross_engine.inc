--disable_query_log
--disable_result_log
--disable_warnings
--disable_abort_on_error
let $conn_id = `SELECT id FROM information_schema.processlist WHERE user='cejuser'@'localhost' LIMIT 1`;
if ($conn_id) {
  eval KILL CONNECTION $conn_id;
}
let $conn_id = `SELECT id FROM information_schema.processlist WHERE user='cejuser'@'127.0.0.1' LIMIT 1`;
if ($conn_id) {
  eval KILL CONNECTION $conn_id;
}
let $conn_id = `SELECT id FROM information_schema.processlist WHERE user='cejuser' LIMIT 1`;
if ($conn_id) {
  eval KILL CONNECTION $conn_id;
}
DROP USER IF EXISTS 'cejuser'@'localhost';
DROP USER IF EXISTS 'cejuser'@'127.0.0.1';
--enable_abort_on_error
--enable_warnings
--enable_result_log
--enable_query_log
