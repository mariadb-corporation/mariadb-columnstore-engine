\! rm -f /tmp/bug3935_outfile.txt

select n_nationkey, n_name into outfile '/tmp/bug3935_outfile.txt'
FIELDS TERMINATED BY '|' LINES TERMINATED BY '/n' from nation;
\! rm -f /tmp/bug3935_outfile.txt

select n_nationkey, n_name from nation into outfile '/tmp/bug3935_outfile.txt'
FIELDS TERMINATED BY '|' LINES TERMINATED BY '/n';
\! rm -f /tmp/bug3935_outfile.txt

select n_nationkey, n_name into outfile '/tmp/bug3935_outfile.txt'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '/n'
from nation;
\! rm -f /tmp/bug3935_outfile.txt

SELECT 'ID', 'Name' UNION (SELECT n_nationkey, n_name INTO OUTFILE
'/tmp/bug3935_outfile.txt' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
FROM nation ORDER BY n_name DESC);
\! rm -f /tmp/bug3935_outfile.txt

SELECT 'ID', 'Name' UNION (SELECT n_nationkey, n_name INTO OUTFILE
'/tmp/bug3935_outfile.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'  FROM nation ORDER BY n_name DESC);
\! rm -f /tmp/bug3935_outfile.txt

SELECT *
INTO OUTFILE '/tmp/bug3935_outfile.txt'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
FROM (SELECT 'ID', 'Name'
       UNION
      SELECT n_nationkey, n_name from nation) a;
\! rm -f /tmp/bug3935_outfile.txt

SELECT a.*
INTO OUTFILE '/tmp/bug3935_outfile.txt'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
FROM (SELECT 'ID', 'Name'
UNION
SELECT n_nationkey, n_name from nation order by 2 desc) a;
\! rm -f /tmp/bug3935_outfile.txt
