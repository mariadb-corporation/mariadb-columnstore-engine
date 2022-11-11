if (`select version() not like '%10.9%'`) 
{ 
    skip Should run with after MariaDB 10.9 binaries;
}
