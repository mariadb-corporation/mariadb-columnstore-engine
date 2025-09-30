if (`SELECT (sys.version_major(), sys.version_minor()) < (10, 9)`) 
{ 
    skip Should run with after MariaDB 10.9 binaries;
}
