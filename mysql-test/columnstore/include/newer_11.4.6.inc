# Skip test if MariaDB version is older than 11.4.6
# Query Accelerator (RBO parallel_ces) requires MariaDB >= 11.4.6
# because extractColumnStatistics() is compiled as empty stub for older versions
if (`SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) < (11, 4, 6)`)
{
    skip Compatible only with server >= 11.4.6;
}
