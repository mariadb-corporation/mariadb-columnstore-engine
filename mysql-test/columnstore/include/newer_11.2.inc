if (`SELECT (sys.version_major(), sys.version_minor()) < (11, 2)`)
{
    skip Compatible only with server >= 11.2;
}
