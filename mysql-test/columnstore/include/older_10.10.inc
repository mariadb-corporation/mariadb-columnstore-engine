if (`SELECT (sys.version_major(), sys.version_minor()) >= (10, 10)`)
{
    skip Compatible only with server < 10.10;
}
