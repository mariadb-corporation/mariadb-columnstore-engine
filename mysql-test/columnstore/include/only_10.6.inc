if (`SELECT (sys.version_major(), sys.version_minor()) != (10, 6)`)
{
    skip This test works only with 10.6;
}
