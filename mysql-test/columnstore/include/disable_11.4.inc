if (`SELECT (sys.version_major(), sys.version_minor()) != (10, 6)`)
{
    skip Temporary disabled due to incompatibility with 11.4, MCOL-5948;
}
