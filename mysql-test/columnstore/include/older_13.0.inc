if (`SELECT (sys.version_major(), sys.version_minor()) >= (13, 0)`)
{
    skip Currently incompatible with the server >= 13.0 (MCOL-6561);
}
