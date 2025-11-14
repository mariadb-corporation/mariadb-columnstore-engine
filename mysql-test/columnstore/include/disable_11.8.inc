if (`SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) >= (11, 8, 0)`)
{ 
    skip MCOL-6173, MCOL-6174, ;
}
