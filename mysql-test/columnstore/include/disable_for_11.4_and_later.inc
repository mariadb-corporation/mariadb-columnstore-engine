if (`SELECT (sys.version_major(), sys.version_minor(), sys.version_patch()) >= (11, 4, 0)`)
{ 
    skip Known multiupdate bug;
}
