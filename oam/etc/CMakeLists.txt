configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ProcessConfig.xml.in" "${CMAKE_CURRENT_SOURCE_DIR}/ProcessConfig.xml" @ONLY)

install(FILES  AlarmConfig.xml 
               Columnstore.xml 
               ProcessConfig.xml 
               ConsoleCmds.xml 
               DESTINATION ${ENGINE_SYSCONFDIR}/columnstore COMPONENT columnstore-engine)
