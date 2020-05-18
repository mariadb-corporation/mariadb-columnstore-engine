configure_file("${CMAKE_CURRENT_SOURCE_DIR}/MCSVolumeCmds.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/MCSVolumeCmds.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/MCSInstanceCmds.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/MCSInstanceCmds.sh" @ONLY)

install(PROGRAMS  MCSInstanceCmds.sh MCSVolumeCmds.sh MCSgetCredentials.sh DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
