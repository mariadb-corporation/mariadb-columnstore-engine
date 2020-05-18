configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcstest-001.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcstest-001.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcstest-002.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcstest-002.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcstest-003.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcstest-003.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcstest-004.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcstest-004.sh" @ONLY)

install(PROGRAMS  columnstore_functions mcstest-001.sh mcstest-002.sh mcstest-003.sh mcstest-004.sh DESTINATION ${ENGINE_SUPPORTDIR} COMPONENT columnstore-engine)

