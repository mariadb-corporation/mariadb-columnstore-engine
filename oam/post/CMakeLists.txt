
install(
  PROGRAMS functions 
          test-001.sh 
          test-002.sh 
          test-003.sh 
          test-004.sh 
  DESTINATION ${CMAKE_INSTALL_DATADIR} 
  COMPONENT platform)

