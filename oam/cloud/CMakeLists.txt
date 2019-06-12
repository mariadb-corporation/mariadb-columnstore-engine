
install(
  PROGRAMS MCSInstanceCmds.sh 
           MCSVolumeCmds.sh 
           MCSgetCredentials.sh 
  DESTINATION ${CMAKE_INSTALL_BINDIR} 
  COMPONENT platform)
