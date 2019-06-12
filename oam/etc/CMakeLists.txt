
install(
  FILES AlarmConfig.xml 
        Columnstore.xml 
        ProcessConfig.xml 
        ConsoleCmds.xml 
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR} 
  COMPONENT platform)
