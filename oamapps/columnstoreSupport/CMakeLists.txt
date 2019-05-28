

cs_add_executable(columnstoreSupport columnstoreSupport.cpp)

target_link_libraries(columnstoreSupport 
  PRIVATE alarmmanager_h
          brm
          common_h
          idbboot
          joblist_h
          Readline::Readline
          Boost::system
          Boost::filesystem
          Threads::Threads
          )

install(TARGETS columnstoreSupport DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

install(
  PROGRAMS alarmReport.sh 
          bulklogReport.sh 
          configReport.sh 
          hadoopReport.sh 
          hardwareReport.sh 
          logReport.sh 
          resourceReport.sh 
          softwareReport.sh 
  DESTINATION ${CMAKE_INSTALL_BINDIR} 
  COMPONENT platform)
              
