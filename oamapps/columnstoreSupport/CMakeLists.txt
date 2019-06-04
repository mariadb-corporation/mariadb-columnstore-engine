

add_executable(columnstoreSupport columnstoreSupport.cpp)

target_link_libraries(columnstoreSupport 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp
          execplan_h
          idbboot
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp
          rwlock_h
          Readline::Readline
          Boost::system
          Boost::filesystem
          )

install(TARGETS columnstoreSupport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(
  PROGRAMS alarmReport.sh 
          bulklogReport.sh 
          configReport.sh 
          hadoopReport.sh 
          hardwareReport.sh 
          logReport.sh 
          resourceReport.sh 
          softwareReport.sh 
  DESTINATION ${ENGINE_BINDIR} 
  COMPONENT platform)
              
