

add_executable(columnstoreSupport columnstoreSupport.cpp)

#target_compile_options(columnstoreSupport PRIVATE -Wno-unused-result)

target_link_libraries(columnstoreSupport 
  PRIVATE alarmmanager_h
          brm_h
          configcpp_h          
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h 
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
              
