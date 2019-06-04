
add_executable(editem editem.cpp)

target_link_libraries(editem 
  PRIVATE alarmmanager_h
          brm
          common_h
          configcpp_h
          dataconvert
          execplan_h 
          joblist
          loggingcpp
          messageqcpp_h 
          oamcpp_h
          rwlock_h)

install(TARGETS editem DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

