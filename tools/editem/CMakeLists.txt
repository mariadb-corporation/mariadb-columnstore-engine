
add_executable(editem editem.cpp)

target_link_libraries(editem 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          dataconvert_h
          execplan_h 
          joblist_h
          loggingcpp_h
          messageqcpp_h 
          oamcpp_h)

install(TARGETS editem DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

