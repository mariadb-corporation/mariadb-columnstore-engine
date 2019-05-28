
add_executable(setConfig main.cpp)

target_link_libraries(setConfig 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          joblist_h
          loggingcpp_h
          messageqcpp_h 
          oamcpp_h)

install(TARGETS setConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(PROGRAMS configxml.sh DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

