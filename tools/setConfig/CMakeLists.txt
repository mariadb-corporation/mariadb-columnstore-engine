
add_executable(setConfig main.cpp)

target_link_libraries(setConfig 
  PRIVATE Boost::thread
          Boost::system
          alarmmanager_h
          brm
          common_h
          configcpp
          execplan_h
          joblist_h
          loggingcpp_h
          messageqcpp_h 
          oamcpp
          rwlock_h)

install(TARGETS setConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(PROGRAMS configxml.sh DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

