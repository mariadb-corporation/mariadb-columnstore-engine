
add_executable(columnstoreDBWrite columnstoreDB.cpp)

target_link_libraries(columnstoreDBWrite 
  PRIVATE alarmmanager_h
          brm
          common_h
          configcpp_h
          execplan_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp
          rwlock_h)

install(TARGETS columnstoreDBWrite DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

