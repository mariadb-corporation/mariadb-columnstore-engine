
add_executable(columnstoreDBWrite columnstoreDB.cpp)

target_link_libraries(columnstoreDBWrite 
  PRIVATE oamcpp_h
          messageqcpp_h
          configcpp_h
          alarmmanager_h
          brm_h
          joblist_h
          loggingcpp_h
          common_h
          idbboot_h)

install(TARGETS columnstoreDBWrite DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

