
add_executable(viewtablelock viewtablelock.cpp)

target_link_libraries(viewtablelock 
  PRIVATE brm_h
          common_h
          configcpp_h
          execplan_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h)


install(TARGETS viewtablelock DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

