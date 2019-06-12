
cs_add_executable(viewtablelock viewtablelock.cpp)

target_link_libraries(viewtablelock 
  PRIVATE Boost::system
          brm
          common_h
          idbboot_h
          joblist_h
          )


install(TARGETS viewtablelock DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

