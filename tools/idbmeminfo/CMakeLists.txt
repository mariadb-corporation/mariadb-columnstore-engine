
cs_add_executable(idbmeminfo idbmeminfo.cpp)

target_link_libraries(idbmeminfo 
  PRIVATE 
    messageqcpp_h
    Boost::system  
  )

install(TARGETS idbmeminfo DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

