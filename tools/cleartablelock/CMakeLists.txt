
# cleartablelock 

cs_add_executable(cleartablelock cleartablelock.cpp cleartablelockthread.cpp)

target_link_libraries(cleartablelock 
  PRIVATE 
    brm
    writeengine_h
    we_shared_h
    we_server_h
    Boost::thread
    )


install(TARGETS cleartablelock DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

