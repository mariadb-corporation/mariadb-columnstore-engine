
# cleartablelock 

add_executable(cleartablelock cleartablelock.cpp cleartablelockthread.cpp)

target_link_libraries(cleartablelock 
  PRIVATE 
    brm
    writeengine_h
    we_shared_h
    we_server_h
    Boost::thread
    )

target_pch(cleartablelock)

install(TARGETS cleartablelock DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

