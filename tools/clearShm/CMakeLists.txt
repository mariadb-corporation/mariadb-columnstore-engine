
add_executable(clearShm main.cpp)

target_link_libraries(clearShm 
  PRIVATE
    messageqcpp
    execplan
    rwlock
    loggingcpp
    configcpp
    Boost::thread
    brm
    rt
    )

install(TARGETS clearShm DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

