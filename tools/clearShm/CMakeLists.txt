
cs_add_executable(clearShm main.cpp)

target_link_libraries(clearShm 
  PRIVATE
    Boost::thread
    brm
    )

install(TARGETS clearShm DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

