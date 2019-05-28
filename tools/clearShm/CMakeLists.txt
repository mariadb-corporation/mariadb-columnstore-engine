
add_executable(clearShm main.cpp)

target_link_libraries(clearShm 
  PRIVATE configcpp_h 
          brm_h)

install(TARGETS clearShm DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

