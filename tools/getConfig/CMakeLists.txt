

cs_add_executable(getConfig main.cpp)

target_link_libraries(getConfig 
  PRIVATE brm
          configcpp
          loggingcpp
          messageqcpp_h
          idbboot
          Boost::system
          Boost::filesystem
          rt
          )

install(TARGETS getConfig DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

