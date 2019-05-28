

add_executable(getConfig main.cpp)

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

install(TARGETS getConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

