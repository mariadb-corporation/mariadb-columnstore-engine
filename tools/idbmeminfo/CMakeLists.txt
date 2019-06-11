
cs_add_executable(idbmeminfo idbmeminfo.cpp)

target_link_libraries(idbmeminfo 
  PRIVATE messageqcpp_h)

install(TARGETS idbmeminfo DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

