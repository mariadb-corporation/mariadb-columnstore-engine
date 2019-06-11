
cs_add_executable(ddlcleanup ddlcleanup.cpp)

target_link_libraries(ddlcleanup 
  PRIVATE
    brm
    ddlpackageproc_h
    ddlpackage_h
    we_shared_h
    writeengine_h
    writeengineclient_h
    ddlcleanuputil
    Boost::thread
  )

install(TARGETS ddlcleanup DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

