target_include_directories(ddlcleanuputil_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(ddlcleanuputil SHARED ddlcleanuputil.cpp)

target_link_libraries(ddlcleanuputil
  INTERFACE ddlcleanuputil_h
  PRIVATE
    brm
    ddlpackageproc
    ddlpackage_h 
    we_shared_h
    writeengine
    writeengineclient_h
    )

set_target_properties(ddlcleanuputil PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlcleanuputil DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

