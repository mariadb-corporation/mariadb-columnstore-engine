
target_include_directories(ddlpackageproc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(ddlpackageproc SHARED 
    ddlpackageprocessor.cpp
    createtableprocessor.cpp
    altertableprocessor.cpp
    droptableprocessor.cpp
    markpartitionprocessor.cpp
    restorepartitionprocessor.cpp
    droppartitionprocessor.cpp)


target_link_libraries(ddlpackageproc 
  INTERFACE ddlpackageproc_h 
  PRIVATE
    ddlpackage
    writeengine_h
    we_shared_h
    mariadb
    we_server_h
    writeengineclient_h
    Boost::regex
  PUBLIC
    brm
)

set_target_properties(ddlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(ddlpackageproc)

install(TARGETS ddlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

