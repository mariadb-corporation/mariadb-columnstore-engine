
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(ddlpackageproc_LIB_SRCS
    ddlpackageprocessor.cpp
    createtableprocessor.cpp
    altertableprocessor.cpp
    droptableprocessor.cpp
    markpartitionprocessor.cpp
    restorepartitionprocessor.cpp
    droppartitionprocessor.cpp)

add_library(ddlpackageproc SHARED ${ddlpackageproc_LIB_SRCS})

add_dependencies(ddlpackageproc libnetsnmpmibs)

set_target_properties(ddlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlpackageproc DESTINATION ${ENGINE_LIBDIR})

