
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

add_dependencies(ddlpackageproc loggingcpp)

target_link_libraries(ddlpackageproc ${NETSNMP_LIBRARIES})

install(TARGETS ddlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

