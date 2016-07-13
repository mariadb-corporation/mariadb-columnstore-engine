
include_directories( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT ddl-gram.cpp ddl-gram-temp.cpp
    COMMAND ./ddl-gram.sh ${BISON_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ddl.y
)
ADD_CUSTOM_TARGET(
    ddlgramSRC ALL DEPENDS ddl-gram-temp.cpp
)

ADD_CUSTOM_COMMAND(
    OUTPUT ddl-scan.cpp ddl-scan-temp.cpp
    COMMAND ./ddl-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ddl.l
)
ADD_CUSTOM_TARGET(
    ddlscanSRC ALL DEPENDS ddl-scan-temp.cpp
)

########### next target ###############

set(ddlpackage_LIB_SRCS
    serialize.cpp
    ddl-scan.cpp
    ddl-gram.cpp
    ddlpkg.cpp
    columndef.cpp
    createtable.cpp
    tabledef.cpp
    sqlstatement.cpp
    sqlstatementlist.cpp
    altertable.cpp
    createindex.cpp
    dropindex.cpp
    droptable.cpp
    sqlparser.cpp
    markpartition.cpp
    restorepartition.cpp
    droppartition.cpp)

add_library(ddlpackage SHARED ${ddlpackage_LIB_SRCS})

add_dependencies(ddlpackage ddlgramSRC ddlscanSRC)

set_target_properties(ddlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlpackage DESTINATION ${ENGINE_LIBDIR})

