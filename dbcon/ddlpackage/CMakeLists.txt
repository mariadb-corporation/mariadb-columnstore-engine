
include_directories( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.cpp
    COMMAND ./ddl-gram.sh ${BISON_EXECUTABLE}
    COMMAND ./ddl-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ddl.y ddl.l
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

add_dependencies(ddlpackage ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.cpp)

set_target_properties(ddlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

