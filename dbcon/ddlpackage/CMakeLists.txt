INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.cpp
    COMMAND ./ddl-gram.sh ${BISON_EXECUTABLE}
    COMMAND ./ddl-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ddl.y ddl.l
)

ADD_CUSTOM_TARGET(ddl-lexer DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.cpp)
ADD_CUSTOM_TARGET(ddl-parser DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.cpp)
# Parser puts extra info to stderr.
MY_CHECK_AND_SET_COMPILER_FLAG("-DYYDEBUG=1" DEBUG)

########### next target ###############

SET(ddlpackage_LIB_SRCS
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

ADD_LIBRARY(ddlpackage SHARED ${ddlpackage_LIB_SRCS})

ADD_DEPENDENCIES(ddlpackage ddl-lexer ddl-parser)

SET_TARGET_PROPERTIES(ddlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

INSTALL(TARGETS ddlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

