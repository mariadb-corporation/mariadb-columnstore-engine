INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

FIND_PACKAGE(BISON REQUIRED)
BISON_TARGET(ddl_gram ddl.y ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.h
  COMPILE_FLAGS "-l -p ddl --defines=${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.h")

FIND_PACKAGE(FLEX REQUIRED)
FLEX_TARGET(ddl_scan ddl.l ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp COMPILE_FLAGS "-i -L -Pddl")
ADD_FLEX_BISON_DEPENDENCY(ddl_scan ddl_gram)

set_source_files_properties(ddl-scan.cpp PROPERTIES COMPILE_FLAGS -Wno-sign-compare)

########### next target ###############

include_directories(${CMAKE_CURRENT_BINARY_DIR}) # to pick up flex/bison output
ADD_LIBRARY(ddlpackage SHARED
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
    droppartition.cpp
    ${BISON_ddl_gram_OUTPUTS}
    ${FLEX_ddl_scan_OUTPUTS}
)

add_dependencies(ddlpackage loggingcpp)

INSTALL(TARGETS ddlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

