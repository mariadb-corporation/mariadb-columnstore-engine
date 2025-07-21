include_directories(${ENGINE_COMMON_INCLUDES})

find_package(BISON REQUIRED)
bison_target(
    ddl_gram ddl.y ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.h
    COMPILE_FLAGS "-l -p ddl --defines=${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.h"
)

find_package(FLEX REQUIRED)
flex_target(ddl_scan ddl.l ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp COMPILE_FLAGS "-i -L -Pddl")
add_flex_bison_dependency(ddl_scan ddl_gram)

set_source_files_properties(
    ddl-scan.cpp PROPERTIES COMPILE_FLAGS "-Wno-register -Wno-deprecated-register -Wno-sign-compare -DYY_NO_INPUT"
)
set_source_files_properties(ddl-gram.cpp PROPERTIES COMPILE_FLAGS "-Wno-unused-but-set-variable")
# ########## next target ###############

include_directories(${CMAKE_CURRENT_BINARY_DIR}) # to pick up flex/bison output
columnstore_library(
    ddlpackage
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
    debugstatement.cpp
    ${BISON_ddl_gram_OUTPUTS}
    ${FLEX_ddl_scan_OUTPUTS}
)

columnstore_link(ddlpackage loggingcpp messageqcpp)
