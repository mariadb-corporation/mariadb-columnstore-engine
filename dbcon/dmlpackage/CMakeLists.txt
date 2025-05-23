include_directories(${ENGINE_COMMON_INCLUDES})

find_package(BISON REQUIRED)
bison_target(
    dml_gram dml.y ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.h
    COMPILE_FLAGS "-l -p dml --defines=${CMAKE_CURRENT_BINARY_DIR}/dml-gram.h"
)

find_package(FLEX REQUIRED)
flex_target(dml_scan dml.l ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp COMPILE_FLAGS "-i -L -Pdml")
add_flex_bison_dependency(dml_scan dml_gram)

set_source_files_properties(
    dml-scan.cpp PROPERTIES COMPILE_FLAGS "-Wno-register -Wno-deprecated-register -Wno-sign-compare -DYY_NO_INPUT"
)
set_source_files_properties(dml-gram.cpp PROPERTIES COMPILE_FLAGS "-Wno-unused-but-set-variable")
# ########## next target ###############

include_directories(${CMAKE_CURRENT_BINARY_DIR}) # to pick up flex/bison output
columnstore_library(
    dmlpackage
    dml-scan.cpp
    dml-gram.cpp
    calpontdmlfactory.cpp
    calpontdmlpackage.cpp
    dmlcolumn.cpp
    deletedmlpackage.cpp
    dmlobject.cpp
    insertdmlpackage.cpp
    mysqldmlstatement.cpp
    oracledmlstatement.cpp
    row.cpp
    dmltable.cpp
    updatedmlpackage.cpp
    vendordmlstatement.cpp
    commanddmlpackage.cpp
    dmlpkg.cpp
    dmlparser.cpp
    ${BISON_dml_gram_OUTPUTS}
    ${FLEX_dml_scan_OUTPUTS}
)

add_dependencies(dmlpackage loggingcpp)

install(
    TARGETS dmlpackage
    DESTINATION ${ENGINE_LIBDIR}
    COMPONENT columnstore-engine
)
