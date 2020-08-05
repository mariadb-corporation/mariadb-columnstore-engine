
INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

FIND_PACKAGE(BISON REQUIRED)
BISON_TARGET(dml_gram dml.y ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.h
  COMPILE_FLAGS "-l -p dml --defines=${CMAKE_CURRENT_BINARY_DIR}/dml-gram.h")

FIND_PACKAGE(FLEX REQUIRED)
FLEX_TARGET(dml_scan dml.l ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp COMPILE_FLAGS "-i -L -Pdml")
ADD_FLEX_BISON_DEPENDENCY(dml_scan dml_gram)

set_source_files_properties(dml-scan.cpp PROPERTIES COMPILE_FLAGS -Wno-sign-compare)

########### next target ###############

include_directories(${CMAKE_CURRENT_BINARY_DIR})  # to pick up flex/bison output
ADD_LIBRARY(dmlpackage SHARED
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

INSTALL(TARGETS dmlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

