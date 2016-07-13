
include_directories( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT dml-gram.cpp dml-gram-temp.cpp
    COMMAND ./dml-gram.sh ${BISON_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS dml.y
)
ADD_CUSTOM_TARGET(
    dmlgramSRC ALL DEPENDS dml-gram-temp.cpp
)

ADD_CUSTOM_COMMAND(
    OUTPUT dml-scan.cpp dml-scan-temp.cpp
    COMMAND ./dml-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS dml.l
)
ADD_CUSTOM_TARGET(
    dmlscanSRC ALL DEPENDS dml-scan-temp.cpp
)

########### next target ###############

set(dmlpackage_LIB_SRCS
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
    dmlparser.cpp)

add_library(dmlpackage SHARED ${dmlpackage_LIB_SRCS})

add_dependencies(dmlpackage dmlgramSRC dmlscanSRC)

set_target_properties(dmlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS dmlpackage DESTINATION ${ENGINE_LIBDIR})

