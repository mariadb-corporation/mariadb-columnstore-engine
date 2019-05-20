
INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

#TODO: put generated files in binary folder
ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/dml-gram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/dml-scan.cpp
    COMMAND /bin/sh ./dml-gram.sh ${BISON_EXECUTABLE}
    COMMAND /bin/sh ./dml-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS dml.y dml.l
)


get_source_file_property(compileOps dml-scan.cpp COMPILE_OPTIONS)
set(compileOps ${compileOps} -Wno-sign-compare)
set_source_files_properties(dml-scan.cpp PROPERTIES COMPILE_OPTIONS ${compileOps})

########### next target ###############

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
	${CMAKE_CURRENT_SOURCE_DIR}/dml-gram.cpp 
	${CMAKE_CURRENT_SOURCE_DIR}/dml-scan.cpp
	)


SET_TARGET_PROPERTIES(dmlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

INSTALL(TARGETS dmlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

