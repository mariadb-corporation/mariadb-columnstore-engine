
INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml-gram.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml-scan.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml.y ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml.l ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND /bin/sh ./dml-gram.sh ${BISON_EXECUTABLE}
    COMMAND /bin/sh ./dml-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS dml.y dml.l
)

########### next target ###############

ADD_LIBRARY(dmlpackage SHARED 
    ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp 
    ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp    
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


target_link_libraries(dmlpackage PUBLIC loggingcpp)

SET_TARGET_PROPERTIES(dmlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

INSTALL(TARGETS dmlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

