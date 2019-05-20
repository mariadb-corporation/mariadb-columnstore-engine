INCLUDE_DIRECTORIES( ${ENGINE_COMMON_INCLUDES} )

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl.y ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl.l ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND /bin/sh ./ddl-gram.sh ${BISON_EXECUTABLE}
    COMMAND /bin/sh ./ddl-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ddl.y ddl.l
)


########### next target ###############

ADD_LIBRARY(ddlpackage SHARED
    serialize.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp 
    ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp
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

target_compile_definitions(ddlpackage PRIVATE $<$<CONFIG:Debug>:YYDEBUG=1>)

target_link_libraries(ddlpackage PUBLIC loggingcpp)

SET_TARGET_PROPERTIES(ddlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

INSTALL(TARGETS ddlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

