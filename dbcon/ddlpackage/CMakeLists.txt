
target_include_directories(ddlpackage_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl.y ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl.l ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl-gram.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ddl-scan.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND /bin/sh ./ddl-gram.sh ${BISON_EXECUTABLE}
    COMMAND /bin/sh ./ddl-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ddl.y ddl.l ddl-gram.sh ddl-scan.sh
)

add_custom_target(ddlpackage_gen DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp)

add_dependencies(ddlpackage_h ddlpackage_gen)

#set_source_files_properties(ddl-scan.cpp PROPERTIES COMPILE_FLAGS -Wno-sign-compare)

# Parser puts extra info to stderr.

########### next target ###############

ADD_LIBRARY(ddlpackage SHARED
    serialize.cpp
    ddl-scan.cpp
    ddl-gram.cpp
    ddlpkg.cpp
    columndef.cpp
    createtable.cpp
    tabledef.cpp
    sqlstatementlist.cpp
    altertable.cpp
    createindex.cpp
    dropindex.cpp
    droptable.cpp
    sqlparser.cpp
    markpartition.cpp
    restorepartition.cpp
    droppartition.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ddl-gram.cpp 
    ${CMAKE_CURRENT_BINARY_DIR}/ddl-scan.cpp
    )

target_compile_definitions(ddlpackage PRIVATE $<$<CONFIG:Debug>:YYDEBUG=1>)

target_link_libraries(ddlpackage 
  INTERFACE ddlpackage_h 
  PRIVATE 
    brm_h
		execplan_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    oamcpp_h
    )


SET_TARGET_PROPERTIES(ddlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

INSTALL(TARGETS ddlpackage DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)

