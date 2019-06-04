target_include_directories(dmlpackage_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

#TODO: put generated files in binary folder
ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml-gram.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml-scan.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml.y ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dml.l ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND /bin/sh ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.sh ${BISON_EXECUTABLE}
    COMMAND /bin/sh ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.sh ${LEX_EXECUTABLE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS dml.y dml.l
)

#set_source_files_properties(dml-scan.cpp PROPERTIES COMPILE_FLAGS -Wno-sign-compare)

add_custom_target(dmlpackage_gen DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp)


# dmlpackage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                

ADD_LIBRARY(dmlpackage SHARED
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
	  ${CMAKE_CURRENT_BINARY_DIR}/dml-gram.cpp 
	  ${CMAKE_CURRENT_BINARY_DIR}/dml-scan.cpp
	)

add_dependencies(dmlpackage dmlpackage_gen)

SET_TARGET_PROPERTIES(dmlpackage PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(dmlpackage 
  INTERFACE dmlpackage_h
  PUBLIC 
  PRIVATE
    messageqcpp_h 
    execplan_h 
    brm_h 
    idbdatafile_h 
    compress_h
    loggingcpp_h
    dataconvert_h
    joblist_h
    we_shared_h
    )

target_pch(dmlpackage)

INSTALL(TARGETS dmlpackage DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

