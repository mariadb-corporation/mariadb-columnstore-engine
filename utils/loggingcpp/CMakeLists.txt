
target_include_directories(loggingcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/messageids.h ${CMAKE_CURRENT_BINARY_DIR}/errorids.h
    BYPRODUCTS genMsgAndErrId.sh  genMsgId.pl genErrId.pl ErrorMessage.txt MessageFile.txt
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/genMsgAndErrId.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/genMsgId.pl ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/genErrId.pl ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ErrorMessage.txt ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/MessageFile.txt ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND /bin/sh genMsgAndErrId.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS genMsgAndErrId.sh genMsgId.pl genErrId.pl ErrorMessage.txt MessageFile.txt
    )

add_custom_target(loggingcpp_gen DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/messageids.h ${CMAKE_CURRENT_BINARY_DIR}/errorids.h)

add_dependencies(loggingcpp_h loggingcpp_gen)
    
add_library(loggingcpp SHARED
    message.cpp
    messagelog.cpp
    logger.cpp
    errorcodes.cpp
    sqllogger.cpp
    stopwatch.cpp
    idberrorinfo.cpp
	)

target_link_libraries(loggingcpp 
  PUBLIC loggingcpp_h
          idbboot
          configcpp
          rt
          Boost::system
          Boost::thread
  PRIVATE
          
          )

set_target_properties(loggingcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(loggingcpp)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_ETCDIR} COMPONENT platform)

