
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############
#TODO: put generated files in binary dir
ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/messageids.h ${CMAKE_CURRENT_SOURCE_DIR}/errorids.h
    COMMAND /bin/sh genMsgAndErrId.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS genMsgId.pl genErrId.pl
    )
    
    
add_library(loggingcpp SHARED
    message.cpp
    messagelog.cpp
    logger.cpp
    errorcodes.cpp
    sqllogger.cpp
    stopwatch.cpp
    idberrorinfo.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/messageids.h
	${CMAKE_CURRENT_SOURCE_DIR}/errorids.h
	)



set_target_properties(loggingcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_SYSCONFDIR}/columnstore COMPONENT platform)

