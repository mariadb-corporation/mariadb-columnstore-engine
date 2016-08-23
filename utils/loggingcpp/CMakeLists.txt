
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(loggingcpp_LIB_SRCS
    message.cpp
    messagelog.cpp
    logger.cpp
    errorcodes.cpp
    sqllogger.cpp
    stopwatch.cpp
    idberrorinfo.cpp)

ADD_CUSTOM_COMMAND(
    OUTPUT messageids.h errorids.h
    COMMAND ./genMsgAndErrId.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS genMsgId.pl genErrId.pl
    )
    
ADD_CUSTOM_TARGET(genMsgAndErrId DEPENDS messageids.h errorids.h)
    
add_library(loggingcpp SHARED ${loggingcpp_LIB_SRCS})

add_dependencies(loggingcpp genMsgAndErrId)

set_target_properties(loggingcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR})

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_ETCDIR})

