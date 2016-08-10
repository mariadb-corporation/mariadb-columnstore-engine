
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

add_library(loggingcpp SHARED ${loggingcpp_LIB_SRCS})

ADD_CUSTOM_COMMAND(TARGET loggingcpp PRE_BUILD
    COMMAND ./genMsgAndErrId.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS genMsgId.pl genErrId.pl
    )
    
set_target_properties(loggingcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR})

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_ETCDIR})

