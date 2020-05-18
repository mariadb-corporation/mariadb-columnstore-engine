
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############
ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/messageids.h
    COMMAND perl ${CMAKE_CURRENT_SOURCE_DIR}/genMsgId.pl < ${CMAKE_CURRENT_SOURCE_DIR}/MessageFile.txt > messageids-temp.h
    COMMAND ${CMAKE_COMMAND} -E copy_if_different messageids-temp.h messageids.h
    DEPENDS genMsgId.pl
    )
ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/errorids.h
    COMMAND perl ${CMAKE_CURRENT_SOURCE_DIR}/genErrId.pl < ${CMAKE_CURRENT_SOURCE_DIR}/ErrorMessage.txt > errorids-temp.h
    COMMAND ${CMAKE_COMMAND} -E copy_if_different errorids-temp.h errorids.h
    DEPENDS genErrId.pl
     )

set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/errorids.h PROPERTIES GENERATED TRUE)

add_library(loggingcpp SHARED
    message.cpp
    messagelog.cpp
    logger.cpp
    errorcodes.cpp
    sqllogger.cpp
    stopwatch.cpp
    idberrorinfo.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/messageids.h
    ${CMAKE_CURRENT_BINARY_DIR}/errorids.h
)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_SYSCONFDIR}/columnstore COMPONENT columnstore-engine)

