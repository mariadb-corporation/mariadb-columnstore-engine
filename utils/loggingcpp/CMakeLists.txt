include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/messageids.h
    COMMAND perl ${CMAKE_CURRENT_SOURCE_DIR}/genMsgId.pl < ${CMAKE_CURRENT_SOURCE_DIR}/MessageFile.txt >
            messageids-temp.h
    COMMAND ${CMAKE_COMMAND} -E copy_if_different messageids-temp.h messageids.h
    DEPENDS genMsgId.pl
)
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/errorids.h
    COMMAND perl ${CMAKE_CURRENT_SOURCE_DIR}/genErrId.pl < ${CMAKE_CURRENT_SOURCE_DIR}/ErrorMessage.txt >
            errorids-temp.h
    COMMAND ${CMAKE_COMMAND} -E copy_if_different errorids-temp.h errorids.h
    DEPENDS genErrId.pl
)

set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/messageids.h PROPERTIES GENERATED TRUE)
set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/errorids.h PROPERTIES GENERATED TRUE)

columnstore_library(
    loggingcpp
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

columnstore_link(loggingcpp configcpp boost_filesystem)

columnstore_install_file(MessageFile.txt ${ENGINE_SYSCONFDIR}/columnstore)
columnstore_install_file(ErrorMessage.txt ${ENGINE_SYSCONFDIR}/columnstore)
