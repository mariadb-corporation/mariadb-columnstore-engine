
include_directories( ${ENGINE_COMMON_INCLUDES}  )


########### next target ###############

set(alarmmanager_LIB_SRCS alarmmanager.cpp alarm.cpp)

add_library(alarmmanager SHARED ${alarmmanager_LIB_SRCS})

add_dependencies(alarmmanager loggingcpp)

target_compile_options(alarmmanager PRIVATE -Wno-unused-result)

install(TARGETS alarmmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


