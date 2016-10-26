
include_directories( ${ENGINE_COMMON_INCLUDES}  )


########### next target ###############

set(alarmmanager_LIB_SRCS alarmmanager.cpp alarm.cpp)

add_library(alarmmanager SHARED ${alarmmanager_LIB_SRCS})

set_target_properties(alarmmanager PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS alarmmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


