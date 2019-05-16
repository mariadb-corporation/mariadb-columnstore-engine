
include_directories( ${ENGINE_COMMON_INCLUDES}  )

add_library(alarmmanager SHARED alarmmanager.cpp alarm.cpp)

target_link_libraries(alarmmanager PUBLIC loggingcpp)

target_compile_options(alarmmanager PRIVATE -Wno-unused-result)

set_target_properties(alarmmanager PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS alarmmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
