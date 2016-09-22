
include_directories( ${ENGINE_COMMON_INCLUDES}  )


########### next target ###############

set(snmpmanager_LIB_SRCS snmpmanager.cpp alarm.cpp)

add_library(snmpmanager SHARED ${snmpmanager_LIB_SRCS})

add_dependencies(snmpmanager libnetsnmpmibs)

set_target_properties(snmpmanager PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS snmpmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


