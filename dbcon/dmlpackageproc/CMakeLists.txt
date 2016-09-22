
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dmlpackageproc_LIB_SRCS
    deletepackageprocessor.cpp
    dmlpackageprocessor.cpp
    insertpackageprocessor.cpp
    updatepackageprocessor.cpp
    commandpackageprocessor.cpp
    autoincrementdata.cpp
    tablelockdata.cpp)

add_library(dmlpackageproc SHARED ${dmlpackageproc_LIB_SRCS})

target_link_libraries(dmlpackageproc ${NETSNMP_LIBRARIES})

set_target_properties(dmlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS dmlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


