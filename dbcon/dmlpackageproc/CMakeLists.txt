
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

install(TARGETS dmlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-libs)


