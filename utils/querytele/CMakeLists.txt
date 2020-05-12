
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(querytele_LIB_SRCS
    querytele.cpp
    queryteleclient.cpp
    querytele_constants.cpp
    querytele_types.cpp
    QueryTeleService.cpp
    queryteleprotoimpl.cpp)

add_library(querytele SHARED ${querytele_LIB_SRCS})

install(TARGETS querytele DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

