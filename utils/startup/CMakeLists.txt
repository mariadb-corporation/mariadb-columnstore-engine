
include_directories( ${ENGINE_COMMON_INCLUDES} )

ADD_DEFINITIONS(-fPIC -DPIC)

add_library(idbboot STATIC installdir.cpp)
add_dependencies(idbboot external_boost)

INSTALL(TARGETS idbboot DESTINATION ${ENGINE_LIBDIR})


