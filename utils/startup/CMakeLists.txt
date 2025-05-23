include_directories(${ENGINE_COMMON_INCLUDES})

add_definitions(-fPIC -DPIC)

add_library(idbboot STATIC installdir.cpp)
add_dependencies(idbboot external_boost)

install(TARGETS idbboot DESTINATION ${ENGINE_LIBDIR})
