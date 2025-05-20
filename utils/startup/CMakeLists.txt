include_directories(${ENGINE_COMMON_INCLUDES})

add_definitions(-fPIC -DPIC)

columnstore_library(idbboot installdir.cpp)
add_dependencies(idbboot external_boost)

install(TARGETS idbboot DESTINATION ${ENGINE_LIBDIR})
