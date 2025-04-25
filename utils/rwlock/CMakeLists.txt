include_directories( ${ENGINE_COMMON_INCLUDES} )

set(rwlock_LIB_SRCS rwlock.cpp rwlock_local.cpp)

columnstore_library(rwlock ${rwlock_LIB_SRCS})
add_dependencies(rwlock external_boost)

install(TARGETS rwlock DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
