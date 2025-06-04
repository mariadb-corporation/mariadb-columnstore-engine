include_directories(${ENGINE_COMMON_INCLUDES})

set(rwlock_LIB_SRCS rwlock.cpp rwlock_local.cpp)
columnstore_library(rwlock ${rwlock_LIB_SRCS})
columnstore_link(rwlock boost_thread)
