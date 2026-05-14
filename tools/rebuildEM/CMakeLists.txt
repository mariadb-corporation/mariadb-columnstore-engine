include_directories(${ENGINE_COMMON_INCLUDES})

set(rebuildEM_SRCS main.cpp rebuildEM.cpp picosat-965/picosat.c)
columnstore_executable(mcsRebuildEM ${rebuildEM_SRCS})
columnstore_link(mcsRebuildEM ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_filesystem)
