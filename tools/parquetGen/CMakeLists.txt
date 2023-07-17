include_directories(${ENGINE_COMMON_INCLUDES})

set(parquetGen_SRCS main.cpp)
add_executable(mcsParquetGen ${parquetGen_SRCS})
target_link_libraries(mcsParquetGen ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_system boost_filesystem arrow parquet)
install(TARGETS mcsParquetGen DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
