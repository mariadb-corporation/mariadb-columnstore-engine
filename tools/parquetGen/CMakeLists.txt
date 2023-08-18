include_directories(${ENGINE_COMMON_INCLUDES})

set(parquetGen_SRCS main.cpp)
add_executable(mcs_parquet_gen ${parquetGen_SRCS})
target_link_libraries(mcs_parquet_gen ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_system boost_filesystem arrow_static parquet_static)
add_dependencies(mcs_parquet_gen external_arrow)
install(TARGETS mcs_parquet_gen DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
