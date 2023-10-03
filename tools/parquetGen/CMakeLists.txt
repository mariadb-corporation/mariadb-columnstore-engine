include_directories(${ENGINE_COMMON_INCLUDES})

set(parquetGen_SRCS main.cpp)
add_executable(mcs_parquet_gen ${parquetGen_SRCS})
target_link_libraries(mcs_parquet_gen boost_system boost_filesystem parquet)
install(TARGETS mcs_parquet_gen DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
