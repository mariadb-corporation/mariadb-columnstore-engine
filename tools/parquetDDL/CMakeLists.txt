include_directories(${ENGINE_COMMON_INCLUDES})

set(parquetDDL_SRCS main.cpp)
add_executable(mcs_parquet_ddl ${parquetDDL_SRCS})
target_link_libraries(mcs_parquet_ddl arrow parquet)
install(TARGETS mcs_parquet_ddl DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
