include_directories(${ENGINE_COMMON_INCLUDES})

set(parquetDDL_SRCS main.cpp)
add_executable(parquetDDL ${parquetDDL_SRCS})
target_link_libraries(parquetDDL ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_system boost_filesystem arrow parquet)
install(TARGETS parquetDDL DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
