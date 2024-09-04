INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

SET (parquetGen_SRCS main.cpp)
ADD_EXECUTABLE (mcs_parquet_gen ${parquetGen_SRCS})
TARGET_LINK_LIBRARIES (mcs_parquet_gen boost_system boost_filesystem parquet)
INSTALL (
  TARGETS mcs_parquet_gen
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
