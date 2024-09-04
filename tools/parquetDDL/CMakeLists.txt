INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

SET (parquetDDL_SRCS main.cpp)
ADD_EXECUTABLE (mcs_parquet_ddl ${parquetDDL_SRCS})
TARGET_LINK_LIBRARIES (mcs_parquet_ddl parquet)
INSTALL (
  TARGETS mcs_parquet_ddl
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
