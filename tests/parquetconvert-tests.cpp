#include "gtest/gtest.h"

#include "dataconvert.h"
using namespace dataconvert;
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include "we_tableinfo.h"

// int32
TEST(ParquetConvertTests, Convert)
{
  arrow::Int32Builder builder;
  int reserve_num = 30;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[1] = 0;
  validity[2] = 0;
  validity[3] = 0;
  std::vector<int32_t> values;
  for (int32_t i = 0; i < reserve_num; i++)
    values.push_back(i);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));
  // TODO: how to call tableInfo method
  WriteEngine::TableInfo* tTable = new WriteEngine::TableInfo
}