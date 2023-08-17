#include <iostream>
#include <string>
#include <boost/algorithm/string/case_conv.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>


void generateIntTable()
{
	// generate data
  arrow::Int32Builder builder;
  int reserve_num = 30;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[1] = 0;
  validity[2] = 0;
  validity[3] = 0;
  std::vector<int32_t> values;
  for (int32_t i = 0; i < reserve_num-1; i++)
    values.push_back(i);
  values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::int32()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("/tmp/mcs_int32.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}

int main(int argc, char** argv)
{
  generateIntTable();
  return 0;
}
