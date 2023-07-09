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
  for (int32_t i = 0; i < reserve_num; i++)
    values.push_back(i);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("int", arrow::int32()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int32.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}

void generateFloatTable()
{
  int reserve_num = 30;
  arrow::FloatBuilder builder;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[2] = 0;
  std::vector<float> values;
  for (int i = 0; i < reserve_num; i++)
    values.push_back(i+1.5);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));

	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("float", arrow::float32()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/float.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateDoubleTable()
{
  // -----------------Float64-----------------------
  int reserve_num = 30;
  arrow::DoubleBuilder doublebuilder;
  PARQUET_THROW_NOT_OK(doublebuilder.Reserve(reserve_num));
  std::vector<bool> dvalidity(reserve_num, true);
  dvalidity[3] = 0;
  std::vector<double> dvalues;
  for (int i = 0; i < reserve_num; i++)
    dvalues.push_back(i+2.5);
  PARQUET_THROW_NOT_OK(doublebuilder.AppendValues(dvalues, dvalidity));
  std::shared_ptr<arrow::Array> doublearray;
  PARQUET_THROW_NOT_OK(doublebuilder.Finish(&doublearray));

	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("double", arrow::float64()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {doublearray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/double.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateTimeTable()
{
	int reserve_num = 30;
  arrow::Time32Builder time32builder(arrow::time32(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  // int reserve_num = 500;
  PARQUET_THROW_NOT_OK(time32builder.Reserve(reserve_num));
  std::vector<bool> time32validity(reserve_num, true);
  std::vector<int32_t> time32values;
  for (int32_t i = 0; i < reserve_num; i++)
    time32values.push_back(i*3605000);
  PARQUET_THROW_NOT_OK(time32builder.AppendValues(time32values, time32validity));
  std::shared_ptr<arrow::Array> time32array;
  PARQUET_THROW_NOT_OK(time32builder.Finish(&time32array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("time", arrow::time32(arrow::TimeUnit::MILLI)),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {time32array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/time.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateStringTable()
{
	int reserve_num = 30;
  // ----------------- String -------------------------
  arrow::StringBuilder strbuilder;
  PARQUET_THROW_NOT_OK(strbuilder.Reserve(reserve_num));
  uint8_t validity1[reserve_num];
  std::vector<std::string> values1;
  for (int64_t i = reserve_num-1; i >= 0; i--)
  {
    values1.push_back(std::string("hhhh"));
    validity1[i] = 1;
  }
  validity1[1] = 0; // set element 1 null
  PARQUET_THROW_NOT_OK(strbuilder.AppendValues(values1, validity1));
  std::shared_ptr<arrow::Array> strarray;
  PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("str", arrow::utf8()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {strarray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/string.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateTimestampTable()
{
	int reserve_num = 30;
  // ----------------- Timestamp -------------------------
  arrow::TimestampBuilder tsbuilder(arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  PARQUET_THROW_NOT_OK(tsbuilder.Reserve(reserve_num));
  std::vector<bool> tsvalidity(reserve_num, true);
  std::vector<int64_t> tsvalues;
  for (int64_t i = 0; i < reserve_num; i++)
    tsvalues.push_back(i * 10000000);
  PARQUET_THROW_NOT_OK(tsbuilder.AppendValues(tsvalues, tsvalidity));
  std::shared_ptr<arrow::Array> tsarray;
  PARQUET_THROW_NOT_OK(tsbuilder.Finish(&tsarray));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {tsarray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/ts.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	

}

void generateDateTable()
{
	int reserve_num = 30;
  // -------------------------DATETIME
  arrow::Date32Builder date32builder;
  PARQUET_THROW_NOT_OK(date32builder.Reserve(reserve_num));
  std::vector<bool> date32validity(reserve_num, true);
  std::vector<int32_t> date32values;
  for (int32_t i = 0; i < reserve_num; i++)
    date32values.push_back(i * 10);
  PARQUET_THROW_NOT_OK(date32builder.AppendValues(date32values, date32validity));
  std::shared_ptr<arrow::Array> date32array;
  PARQUET_THROW_NOT_OK(date32builder.Finish(&date32array));

	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("date", arrow::date32()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {date32array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/date.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateInt16Table()
{
	int reserve_num = 30;
  // ---------------int16
  arrow::Int16Builder i16builder;
  PARQUET_THROW_NOT_OK(i16builder.Reserve(reserve_num));
  std::vector<bool> i16validity(reserve_num, true);
  std::vector<int16_t> i16values;
  for (int16_t i = 0; i < reserve_num; i++)
    i16values.push_back(i);
  PARQUET_THROW_NOT_OK(i16builder.AppendValues(i16values, i16validity));
  std::shared_ptr<arrow::Array> i16array;
  PARQUET_THROW_NOT_OK(i16builder.Finish(&i16array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("int16", arrow::int16()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i16array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int16.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateDecimalTable()
{
  // ----------------------decimal
  auto t = arrow::Decimal128Type::Make(9, 3);
  PARQUET_ASSIGN_OR_THROW(auto t1, t);
  arrow::Decimal128Builder d128builder(t1, arrow::default_memory_pool());
  // std::cout << arrow::Decimal128("138.3433").kBitWidth << std::endl;
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  std::shared_ptr<arrow::Array> decimalArray;
  PARQUET_THROW_NOT_OK(d128builder.Finish(&decimalArray));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("decimal", arrow::decimal128(9, 3)),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {decimalArray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
	//TODO: revise the path
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/decimal.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

int main(int argc, char** argv)
{
  int32_t option;

  while ((option = getopt(argc, argv, "d:")) != EOF)
  {
    switch (option)
    {
      case 'd':
      {
        char col = optarg[0];

        switch (col)
        {
          case 'a':
          {
						generateIntTable();
						break;
          }
					case 'b':
					{
						generateFloatTable();
						break;
					}
					case 'c':
					{
						generateDoubleTable();
						break;
					}
					case 'd':
					{
						generateTimeTable();
						break;
					}
					case 'e':
					{
						generateStringTable();
						break;
					}
					case 'f':
					{
						generateTimestampTable();
						break;
					}
					case 'g':
					{
						generateDateTable();
						break;
					}
					case 'h':
					{
						generateInt16Table();
						break;
					}
					case 'i':
					{
						generateDecimalTable();
						break;
					}
					
        }
      }
    }
  }
  return 0;
}