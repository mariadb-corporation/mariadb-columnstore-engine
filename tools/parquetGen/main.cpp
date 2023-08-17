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
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int32.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}

void generateInt64Table()
{
	// generate data
  arrow::Int64Builder builder;
  int reserve_num = 30;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[1] = 0;
  validity[2] = 0;
  validity[3] = 0;
  std::vector<int64_t> values;
  for (int64_t i = 0; i < reserve_num-1; i++)
    values.push_back(i);
  values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::int64()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int64.parquet"));
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
		arrow::field("col1", arrow::float32()),
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
		arrow::field("col1", arrow::float64()),
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
		arrow::field("col1", arrow::time32(arrow::TimeUnit::MILLI)),
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
	const int reserve_num = 30;
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
		arrow::field("col1", arrow::utf8()),
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
		arrow::field("col1", arrow::timestamp(arrow::TimeUnit::MILLI)),
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
		arrow::field("col1", arrow::date32()),
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
		arrow::field("col1", arrow::int16()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i16array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int16.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateInt8Table()
{
	int reserve_num = 30;
  // ---------------int16
  arrow::Int8Builder i8builder;
  PARQUET_THROW_NOT_OK(i8builder.Reserve(reserve_num));
  std::vector<bool> i8validity(reserve_num, true);
  std::vector<int8_t> i8values;
  for (int8_t i = 0; i < reserve_num; i++)
    i8values.push_back(i);
  PARQUET_THROW_NOT_OK(i8builder.AppendValues(i8values, i8validity));
  std::shared_ptr<arrow::Array> i8array;
  PARQUET_THROW_NOT_OK(i8builder.Finish(&i8array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::int8()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i8array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/int8.parquet"));
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
		arrow::field("col1", arrow::decimal128(9, 3)),
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


void generateUintTable()
{
	// generate data
  arrow::UInt32Builder builder;
  uint reserve_num = 30;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[1] = 0;
  validity[2] = 0;
  validity[3] = 0;
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i < reserve_num-1; i++)
    values.push_back(i);
  values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::uint32()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/uint32.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}


void generateUint16Table()
{
	uint16_t reserve_num = 30;
  // ---------------int16
  arrow::UInt16Builder i16builder;
  PARQUET_THROW_NOT_OK(i16builder.Reserve(reserve_num));
  std::vector<bool> i16validity(reserve_num, true);
  std::vector<uint16_t> i16values;
  for (uint16_t i = 0; i < reserve_num; i++)
    i16values.push_back(i);
  PARQUET_THROW_NOT_OK(i16builder.AppendValues(i16values, i16validity));
  std::shared_ptr<arrow::Array> i16array;
  PARQUET_THROW_NOT_OK(i16builder.Finish(&i16array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::uint16()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i16array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/uint16.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateUint8Table()
{
	uint8_t reserve_num = 30;
  // ---------------int16
  arrow::UInt8Builder i8builder;
  PARQUET_THROW_NOT_OK(i8builder.Reserve(reserve_num));
  std::vector<bool> i8validity(reserve_num, true);
  std::vector<uint8_t> i8values;
  for (uint8_t i = 0; i < reserve_num; i++)
    i8values.push_back(i);
  PARQUET_THROW_NOT_OK(i8builder.AppendValues(i8values, i8validity));
  std::shared_ptr<arrow::Array> i8array;
  PARQUET_THROW_NOT_OK(i8builder.Finish(&i8array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::uint8()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i8array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/uint8.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));
	
}

void generateUint64Table()
{
	// generate data
  arrow::UInt64Builder builder;
  uint64_t reserve_num = 30;
  PARQUET_THROW_NOT_OK(builder.Reserve(reserve_num));
  std::vector<bool> validity(reserve_num, true);
  validity[1] = 0;
  validity[2] = 0;
  validity[3] = 0;
  std::vector<uint64_t> values;
  for (uint64_t i = 0; i < reserve_num-1; i++)
    values.push_back(i);
  values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(builder.AppendValues(values, validity));
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder.Finish(&array));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::uint64()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/uint64.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}

void generateBoolTable()
{
  int reserve_num = 30;
  // ----------------------boolean
  arrow::BooleanBuilder boolBuilder;
  PARQUET_THROW_NOT_OK(boolBuilder.Reserve(reserve_num));
  std::vector<bool> boolValidity(reserve_num, true);
  std::vector<bool> boolValues;
  for (int i = 0; i < reserve_num; i++)
    boolValues.push_back(true);
  boolValues[5] = false;
  PARQUET_THROW_NOT_OK(boolBuilder.AppendValues(boolValues, boolValidity));
  std::shared_ptr<arrow::Array> boolArray;
  PARQUET_THROW_NOT_OK(boolBuilder.Finish(&boolArray));


	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::boolean()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {boolArray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/bool.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));


}


void generateNullTable()
{
  int reserve_num = 30;
  // ---------------------null
  arrow::NullBuilder nullBuilder;
  PARQUET_THROW_NOT_OK(nullBuilder.Reserve(reserve_num));
  PARQUET_THROW_NOT_OK(nullBuilder.AppendNulls(reserve_num));
  std::shared_ptr<arrow::Array> nullarray;
  PARQUET_THROW_NOT_OK(nullBuilder.Finish(&nullarray));

	std::shared_ptr<arrow::Schema> schema = arrow::schema({
		arrow::field("col1", arrow::null()),
  });
	std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {nullarray});

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/null.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

}


void generateTable()
{
  generateBoolTable();
  generateDateTable();
  generateDecimalTable();
  generateDoubleTable();
  generateFloatTable();
  generateInt16Table();
  generateInt64Table();
  generateInt8Table();
  generateIntTable();
  generateNullTable();
  generateStringTable();
  generateTimestampTable();
  generateTimeTable();
  generateUint16Table();
  generateUint64Table();
  generateUint8Table();
  generateUintTable();
}

void generateAllTable()
{
  const int reserve_num = 30;
  // boolean
  arrow::BooleanBuilder boolBuilder;
  PARQUET_THROW_NOT_OK(boolBuilder.Reserve(reserve_num));
  std::vector<bool> boolValidity(reserve_num, true);
  std::vector<bool> boolValues;
  for (int i = 0; i < reserve_num; i++)
    boolValues.push_back(true);
  boolValues[5] = false;
  PARQUET_THROW_NOT_OK(boolBuilder.AppendValues(boolValues, boolValidity));
  std::shared_ptr<arrow::Array> boolArray;
  PARQUET_THROW_NOT_OK(boolBuilder.Finish(&boolArray));
  
  // int32
  arrow::Int32Builder int32Builder;
  PARQUET_THROW_NOT_OK(int32Builder.Reserve(reserve_num));
  std::vector<bool> int32Validity(reserve_num, true);
  int32Validity[1] = 0;
  int32Validity[2] = 0;
  int32Validity[3] = 0;
  std::vector<int32_t> int32Values;
  for (int32_t i = 0; i < reserve_num-1; i++)
    int32Values.push_back(i);
  int32Values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(int32Builder.AppendValues(int32Values, int32Validity));
  std::shared_ptr<arrow::Array> int32Array;
  PARQUET_THROW_NOT_OK(int32Builder.Finish(&int32Array));

  // int64
  arrow::Int64Builder int64Builder;
  PARQUET_THROW_NOT_OK(int64Builder.Reserve(reserve_num));
  std::vector<bool> int64Validity(reserve_num, true);
  int64Validity[1] = 0;
  int64Validity[2] = 0;
  int64Validity[3] = 0;
  std::vector<int64_t> int64Values;
  for (int64_t i = 0; i < reserve_num-1; i++)
    int64Values.push_back(i);
  int64Values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(int64Builder.AppendValues(int64Values, int64Validity));
  std::shared_ptr<arrow::Array> int64Array;
  PARQUET_THROW_NOT_OK(int64Builder.Finish(&int64Array));

  // float
  arrow::FloatBuilder floatBuilder;
  PARQUET_THROW_NOT_OK(floatBuilder.Reserve(reserve_num));
  std::vector<bool> floatValidity(reserve_num, true);
  floatValidity[2] = 0;
  std::vector<float> floatValues;
  for (int i = 0; i < reserve_num; i++)
    floatValues.push_back(i+1.5);
  PARQUET_THROW_NOT_OK(floatBuilder.AppendValues(floatValues, floatValidity));
  std::shared_ptr<arrow::Array> floatArray;
  PARQUET_THROW_NOT_OK(floatBuilder.Finish(&floatArray));

  // float64
  arrow::DoubleBuilder doubleBuilder;
  PARQUET_THROW_NOT_OK(doubleBuilder.Reserve(reserve_num));
  std::vector<bool> dvalidity(reserve_num, true);
  dvalidity[3] = 0;
  std::vector<double> dvalues;
  for (int i = 0; i < reserve_num; i++)
    dvalues.push_back(i+2.5);
  PARQUET_THROW_NOT_OK(doubleBuilder.AppendValues(dvalues, dvalidity));
  std::shared_ptr<arrow::Array> doubleArray;
  PARQUET_THROW_NOT_OK(doubleBuilder.Finish(&doubleArray));

  // time
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

  // string
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

  // timestamp
  arrow::TimestampBuilder tsbuilder(arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  PARQUET_THROW_NOT_OK(tsbuilder.Reserve(reserve_num));
  std::vector<bool> tsvalidity(reserve_num, true);
  std::vector<int64_t> tsvalues;
  for (int64_t i = 0; i < reserve_num; i++)
    tsvalues.push_back(i * 10000000);
  PARQUET_THROW_NOT_OK(tsbuilder.AppendValues(tsvalues, tsvalidity));
  std::shared_ptr<arrow::Array> tsarray;
  PARQUET_THROW_NOT_OK(tsbuilder.Finish(&tsarray));

  // datetime
  arrow::Date32Builder date32builder;
  PARQUET_THROW_NOT_OK(date32builder.Reserve(reserve_num));
  std::vector<bool> date32validity(reserve_num, true);
  std::vector<int32_t> date32values;
  for (int32_t i = 0; i < reserve_num; i++)
    date32values.push_back(i * 10);
  PARQUET_THROW_NOT_OK(date32builder.AppendValues(date32values, date32validity));
  std::shared_ptr<arrow::Array> date32array;
  PARQUET_THROW_NOT_OK(date32builder.Finish(&date32array));

  // int16
  arrow::Int16Builder i16builder;
  PARQUET_THROW_NOT_OK(i16builder.Reserve(reserve_num));
  std::vector<bool> i16validity(reserve_num, true);
  std::vector<int16_t> i16values;
  for (int16_t i = 0; i < reserve_num; i++)
    i16values.push_back(i);
  PARQUET_THROW_NOT_OK(i16builder.AppendValues(i16values, i16validity));
  std::shared_ptr<arrow::Array> i16array;
  PARQUET_THROW_NOT_OK(i16builder.Finish(&i16array));

  // int8
  arrow::Int8Builder i8builder;
  PARQUET_THROW_NOT_OK(i8builder.Reserve(reserve_num));
  std::vector<bool> i8validity(reserve_num, true);
  std::vector<int8_t> i8values;
  for (int8_t i = 0; i < reserve_num; i++)
    i8values.push_back(i);
  PARQUET_THROW_NOT_OK(i8builder.AppendValues(i8values, i8validity));
  std::shared_ptr<arrow::Array> i8array;
  PARQUET_THROW_NOT_OK(i8builder.Finish(&i8array));

  // decimal
  auto t = arrow::Decimal128Type::Make(9, 3);
  PARQUET_ASSIGN_OR_THROW(auto t1, t);
  arrow::Decimal128Builder d128builder(t1, arrow::default_memory_pool());
  // std::cout << arrow::Decimal128("138.3433").kBitWidth << std::endl;
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("138.3433")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("532235.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  PARQUET_THROW_NOT_OK(d128builder.Append(arrow::Decimal128("5325.234")));
  PARQUET_THROW_NOT_OK(d128builder.AppendNull());
  std::shared_ptr<arrow::Array> decimalArray;
  PARQUET_THROW_NOT_OK(d128builder.Finish(&decimalArray));

  // uint32
  arrow::UInt32Builder uint32Builder;
  PARQUET_THROW_NOT_OK(uint32Builder.Reserve(reserve_num));
  std::vector<bool> uint32Validity(reserve_num, true);
  uint32Validity[1] = 0;
  uint32Validity[2] = 0;
  uint32Validity[3] = 0;
  std::vector<uint32_t> uint32Values;
  for (uint32_t i = 0; i < reserve_num-1; i++)
    uint32Values.push_back(i);
  uint32Values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(uint32Builder.AppendValues(uint32Values, uint32Validity));
  std::shared_ptr<arrow::Array> uint32Array;
  PARQUET_THROW_NOT_OK(uint32Builder.Finish(&uint32Array));

  // uint16
  arrow::UInt16Builder ui16builder;
  PARQUET_THROW_NOT_OK(ui16builder.Reserve(reserve_num));
  std::vector<bool> ui16validity(reserve_num, true);
  std::vector<uint16_t> ui16values;
  for (uint16_t i = 0; i < reserve_num; i++)
    ui16values.push_back(i);
  PARQUET_THROW_NOT_OK(ui16builder.AppendValues(ui16values, ui16validity));
  std::shared_ptr<arrow::Array> ui16array;
  PARQUET_THROW_NOT_OK(ui16builder.Finish(&ui16array));

  // uint8
  arrow::UInt8Builder ui8builder;
  PARQUET_THROW_NOT_OK(ui8builder.Reserve(reserve_num));
  std::vector<bool> ui8validity(reserve_num, true);
  std::vector<uint8_t> ui8values;
  for (uint8_t i = 0; i < reserve_num; i++)
    ui8values.push_back(i);
  PARQUET_THROW_NOT_OK(ui8builder.AppendValues(ui8values, ui8validity));
  std::shared_ptr<arrow::Array> ui8array;
  PARQUET_THROW_NOT_OK(ui8builder.Finish(&ui8array));

  // uint64
  arrow::UInt64Builder uint64Builder;
  uint64_t ureserve_num = 30;
  PARQUET_THROW_NOT_OK(uint64Builder.Reserve(ureserve_num));
  std::vector<bool> uint64Validity(ureserve_num, true);
  uint64Validity[1] = 0;
  uint64Validity[2] = 0;
  uint64Validity[3] = 0;
  std::vector<uint64_t> uint64Values;
  for (uint64_t i = 0; i < ureserve_num-1; i++)
    uint64Values.push_back(i);
  uint64Values.push_back(2147483648);
  PARQUET_THROW_NOT_OK(uint64Builder.AppendValues(uint64Values, uint64Validity));
  std::shared_ptr<arrow::Array> uint64Array;
  PARQUET_THROW_NOT_OK(uint64Builder.Finish(&uint64Array));

  // null(19 cols except varchar(20) and char(20) and timestamp)







  // make schema
  // 22 cols
	std::shared_ptr<arrow::Schema> schema = arrow::schema({
    arrow::field("col1", arrow::int32()),
    arrow::field("col2", arrow::int64()),
    arrow::field("col3", arrow::float32()),
    arrow::field("col4", arrow::float64()),
    arrow::field("col5", arrow::time32(arrow::TimeUnit::MILLI)),
    arrow::field("col6", arrow::utf8()),
    arrow::field("col7", arrow::utf8()),
    arrow::field("col8", arrow::utf8()),
    arrow::field("col9", arrow::utf8()),
    arrow::field("col10", arrow::utf8()),
    arrow::field("col11", arrow::utf8()),
    arrow::field("col12", arrow::timestamp(arrow::TimeUnit::MILLI)),
    arrow::field("col13", arrow::date32()),
    arrow::field("col14", arrow::timestamp(arrow::TimeUnit::MILLI)),
    arrow::field("col15", arrow::int16()),
    arrow::field("col16", arrow::int8()),
    arrow::field("col17", arrow::decimal128(9, 3)),
    arrow::field("col18", arrow::uint32()),
    arrow::field("col19", arrow::uint16()),
    arrow::field("col20", arrow::uint8()),
    arrow::field("col21", arrow::uint64()),
		arrow::field("col22", arrow::boolean()),
  });
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {
    int32Array,
    int64Array,
    floatArray,
    doubleArray,
    time32array,
    strarray,
    strarray,
    strarray,
    strarray,
    strarray,
    strarray,
    tsarray,
    date32array,
    tsarray,
    i16array,
    i8array,
    decimalArray,
    uint32Array,
    ui16array,
    ui8array,
    uint64Array,
    boolArray
    });

	// write to file
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/tests.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 3));

  // null file
  arrow::NullBuilder nullBuilder;
  PARQUET_THROW_NOT_OK(nullBuilder.Reserve(reserve_num));
  PARQUET_THROW_NOT_OK(nullBuilder.AppendNulls(reserve_num));
  std::shared_ptr<arrow::Array> nullarray;
  PARQUET_THROW_NOT_OK(nullBuilder.Finish(&nullarray));


	std::shared_ptr<arrow::Schema> schema1 = arrow::schema({
		arrow::field("col1", arrow::null()),
		arrow::field("col2", arrow::null()),
		arrow::field("col3", arrow::null()),
		arrow::field("col4", arrow::null()),
		arrow::field("col5", arrow::null()),
		arrow::field("col6", arrow::null()),
		arrow::field("col7", arrow::null()),
    arrow::field("col8", arrow::utf8()),
		arrow::field("col9", arrow::null()),
		arrow::field("col10", arrow::null()),
    arrow::field("col11", arrow::utf8()),
    arrow::field("col12", arrow::timestamp(arrow::TimeUnit::MILLI)),
		arrow::field("col13", arrow::null()),
		arrow::field("col14", arrow::null()),
		arrow::field("col15", arrow::null()),
		arrow::field("col16", arrow::null()),
		arrow::field("col17", arrow::null()),
		arrow::field("col18", arrow::null()),
		arrow::field("col19", arrow::null()),
		arrow::field("col20", arrow::null()),
		arrow::field("col21", arrow::null()),
		arrow::field("col22", arrow::null()),
  });
	std::shared_ptr<arrow::Table> table1 = arrow::Table::Make(schema1, {
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    strarray,
    nullarray,
    nullarray,
    strarray,
    tsarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    nullarray,
    });

  std::shared_ptr<arrow::io::FileOutputStream> outfile1;
  PARQUET_ASSIGN_OR_THROW(
      outfile1, arrow::io::FileOutputStream::Open("../storage/columnstore/columnstore/mysql-test/columnstore/std_data/nulls.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table1, pool, outfile1, 3));


}

int main(int argc, char** argv)
{
  int32_t option;

  while ((option = getopt(argc, argv, "ad")) != EOF)
  {
    switch (option)
    {
      case 'd':
      {
        generateTable();
        break;
      }
      case 'a':
      {
        generateAllTable();
      }
    }
  }
  return 0;
}