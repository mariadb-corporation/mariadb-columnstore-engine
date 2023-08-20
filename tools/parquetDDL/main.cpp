#include <iostream>
#include <string>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include <parquet/arrow/reader.h>
#include <vector>
#include <fstream>

void getSchema(std::string filePth, std::shared_ptr<arrow::Schema>* parquetSchema)
{
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filePth, arrow::default_memory_pool()));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  PARQUET_THROW_NOT_OK(reader->GetSchema(parquetSchema));
}

std::string convert2mcs(std::shared_ptr<arrow::DataType> dataType, arrow::Type::type typeId)
{
  switch (typeId)
  {
    case arrow::Type::type::BOOL:
    {
      return "BOOLEAN";
    }
    case arrow::Type::type::UINT8:
    {
      return "TINYINT UNSIGNED";
    }
    case arrow::Type::type::INT8:
    {
      return "TINYINT";
    }
    case arrow::Type::type::UINT16:
    {
      return "SMALLINT UNSIGNED";
    }
    case arrow::Type::type::INT16:
    {
      return "SMALLINT";
    }
    case arrow::Type::type::UINT32:
    {
      return "INT UNSIGNED";
    }
    case arrow::Type::type::INT32:
    {
      return "INT";
    }
    case arrow::Type::type::UINT64:
    {
      return "BIGINT UNSIGNED";
    }
    case arrow::Type::type::INT64:
    {
      return "BIGINT";
    }
    case arrow::Type::type::FLOAT:
    {
      return "FLOAT";
    }
    case arrow::Type::type::DOUBLE:
    {
      return "DOUBLE";
    }
    case arrow::Type::type::STRING:
    {
      //varchar here and what about the length
      // or maybe here we need to load this column data to get the maximum length
      // return "TINYINT";
      return "VARCHAR(2000)";
    }
    case arrow::Type::type::BINARY:
    {
      return "VARCHAR(8000)";
    }
    case arrow::Type::type::FIXED_SIZE_BINARY:
    {
      std::shared_ptr<arrow::FixedSizeBinaryType> fType = std::static_pointer_cast<arrow::FixedSizeBinaryType>(dataType);
      int byteWidth = fType->byte_width();
      return "CHAR(" + std::to_string(byteWidth) + ")";
    }
    case arrow::Type::type::DATE32:
    {
      return "DATE";
    }
    case arrow::Type::type::DATE64:
    {
      return "DATE";
    }
    case arrow::Type::type::TIMESTAMP:
    {
      return "TIMESTAMP";
    }
    case arrow::Type::type::TIME32:
    {
      return "TIME";
    }
    case arrow::Type::type::DECIMAL128:
    {
      // get precision and scale
      std::shared_ptr<arrow::DecimalType> fType = std::static_pointer_cast<arrow::DecimalType>(dataType);
      int32_t fPrecision = fType->precision();
      int32_t fScale = fType->scale();

      return "DECIMAL(" + std::to_string(fPrecision) + "," + std::to_string(fScale) + ")";
    }
    default:
      break;
  }
  return "FAULT";
}

void generateDDL(std::string filePth, std::string targetPth, std::string tableName)
{
  std::shared_ptr<arrow::Schema> parquetSchema;
  getSchema(filePth, &parquetSchema);
  std::vector<std::string> parquetCols;
  std::vector<std::string> parquetTypes;

  int fieldsNum = parquetSchema->num_fields();
  for (int i = 0; i < fieldsNum; i++)
  {
    const std::shared_ptr<arrow::Field> tField = parquetSchema->field(i);
    const std::string tName = tField->name();
    auto tType = tField->type();
    parquetCols.push_back(tName);
    std::string colType = convert2mcs(tType, tType->id());
    if (colType == "FAULT")
    {
      // emit fault error
      std::cout << "Not allowed data type" << std::endl;
      return;
    }
    parquetTypes.push_back(colType);
  }
  std::string str1 = "CREATE TABLE " + tableName + "(";
  std::string str2 = ") ENGINE=Columnstore;";
  for (int i = 0; i < fieldsNum; i++)
  {
    str1 += parquetCols[i] + " " + parquetTypes[i] + (i == fieldsNum-1 ? "" : ",");
  }
  str1 += str2;
  std::ofstream outfile(targetPth + tableName + ".ddl");
  outfile << str1;
  outfile.close();
  std::cout << "Successfully generate " + tableName + ".ddl" << std::endl;
  return;
}

int main(int argc, char** argv)
{
  // parquet file argv[1]
  // ddl file argv[2]
  // input parameter should be 3 (no more than 3 also)
  if (argc < 3)
  {
    std::cout << "Please input source parquet file and target ddl file" << std::endl;
    return 0;
  }
  std::string parquetFile(argv[1]);
  std::string ddlFile(argv[2]);

  // check file extension
  std::string::size_type endBase = ddlFile.rfind('.');
  std::string::size_type endBase1 = parquetFile.rfind('.');
  if (endBase == std::string::npos || endBase1 == std::string::npos || 
      parquetFile.substr(endBase1 + 1) != "parquet" ||
      ddlFile.substr(endBase + 1) != "ddl")
  {
    std::cout << "File type not supported" << std::endl;
    return 0;
  }

  std::string targetPth;
  std::string tableName;
  std::string::size_type startBase = ddlFile.rfind('/');
  targetPth.assign(argv[2], startBase + 1);
  tableName.assign(argv[2] + startBase + 1, endBase - startBase - 1);
  std::cout << "Reading " + parquetFile << std::endl;
  generateDDL(parquetFile, targetPth, tableName);
  return 0;
}