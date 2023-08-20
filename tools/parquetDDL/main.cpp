#include <iostream>
#include <string>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include <parquet/arrow/reader.h>
#include <vector>
#include <fstream>
#include <unistd.h>

enum STATUS_CODE
{
  NO_ERROR,
  EMPTY_FIELD,
  UNSUPPORTED_DATA_TYPE,
  UNSUPPORTED_FILE_TYPE,
  FILE_NUM_ERROR
};

/**
 * print the usage information
*/
static void usage()
{
  std::cout << "usage: " << std::endl;
  std::cout << "Reading parquet then output its corresponding .ddl file." << std::endl;
  std::cout << "mcs_parquet_ddl [input_parquet_file] [output_ddl_file]" << std::endl;
}

/**
 * get the schema of the parquet file
*/
void getSchema(std::string filePath, std::shared_ptr<arrow::Schema>* parquetSchema)
{
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  PARQUET_THROW_NOT_OK(reader->GetSchema(parquetSchema));
  PARQUET_THROW_NOT_OK(infile->Close());
}

/**
 * convert arrow data type id to corresponding columnstore type string
*/
int convert2mcs(std::shared_ptr<arrow::DataType> dataType, arrow::Type::type typeId, std::string& colType)
{
  switch (typeId)
  {
    case arrow::Type::type::BOOL:
    {
      colType = "BOOLEAN";
      break;
    }
    case arrow::Type::type::UINT8:
    {
      colType = "TINYINT UNSIGNED";
      break;
    }
    case arrow::Type::type::INT8:
    {
      colType = "TINYINT";
      break;
    }
    case arrow::Type::type::UINT16:
    {
      colType = "SMALLINT UNSIGNED";
      break;
    }
    case arrow::Type::type::INT16:
    {
      colType = "SMALLINT";
      break;
    }
    case arrow::Type::type::UINT32:
    {
      colType = "INT UNSIGNED";
      break;
    }
    case arrow::Type::type::INT32:
    {
      colType = "INT";
      break;
    }
    case arrow::Type::type::UINT64:
    {
      colType = "BIGINT UNSIGNED";
      break;
    }
    case arrow::Type::type::INT64:
    {
      colType = "BIGINT";
      break;
    }
    case arrow::Type::type::FLOAT:
    {
      colType = "FLOAT";
      break;
    }
    case arrow::Type::type::DOUBLE:
    {
      colType = "DOUBLE";
      break;
    }
    case arrow::Type::type::STRING:
    {
      // set 2000 as the maximum length and VARCHAR as column type
      colType = "VARCHAR(2000)";
      break;
    }
    case arrow::Type::type::BINARY:
    {
      // set 8000 as the maximum length and VARCHAR as column type
      colType = "VARCHAR(8000) character set 'binary'";
      break;
    }
    case arrow::Type::type::FIXED_SIZE_BINARY:
    {
      std::shared_ptr<arrow::FixedSizeBinaryType> fType = std::static_pointer_cast<arrow::FixedSizeBinaryType>(dataType);
      int byteWidth = fType->byte_width();
      colType = "CHAR(" + std::to_string(byteWidth) + ")";
      break;
    }
    case arrow::Type::type::DATE32:
    {
      colType = "DATE";
      break;
    }
    case arrow::Type::type::DATE64:
    {
      colType = "DATE";
      break;
    }
    case arrow::Type::type::TIMESTAMP:
    {
      std::shared_ptr<arrow::TimestampType> fType = std::static_pointer_cast<arrow::TimestampType>(dataType);

      if (fType->unit() == arrow::TimeUnit::MILLI)
        colType = "TIMESTAMP(3)";
      else if (fType->unit() == arrow::TimeUnit::MICRO)
        colType = "TIMESTAMP(6)";
      else
        return UNSUPPORTED_DATA_TYPE;

      break;
    }
    case arrow::Type::type::TIME32:
    {
      colType = "TIME(3)";
      break;
    }
    case arrow::Type::type::TIME64:
    {
      std::shared_ptr<arrow::Time64Type> fType = std::static_pointer_cast<arrow::Time64Type>(dataType);

      if (fType->unit() == arrow::TimeUnit::MICRO)
        colType = "TIME(6)";
      else
        return UNSUPPORTED_DATA_TYPE;
        
      break;
    }
    case arrow::Type::type::DECIMAL128:
    {
      // get precision and scale
      std::shared_ptr<arrow::DecimalType> fType = std::static_pointer_cast<arrow::DecimalType>(dataType);
      int32_t fPrecision = fType->precision();
      int32_t fScale = fType->scale();
      colType = "DECIMAL(" + std::to_string(fPrecision) + "," + std::to_string(fScale) + ")";
      break;
    }
    default:
    {
      return UNSUPPORTED_DATA_TYPE;
    }
  }
  return NO_ERROR;
}

/**
 * main function to generate DDL file
*/
int generateDDL(std::string filePath, std::string targetPath, std::string tableName)
{
  std::shared_ptr<arrow::Schema> parquetSchema;
  getSchema(filePath, &parquetSchema);
  std::vector<std::string> parquetCols;
  std::vector<std::string> parquetTypes;
  int rc = NO_ERROR;
  int fieldsNum = parquetSchema->num_fields();

  if (fieldsNum == 0)
  {
    return EMPTY_FIELD;
  }

  for (int i = 0; i < fieldsNum; i++)
  {
    const std::shared_ptr<arrow::Field> tField = parquetSchema->field(i);
    const std::string tName = tField->name();
    std::string colType;
    auto tType = tField->type();
    parquetCols.push_back(tName);
    rc = convert2mcs(tType, tType->id(), colType);

    if (rc != NO_ERROR)
    {
      std::cout << "Not allowed data type: " << tName << std::endl;
      return rc;
    }

    parquetTypes.push_back(colType);
  }

  std::string str1 = "CREATE TABLE " + tableName + "(\n";
  std::string str2 = ") ENGINE=Columnstore;";

  for (int i = 0; i < fieldsNum; i++)
  {
    str1 += parquetCols[i] + " " + parquetTypes[i] + (i == fieldsNum-1 ? "\n" : ",\n");
  }

  str1 += str2;
  std::ofstream outfile(targetPath + tableName + ".ddl");
  outfile << str1;
  outfile.close();
  std::cout << "Successfully generate " + tableName + ".ddl" << std::endl;
  return rc;
}

int main(int argc, char** argv)
{
  int32_t option;

  while ((option = getopt(argc, argv, "h")) != EOF)
  {
    switch (option)
    {
      case 'h':
      case '?':
      default:
        usage();
        return (option == 'h' ? 0 : -1);
        break;
    }
  }

  // parquet file argv[1]
  // ddl file argv[2]
  // input parameter should be 3 (no more than 3 also)
  if (argc != 3)
  {
    std::cout << "Please input source parquet file and target ddl file" << std::endl;
    return FILE_NUM_ERROR;
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
    usage();
    return UNSUPPORTED_FILE_TYPE;
  }

  std::string targetPath;
  std::string tableName;
  std::string::size_type startBase = ddlFile.rfind('/');
  targetPath.assign(argv[2], startBase + 1);
  tableName.assign(argv[2] + startBase + 1, endBase - startBase - 1);
  std::cout << "Reading " + parquetFile << std::endl;
  int rc = generateDDL(parquetFile, targetPath, tableName);

  if (rc != NO_ERROR)
  {
    std::cout << "Input parquet file illegal: no data field" << std::endl;
  }

  return rc;
}