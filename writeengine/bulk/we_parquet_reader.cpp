/* Copyright (C) 2026 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "we_parquet_reader.h"

#include <chrono>
#include <fstream>
#include <memory>
#include <sstream>
#include <unordered_set>

#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>

#include "we_define.h"

namespace WriteEngine
{

namespace
{

int setArrowError(const std::string& prefix, const arrow::Status& st, std::string& errMsg)
{
  std::ostringstream oss;
  oss << prefix << ": " << st.ToString();
  errMsg = oss.str();
  return ERR_FILE_READ;
}

template <typename T>
int setArrowError(const std::string& prefix, const arrow::Result<T>& result, std::string& errMsg)
{
  return setArrowError(prefix, result.status(), errMsg);
}

bool isSupportedType(const std::shared_ptr<arrow::DataType>& type)
{
  using arrow::Type;
  switch (type->id())
  {
    case Type::BOOL:
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DECIMAL128:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIME32:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::STRING:
    case Type::LARGE_STRING:
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::FIXED_SIZE_BINARY:
      return true;
    default:
      return false;
  }
}

std::string inferConversionKind(const std::shared_ptr<arrow::DataType>& type)
{
  using arrow::Type;
  switch (type->id())
  {
    case Type::BOOL:
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      return "integer";
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DECIMAL128:
      return "numeric";
    case Type::DATE32:
    case Type::DATE64:
      return "date";
    case Type::TIME32:
    case Type::TIME64:
      return "time";
    case Type::TIMESTAMP:
      return "timestamp";
    case Type::STRING:
    case Type::LARGE_STRING:
      return "string";
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::FIXED_SIZE_BINARY:
      return "binary_text";
    default:
      return "unsupported";
  }
}

std::string escapeStringValue(const std::string& value)
{
  std::string out;
  out.reserve(value.size() + 8);
  for (char c : value)
  {
    switch (c)
    {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out.push_back(c);
        break;
    }
  }
  return out;
}

int writeBatchAsDelimited(const std::shared_ptr<arrow::RecordBatch>& batch, std::ofstream& out, std::string& errMsg)
{
  if (!batch)
  {
    return NO_ERROR;
  }

  const int64_t rows = batch->num_rows();
  const int columns = batch->num_columns();

  for (int64_t row = 0; row < rows; ++row)
  {
    for (int col = 0; col < columns; ++col)
    {
      const auto& array = batch->column(col);
      if (!array)
      {
        errMsg = "Null array pointer while converting parquet batch";
        return ERR_FILE_READ;
      }

      if (array->IsNull(row))
      {
        out << "\\N";
      }
      else
      {
        auto scalarResult = array->GetScalar(row);
        if (!scalarResult.ok())
        {
          return setArrowError("Unable to materialize parquet scalar", scalarResult, errMsg);
        }

        std::shared_ptr<arrow::Scalar> scalar = scalarResult.ValueOrDie();
        if (!scalar)
        {
          errMsg = "Arrow scalar conversion returned null scalar";
          return ERR_FILE_READ;
        }

        if (array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LARGE_STRING ||
            array->type_id() == arrow::Type::BINARY || array->type_id() == arrow::Type::LARGE_BINARY ||
            array->type_id() == arrow::Type::FIXED_SIZE_BINARY)
        {
          out << "\"" << escapeStringValue(scalar->ToString()) << "\"";
        }
        else
        {
          out << scalar->ToString();
        }
      }

      if (col + 1 < columns)
      {
        out << '\t';
      }
    }
    out << '\n';
  }

  return NO_ERROR;
}

int buildMappings(const std::shared_ptr<arrow::Schema>& schema, std::vector<ParquetColumnMapping>& mappings,
                  std::string& errMsg)
{
  mappings.clear();
  if (!schema)
  {
    errMsg = "Parquet schema is empty";
    return ERR_INVALID_PARAM;
  }

  std::unordered_set<std::string> seenNames;
  for (const auto& field : schema->fields())
  {
    if (!field)
    {
      errMsg = "Parquet schema contains null field metadata";
      return ERR_INVALID_PARAM;
    }
    if (field->name().empty())
    {
      errMsg = "Parquet schema contains an unnamed column";
      return ERR_INVALID_PARAM;
    }
    if (!seenNames.insert(field->name()).second)
    {
      std::ostringstream oss;
      oss << "Parquet schema contains duplicate column name '" << field->name() << "'";
      errMsg = oss.str();
      return ERR_INVALID_PARAM;
    }
    if (!isSupportedType(field->type()))
    {
      std::ostringstream oss;
      oss << "Unsupported Arrow type for column '" << field->name() << "': " << field->type()->ToString();
      errMsg = oss.str();
      return ERR_INVALID_PARAM;
    }

    ParquetColumnMapping mapping;
    mapping.name = field->name();
    mapping.arrowType = field->type()->ToString();
    mapping.conversion = inferConversionKind(field->type());
    mappings.push_back(std::move(mapping));
  }

  return NO_ERROR;
}

}  // namespace

int ParquetReader::readFile(const std::string& filePath, ParquetReadStats& stats, std::string& errMsg)
{
  stats = {};
  errMsg.clear();

  const auto start = std::chrono::steady_clock::now();

  auto inputResult = arrow::io::ReadableFile::Open(filePath);
  if (!inputResult.ok())
  {
    return setArrowError("Unable to open parquet file", inputResult, errMsg);
  }
  std::shared_ptr<arrow::io::ReadableFile> input = inputResult.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> reader;
  arrow::Status openStatus = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
  if (!openStatus.ok())
  {
    return setArrowError("Unable to initialize parquet reader", openStatus, errMsg);
  }
  reader->set_use_threads(true);

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status schemaStatus = reader->GetSchema(&schema);
  if (!schemaStatus.ok())
  {
    return setArrowError("Unable to read parquet schema", schemaStatus, errMsg);
  }

  stats.columnCount = schema ? schema->num_fields() : 0;
  stats.rowGroupCount = reader->num_row_groups();

  std::shared_ptr<arrow::RecordBatchReader> batchReader;
  arrow::Status batchReaderStatus = reader->GetRecordBatchReader(&batchReader);
  if (!batchReaderStatus.ok())
  {
    return setArrowError("Unable to create parquet record batch reader", batchReaderStatus, errMsg);
  }

  while (true)
  {
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status batchStatus = batchReader->ReadNext(&batch);
    if (!batchStatus.ok())
    {
      return setArrowError("Unable to read parquet record batch", batchStatus, errMsg);
    }
    if (!batch)
    {
      break;
    }
    stats.batchCount++;
    stats.totalRows += batch->num_rows();
  }

  const auto end = std::chrono::steady_clock::now();
  stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();

  return NO_ERROR;
}

int ParquetReader::convertToDelimitedFile(const std::string& parquetFilePath, const std::string& outputFilePath,
                                          ParquetConversionResult& result, std::string& errMsg)
{
  result = {};
  errMsg.clear();

  const auto start = std::chrono::steady_clock::now();

  auto inputResult = arrow::io::ReadableFile::Open(parquetFilePath);
  if (!inputResult.ok())
  {
    return setArrowError("Unable to open parquet file", inputResult, errMsg);
  }
  std::shared_ptr<arrow::io::ReadableFile> input = inputResult.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> reader;
  arrow::Status openStatus = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
  if (!openStatus.ok())
  {
    return setArrowError("Unable to initialize parquet reader", openStatus, errMsg);
  }
  reader->set_use_threads(true);

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status schemaStatus = reader->GetSchema(&schema);
  if (!schemaStatus.ok())
  {
    return setArrowError("Unable to read parquet schema", schemaStatus, errMsg);
  }

  int mappingRc = buildMappings(schema, result.mappings, errMsg);
  if (mappingRc != NO_ERROR)
  {
    return mappingRc;
  }

  std::ofstream output(outputFilePath.c_str(), std::ios::out | std::ios::trunc | std::ios::binary);
  if (!output.is_open())
  {
    std::ostringstream oss;
    oss << "Unable to open materialized parquet staging file " << outputFilePath;
    errMsg = oss.str();
    return ERR_FILE_OPEN;
  }

  std::shared_ptr<arrow::RecordBatchReader> batchReader;
  arrow::Status batchReaderStatus = reader->GetRecordBatchReader(&batchReader);
  if (!batchReaderStatus.ok())
  {
    output.close();
    return setArrowError("Unable to create parquet record batch reader", batchReaderStatus, errMsg);
  }

  result.stats.rowGroupCount = reader->num_row_groups();
  result.stats.columnCount = schema ? schema->num_fields() : 0;

  while (true)
  {
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status batchStatus = batchReader->ReadNext(&batch);
    if (!batchStatus.ok())
    {
      output.close();
      return setArrowError("Unable to read parquet record batch", batchStatus, errMsg);
    }
    if (!batch)
    {
      break;
    }

    int writeRc = writeBatchAsDelimited(batch, output, errMsg);
    if (writeRc != NO_ERROR)
    {
      output.close();
      return writeRc;
    }

    result.stats.batchCount++;
    result.stats.totalRows += batch->num_rows();
  }

  output.close();
  if (!output)
  {
    errMsg = "Error while writing parquet materialized staging file";
    return ERR_FILE_WRITE;
  }

  const auto end = std::chrono::steady_clock::now();
  result.stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();
  result.materializedFilePath = outputFilePath;
  return NO_ERROR;
}

}  // namespace WriteEngine
