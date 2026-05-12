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
#include <cmath>
#include <cstring>
#include <fstream>
#include <memory>
#include <numeric>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>

#include "we_define.h"
#include "we_tableinfo.h"

namespace WriteEngine
{

namespace
{
constexpr uint32_t DIRECT_IMPORT_MAX_CHUNK_ROWS = 16384;
// Below Arrow's default 64k; keeps each decoded batch smaller under tight RLIMIT_AS.
constexpr int64_t PARQUET_STREAM_BATCH_ROWS = 1024;
constexpr int DEFAULT_PARQUET_READ_THREADS = 1;
constexpr int64_t DEFAULT_PARQUET_QUEUE_BYTES = 134217728;

ParquetImportRuntimeConfig gImportRuntimeConfig{DEFAULT_PARQUET_READ_THREADS, DEFAULT_PARQUET_QUEUE_BYTES, 1, 0};

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

int openParquetFileReaderForBulk(const std::shared_ptr<arrow::io::ReadableFile>& inputHandle,
                                 std::unique_ptr<parquet::arrow::FileReader>& reader, std::string& errMsg)
{
  std::shared_ptr<arrow::io::RandomAccessFile> input = inputHandle;

  parquet::ArrowReaderProperties arrowProps;
  // External parquet reader + column-writer threads provide parallelism; leave Arrow decode threads off.
  arrowProps.set_use_threads(false);
  arrowProps.set_batch_size(PARQUET_STREAM_BATCH_ROWS);
  // Default ArrowReaderProperties pre_buffer=true coalesces file reads into large
  // in-memory buffers (bad under small RLIMIT_AS even for mmap-backed files).
  arrowProps.set_pre_buffer(false);

  parquet::arrow::FileReaderBuilder builder;
  builder.memory_pool(arrow::default_memory_pool());
  builder.properties(arrowProps);

  arrow::Status st = builder.Open(input, parquet::default_reader_properties(), nullptr);
  if (!st.ok())
    return setArrowError("Unable to initialize parquet reader", st, errMsg);

  st = builder.Build(&reader);
  if (!st.ok())
    return setArrowError("Unable to build parquet file reader", st, errMsg);

  return NO_ERROR;
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

struct DirectColumnBinding
{
  ColumnInfo* columnInfo{nullptr};
  int schemaIndex{-1};  // -1 means "not provided by parquet schema"
  /** Index into RecordBatch::column() (subset reader reorders columns vs full schema). */
  int recordBatchColumnIndex{-1};
};

int buildDirectBindings(const std::shared_ptr<arrow::Schema>& schema, TableInfo& tableInfo,
                        std::vector<DirectColumnBinding>& bindings, std::string& errMsg)
{
  bindings.clear();
  auto& columns = tableInfo.directImportColumns();
  std::unordered_map<std::string, int> tableColumnIndex;
  tableColumnIndex.reserve(columns.size());
  for (unsigned i = 0; i < columns.size(); ++i)
  {
    tableColumnIndex.emplace(columns[i].column.colName, static_cast<int>(i));
  }

  std::unordered_set<std::string> seenInputCols;
  for (int i = 0; i < schema->num_fields(); ++i)
  {
    const auto field = schema->field(i);
    if (!field)
    {
      errMsg = "Parquet schema contains null field metadata";
      return ERR_INVALID_PARAM;
    }
    if (!seenInputCols.insert(field->name()).second)
    {
      std::ostringstream oss;
      oss << "Parquet schema contains duplicate column name '" << field->name() << "'";
      errMsg = oss.str();
      return ERR_INVALID_PARAM;
    }
    // Extra Parquet columns are ignored; only columns present in the target
    // table are imported (same flexibility as omitting trailing CSV fields).
  }

  bindings.reserve(columns.size());
  for (unsigned colIdx = 0; colIdx < columns.size(); ++colIdx)
  {
    DirectColumnBinding binding;
    binding.columnInfo = &columns[colIdx];
    const std::string& columnName = columns[colIdx].column.colName;
    const auto field = schema->GetFieldByName(columnName);
    if (field)
    {
      if (!isSupportedType(field->type()))
      {
        std::ostringstream oss;
        oss << "Unsupported Arrow type for column '" << columnName << "': " << field->type()->ToString();
        errMsg = oss.str();
        return ERR_INVALID_PARAM;
      }
      binding.schemaIndex = schema->GetFieldIndex(columnName);
    }
    bindings.push_back(binding);
  }

  return NO_ERROR;
}

struct DirectColumnSelection
{
  std::vector<int> colIndices;
  bool useSubset{false};
};

int prepareDirectImportColumnSelection(const std::shared_ptr<arrow::Schema>& schema,
                                       std::vector<DirectColumnBinding>& bindings,
                                       DirectColumnSelection& selection, std::string& errMsg)
{
  selection = {};
  auto& colIndices = selection.colIndices;
  for (const auto& b : bindings)
  {
    if (b.schemaIndex >= 0)
      colIndices.push_back(b.schemaIndex);
  }
  std::sort(colIndices.begin(), colIndices.end());
  colIndices.erase(std::unique(colIndices.begin(), colIndices.end()), colIndices.end());

  const int schemaFieldCount = schema->num_fields();
  selection.useSubset =
      !colIndices.empty() && static_cast<int>(colIndices.size()) < schemaFieldCount;

  if (selection.useSubset)
  {
    for (auto& b : bindings)
    {
      if (b.schemaIndex < 0)
      {
        b.recordBatchColumnIndex = -1;
        continue;
      }
      const auto it = std::lower_bound(colIndices.begin(), colIndices.end(), b.schemaIndex);
      if (it == colIndices.end() || *it != b.schemaIndex)
      {
        errMsg = "Internal error: parquet column index mapping failed";
        return ERR_INVALID_PARAM;
      }
      b.recordBatchColumnIndex = static_cast<int>(it - colIndices.begin());
    }
  }
  else
  {
    for (auto& b : bindings)
      b.recordBatchColumnIndex = b.schemaIndex;
  }

  return NO_ERROR;
}

int createDirectImportRecordBatchReader(parquet::arrow::FileReader& reader, const std::vector<int>& rowGroups,
                                        const DirectColumnSelection& selection,
                                        std::shared_ptr<arrow::RecordBatchReader>& batchReader, std::string& errMsg)
{
  arrow::Status st;
  if (selection.useSubset)
    st = reader.GetRecordBatchReader(rowGroups, selection.colIndices, &batchReader);
  else
    st = reader.GetRecordBatchReader(rowGroups, &batchReader);

  if (!st.ok())
    return setArrowError("Unable to create parquet record batch reader", st, errMsg);

  return NO_ERROR;
}

int64_t estimateBatchBytes(const std::shared_ptr<arrow::RecordBatch>& batch)
{
  if (!batch)
    return 0;

  int64_t total = 0;
  for (int i = 0; i < batch->num_columns(); ++i)
  {
    const auto& array = batch->column(i);
    if (!array)
      continue;
    const auto data = array->data();
    if (!data)
      continue;
    for (const auto& buf : data->buffers)
    {
      if (buf)
        total += static_cast<int64_t>(buf->size());
    }
  }
  return std::max<int64_t>(1, total);
}

struct BatchEnvelope
{
  RID globalStartRow{0};
  int64_t numRows{0};
  int64_t estimatedBytes{0};
  std::shared_ptr<arrow::RecordBatch> batch;
};

class BoundedBatchQueue
{
 public:
  explicit BoundedBatchQueue(int64_t maxBytes) : maxBytes_(std::max<int64_t>(1, maxBytes)) {}

  void close()
  {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    cvNotEmpty_.notify_all();
    cvNotFull_.notify_all();
  }

  bool push(BatchEnvelope&& env, std::atomic<bool>& stopFlag)
  {
    std::unique_lock<std::mutex> lock(mu_);
    const int64_t bytes = std::max<int64_t>(1, env.estimatedBytes);
    // If a single batch estimate exceeds maxBytes_, still allow enqueue when the queue is
    // empty so producers cannot block forever (--parquet-queue-bytes softer than batch size).
    cvNotFull_.wait(lock, [&] {
      return closed_ || stopFlag.load() || queue_.empty() || (bytesInQueue_ + bytes <= maxBytes_);
    });
    if (closed_ || stopFlag.load())
      return false;
    bytesInQueue_ += bytes;
    queue_.push_back(std::move(env));
    cvNotEmpty_.notify_one();
    return true;
  }

  bool pop(BatchEnvelope& out, std::atomic<bool>& stopFlag)
  {
    std::unique_lock<std::mutex> lock(mu_);
    cvNotEmpty_.wait(lock, [&] { return closed_ || stopFlag.load() || !queue_.empty(); });
    if (queue_.empty())
      return false;
    out = std::move(queue_.front());
    queue_.pop_front();
    bytesInQueue_ -= std::max<int64_t>(1, out.estimatedBytes);
    cvNotFull_.notify_one();
    return true;
  }

 private:
  std::mutex mu_;
  std::condition_variable cvNotEmpty_;
  std::condition_variable cvNotFull_;
  std::deque<BatchEnvelope> queue_;
  int64_t maxBytes_{1};
  int64_t bytesInQueue_{0};
  bool closed_{false};
};

int getValueAsString(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, bool& isNull, std::string& value,
                     std::string& errMsg)
{
  if (!array)
  {
    errMsg = "Arrow array pointer is null";
    return ERR_INVALID_PARAM;
  }
  if (array->IsNull(rowIndex))
  {
    isNull = true;
    value.clear();
    return NO_ERROR;
  }

  auto scalarResult = array->GetScalar(rowIndex);
  if (!scalarResult.ok())
    return setArrowError("Unable to materialize parquet scalar", scalarResult, errMsg);

  const std::shared_ptr<arrow::Scalar> scalar = scalarResult.ValueOrDie();
  if (!scalar)
  {
    errMsg = "Arrow scalar conversion returned null scalar";
    return ERR_FILE_READ;
  }

  isNull = false;
  value = scalar->ToString();
  return NO_ERROR;
}

bool isDirectNumericFastPathEligible(const JobColumn& column)
{
  using DT = execplan::CalpontSystemCatalog::ColDataType;
  if (column.dataType == DT::DATE || column.dataType == DT::DATETIME || column.dataType == DT::TIMESTAMP ||
      column.dataType == DT::TIME || column.dataType == DT::DECIMAL || column.dataType == DT::UDECIMAL)
  {
    return false;
  }

  switch (column.weType)
  {
    case WriteEngine::WR_BYTE:
    case WriteEngine::WR_SHORT:
    case WriteEngine::WR_INT:
    case WriteEngine::WR_LONGLONG:
    case WriteEngine::WR_UBYTE:
    case WriteEngine::WR_USHORT:
    case WriteEngine::WR_UINT:
    case WriteEngine::WR_ULONGLONG:
    case WriteEngine::WR_FLOAT:
    case WriteEngine::WR_DOUBLE:
      return true;
    default:
      return false;
  }
}

bool tryExtractSignedValue(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, int64_t& out)
{
  using arrow::Type;
  switch (array->type_id())
  {
    case Type::BOOL:
      out = std::static_pointer_cast<arrow::BooleanArray>(array)->Value(rowIndex) ? 1 : 0;
      return true;
    case Type::INT8:
      out = std::static_pointer_cast<arrow::Int8Array>(array)->Value(rowIndex);
      return true;
    case Type::INT16:
      out = std::static_pointer_cast<arrow::Int16Array>(array)->Value(rowIndex);
      return true;
    case Type::INT32:
      out = std::static_pointer_cast<arrow::Int32Array>(array)->Value(rowIndex);
      return true;
    case Type::INT64:
      out = std::static_pointer_cast<arrow::Int64Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT8:
      out = std::static_pointer_cast<arrow::UInt8Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT16:
      out = std::static_pointer_cast<arrow::UInt16Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT32:
      out = std::static_pointer_cast<arrow::UInt32Array>(array)->Value(rowIndex);
      return true;
    default:
      return false;
  }
}

bool tryExtractUnsignedValue(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, uint64_t& out)
{
  using arrow::Type;
  switch (array->type_id())
  {
    case Type::BOOL:
      out = std::static_pointer_cast<arrow::BooleanArray>(array)->Value(rowIndex) ? 1 : 0;
      return true;
    case Type::UINT8:
      out = std::static_pointer_cast<arrow::UInt8Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT16:
      out = std::static_pointer_cast<arrow::UInt16Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT32:
      out = std::static_pointer_cast<arrow::UInt32Array>(array)->Value(rowIndex);
      return true;
    case Type::UINT64:
      out = std::static_pointer_cast<arrow::UInt64Array>(array)->Value(rowIndex);
      return true;
    case Type::INT8:
    {
      const int8_t v = std::static_pointer_cast<arrow::Int8Array>(array)->Value(rowIndex);
      out = (v < 0) ? 0 : static_cast<uint64_t>(v);
      return true;
    }
    case Type::INT16:
    {
      const int16_t v = std::static_pointer_cast<arrow::Int16Array>(array)->Value(rowIndex);
      out = (v < 0) ? 0 : static_cast<uint64_t>(v);
      return true;
    }
    case Type::INT32:
    {
      const int32_t v = std::static_pointer_cast<arrow::Int32Array>(array)->Value(rowIndex);
      out = (v < 0) ? 0 : static_cast<uint64_t>(v);
      return true;
    }
    case Type::INT64:
    {
      const int64_t v = std::static_pointer_cast<arrow::Int64Array>(array)->Value(rowIndex);
      out = (v < 0) ? 0 : static_cast<uint64_t>(v);
      return true;
    }
    default:
      return false;
  }
}

bool tryExtractFloatingValue(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, double& out)
{
  using arrow::Type;
  switch (array->type_id())
  {
    case Type::FLOAT:
      out = std::static_pointer_cast<arrow::FloatArray>(array)->Value(rowIndex);
      return true;
    case Type::DOUBLE:
      out = std::static_pointer_cast<arrow::DoubleArray>(array)->Value(rowIndex);
      return true;
    default:
      return false;
  }
}

bool tryConvertFieldDirectBinary(const std::shared_ptr<arrow::Array>& sourceArray, int64_t rowIndex, const JobColumn& column,
                                 BLBufferStats& bufferStats, unsigned char* output)
{
  if (!sourceArray || sourceArray->IsNull(rowIndex))
    return false;

  switch (column.weType)
  {
    case WriteEngine::WR_BYTE:
    case WriteEngine::WR_SHORT:
    case WriteEngine::WR_INT:
    case WriteEngine::WR_LONGLONG:
    {
      int64_t value = 0;
      if (!tryExtractSignedValue(sourceArray, rowIndex, value))
        return false;

      const int64_t minSat = static_cast<int64_t>(column.fMinIntSat);
      const int64_t maxSat = static_cast<int64_t>(column.fMaxIntSat);
      if (value < minSat)
      {
        value = minSat;
        bufferStats.satCount++;
      }
      else if (value > maxSat)
      {
        value = maxSat;
        bufferStats.satCount++;
      }

      if (value < bufferStats.minBufferVal)
        bufferStats.minBufferVal = value;
      if (value > bufferStats.maxBufferVal)
        bufferStats.maxBufferVal = value;

      switch (column.weType)
      {
        case WriteEngine::WR_BYTE:
        {
          const int8_t v = static_cast<int8_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_SHORT:
        {
          const int16_t v = static_cast<int16_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_INT:
        {
          const int32_t v = static_cast<int32_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_LONGLONG:
        {
          memcpy(output, &value, sizeof(value));
          return true;
        }
        default:
          return false;
      }
    }
    case WriteEngine::WR_UBYTE:
    case WriteEngine::WR_USHORT:
    case WriteEngine::WR_UINT:
    case WriteEngine::WR_ULONGLONG:
    {
      uint64_t value = 0;
      if (!tryExtractUnsignedValue(sourceArray, rowIndex, value))
        return false;

      const uint64_t minSat = static_cast<uint64_t>(column.fMinIntSat < 0 ? 0 : column.fMinIntSat);
      const uint64_t maxSat = static_cast<uint64_t>(column.fMaxIntSat);
      if (value < minSat)
      {
        value = minSat;
        bufferStats.satCount++;
      }
      else if (value > maxSat)
      {
        value = maxSat;
        bufferStats.satCount++;
      }

      if (value < static_cast<uint64_t>(bufferStats.minBufferVal))
        bufferStats.minBufferVal = static_cast<int64_t>(value);
      if (value > static_cast<uint64_t>(bufferStats.maxBufferVal))
        bufferStats.maxBufferVal = static_cast<int64_t>(value);

      switch (column.weType)
      {
        case WriteEngine::WR_UBYTE:
        {
          const uint8_t v = static_cast<uint8_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_USHORT:
        {
          const uint16_t v = static_cast<uint16_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_UINT:
        {
          const uint32_t v = static_cast<uint32_t>(value);
          memcpy(output, &v, sizeof(v));
          return true;
        }
        case WriteEngine::WR_ULONGLONG:
        {
          memcpy(output, &value, sizeof(value));
          return true;
        }
        default:
          return false;
      }
    }
    case WriteEngine::WR_FLOAT:
    {
      double value = 0.0;
      if (!tryExtractFloatingValue(sourceArray, rowIndex, value))
        return false;

      float outValue = static_cast<float>(value);
      const float minSat = static_cast<float>(column.fMinDblSat);
      const float maxSat = static_cast<float>(column.fMaxDblSat);
      if (std::isnan(outValue))
      {
        outValue = std::signbit(outValue) ? minSat : maxSat;
        bufferStats.satCount++;
      }
      else if (outValue > maxSat)
      {
        outValue = maxSat;
        bufferStats.satCount++;
      }
      else if (outValue < minSat)
      {
        outValue = minSat;
        bufferStats.satCount++;
      }

      memcpy(output, &outValue, sizeof(outValue));
      return true;
    }
    case WriteEngine::WR_DOUBLE:
    {
      double outValue = 0.0;
      if (!tryExtractFloatingValue(sourceArray, rowIndex, outValue))
        return false;

      if (std::isnan(outValue))
      {
        outValue = std::signbit(outValue) ? column.fMinDblSat : column.fMaxDblSat;
        bufferStats.satCount++;
      }
      else if (outValue > column.fMaxDblSat)
      {
        outValue = column.fMaxDblSat;
        bufferStats.satCount++;
      }
      else if (outValue < column.fMinDblSat)
      {
        outValue = column.fMinDblSat;
        bufferStats.satCount++;
      }

      memcpy(output, &outValue, sizeof(outValue));
      return true;
    }
    default:
      return false;
  }
}

void resetBufferStats(const JobColumn& column, BLBufferStats& stats)
{
  if (isUnsigned(column.dataType))
  {
    if (column.width <= 8)
    {
      stats.minBufferVal = static_cast<int64_t>(MAX_UBIGINT);
      stats.maxBufferVal = static_cast<int64_t>(MIN_UBIGINT);
    }
    else
    {
      stats.bigMinBufferVal = -1;
      stats.bigMaxBufferVal = 0;
    }
  }
  else
  {
    if (column.width <= 8)
    {
      stats.minBufferVal = MAX_BIGINT;
      stats.maxBufferVal = MIN_BIGINT;
    }
    else
    {
      utils::int128Max(stats.bigMinBufferVal);
      utils::int128Min(stats.bigMaxBufferVal);
    }
  }
}

int processFixedColumnBatch(const DirectColumnBinding& binding, const std::shared_ptr<arrow::RecordBatch>& batch,
                            RID batchStartRow, BulkLoadBuffer& converter, std::string& errMsg)
{
  ColumnInfo& columnInfo = *binding.columnInfo;
  std::shared_ptr<arrow::Array> sourceArray;
  if (binding.recordBatchColumnIndex >= 0)
    sourceArray = batch->column(binding.recordBatchColumnIndex);

  const int64_t batchRows = batch->num_rows();
  uint32_t totalProcessed = 0;
  BLBufferStats bufferStats(columnInfo.column.dataType);
  bool updateCPInfoPending = false;
  const uint32_t maxChunkRows = std::min<uint32_t>(static_cast<uint32_t>(batchRows), DIRECT_IMPORT_MAX_CHUNK_ROWS);
  std::vector<unsigned char> outputBuffer(static_cast<size_t>(maxChunkRows) * columnInfo.column.width);
  char fieldBuffer[MAX_FIELD_SIZE + 1];
  std::string scalarText;
  const bool enableDirectBinaryFastPath = isDirectNumericFastPathEligible(columnInfo.column);

  while (totalProcessed < static_cast<uint32_t>(batchRows))
  {
    ColumnBufferSection* section = nullptr;
    uint32_t nRowsParsed = 0;
    RID lastInputRowInExtent = 0;
    const RID sectionStartRow = batchStartRow + totalProcessed;
    const uint32_t remainingRows = static_cast<uint32_t>(batchRows) - totalProcessed;
    const uint32_t requestedRows = std::min<uint32_t>(remainingRows, DIRECT_IMPORT_MAX_CHUNK_ROWS);
    const int rc =
        columnInfo.fColBufferMgr->reserveSection(sectionStartRow, requestedRows, nRowsParsed, &section, lastInputRowInExtent);
    if (rc != NO_ERROR)
      return rc;
    if (nRowsParsed == 0 || !section)
    {
      errMsg = "reserveSection returned empty section for fixed-width parquet column";
      return ERR_INVALID_PARAM;
    }

    if (columnInfo.column.autoIncFlag)
    {
      uint64_t nextAutoIncValue = 0;
      const int autoRc = columnInfo.reserveAutoIncNums(nRowsParsed, nextAutoIncValue);
      if (autoRc != NO_ERROR)
        return autoRc;
      converter.setAutoIncNextValue(nextAutoIncValue);
    }

    for (uint32_t row = 0; row < nRowsParsed; ++row)
    {
      const int64_t batchRow = static_cast<int64_t>(totalProcessed + row);
      bool nullFlag = (binding.schemaIndex < 0);
      int fieldLength = 0;
      if (!nullFlag)
      {
        const bool usedFastPath =
            enableDirectBinaryFastPath &&
            tryConvertFieldDirectBinary(sourceArray, batchRow, columnInfo.column, bufferStats,
                                        outputBuffer.data() + static_cast<size_t>(row) * columnInfo.column.width);
        if (!usedFastPath)
        {
          const int valueRc = getValueAsString(sourceArray, batchRow, nullFlag, scalarText, errMsg);
          if (valueRc != NO_ERROR)
            return valueRc;
          if (!nullFlag)
          {
            if (scalarText.size() > MAX_FIELD_SIZE)
            {
              std::ostringstream oss;
              oss << "Parquet value exceeds max field size for column '" << columnInfo.column.colName << "'";
              errMsg = oss.str();
              return ERR_BULK_ROW_FILL_BUFFER;
            }
            fieldLength = static_cast<int>(scalarText.size());
            if (fieldLength > 0)
              memcpy(fieldBuffer, scalarText.data(), fieldLength);
            fieldBuffer[fieldLength] = '\0';
          }

          converter.convertField(fieldBuffer, fieldLength, nullFlag,
                                 outputBuffer.data() + static_cast<size_t>(row) * columnInfo.column.width,
                                 columnInfo.column, bufferStats);
        }
      }
      else
      {
        converter.convertField(fieldBuffer, fieldLength, nullFlag,
                               outputBuffer.data() + static_cast<size_t>(row) * columnInfo.column.width,
                               columnInfo.column, bufferStats);
      }
      updateCPInfoPending = true;

      const RID currentInputRow = sectionStartRow + row;
      if (currentInputRow == lastInputRowInExtent)
      {
        if (columnInfo.column.width <= 8)
        {
          columnInfo.updateCPInfo(lastInputRowInExtent, bufferStats.minBufferVal, bufferStats.maxBufferVal,
                                  columnInfo.column.dataType, columnInfo.column.width);
        }
        else
        {
          columnInfo.updateCPInfo(lastInputRowInExtent, bufferStats.bigMinBufferVal, bufferStats.bigMaxBufferVal,
                                  columnInfo.column.dataType, columnInfo.column.width);
        }

        lastInputRowInExtent += columnInfo.rowsPerExtent();
        updateCPInfoPending = false;
        resetBufferStats(columnInfo.column, bufferStats);
      }
    }

    if (updateCPInfoPending)
    {
      if (columnInfo.column.width <= 8)
      {
        columnInfo.updateCPInfo(lastInputRowInExtent, bufferStats.minBufferVal, bufferStats.maxBufferVal,
                                columnInfo.column.dataType, columnInfo.column.width);
      }
      else
      {
        columnInfo.updateCPInfo(lastInputRowInExtent, bufferStats.bigMinBufferVal, bufferStats.bigMaxBufferVal,
                                columnInfo.column.dataType, columnInfo.column.width);
      }
    }

    if (bufferStats.satCount)
    {
      columnInfo.incSaturatedCnt(bufferStats.satCount);
      bufferStats.satCount = 0;
    }

    section->write(outputBuffer.data(), static_cast<int>(nRowsParsed));
    const int releaseRc = columnInfo.fColBufferMgr->releaseSection(section);
    if (releaseRc != NO_ERROR)
      return releaseRc;
    totalProcessed += nRowsParsed;
  }

  return NO_ERROR;
}

int fillDictionaryInput(const DirectColumnBinding& binding, const std::shared_ptr<arrow::RecordBatch>& batch,
                        uint32_t startOffset, uint32_t nRows, size_t totalColumns, std::vector<char>& dataBuffer,
                        std::vector<ColPosPair>& positions, std::vector<ColPosPair*>& rowPtrs, std::string& errMsg)
{
  dataBuffer.clear();
  positions.assign(static_cast<size_t>(nRows) * totalColumns, ColPosPair{0, 0});
  rowPtrs.resize(nRows);
  for (uint32_t row = 0; row < nRows; ++row)
    rowPtrs[row] = &positions[static_cast<size_t>(row) * totalColumns];

  std::shared_ptr<arrow::Array> sourceArray;
  if (binding.recordBatchColumnIndex >= 0)
    sourceArray = batch->column(binding.recordBatchColumnIndex);

  std::string scalarText;
  for (uint32_t row = 0; row < nRows; ++row)
  {
    auto& pos = rowPtrs[row][binding.columnInfo->id];
    bool nullFlag = (binding.schemaIndex < 0);
    if (!nullFlag)
    {
      const int valueRc = getValueAsString(sourceArray, static_cast<int64_t>(startOffset + row), nullFlag, scalarText, errMsg);
      if (valueRc != NO_ERROR)
        return valueRc;
    }

    if (nullFlag)
    {
      pos.start = 0;
      pos.offset = 0;
      continue;
    }

    if (scalarText.size() > MAX_FIELD_SIZE)
    {
      std::ostringstream oss;
      oss << "Parquet value exceeds max field size for dictionary column '" << binding.columnInfo->column.colName
          << "'";
      errMsg = oss.str();
      return ERR_BULK_ROW_FILL_BUFFER;
    }

    pos.start = static_cast<uint32_t>(dataBuffer.size());
    pos.offset = static_cast<uint32_t>(scalarText.size());
    dataBuffer.insert(dataBuffer.end(), scalarText.begin(), scalarText.end());
  }

  return NO_ERROR;
}

int processDictionaryColumnBatch(const DirectColumnBinding& binding, const std::shared_ptr<arrow::RecordBatch>& batch,
                                 RID batchStartRow, size_t totalColumns, std::string& errMsg)
{
  ColumnInfo& columnInfo = *binding.columnInfo;
  const uint32_t batchRows = static_cast<uint32_t>(batch->num_rows());
  uint32_t totalProcessed = 0;

  std::vector<char> dataBuffer;
  std::vector<ColPosPair> positions;
  std::vector<ColPosPair*> rowPtrs;
  std::vector<char> tokenBuffer;
  tokenBuffer.resize(static_cast<size_t>(batchRows) * 8);

  while (totalProcessed < batchRows)
  {
    ColumnBufferSection* section = nullptr;
    uint32_t nRowsParsed = 0;
    RID lastInputRowInExtent = 0;
    const RID sectionStartRow = batchStartRow + totalProcessed;
    const uint32_t remainingRows = batchRows - totalProcessed;
    const uint32_t requestedRows = std::min<uint32_t>(remainingRows, DIRECT_IMPORT_MAX_CHUNK_ROWS);

    // ColumnBufferManagerDctnry::rowsExtentCheck can return 0 rows when the token
    // extent is full. BulkLoadBuffer::parseDict flushes, extends the token column,
    // and rotates the dictionary store before retrying; mirror that here.
    int extentAdvanceAttempts = 0;
    int rc = NO_ERROR;
    bool pendingDctTruncate = false;
    uint16_t truncDbRoot = 0;
    uint32_t truncPart = 0;
    uint16_t truncSeg = 0;
    while (true)
    {
      nRowsParsed = 0;
      section = nullptr;
      rc = columnInfo.fColBufferMgr->reserveSection(sectionStartRow, requestedRows, nRowsParsed, &section,
                                                  lastInputRowInExtent);
      if (rc != NO_ERROR)
        return rc;
      if (nRowsParsed > 0 && section)
        break;

      if (requestedRows == 0)
      {
        errMsg = "reserveSection returned empty section for dictionary parquet column";
        return ERR_INVALID_PARAM;
      }
      if (++extentAdvanceAttempts > 128)
      {
        errMsg = "Parquet dictionary import: token extent stayed full after repeated expand attempts";
        return ERR_INVALID_PARAM;
      }

      rc = columnInfo.fColBufferMgr->intermediateFlush();
      if (rc != NO_ERROR)
        return rc;

      if (columnInfo.isFileComplete())
      {
        pendingDctTruncate = true;
        truncDbRoot = columnInfo.curCol.dataFile.fDbRoot;
        truncPart = columnInfo.curCol.dataFile.fPartition;
        truncSeg = columnInfo.curCol.dataFile.fSegment;
      }

      rc = columnInfo.fColBufferMgr->extendTokenColumn();
      if (rc != NO_ERROR)
        return rc;

      rc = columnInfo.closeDctnryStore(false);
      if (rc != NO_ERROR)
        return rc;

      rc = columnInfo.openDctnryStore(false);
      if (rc != NO_ERROR)
        return rc;
    }

    rc = fillDictionaryInput(binding, batch, totalProcessed, nRowsParsed, totalColumns, dataBuffer, positions,
                             rowPtrs, errMsg);
    if (rc != NO_ERROR)
      return rc;

    char dummy = '\0';
    char* dictionaryInput = dataBuffer.empty() ? &dummy : dataBuffer.data();
    rc = columnInfo.updateDctnryStore(dictionaryInput, rowPtrs.data(), static_cast<int>(nRowsParsed), tokenBuffer.data());
    if (rc != NO_ERROR)
      return rc;

    section->write(tokenBuffer.data(), static_cast<int>(nRowsParsed));
    rc = columnInfo.fColBufferMgr->releaseSection(section);
    if (rc != NO_ERROR)
      return rc;

    if (pendingDctTruncate)
    {
      rc = columnInfo.truncateDctnryStore(columnInfo.column.dctnry.dctnryOid, truncDbRoot, truncPart, truncSeg);
      if (rc != NO_ERROR)
        return rc;
      pendingDctTruncate = false;
    }

    totalProcessed += nRowsParsed;

    if (totalProcessed < batchRows)
    {
      rc = columnInfo.fColBufferMgr->intermediateFlush();
      if (rc != NO_ERROR)
        return rc;

      const bool fileComplete = columnInfo.isFileComplete();
      const uint16_t root = columnInfo.curCol.dataFile.fDbRoot;
      const uint32_t partNum = columnInfo.curCol.dataFile.fPartition;
      const uint16_t segNum = columnInfo.curCol.dataFile.fSegment;

      rc = columnInfo.fColBufferMgr->extendTokenColumn();
      if (rc != NO_ERROR)
        return rc;

      rc = columnInfo.closeDctnryStore(false);
      if (rc != NO_ERROR)
        return rc;

      rc = columnInfo.openDctnryStore(false);
      if (rc != NO_ERROR)
        return rc;

      if (fileComplete)
      {
        rc = columnInfo.truncateDctnryStore(columnInfo.column.dctnry.dctnryOid, root, partNum, segNum);
        if (rc != NO_ERROR)
          return rc;
      }
    }
  }

  return NO_ERROR;
}

constexpr char kParquetWriterQueueFailMsg[] =
    "Parquet direct import: column writer queue closed while dispatching batch";

template <typename T>
class ParquetWorkQueue
{
 public:
  void close()
  {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    cv_.notify_all();
  }

  /** @return false if the queue is closed (caller must not drop work silently). */
  bool push(T&& item)
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (closed_)
      return false;
    q_.push_back(std::move(item));
    cv_.notify_one();
    return true;
  }

  bool pop(T& out, std::atomic<bool>& stop)
  {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return closed_ || stop.load() || !q_.empty(); });
    if (q_.empty())
      return false;
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> q_;
  bool closed_{false};
};

struct ColumnTask
{
  std::shared_ptr<arrow::RecordBatch> batch;
  RID batchStartRow{0};
  std::vector<size_t> bindingIndices;
  std::shared_ptr<struct BatchCompletion> completion;
};

struct BatchCompletion
{
  BatchCompletion(int rem, int64_t rows, ParquetConversionResult* res, std::condition_variable* cv,
                  std::atomic<int>* bi, std::string* sharedErr, std::mutex* importStateMu)
   : remaining(rem)
   , batchRows(rows)
   , result(res)
   , drainCv(cv)
   , batchesInflight(bi)
   , sharedErrMsg(sharedErr)
   , importStateMutex(importStateMu)
  {
  }

  std::atomic<int> remaining;
  const int64_t batchRows;
  ParquetConversionResult* result;
  std::condition_variable* drainCv;
  std::atomic<int>* batchesInflight;
  std::string* sharedErrMsg;
  std::mutex* importStateMutex;

  void notifyWorkerDone(int rc, const std::string& err, std::atomic<bool>& stop)
  {
    if (rc != NO_ERROR)
    {
      if (importStateMutex && sharedErrMsg)
      {
        std::lock_guard<std::mutex> lk(*importStateMutex);
        if (sharedErrMsg->empty())
          *sharedErrMsg = err;
      }
      stop.store(true);
    }

    const int left = remaining.fetch_sub(1) - 1;
    if (left == 0)
    {
      if (importStateMutex && sharedErrMsg && result)
      {
        std::lock_guard<std::mutex> lk(*importStateMutex);
        if (sharedErrMsg->empty())
        {
          result->stats.batchCount++;
          result->stats.totalRows += batchRows;
        }
      }
      if (batchesInflight)
        batchesInflight->fetch_sub(1);
      if (drainCv)
        drainCv->notify_all();
    }
  }
};

void partitionParquetBindingsToWriters(const std::vector<DirectColumnBinding>& bindings, int writerCount,
                                       std::vector<std::vector<size_t>>& perWriter)
{
  perWriter.assign(static_cast<size_t>(writerCount), {});
  std::vector<size_t> dictIdx;
  std::vector<size_t> fixedIdx;
  dictIdx.reserve(bindings.size());
  fixedIdx.reserve(bindings.size());
  for (size_t i = 0; i < bindings.size(); ++i)
  {
    if (bindings[i].columnInfo->column.colType == COL_TYPE_DICT)
      dictIdx.push_back(i);
    else
      fixedIdx.push_back(i);
  }
  if (writerCount > 0)
  {
    for (size_t k = 0; k < dictIdx.size(); ++k)
      perWriter[k % static_cast<size_t>(writerCount)].push_back(dictIdx[k]);
  }
  for (size_t idx : fixedIdx)
  {
    size_t best = 0;
    for (int w = 1; w < writerCount; ++w)
    {
      if (perWriter[static_cast<size_t>(w)].size() < perWriter[best].size())
        best = static_cast<size_t>(w);
    }
    perWriter[best].push_back(idx);
  }
}

int validatePartitionParquetBindings(const std::vector<DirectColumnBinding>& bindings, int writerCount,
                                     const std::vector<std::vector<size_t>>& perWriter, std::string& errMsg)
{
  const size_t n = bindings.size();
  std::vector<int> seen(n, 0);
  for (int w = 0; w < writerCount; ++w)
  {
    for (size_t idx : perWriter[static_cast<size_t>(w)])
    {
      if (idx >= n)
      {
        errMsg = "Parquet direct import: internal binding index out of range";
        return ERR_INVALID_PARAM;
      }
      if (seen[idx])
      {
        errMsg = "Parquet direct import: duplicate binding index in writer partition";
        return ERR_INVALID_PARAM;
      }
      seen[idx] = 1;
    }
  }
  for (size_t i = 0; i < n; ++i)
  {
    if (!seen[i])
    {
      errMsg = "Parquet direct import: binding index missing from writer partition";
      return ERR_INVALID_PARAM;
    }
  }
  return NO_ERROR;
}

}  // namespace

void ParquetReader::setImportRuntimeConfig(const ParquetImportRuntimeConfig& cfg)
{
  gImportRuntimeConfig.readThreads = std::max(1, cfg.readThreads);
  gImportRuntimeConfig.queueBytes = std::max<int64_t>(1, cfg.queueBytes);
  gImportRuntimeConfig.columnWriteThreads = std::max(1, cfg.columnWriteThreads);
  gImportRuntimeConfig.maxParquetInflightBatches = cfg.maxParquetInflightBatches;
}

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
  int openRc = openParquetFileReaderForBulk(input, reader, errMsg);
  if (openRc != NO_ERROR)
    return openRc;

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
  int openRc = openParquetFileReaderForBulk(input, reader, errMsg);
  if (openRc != NO_ERROR)
    return openRc;

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

int ParquetReader::importIntoTableDirect(const std::string& parquetFilePath, TableInfo& tableInfo,
                                         ParquetConversionResult& result, std::string& errMsg)
{
  result = {};
  errMsg.clear();

  const auto start = std::chrono::steady_clock::now();

  auto inputResult = arrow::io::ReadableFile::Open(parquetFilePath);
  if (!inputResult.ok())
    return setArrowError("Unable to open parquet file", inputResult, errMsg);
  std::shared_ptr<arrow::io::ReadableFile> input = inputResult.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> reader;
  int openRc = openParquetFileReaderForBulk(input, reader, errMsg);
  if (openRc != NO_ERROR)
    return openRc;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status schemaStatus = reader->GetSchema(&schema);
  if (!schemaStatus.ok())
    return setArrowError("Unable to read parquet schema", schemaStatus, errMsg);

  int rc = buildMappings(schema, result.mappings, errMsg);
  if (rc != NO_ERROR)
    return rc;

  std::vector<DirectColumnBinding> bindings;
  rc = buildDirectBindings(schema, tableInfo, bindings, errMsg);
  if (rc != NO_ERROR)
    return rc;
  DirectColumnSelection columnSelection;
  rc = prepareDirectImportColumnSelection(schema, bindings, columnSelection, errMsg);
  if (rc != NO_ERROR)
    return rc;

  result.stats.rowGroupCount = reader->num_row_groups();
  result.stats.columnCount = schema ? schema->num_fields() : 0;
  const int rowGroupCount = reader->num_row_groups();
  std::vector<RID> rowGroupStarts(static_cast<size_t>(std::max(0, rowGroupCount)), 0);
  int64_t totalRowsFromMeta = 0;
  if (auto* parquetReader = reader->parquet_reader())
  {
    auto metadata = parquetReader->metadata();
    if (metadata)
    {
      totalRowsFromMeta = metadata->num_rows();
      RID runningStart = 0;
      for (int rg = 0; rg < rowGroupCount; ++rg)
      {
        rowGroupStarts[static_cast<size_t>(rg)] = runningStart;
        const auto rgMeta = metadata->RowGroup(rg);
        if (rgMeta)
          runningStart += static_cast<RID>(rgMeta->num_rows());
      }
    }
  }

  if (totalRowsFromMeta == 0)
  {
    tableInfo.prepareDirectImportCompletion(0);
    rc = tableInfo.finalizeDirectImport(errMsg);
    if (rc != NO_ERROR)
      return rc;
    const auto end = std::chrono::steady_clock::now();
    result.stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();
    return NO_ERROR;
  }

  JobFieldRefList emptyFieldRefList;
  int columnWriterCount = std::max(1, gImportRuntimeConfig.columnWriteThreads);
  columnWriterCount = std::min(columnWriterCount, static_cast<int>(bindings.size()));
  std::vector<std::vector<size_t>> bindingsPerWriter;
  partitionParquetBindingsToWriters(bindings, columnWriterCount, bindingsPerWriter);
  rc = validatePartitionParquetBindings(bindings, columnWriterCount, bindingsPerWriter, errMsg);
  if (rc != NO_ERROR)
    return rc;

  int maxBatchesInflight = gImportRuntimeConfig.maxParquetInflightBatches;
  if (maxBatchesInflight <= 0)
    maxBatchesInflight = std::max(2, columnWriterCount * 2);
  else
    maxBatchesInflight = std::max(1, maxBatchesInflight);

  std::vector<std::unique_ptr<BulkLoadBuffer>> columnConverters;
  columnConverters.reserve(static_cast<size_t>(columnWriterCount));
  for (int w = 0; w < columnWriterCount; ++w)
  {
    auto conv = std::make_unique<BulkLoadBuffer>(static_cast<unsigned>(tableInfo.directImportColumns().size()), 4096,
                                                   nullptr, 0, tableInfo.directImportTableName(), emptyFieldRefList);
    conv->setImportDataMode(IMPORT_DATA_TEXT, 0);
    conv->setTimeZone(tableInfo.getTimeZone());
    columnConverters.push_back(std::move(conv));
  }

  std::vector<ParquetWorkQueue<ColumnTask>> columnWriterQueues(static_cast<size_t>(columnWriterCount));
  std::mutex drainMutex;
  std::condition_variable drainCv;
  std::atomic<int> batchesInflight{0};
  const size_t totalColumns = tableInfo.directImportColumns().size();

  int workerCount = std::max(1, gImportRuntimeConfig.readThreads);
  workerCount = std::min(workerCount, std::max(1, rowGroupCount));
  const int groupsPerWorker = std::max(1, (rowGroupCount + workerCount - 1) / workerCount);
  std::vector<std::vector<int>> workerRowGroups(static_cast<size_t>(workerCount));
  for (int worker = 0; worker < workerCount; ++worker)
  {
    const int begin = worker * groupsPerWorker;
    const int end = std::min(rowGroupCount, begin + groupsPerWorker);
    if (begin >= end)
      break;
    auto& assignment = workerRowGroups[static_cast<size_t>(worker)];
    assignment.reserve(static_cast<size_t>(end - begin));
    for (int rg = begin; rg < end; ++rg)
      assignment.push_back(rg);
  }

  BoundedBatchQueue queue(gImportRuntimeConfig.queueBytes);
  std::atomic<bool> stopRequested(false);
  std::atomic<int> activeProducers(0);
  std::mutex importStateMutex;
  std::string sharedErrMsg;
  auto setImportError = [&](const std::string& msg) {
    if (msg.empty())
      return;
    std::lock_guard<std::mutex> lock(importStateMutex);
    if (sharedErrMsg.empty())
      sharedErrMsg = msg;
  };

  for (const auto& binding : bindings)
  {
    rc = binding.columnInfo->createDelayedFileIfNeeded(tableInfo.directImportTableName());
    if (rc != NO_ERROR)
      return rc;
  }

  std::vector<std::thread> columnWriters;
  columnWriters.reserve(static_cast<size_t>(columnWriterCount));
  for (int w = 0; w < columnWriterCount; ++w)
  {
    columnWriters.emplace_back([&, w]() {
      while (true)
      {
        ColumnTask task;
        if (!columnWriterQueues[static_cast<size_t>(w)].pop(task, stopRequested))
          break;
        std::string localErr;
        int taskRc = NO_ERROR;
        for (size_t bi : task.bindingIndices)
        {
          const auto& binding = bindings[bi];
          if (binding.columnInfo->column.colType == COL_TYPE_DICT)
            taskRc = processDictionaryColumnBatch(binding, task.batch, task.batchStartRow, totalColumns, localErr);
          else
            taskRc = processFixedColumnBatch(binding, task.batch, task.batchStartRow,
                                             *columnConverters[static_cast<size_t>(w)], localErr);
          if (taskRc != NO_ERROR)
            break;
        }
        task.completion->notifyWorkerDone(taskRc, localErr, stopRequested);
      }
    });
  }

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(workerCount));
  for (int workerId = 0; workerId < workerCount; ++workerId)
  {
    const auto rowGroups = workerRowGroups[static_cast<size_t>(workerId)];
    if (rowGroups.empty())
      continue;
    activeProducers.fetch_add(1);
    workers.emplace_back([&, rowGroups]() {
      std::string localErr;
      auto inputWorkerResult = arrow::io::ReadableFile::Open(parquetFilePath);
      if (!inputWorkerResult.ok())
      {
        setImportError("Unable to open parquet file for reader worker");
        stopRequested.store(true);
        queue.close();
      }
      else
      {
        std::shared_ptr<arrow::io::ReadableFile> workerInput = inputWorkerResult.ValueOrDie();
        std::unique_ptr<parquet::arrow::FileReader> workerReader;
        if (openParquetFileReaderForBulk(workerInput, workerReader, localErr) != NO_ERROR)
        {
          setImportError(localErr);
          stopRequested.store(true);
          queue.close();
        }
        else
        {
          std::shared_ptr<arrow::RecordBatchReader> batchReader;
          if (createDirectImportRecordBatchReader(*workerReader, rowGroups, columnSelection, batchReader, localErr) !=
              NO_ERROR)
          {
            setImportError(localErr);
            stopRequested.store(true);
            queue.close();
          }
          else
          {
            RID workerRowCursor = rowGroupStarts[static_cast<size_t>(rowGroups.front())];
            while (!stopRequested.load())
            {
              std::shared_ptr<arrow::RecordBatch> batch;
              const arrow::Status st = batchReader->ReadNext(&batch);
              if (!st.ok())
              {
                setImportError("Unable to read parquet record batch: " + st.ToString());
                stopRequested.store(true);
                queue.close();
                break;
              }
              if (!batch)
                break;
              if (batch->num_rows() <= 0)
                continue;
              BatchEnvelope env;
              env.globalStartRow = workerRowCursor;
              env.numRows = batch->num_rows();
              env.estimatedBytes = estimateBatchBytes(batch);
              env.batch = std::move(batch);
              workerRowCursor += static_cast<RID>(env.numRows);
              if (!queue.push(std::move(env), stopRequested))
                break;
            }
          }
        }
      }

      if (activeProducers.fetch_sub(1) == 1)
        queue.close();
    });
  }

  rc = NO_ERROR;
  RID expectedStartRow = 0;
  int64_t rowsDispatched = 0;
  std::map<RID, BatchEnvelope> reorder;

  auto dispatchOrderedBatch = [&](BatchEnvelope& current, RID batchStart) {
    int taskCount = 0;
    for (int w = 0; w < columnWriterCount; ++w)
    {
      if (!bindingsPerWriter[static_cast<size_t>(w)].empty())
        ++taskCount;
    }
    if (taskCount == 0 || current.numRows <= 0)
      return;
    auto completion = std::make_shared<BatchCompletion>(taskCount, current.numRows, &result, &drainCv, &batchesInflight,
                                                          &sharedErrMsg, &importStateMutex);
    int pushesSucceeded = 0;
    for (int w = 0; w < columnWriterCount; ++w)
    {
      if (bindingsPerWriter[static_cast<size_t>(w)].empty())
        continue;
      ColumnTask task;
      task.batch = current.batch;
      task.batchStartRow = batchStart;
      task.bindingIndices = bindingsPerWriter[static_cast<size_t>(w)];
      task.completion = completion;
      if (!columnWriterQueues[static_cast<size_t>(w)].push(std::move(task)))
      {
        stopRequested.store(true);
        {
          std::lock_guard<std::mutex> lk(importStateMutex);
          if (sharedErrMsg.empty())
            sharedErrMsg = kParquetWriterQueueFailMsg;
        }
        const int needSynthetic = taskCount - pushesSucceeded;
        for (int k = 0; k < needSynthetic; ++k)
          completion->notifyWorkerDone(ERR_FILE_READ, kParquetWriterQueueFailMsg, stopRequested);
        return;
      }
      pushesSucceeded++;
    }
    batchesInflight.fetch_add(1);
  };

  while (rowsDispatched < totalRowsFromMeta && !stopRequested.load())
  {
    BatchEnvelope env;
    if (!queue.pop(env, stopRequested))
    {
      if (activeProducers.load() == 0)
        break;
      continue;
    }
    reorder.emplace(env.globalStartRow, std::move(env));

    while (!stopRequested.load())
    {
      auto it = reorder.find(expectedStartRow);
      if (it == reorder.end())
        break;
      {
        std::unique_lock<std::mutex> lk(drainMutex);
        drainCv.wait(lk, [&] {
          return batchesInflight.load() < maxBatchesInflight || stopRequested.load();
        });
      }
      if (stopRequested.load())
        break;
      BatchEnvelope current = std::move(it->second);
      reorder.erase(it);
      const RID batchStart = expectedStartRow;
      dispatchOrderedBatch(current, batchStart);
      expectedStartRow += static_cast<RID>(current.numRows);
      rowsDispatched += current.numRows;
    }
  }

  queue.close();
  for (auto& worker : workers)
  {
    if (worker.joinable())
      worker.join();
  }

  {
    std::unique_lock<std::mutex> lk(drainMutex);
    drainCv.wait(lk, [&] { return batchesInflight.load() == 0 || stopRequested.load(); });
  }

  for (auto& cwq : columnWriterQueues)
    cwq.close();
  for (auto& cw : columnWriters)
  {
    if (cw.joinable())
      cw.join();
  }

  if (!sharedErrMsg.empty() && rc == NO_ERROR)
  {
    errMsg = sharedErrMsg;
    return ERR_FILE_READ;
  }
  if (rc != NO_ERROR)
    return rc;
  if (result.stats.totalRows != totalRowsFromMeta)
  {
    std::ostringstream oss;
    oss << "Parquet import ended early: processed " << result.stats.totalRows << " of " << totalRowsFromMeta << " rows";
    errMsg = oss.str();
    return ERR_FILE_READ;
  }

  tableInfo.prepareDirectImportCompletion(static_cast<RID>(result.stats.totalRows));
  rc = tableInfo.finalizeDirectImport(errMsg);
  if (rc != NO_ERROR)
    return rc;

  const auto end = std::chrono::steady_clock::now();
  result.stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();
  return NO_ERROR;
}

}  // namespace WriteEngine
