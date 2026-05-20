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
#include <ctime>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <limits>
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
#include <string_view>

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
#include "dataconvert.h"

#include <arrow/type.h>

namespace WriteEngine
{

namespace
{
constexpr uint32_t DIRECT_IMPORT_MAX_CHUNK_ROWS = 16384;
// Below Arrow's default 64k; keeps each decoded batch smaller under tight RLIMIT_AS.
constexpr int64_t PARQUET_STREAM_BATCH_ROWS = 1024;
constexpr int DEFAULT_PARQUET_READ_THREADS = 1;
constexpr int64_t DEFAULT_PARQUET_QUEUE_BYTES = 134217728;

ParquetImportRuntimeConfig gImportRuntimeConfig{DEFAULT_PARQUET_READ_THREADS, DEFAULT_PARQUET_QUEUE_BYTES, 1, 0,
                                                false, false};

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
  // When false, only cpimport's own reader/column-writer threads decode; when true, Arrow may parallelize too.
  arrowProps.set_use_threads(gImportRuntimeConfig.arrowReaderUseThreads);
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

int getValueAsString(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, bool& isNull, std::string& value,
                     std::string& errMsg);

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

      std::string cell;
      bool cellNull = false;
      const int cellRc = getValueAsString(array, row, cellNull, cell, errMsg);
      if (cellRc != NO_ERROR)
      {
        return cellRc;
      }
      if (cellNull)
      {
        out << "\\N";
      }
      else
      {
        const auto tid = array->type_id();
        if (tid == arrow::Type::STRING || tid == arrow::Type::LARGE_STRING ||
            tid == arrow::Type::BINARY || tid == arrow::Type::LARGE_BINARY ||
            tid == arrow::Type::FIXED_SIZE_BINARY || tid == arrow::Type::DICTIONARY)
        {
          out << "\"" << escapeStringValue(cell) << "\"";
        }
        else
        {
          out << cell;
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

using PipelineClock = std::chrono::steady_clock;

static inline uint64_t pipelineNsSince(const PipelineClock::time_point& start)
{
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(PipelineClock::now() - start).count());
}

static inline void pipelineAddNs(std::atomic<uint64_t>& dst, uint64_t ns)
{
  dst.fetch_add(ns, std::memory_order_relaxed);
}

static inline void pipelineUpdateMaxAtomic(std::atomic<uint64_t>& maxValue, uint64_t value)
{
  uint64_t current = maxValue.load(std::memory_order_relaxed);
  while (current < value &&
         !maxValue.compare_exchange_weak(current, value, std::memory_order_relaxed, std::memory_order_relaxed))
  {
  }
}

struct ParquetPipelineInstrumentation
{
  std::atomic<uint64_t> readerBatches{0};
  std::atomic<uint64_t> readerRows{0};
  std::atomic<uint64_t> readerDecodeNs{0};
  std::atomic<uint64_t> readerPushWaitNs{0};
  std::atomic<uint64_t> readerPushCount{0};

  std::atomic<uint64_t> coordinatorPopWaitNs{0};
  std::atomic<uint64_t> coordinatorPopCount{0};
  std::atomic<uint64_t> batchPreDispatchResidenceNs{0};
  std::atomic<uint64_t> trueReorderHoldNs{0};
  std::atomic<uint64_t> trueReorderHeldBatches{0};
  std::atomic<uint64_t> coordinatorDispatchedBatches{0};
  std::atomic<uint64_t> coordinatorDispatchedTasks{0};
  std::atomic<uint64_t> coordinatorInflightWaitNs{0};
  std::atomic<uint64_t> coordinatorInflightWaitCount{0};

  std::atomic<uint64_t> writerQueuePopWaitNs{0};
  std::atomic<uint64_t> writerQueuePopCount{0};
  std::atomic<uint64_t> writerTaskProcessNs{0};
  std::atomic<uint64_t> writerTasks{0};

  std::atomic<uint64_t> fixedColumnNs{0};
  std::atomic<uint64_t> fixedColumnCalls{0};
  std::atomic<uint64_t> dictionaryColumnNs{0};
  std::atomic<uint64_t> dictionaryColumnCalls{0};

  std::atomic<uint64_t> maxQueueBytesObserved{0};
  std::atomic<uint64_t> maxInflightBatchesObserved{0};
};

static void copyPipelineInstrToResult(ParquetPipelineInstrumentation& src, ParquetConversionResult& result)
{
  result.hasPipelineInstrumentation = true;
  ParquetPipelineInstrSnapshot& d = result.pipelineInstrumentation;
  d.readerBatches = src.readerBatches.load(std::memory_order_relaxed);
  d.readerRows = src.readerRows.load(std::memory_order_relaxed);
  d.readerDecodeNs = src.readerDecodeNs.load(std::memory_order_relaxed);
  d.readerPushWaitNs = src.readerPushWaitNs.load(std::memory_order_relaxed);
  d.readerPushCount = src.readerPushCount.load(std::memory_order_relaxed);
  d.coordinatorPopWaitNs = src.coordinatorPopWaitNs.load(std::memory_order_relaxed);
  d.coordinatorPopCount = src.coordinatorPopCount.load(std::memory_order_relaxed);
  d.batchPreDispatchResidenceNs = src.batchPreDispatchResidenceNs.load(std::memory_order_relaxed);
  d.trueReorderHoldNs = src.trueReorderHoldNs.load(std::memory_order_relaxed);
  d.trueReorderHeldBatches = src.trueReorderHeldBatches.load(std::memory_order_relaxed);
  d.coordinatorDispatchedBatches = src.coordinatorDispatchedBatches.load(std::memory_order_relaxed);
  d.coordinatorDispatchedTasks = src.coordinatorDispatchedTasks.load(std::memory_order_relaxed);
  d.coordinatorInflightWaitNs = src.coordinatorInflightWaitNs.load(std::memory_order_relaxed);
  d.coordinatorInflightWaitCount = src.coordinatorInflightWaitCount.load(std::memory_order_relaxed);
  d.writerQueuePopWaitNs = src.writerQueuePopWaitNs.load(std::memory_order_relaxed);
  d.writerQueuePopCount = src.writerQueuePopCount.load(std::memory_order_relaxed);
  d.writerTaskProcessNs = src.writerTaskProcessNs.load(std::memory_order_relaxed);
  d.writerTasks = src.writerTasks.load(std::memory_order_relaxed);
  d.fixedColumnNs = src.fixedColumnNs.load(std::memory_order_relaxed);
  d.fixedColumnCalls = src.fixedColumnCalls.load(std::memory_order_relaxed);
  d.dictionaryColumnNs = src.dictionaryColumnNs.load(std::memory_order_relaxed);
  d.dictionaryColumnCalls = src.dictionaryColumnCalls.load(std::memory_order_relaxed);
  d.maxQueueBytesObserved = src.maxQueueBytesObserved.load(std::memory_order_relaxed);
  d.maxInflightBatchesObserved = src.maxInflightBatchesObserved.load(std::memory_order_relaxed);
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
  /** When pipeline instrumentation is on: time batch was pushed onto the bounded queue. */
  PipelineClock::time_point queuedAt{};
  /** Set when batch is stored in reorder map because globalStartRow != expectedStartRow. */
  PipelineClock::time_point reorderInsertedAt{};
  bool heldForReorder{false};
};

struct ParquetWallClockInstrumentation
{
  uint64_t totalNs{0};
  uint64_t setupNs{0};
  uint64_t startWritersNs{0};
  uint64_t startReadersNs{0};
  uint64_t coordinatorLoopNs{0};
  uint64_t readerJoinNs{0};
  uint64_t writerDrainNs{0};
  uint64_t writerJoinNs{0};
  uint64_t finalizeNs{0};
};

static void copyWallClockInstrToResult(const ParquetWallClockInstrumentation& src, ParquetConversionResult& result)
{
  result.hasWallClockInstrumentation = true;
  ParquetWallClockInstrSnapshot& d = result.wallClockInstrumentation;
  d.totalNs = src.totalNs;
  d.setupNs = src.setupNs;
  d.startWritersNs = src.startWritersNs;
  d.startReadersNs = src.startReadersNs;
  d.coordinatorLoopNs = src.coordinatorLoopNs;
  d.readerJoinNs = src.readerJoinNs;
  d.writerDrainNs = src.writerDrainNs;
  d.writerJoinNs = src.writerJoinNs;
  d.finalizeNs = src.finalizeNs;
}

class BoundedBatchQueue
{
 public:
  explicit BoundedBatchQueue(int64_t maxBytes) : maxBytes_(std::max<int64_t>(1, maxBytes)) {}

  void attachPipelineInstr(ParquetPipelineInstrumentation* p)
  {
    pipeInstr_ = p;
  }

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
    if (pipeInstr_)
      pipelineUpdateMaxAtomic(pipeInstr_->maxQueueBytesObserved, static_cast<uint64_t>(bytesInQueue_));
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
    if (pipeInstr_)
      pipelineUpdateMaxAtomic(pipeInstr_->maxQueueBytesObserved, static_cast<uint64_t>(bytesInQueue_));
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
  ParquetPipelineInstrumentation* pipeInstr_{nullptr};
};

int readDictionaryIndexSlot(const std::shared_ptr<arrow::Array>& indices, int64_t row, int64_t& outSlot,
                            std::string& errMsg);

/**
 * Typed extraction for STRING/LARGE_STRING/BINARY/LARGE_BINARY/FIXED_SIZE_BINARY and
 * DICTIONARY with string/binary values — avoids Arrow GetScalar + Scalar::ToString on hot paths.
 * Returns true if \p value / \p isNull were set without scalar materialization.
 */
bool tryMaterializeArrowCellAsBulkLoadText(const std::shared_ptr<arrow::Array>& array, int64_t rowIndex, bool& isNull,
                                           std::string& value, std::string& errMsg)
{
  isNull = false;
  using arrow::Type;
  switch (array->type_id())
  {
    case Type::STRING:
    {
      const auto* sa = static_cast<const arrow::StringArray*>(array.get());
      if (sa->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      {
        const auto v = sa->GetView(rowIndex);
        value.assign(v.data(), v.size());
      }
      return true;
    }
    case Type::LARGE_STRING:
    {
      const auto* sa = static_cast<const arrow::LargeStringArray*>(array.get());
      if (sa->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      {
        const auto v = sa->GetView(rowIndex);
        value.assign(v.data(), v.size());
      }
      return true;
    }
    case Type::BINARY:
    {
      const auto* ba = static_cast<const arrow::BinaryArray*>(array.get());
      if (ba->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      {
        const auto v = ba->GetView(rowIndex);
        value.assign(v.data(), v.size());
      }
      return true;
    }
    case Type::LARGE_BINARY:
    {
      const auto* ba = static_cast<const arrow::LargeBinaryArray*>(array.get());
      if (ba->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      {
        const auto v = ba->GetView(rowIndex);
        value.assign(v.data(), v.size());
      }
      return true;
    }
    case Type::FIXED_SIZE_BINARY:
    {
      const auto* fsb = static_cast<const arrow::FixedSizeBinaryArray*>(array.get());
      if (fsb->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      {
        const auto v = fsb->GetView(rowIndex);
        value.assign(v.data(), v.size());
      }
      return true;
    }
    case Type::DICTIONARY:
    {
      const auto* dictArr = static_cast<const arrow::DictionaryArray*>(array.get());
      if (dictArr->IsNull(rowIndex))
      {
        isNull = true;
        value.clear();
        return true;
      }
      const std::shared_ptr<arrow::Array> indices = dictArr->indices();
      const std::shared_ptr<arrow::Array> dictionary = dictArr->dictionary();
      if (!indices || !dictionary)
      {
        errMsg = "Arrow dictionary column has null indices or dictionary array";
        return false;
      }
      int64_t dictSlot = 0;
      if (readDictionaryIndexSlot(indices, rowIndex, dictSlot, errMsg) != NO_ERROR)
        return false;
      if (dictSlot < 0 || dictSlot >= dictionary->length())
      {
        errMsg = "Arrow dictionary index out of range";
        return false;
      }
      if (dictionary->IsNull(dictSlot))
      {
        isNull = true;
        value.clear();
        return true;
      }
      switch (dictionary->type_id())
      {
        case Type::STRING:
        {
          const auto* dv = static_cast<const arrow::StringArray*>(dictionary.get());
          const auto v = dv->GetView(dictSlot);
          value.assign(v.data(), v.size());
          return true;
        }
        case Type::LARGE_STRING:
        {
          const auto* dv = static_cast<const arrow::LargeStringArray*>(dictionary.get());
          const auto v = dv->GetView(dictSlot);
          value.assign(v.data(), v.size());
          return true;
        }
        case Type::BINARY:
        {
          const auto* dv = static_cast<const arrow::BinaryArray*>(dictionary.get());
          const auto v = dv->GetView(dictSlot);
          value.assign(v.data(), v.size());
          return true;
        }
        case Type::LARGE_BINARY:
        {
          const auto* dv = static_cast<const arrow::LargeBinaryArray*>(dictionary.get());
          const auto v = dv->GetView(dictSlot);
          value.assign(v.data(), v.size());
          return true;
        }
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

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

  if (tryMaterializeArrowCellAsBulkLoadText(array, rowIndex, isNull, value, errMsg))
  {
    return NO_ERROR;
  }
  if (!errMsg.empty())
  {
    return ERR_FILE_READ;
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

struct ParquetColumnInstrLive
{
  std::atomic<uint64_t> dictRows{0};
  std::atomic<uint64_t> dictNulls{0};
  std::atomic<uint64_t> dictChunkDistinctSum{0};
  std::atomic<uint64_t> dictChunks{0};
  std::atomic<uint64_t> dictDctnryCalls{0};
  std::atomic<uint64_t> dictCanonHits{0};
  std::atomic<uint64_t> temporalArrowFast{0};
  std::atomic<uint64_t> temporalScalarFallback{0};
  std::atomic<uint64_t> colFixedNs{0};
  std::atomic<uint64_t> colDictNs{0};
  std::atomic<uint64_t> colFixedCalls{0};
  std::atomic<uint64_t> colDictCalls{0};
};

/** Howard Hinnant civil-from-days; z = days since 1970-01-01 (Unix). */
static void civilFromUnixEpochDays(int32_t z, int& y, int& m, int& d)
{
  const int zz = static_cast<int>(z) + 719468;
  const int era = (zz >= 0 ? zz : zz - 146096) / 146097;
  const unsigned doe = static_cast<unsigned>(zz - era * 146097);
  const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  y = static_cast<int>(yoe) + era * 400;
  const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const unsigned mp = (5 * doy + 2) / 153;
  d = static_cast<int>(doy - (153 * mp + 2) / 5 + 1);
  m = static_cast<int>(mp) + (mp < 10u ? 3 : -9);
  if (m <= 2)
    ++y;
}

static int64_t arrowTimestampToNanos(int64_t v, arrow::TimeUnit::type unit)
{
  switch (unit)
  {
    case arrow::TimeUnit::NANO:
      return v;
    case arrow::TimeUnit::MICRO:
      return v * 1000LL;
    case arrow::TimeUnit::MILLI:
      return v * 1000000LL;
    case arrow::TimeUnit::SECOND:
      return v * 1000000000LL;
    default:
      return v;
  }
}

static void divmodNanos(int64_t nanos, int64_t& sec, int64_t& usec)
{
  const int64_t div = 1000000000LL;
  sec = nanos / div;
  int64_t rem = nanos % div;
  if (rem < 0)
  {
    rem += div;
    sec -= 1;
  }
  usec = rem / 1000;
}

/**
 * Follow one level of Arrow dictionary to a leaf array index (physical row).
 * @return false on structural error (errMsg set); true with logicalNull if value is null.
 */
static bool resolveDictionaryLeafOneLevel(const std::shared_ptr<arrow::Array>& root, int64_t logicalRow,
                                          const arrow::Array*& outLeaf, int64_t& outIndex, bool& logicalNull,
                                          std::string& errMsg)
{
  logicalNull = false;
  if (!root)
  {
    errMsg = "Arrow array pointer is null";
    return false;
  }
  if (root->IsNull(logicalRow))
  {
    logicalNull = true;
    outLeaf = nullptr;
    outIndex = 0;
    return true;
  }
  const arrow::Array* cur = root.get();
  int64_t idx = logicalRow;
  if (cur->type_id() == arrow::Type::DICTIONARY)
  {
    const auto* dictArr = static_cast<const arrow::DictionaryArray*>(cur);
    const std::shared_ptr<arrow::Array> indices = dictArr->indices();
    const std::shared_ptr<arrow::Array> dictionary = dictArr->dictionary();
    if (!indices || !dictionary)
    {
      errMsg = "Arrow dictionary column has null indices or dictionary array";
      return false;
    }
    int64_t dictSlot = 0;
    if (readDictionaryIndexSlot(indices, idx, dictSlot, errMsg) != NO_ERROR)
      return false;
    if (dictSlot < 0 || dictSlot >= dictionary->length())
    {
      errMsg = "Arrow dictionary index out of range";
      return false;
    }
    if (dictionary->IsNull(dictSlot))
    {
      logicalNull = true;
      outLeaf = nullptr;
      outIndex = 0;
      return true;
    }
    cur = dictionary.get();
    idx = dictSlot;
  }
  outLeaf = cur;
  outIndex = idx;
  return true;
}

/**
 * Fast path: Arrow DATE32 / TIMESTAMP (incl. dictionary-wrapped) -> ColumnStore DATE / DATETIME / TIMESTAMP
 * binary layout matching BulkLoadBuffer text-mode output (no Arrow Scalar::ToString).
 * DATETIME uses UTC civil fields from the instant (Arrow/Parquet epoch semantics).
 */
static bool tryConvertArrowTemporalToBinary(const std::shared_ptr<arrow::Array>& sourceArray, int64_t batchRow,
                                            const JobColumn& column, BLBufferStats& bufferStats, unsigned char* output)
{
  using DT = execplan::CalpontSystemCatalog::ColDataType;
  if (!sourceArray)
    return false;

  const arrow::Array* leaf = nullptr;
  int64_t ix = 0;
  bool logicalNull = false;
  std::string errTmp;
  if (!resolveDictionaryLeafOneLevel(sourceArray, batchRow, leaf, ix, logicalNull, errTmp))
    return false;
  if (logicalNull || !leaf)
    return false;
  if (leaf->IsNull(ix))
    return false;

  switch (leaf->type_id())
  {
    case arrow::Type::DATE32:
    {
      if (column.dataType != DT::DATE || column.weType != WriteEngine::WR_INT)
        return false;
      const auto* da = static_cast<const arrow::Date32Array*>(leaf);
      const int32_t days = da->Value(ix);
      int y = 0;
      int mo = 0;
      int day = 0;
      civilFromUnixEpochDays(days, y, mo, day);
      if (!dataconvert::isDateValid(day, mo, y))
      {
        const int32_t zero = 0;
        memcpy(output, &zero, sizeof(zero));
        bufferStats.satCount++;
        return true;
      }
      dataconvert::Date cal(static_cast<unsigned>(y), static_cast<unsigned>(mo), static_cast<unsigned>(day));
      int32_t iDate = 0;
      memcpy(&iDate, &cal, sizeof(iDate));
      if (!dataconvert::DataConvert::isColumnDateValid(iDate))
      {
        const int32_t zero = 0;
        memcpy(output, &zero, sizeof(zero));
        bufferStats.satCount++;
        return true;
      }
      memcpy(output, &iDate, sizeof(iDate));
      if (iDate < bufferStats.minBufferVal)
        bufferStats.minBufferVal = iDate;
      if (iDate > bufferStats.maxBufferVal)
        bufferStats.maxBufferVal = iDate;
      return true;
    }
    case arrow::Type::TIMESTAMP:
    {
      if (column.dataType != DT::DATETIME && column.dataType != DT::TIMESTAMP)
        return false;
      if (column.weType != WriteEngine::WR_LONGLONG)
        return false;
      const auto* ta = static_cast<const arrow::TimestampArray*>(leaf);
      const auto tsType = std::static_pointer_cast<arrow::TimestampType>(leaf->type());
      if (!tsType)
        return false;
      const int64_t nanos = arrowTimestampToNanos(ta->Value(ix), tsType->unit());
      int64_t sec = 0;
      int64_t usec = 0;
      divmodNanos(nanos, sec, usec);
      if (usec < 0 || usec > 999999)
        return false;

      if (column.dataType == DT::TIMESTAMP)
      {
        if (sec < 0)
          return false;
        if (!dataconvert::isTimestampValid(static_cast<uint64_t>(sec), static_cast<uint64_t>(usec)))
        {
          int64_t zero = 0;
          memcpy(output, &zero, sizeof(zero));
          bufferStats.satCount++;
          return true;
        }
        dataconvert::TimeStamp ts(static_cast<unsigned>(usec), static_cast<unsigned long long>(sec));
        int64_t packed = 0;
        memcpy(&packed, &ts, sizeof(packed));
        if (!dataconvert::DataConvert::isColumnTimeStampValid(packed))
        {
          packed = 0;
          memcpy(output, &packed, sizeof(packed));
          bufferStats.satCount++;
          return true;
        }
        memcpy(output, &packed, sizeof(packed));
        if (packed < bufferStats.minBufferVal)
          bufferStats.minBufferVal = packed;
        if (packed > bufferStats.maxBufferVal)
          bufferStats.maxBufferVal = packed;
        return true;
      }

      // DATETIME: UTC civil from unix seconds (instant semantics).
      if (sec < std::numeric_limits<time_t>::min() || sec > std::numeric_limits<time_t>::max())
        return false;
      time_t tt = static_cast<time_t>(sec);
      struct tm tmBuf
      {
      };
      if (!gmtime_r(&tt, &tmBuf))
        return false;
      const int y = tmBuf.tm_year + 1900;
      const unsigned mo = static_cast<unsigned>(tmBuf.tm_mon + 1);
      const unsigned day = static_cast<unsigned>(tmBuf.tm_mday);
      if (!dataconvert::isDateValid(static_cast<int>(day), static_cast<int>(mo), y) ||
          !dataconvert::isDateTimeValid(tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, static_cast<int>(usec)))
      {
        int64_t zero = 0;
        memcpy(output, &zero, sizeof(zero));
        bufferStats.satCount++;
        return true;
      }
      dataconvert::DateTime dt(static_cast<unsigned>(y), mo, day, static_cast<unsigned>(tmBuf.tm_hour),
                               static_cast<unsigned>(tmBuf.tm_min), static_cast<unsigned>(tmBuf.tm_sec),
                               static_cast<unsigned>(usec));
      int64_t packed = 0;
      memcpy(&packed, &dt, sizeof(packed));
      if (!dataconvert::DataConvert::isColumnDateTimeValid(packed))
      {
        packed = 0;
        memcpy(output, &packed, sizeof(packed));
        bufferStats.satCount++;
        return true;
      }
      memcpy(output, &packed, sizeof(packed));
      if (packed < bufferStats.minBufferVal)
        bufferStats.minBufferVal = packed;
      if (packed > bufferStats.maxBufferVal)
        bufferStats.maxBufferVal = packed;
      return true;
    }
    default:
      return false;
  }
}

struct StringViewHash
{
  size_t operator()(std::string_view sv) const noexcept
  {
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : sv)
      h = (h ^ static_cast<uint64_t>(c)) * 1099511628211ULL;
    return static_cast<size_t>(h);
  }
};

/** Remap duplicate string payloads in a chunk to the first ColPosPair (reduces Dctnry signature lookups). */
static void dedupeDictionaryStringsInChunk(std::vector<char>& dataBuffer, std::vector<ColPosPair*>& rowPtrs,
                                           uint32_t nRows, int colId, ParquetColumnInstrLive* instr,
                                           size_t instrCols)
{
  if (nRows == 0 || dataBuffer.empty())
    return;
  std::unordered_map<std::string_view, ColPosPair, StringViewHash, std::equal_to<>> canon;
  canon.reserve(static_cast<size_t>(nRows) * 2 + 1);
  uint64_t hits = 0;
  for (uint32_t r = 0; r < nRows; ++r)
  {
    ColPosPair& pos = rowPtrs[r][colId];
    if (pos.offset == 0)
      continue;
    const size_t end = static_cast<size_t>(pos.start) + static_cast<size_t>(pos.offset);
    if (end > dataBuffer.size())
      continue;
    const std::string_view key(dataBuffer.data() + pos.start, pos.offset);
    const auto [it, inserted] = canon.emplace(key, pos);
    if (!inserted)
    {
      pos = it->second;
      ++hits;
    }
  }
  if (instr && colId >= 0 && static_cast<size_t>(colId) < instrCols)
  {
    instr[colId].dictCanonHits.fetch_add(hits, std::memory_order_relaxed);
    instr[colId].dictChunkDistinctSum.fetch_add(canon.size(), std::memory_order_relaxed);
    instr[colId].dictChunks.fetch_add(1, std::memory_order_relaxed);
  }
}

int processFixedColumnBatch(const DirectColumnBinding& binding, const std::shared_ptr<arrow::RecordBatch>& batch,
                            RID batchStartRow, BulkLoadBuffer& converter, std::string& errMsg,
                            ParquetColumnInstrLive* instr, size_t instrCols)
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
  using DT = execplan::CalpontSystemCatalog::ColDataType;
  const bool colIsTemporal = columnInfo.column.dataType == DT::DATE ||
                             columnInfo.column.dataType == DT::DATETIME ||
                             columnInfo.column.dataType == DT::TIMESTAMP;
  const int instrColId = columnInfo.id;

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
        bool filled = enableDirectBinaryFastPath &&
                      tryConvertFieldDirectBinary(sourceArray, batchRow, columnInfo.column, bufferStats,
                                                  outputBuffer.data() +
                                                      static_cast<size_t>(row) * columnInfo.column.width);
        if (!filled)
        {
          const bool temporalOk = tryConvertArrowTemporalToBinary(
              sourceArray, batchRow, columnInfo.column, bufferStats,
              outputBuffer.data() + static_cast<size_t>(row) * columnInfo.column.width);
          if (temporalOk)
          {
            filled = true;
            if (colIsTemporal && instr && instrColId >= 0 && static_cast<size_t>(instrColId) < instrCols)
              instr[instrColId].temporalArrowFast.fetch_add(1, std::memory_order_relaxed);
          }
        }
        if (!filled)
        {
          if (colIsTemporal && instr && instrColId >= 0 && static_cast<size_t>(instrColId) < instrCols)
            instr[instrColId].temporalScalarFallback.fetch_add(1, std::memory_order_relaxed);
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

int readDictionaryIndexSlot(const std::shared_ptr<arrow::Array>& indices, int64_t row, int64_t& outSlot,
                            std::string& errMsg)
{
  using arrow::Type;
  switch (indices->type_id())
  {
    case Type::INT8:
      outSlot = std::static_pointer_cast<arrow::Int8Array>(indices)->Value(row);
      return NO_ERROR;
    case Type::UINT8:
      outSlot = static_cast<int64_t>(std::static_pointer_cast<arrow::UInt8Array>(indices)->Value(row));
      return NO_ERROR;
    case Type::INT16:
      outSlot = std::static_pointer_cast<arrow::Int16Array>(indices)->Value(row);
      return NO_ERROR;
    case Type::UINT16:
      outSlot = static_cast<int64_t>(std::static_pointer_cast<arrow::UInt16Array>(indices)->Value(row));
      return NO_ERROR;
    case Type::INT32:
      outSlot = std::static_pointer_cast<arrow::Int32Array>(indices)->Value(row);
      return NO_ERROR;
    case Type::UINT32:
      outSlot = static_cast<int64_t>(std::static_pointer_cast<arrow::UInt32Array>(indices)->Value(row));
      return NO_ERROR;
    case Type::INT64:
      outSlot = std::static_pointer_cast<arrow::Int64Array>(indices)->Value(row);
      return NO_ERROR;
    case Type::UINT64:
      outSlot = static_cast<int64_t>(std::static_pointer_cast<arrow::UInt64Array>(indices)->Value(row));
      return NO_ERROR;
    default:
      errMsg = "Unsupported Arrow dictionary index array type";
      return ERR_INVALID_PARAM;
  }
}

/** Append one dictionary-column cell; uses getValueAsString (typed fast path + scalar fallback). */
int appendDictionaryCellFromArrow(const std::shared_ptr<arrow::Array>& array, int64_t row, bool& cellNull,
                                  std::vector<char>& dataBuffer, ColPosPair& pos, std::string& fallbackStorage,
                                  const std::string& columnNameForErr, std::string& errMsg)
{
  cellNull = false;
  if (!array)
  {
    errMsg = "Arrow array pointer is null";
    return ERR_INVALID_PARAM;
  }

  auto appendView = [&](std::string_view v) -> int {
    if (v.size() > static_cast<size_t>(MAX_FIELD_SIZE))
    {
      std::ostringstream oss;
      oss << "Parquet value exceeds max field size for dictionary column '" << columnNameForErr << "'";
      errMsg = oss.str();
      return ERR_BULK_ROW_FILL_BUFFER;
    }
    pos.start = static_cast<uint32_t>(dataBuffer.size());
    pos.offset = static_cast<uint32_t>(v.size());
    dataBuffer.insert(dataBuffer.end(), v.begin(), v.end());
    return NO_ERROR;
  };

  const int valueRc = getValueAsString(array, row, cellNull, fallbackStorage, errMsg);
  if (valueRc != NO_ERROR)
    return valueRc;
  if (cellNull)
    return NO_ERROR;
  return appendView(std::string_view(fallbackStorage.data(), fallbackStorage.size()));
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

  std::string fallbackScalarText;
  const std::string& colName = binding.columnInfo->column.colName;
  for (uint32_t row = 0; row < nRows; ++row)
  {
    auto& pos = rowPtrs[row][binding.columnInfo->id];
    bool cellNull = (binding.schemaIndex < 0);
    if (!cellNull)
    {
      const int valueRc = appendDictionaryCellFromArrow(sourceArray, static_cast<int64_t>(startOffset + row), cellNull,
                                                        dataBuffer, pos, fallbackScalarText, colName, errMsg);
      if (valueRc != NO_ERROR)
        return valueRc;
    }

    if (cellNull)
    {
      pos.start = 0;
      pos.offset = 0;
      continue;
    }
  }

  return NO_ERROR;
}

int processDictionaryColumnBatch(const DirectColumnBinding& binding, const std::shared_ptr<arrow::RecordBatch>& batch,
                                 RID batchStartRow, size_t totalColumns, std::string& errMsg,
                                 ParquetColumnInstrLive* instr, size_t instrCols)
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

    const int dictColId = binding.columnInfo->id;
    if (instr && dictColId >= 0 && static_cast<size_t>(dictColId) < instrCols)
    {
      instr[dictColId].dictRows.fetch_add(nRowsParsed, std::memory_order_relaxed);
      uint64_t nullsInChunk = 0;
      for (uint32_t r = 0; r < nRowsParsed; ++r)
      {
        const ColPosPair& p = rowPtrs[r][dictColId];
        if (p.start == 0 && p.offset == 0)
          ++nullsInChunk;
      }
      instr[dictColId].dictNulls.fetch_add(nullsInChunk, std::memory_order_relaxed);
    }
    if (gImportRuntimeConfig.dictChunkDedupe)
      dedupeDictionaryStringsInChunk(dataBuffer, rowPtrs, nRowsParsed, dictColId, instr, instrCols);
    if (instr && dictColId >= 0 && static_cast<size_t>(dictColId) < instrCols)
      instr[dictColId].dictDctnryCalls.fetch_add(1, std::memory_order_relaxed);

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
  gImportRuntimeConfig.dictChunkDedupe = cfg.dictChunkDedupe;
  gImportRuntimeConfig.arrowReaderUseThreads = cfg.arrowReaderUseThreads;
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
  result.dictChunkDedupeEnabled = gImportRuntimeConfig.dictChunkDedupe;

  const char* parquetInstrEnv = std::getenv("COLUMNSTORE_PARQUET_IMPORT_INSTR");
  const bool enablePipelineInstr =
      parquetInstrEnv && parquetInstrEnv[0] != '\0' && parquetInstrEnv[0] != '0';

  const auto start = std::chrono::steady_clock::now();
  ParquetWallClockInstrumentation wallClock;
  const PipelineClock::time_point totalWallStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
  PipelineClock::time_point setupStart;
  if (enablePipelineInstr)
    setupStart = PipelineClock::now();

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
    for (const auto& ci : tableInfo.directImportColumns())
      result.columnNames.push_back(ci.column.colName);
    result.columnInstrumentation.assign(result.columnNames.size(), ParquetColumnInstrSnapshot{});
    tableInfo.prepareDirectImportCompletion(0);
    rc = tableInfo.finalizeDirectImport(errMsg);
    if (enablePipelineInstr)
    {
      wallClock.setupNs = pipelineNsSince(setupStart);
      wallClock.totalNs = pipelineNsSince(totalWallStart);
      copyWallClockInstrToResult(wallClock, result);
    }
    if (rc != NO_ERROR)
      return rc;
    const auto end = std::chrono::steady_clock::now();
    result.stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();
    return NO_ERROR;
  }

  std::unique_ptr<ParquetPipelineInstrumentation> pipelineInstrStorage;
  ParquetPipelineInstrumentation* pipeInstr = nullptr;
  if (enablePipelineInstr)
  {
    pipelineInstrStorage.reset(new ParquetPipelineInstrumentation());
    pipeInstr = pipelineInstrStorage.get();
  }

  JobFieldRefList emptyFieldRefList;
  int columnWriterCount = std::max(1, gImportRuntimeConfig.columnWriteThreads);
  columnWriterCount = std::min(columnWriterCount, static_cast<int>(bindings.size()));
  std::vector<std::vector<size_t>> bindingsPerWriter;
  partitionParquetBindingsToWriters(bindings, columnWriterCount, bindingsPerWriter);
  rc = validatePartitionParquetBindings(bindings, columnWriterCount, bindingsPerWriter, errMsg);
  if (rc != NO_ERROR)
  {
    if (pipeInstr)
      copyPipelineInstrToResult(*pipeInstr, result);
    if (enablePipelineInstr)
    {
      wallClock.setupNs = pipelineNsSince(setupStart);
      wallClock.totalNs = pipelineNsSince(totalWallStart);
      copyWallClockInstrToResult(wallClock, result);
    }
    return rc;
  }

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

  result.columnNames.clear();
  result.columnNames.reserve(totalColumns);
  for (const auto& ci : tableInfo.directImportColumns())
    result.columnNames.push_back(ci.column.colName);
  std::unique_ptr<ParquetColumnInstrLive[]> instrLive;
  if (totalColumns > 0)
    instrLive.reset(new ParquetColumnInstrLive[totalColumns]);
  ParquetColumnInstrLive* const parquetInstrPtr = instrLive.get();

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
  if (pipeInstr)
    queue.attachPipelineInstr(pipeInstr);
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
    {
      if (pipeInstr)
        copyPipelineInstrToResult(*pipeInstr, result);
      if (enablePipelineInstr)
      {
        wallClock.setupNs = pipelineNsSince(setupStart);
        wallClock.totalNs = pipelineNsSince(totalWallStart);
        copyWallClockInstrToResult(wallClock, result);
      }
      return rc;
    }
  }

  if (enablePipelineInstr)
    wallClock.setupNs = pipelineNsSince(setupStart);

  const PipelineClock::time_point startWritersStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
  std::vector<std::thread> columnWriters;
  columnWriters.reserve(static_cast<size_t>(columnWriterCount));
  for (int w = 0; w < columnWriterCount; ++w)
  {
    columnWriters.emplace_back([&, w]() {
      while (true)
      {
        ColumnTask task;
        const auto popStart = PipelineClock::now();
        const bool popped = columnWriterQueues[static_cast<size_t>(w)].pop(task, stopRequested);
        if (pipeInstr)
        {
          pipelineAddNs(pipeInstr->writerQueuePopWaitNs, pipelineNsSince(popStart));
          if (popped)
            pipeInstr->writerQueuePopCount.fetch_add(1, std::memory_order_relaxed);
        }
        if (!popped)
          break;
        const auto taskStart = PipelineClock::now();
        std::string localErr;
        int taskRc = NO_ERROR;
        for (size_t bi : task.bindingIndices)
        {
          const auto& binding = bindings[bi];
          if (binding.columnInfo->column.colType == COL_TYPE_DICT)
          {
            const auto colT0 = PipelineClock::now();
            taskRc = processDictionaryColumnBatch(binding, task.batch, task.batchStartRow, totalColumns, localErr,
                                                  parquetInstrPtr, totalColumns);
            if (pipeInstr)
            {
              const uint64_t dt = pipelineNsSince(colT0);
              pipelineAddNs(pipeInstr->dictionaryColumnNs, dt);
              pipeInstr->dictionaryColumnCalls.fetch_add(1, std::memory_order_relaxed);
              const int cid = binding.columnInfo->id;
              if (cid >= 0 && static_cast<size_t>(cid) < totalColumns && parquetInstrPtr)
              {
                pipelineAddNs(parquetInstrPtr[cid].colDictNs, dt);
                parquetInstrPtr[cid].colDictCalls.fetch_add(1, std::memory_order_relaxed);
              }
            }
          }
          else
          {
            const auto colT0 = PipelineClock::now();
            taskRc = processFixedColumnBatch(binding, task.batch, task.batchStartRow,
                                             *columnConverters[static_cast<size_t>(w)], localErr, parquetInstrPtr,
                                             totalColumns);
            if (pipeInstr)
            {
              const uint64_t dt = pipelineNsSince(colT0);
              pipelineAddNs(pipeInstr->fixedColumnNs, dt);
              pipeInstr->fixedColumnCalls.fetch_add(1, std::memory_order_relaxed);
              const int cid = binding.columnInfo->id;
              if (cid >= 0 && static_cast<size_t>(cid) < totalColumns && parquetInstrPtr)
              {
                pipelineAddNs(parquetInstrPtr[cid].colFixedNs, dt);
                parquetInstrPtr[cid].colFixedCalls.fetch_add(1, std::memory_order_relaxed);
              }
            }
          }
          if (taskRc != NO_ERROR)
            break;
        }
        if (pipeInstr)
        {
          pipelineAddNs(pipeInstr->writerTaskProcessNs, pipelineNsSince(taskStart));
          pipeInstr->writerTasks.fetch_add(1, std::memory_order_relaxed);
        }
        task.completion->notifyWorkerDone(taskRc, localErr, stopRequested);
      }
    });
  }
  if (enablePipelineInstr)
    wallClock.startWritersNs = pipelineNsSince(startWritersStart);

  const PipelineClock::time_point startReadersStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
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
              const auto decodeStart = PipelineClock::now();
              const arrow::Status st = batchReader->ReadNext(&batch);
              if (pipeInstr)
                pipelineAddNs(pipeInstr->readerDecodeNs, pipelineNsSince(decodeStart));
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
              if (pipeInstr)
              {
                pipeInstr->readerBatches.fetch_add(1, std::memory_order_relaxed);
                pipeInstr->readerRows.fetch_add(static_cast<uint64_t>(batch->num_rows()),
                                                 std::memory_order_relaxed);
              }
              BatchEnvelope env;
              env.globalStartRow = workerRowCursor;
              env.numRows = batch->num_rows();
              env.estimatedBytes = estimateBatchBytes(batch);
              env.batch = std::move(batch);
              if (pipeInstr)
                env.queuedAt = PipelineClock::now();
              workerRowCursor += static_cast<RID>(env.numRows);
              const auto pushStart = PipelineClock::now();
              const bool pushed = queue.push(std::move(env), stopRequested);
              if (pipeInstr)
              {
                pipelineAddNs(pipeInstr->readerPushWaitNs, pipelineNsSince(pushStart));
                if (pushed)
                  pipeInstr->readerPushCount.fetch_add(1, std::memory_order_relaxed);
              }
              if (!pushed)
                break;
            }
          }
        }
      }

      if (activeProducers.fetch_sub(1) == 1)
        queue.close();
    });
  }
  if (enablePipelineInstr)
    wallClock.startReadersNs = pipelineNsSince(startReadersStart);

  rc = NO_ERROR;
  RID expectedStartRow = 0;
  int64_t rowsDispatched = 0;
  std::map<RID, BatchEnvelope> reorder;

  const PipelineClock::time_point coordinatorStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};

  auto dispatchOrderedBatch = [&](BatchEnvelope& current, RID batchStart) {
    int taskCount = 0;
    for (int w = 0; w < columnWriterCount; ++w)
    {
      if (!bindingsPerWriter[static_cast<size_t>(w)].empty())
        ++taskCount;
    }
    if (taskCount == 0 || current.numRows <= 0)
      return;
    if (pipeInstr)
    {
      pipeInstr->coordinatorDispatchedBatches.fetch_add(1, std::memory_order_relaxed);
      pipeInstr->coordinatorDispatchedTasks.fetch_add(static_cast<uint64_t>(taskCount),
                                                       std::memory_order_relaxed);
    }
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
    batchesInflight.fetch_add(1, std::memory_order_relaxed);
    if (pipeInstr)
    {
      pipelineUpdateMaxAtomic(pipeInstr->maxInflightBatchesObserved,
                              static_cast<uint64_t>(batchesInflight.load(std::memory_order_relaxed)));
    }
  };

  while (rowsDispatched < totalRowsFromMeta && !stopRequested.load())
  {
    BatchEnvelope env;
    const auto coordPopStart = PipelineClock::now();
    const bool gotBatch = queue.pop(env, stopRequested);
    if (pipeInstr)
    {
      pipelineAddNs(pipeInstr->coordinatorPopWaitNs, pipelineNsSince(coordPopStart));
      if (gotBatch)
        pipeInstr->coordinatorPopCount.fetch_add(1, std::memory_order_relaxed);
    }
    if (!gotBatch)
    {
      if (activeProducers.load() == 0)
        break;
      continue;
    }
    if (pipeInstr && env.globalStartRow != expectedStartRow)
    {
      env.heldForReorder = true;
      env.reorderInsertedAt = PipelineClock::now();
    }
    reorder.emplace(env.globalStartRow, std::move(env));

    while (!stopRequested.load())
    {
      auto it = reorder.find(expectedStartRow);
      if (it == reorder.end())
        break;
      {
        std::unique_lock<std::mutex> lk(drainMutex);
        while (batchesInflight.load(std::memory_order_acquire) >= maxBatchesInflight && !stopRequested.load())
        {
          if (pipeInstr)
            pipeInstr->coordinatorInflightWaitCount.fetch_add(1, std::memory_order_relaxed);
          const auto inflightWait0 = PipelineClock::now();
          drainCv.wait(lk, [&] {
            return batchesInflight.load(std::memory_order_acquire) < maxBatchesInflight || stopRequested.load();
          });
          if (pipeInstr)
            pipelineAddNs(pipeInstr->coordinatorInflightWaitNs, pipelineNsSince(inflightWait0));
        }
        if (pipeInstr)
        {
          pipelineUpdateMaxAtomic(pipeInstr->maxInflightBatchesObserved,
                                  static_cast<uint64_t>(batchesInflight.load(std::memory_order_relaxed)));
        }
      }
      if (stopRequested.load())
        break;
      BatchEnvelope current = std::move(it->second);
      reorder.erase(it);
      const RID batchStart = expectedStartRow;
      if (pipeInstr)
      {
        pipelineAddNs(pipeInstr->batchPreDispatchResidenceNs, pipelineNsSince(current.queuedAt));
        if (current.heldForReorder)
        {
          pipelineAddNs(pipeInstr->trueReorderHoldNs, pipelineNsSince(current.reorderInsertedAt));
          pipeInstr->trueReorderHeldBatches.fetch_add(1, std::memory_order_relaxed);
        }
      }
      dispatchOrderedBatch(current, batchStart);
      expectedStartRow += static_cast<RID>(current.numRows);
      rowsDispatched += current.numRows;
    }
  }

  if (enablePipelineInstr)
    wallClock.coordinatorLoopNs = pipelineNsSince(coordinatorStart);

  queue.close();
  const PipelineClock::time_point readerJoinStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
  for (auto& worker : workers)
  {
    if (worker.joinable())
      worker.join();
  }
  if (enablePipelineInstr)
    wallClock.readerJoinNs = pipelineNsSince(readerJoinStart);

  const PipelineClock::time_point drainStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
  {
    std::unique_lock<std::mutex> lk(drainMutex);
    drainCv.wait(lk, [&] { return batchesInflight.load() == 0 || stopRequested.load(); });
  }
  if (enablePipelineInstr)
    wallClock.writerDrainNs = pipelineNsSince(drainStart);

  const PipelineClock::time_point writerJoinStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};
  for (auto& cwq : columnWriterQueues)
    cwq.close();
  for (auto& cw : columnWriters)
  {
    if (cw.joinable())
      cw.join();
  }
  if (enablePipelineInstr)
    wallClock.writerJoinNs = pipelineNsSince(writerJoinStart);

  if (instrLive)
  {
    result.columnInstrumentation.resize(totalColumns);
    for (size_t i = 0; i < totalColumns; ++i)
    {
      result.columnInstrumentation[i].dictRows = instrLive[i].dictRows.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictNulls = instrLive[i].dictNulls.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictChunkDistinctSum =
          instrLive[i].dictChunkDistinctSum.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictChunks = instrLive[i].dictChunks.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictDctnryCalls = instrLive[i].dictDctnryCalls.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictCanonHits = instrLive[i].dictCanonHits.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].temporalArrowFast =
          instrLive[i].temporalArrowFast.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].temporalScalarFallback =
          instrLive[i].temporalScalarFallback.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].fixedColumnNs = instrLive[i].colFixedNs.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictionaryColumnNs = instrLive[i].colDictNs.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].fixedColumnCalls = instrLive[i].colFixedCalls.load(std::memory_order_relaxed);
      result.columnInstrumentation[i].dictionaryColumnCalls = instrLive[i].colDictCalls.load(std::memory_order_relaxed);
    }
  }

  if (pipeInstr)
    copyPipelineInstrToResult(*pipeInstr, result);

  const PipelineClock::time_point finalizeStart =
      enablePipelineInstr ? PipelineClock::now() : PipelineClock::time_point{};

  if (!sharedErrMsg.empty() && rc == NO_ERROR)
  {
    if (enablePipelineInstr)
    {
      wallClock.finalizeNs = pipelineNsSince(finalizeStart);
      wallClock.totalNs = pipelineNsSince(totalWallStart);
      copyWallClockInstrToResult(wallClock, result);
    }
    errMsg = sharedErrMsg;
    return ERR_FILE_READ;
  }
  if (rc != NO_ERROR)
  {
    if (enablePipelineInstr)
    {
      wallClock.finalizeNs = pipelineNsSince(finalizeStart);
      wallClock.totalNs = pipelineNsSince(totalWallStart);
      copyWallClockInstrToResult(wallClock, result);
    }
    return rc;
  }
  if (result.stats.totalRows != totalRowsFromMeta)
  {
    if (enablePipelineInstr)
    {
      wallClock.finalizeNs = pipelineNsSince(finalizeStart);
      wallClock.totalNs = pipelineNsSince(totalWallStart);
      copyWallClockInstrToResult(wallClock, result);
    }
    std::ostringstream oss;
    oss << "Parquet import ended early: processed " << result.stats.totalRows << " of " << totalRowsFromMeta << " rows";
    errMsg = oss.str();
    return ERR_FILE_READ;
  }

  tableInfo.prepareDirectImportCompletion(static_cast<RID>(result.stats.totalRows));
  rc = tableInfo.finalizeDirectImport(errMsg);
  if (enablePipelineInstr)
  {
    wallClock.finalizeNs = pipelineNsSince(finalizeStart);
    wallClock.totalNs = pipelineNsSince(totalWallStart);
    copyWallClockInstrToResult(wallClock, result);
  }
  if (rc != NO_ERROR)
    return rc;

  const auto end = std::chrono::steady_clock::now();
  result.stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();
  return NO_ERROR;
}

}  // namespace WriteEngine
