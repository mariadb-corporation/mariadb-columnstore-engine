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
#include <cstring>
#include <fstream>
#include <memory>
#include <numeric>
#include <sstream>
#include <algorithm>
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

int makeDirectImportRecordBatchReader(parquet::arrow::FileReader& reader,
                                      const std::shared_ptr<arrow::Schema>& schema,
                                      std::vector<DirectColumnBinding>& bindings,
                                      std::shared_ptr<arrow::RecordBatchReader>& batchReader, std::string& errMsg)
{
  std::vector<int> colIndices;
  for (const auto& b : bindings)
  {
    if (b.schemaIndex >= 0)
      colIndices.push_back(b.schemaIndex);
  }
  std::sort(colIndices.begin(), colIndices.end());
  colIndices.erase(std::unique(colIndices.begin(), colIndices.end()), colIndices.end());

  const int schemaFieldCount = schema->num_fields();
  const bool useSubset =
      !colIndices.empty() && static_cast<int>(colIndices.size()) < schemaFieldCount;

  if (useSubset)
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

    const int nrg = reader.num_row_groups();
    std::vector<int> rowGroups(static_cast<size_t>(std::max(0, nrg)));
    std::iota(rowGroups.begin(), rowGroups.end(), 0);
    const arrow::Status st = reader.GetRecordBatchReader(rowGroups, colIndices, &batchReader);
    if (!st.ok())
      return setArrowError("Unable to create parquet record batch reader", st, errMsg);
  }
  else
  {
    for (auto& b : bindings)
      b.recordBatchColumnIndex = b.schemaIndex;

    const arrow::Status st = reader.GetRecordBatchReader(&batchReader);
    if (!st.ok())
      return setArrowError("Unable to create parquet record batch reader", st, errMsg);
  }

  return NO_ERROR;
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
      }

      converter.convertField(fieldBuffer, fieldLength, nullFlag,
                             outputBuffer.data() + static_cast<size_t>(row) * columnInfo.column.width,
                             columnInfo.column, bufferStats);
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
      columnInfo.incSaturatedCnt(bufferStats.satCount);

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

  std::shared_ptr<arrow::RecordBatchReader> batchReader;
  rc = makeDirectImportRecordBatchReader(*reader, schema, bindings, batchReader, errMsg);
  if (rc != NO_ERROR)
    return rc;

  JobFieldRefList emptyFieldRefList;
  BulkLoadBuffer converter(static_cast<unsigned>(tableInfo.directImportColumns().size()), 4096, nullptr, 0,
                           tableInfo.directImportTableName(), emptyFieldRefList);
  converter.setImportDataMode(IMPORT_DATA_TEXT, 0);
  converter.setTimeZone(tableInfo.getTimeZone());

  for (const auto& binding : bindings)
  {
    rc = binding.columnInfo->createDelayedFileIfNeeded(tableInfo.directImportTableName());
    if (rc != NO_ERROR)
      return rc;
  }

  result.stats.rowGroupCount = reader->num_row_groups();
  result.stats.columnCount = schema ? schema->num_fields() : 0;
  RID nextBatchStartRow = 0;
  const size_t totalColumns = tableInfo.directImportColumns().size();

  while (true)
  {
    std::shared_ptr<arrow::RecordBatch> batch;
    const arrow::Status batchStatus = batchReader->ReadNext(&batch);
    if (!batchStatus.ok())
      return setArrowError("Unable to read parquet record batch", batchStatus, errMsg);
    if (!batch)
      break;

    const int64_t rowsInBatch = batch->num_rows();
    for (const auto& binding : bindings)
    {
      if (binding.columnInfo->column.colType == COL_TYPE_DICT)
      {
        rc = processDictionaryColumnBatch(binding, batch, nextBatchStartRow, totalColumns, errMsg);
      }
      else
      {
        rc = processFixedColumnBatch(binding, batch, nextBatchStartRow, converter, errMsg);
      }
      if (rc != NO_ERROR)
        return rc;
    }

    nextBatchStartRow += rowsInBatch;
    result.stats.batchCount++;
    result.stats.totalRows += rowsInBatch;
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
