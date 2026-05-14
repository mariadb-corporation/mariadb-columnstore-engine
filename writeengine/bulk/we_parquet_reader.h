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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace WriteEngine
{
class TableInfo;

struct ParquetReadStats
{
  int64_t totalRows{0};
  int64_t batchCount{0};
  int64_t rowGroupCount{0};
  int64_t columnCount{0};
  double elapsedSeconds{0.0};
};

struct ParquetColumnMapping
{
  std::string name;
  std::string arrowType;
  std::string conversion;
};

/** Per-column counters merged after direct parquet import (see COLUMNSTORE_PARQUET_IMPORT_INSTR). */
struct ParquetColumnInstrSnapshot
{
  uint64_t dictRows{0};
  uint64_t dictNulls{0};
  /** Sum of distinct string values per dictionary chunk (divide by dictChunks for average). */
  uint64_t dictChunkDistinctSum{0};
  uint64_t dictChunks{0};
  uint64_t dictDctnryCalls{0};
  /** Rows remapped to an earlier identical string in the same chunk (token lookup savings). */
  uint64_t dictCanonHits{0};
  uint64_t temporalArrowFast{0};
  uint64_t temporalScalarFallback{0};
  /** Writer-side time in processFixedColumnBatch (nanoseconds). */
  uint64_t fixedColumnNs{0};
  /** Writer-side time in processDictionaryColumnBatch (nanoseconds). */
  uint64_t dictionaryColumnNs{0};
  uint64_t fixedColumnCalls{0};
  uint64_t dictionaryColumnCalls{0};
};

/** Snapshot of pipeline timing/counters (filled when COLUMNSTORE_PARQUET_IMPORT_INSTR is enabled). */
struct ParquetPipelineInstrSnapshot
{
  uint64_t readerBatches{0};
  uint64_t readerRows{0};
  uint64_t readerDecodeNs{0};
  uint64_t readerPushWaitNs{0};
  uint64_t readerPushCount{0};

  uint64_t coordinatorPopWaitNs{0};
  uint64_t coordinatorPopCount{0};
  uint64_t coordinatorReorderHoldNs{0};
  uint64_t coordinatorDispatchedBatches{0};
  uint64_t coordinatorDispatchedTasks{0};
  uint64_t coordinatorInflightWaitNs{0};
  uint64_t coordinatorInflightWaitCount{0};

  uint64_t writerQueuePopWaitNs{0};
  uint64_t writerQueuePopCount{0};
  uint64_t writerTaskProcessNs{0};
  uint64_t writerTasks{0};

  uint64_t fixedColumnNs{0};
  uint64_t fixedColumnCalls{0};
  uint64_t dictionaryColumnNs{0};
  uint64_t dictionaryColumnCalls{0};

  uint64_t maxQueueBytesObserved{0};
  uint64_t maxInflightBatchesObserved{0};
};

struct ParquetConversionResult
{
  ParquetReadStats stats;
  std::vector<ParquetColumnMapping> mappings;
  std::string materializedFilePath;
  /** Column names aligned with columnInstrumentation indices (direct-import column order). */
  std::vector<std::string> columnNames;
  std::vector<ParquetColumnInstrSnapshot> columnInstrumentation;
  /** Per-chunk dictionary string dedupe was enabled for this run (`--parquet-dict-dedupe`). */
  bool dictChunkDedupeEnabled{false};
  /** Filled when COLUMNSTORE_PARQUET_IMPORT_INSTR is set (non-empty, first char not '0'). */
  bool hasPipelineInstrumentation{false};
  ParquetPipelineInstrSnapshot pipelineInstrumentation{};
};

struct ParquetImportRuntimeConfig
{
  int readThreads{1};
  int64_t queueBytes{134217728};
  /** Parallel column writers for direct import (`cpimport` maps `-w` / `--writers` here). */
  int columnWriteThreads{1};
  /**
   * Max batches concurrently in the coordinator→writer pipeline (0 = auto:
   * max(2, columnWriteThreads * 2)). Limits writer-queue growth under slow consumers.
   */
  int maxParquetInflightBatches{0};
  /** Remap duplicate strings within each chunk before updateDctnryStore (opt-in; default off on typical workloads). */
  bool dictChunkDedupe{false};
  /**
   * Parquet-Arrow FileReader: ArrowReaderProperties::use_threads (decode row groups with Arrow's thread pool).
   * Default false: cpimport already uses external reader workers + column writers.
   */
  bool arrowReaderUseThreads{false};
};

class ParquetReader
{
 public:
  static void setImportRuntimeConfig(const ParquetImportRuntimeConfig& cfg);
  static int readFile(const std::string& filePath, ParquetReadStats& stats, std::string& errMsg);
  static int convertToDelimitedFile(const std::string& parquetFilePath, const std::string& outputFilePath,
                                    ParquetConversionResult& result, std::string& errMsg);
  static int importIntoTableDirect(const std::string& parquetFilePath, TableInfo& tableInfo,
                                   ParquetConversionResult& result, std::string& errMsg);
};

}  // namespace WriteEngine
