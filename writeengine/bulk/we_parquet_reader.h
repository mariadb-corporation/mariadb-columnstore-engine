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

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <boost/ptr_container/ptr_vector.hpp>

namespace WriteEngine
{
class ColumnInfo;
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

/** Wall-clock stage budget for direct parquet import (COLUMNSTORE_PARQUET_IMPORT_INSTR). */
struct ParquetWallClockInstrSnapshot
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

/** Snapshot of pipeline timing/counters (filled when COLUMNSTORE_PARQUET_IMPORT_INSTR is enabled). */
struct ParquetPipelineInstrSnapshot
{
  uint64_t readerBatches{0};
  uint64_t readerRows{0};
  /** Sum of decode time across all reader threads (may exceed wall-clock). */
  uint64_t readerDecodeNs{0};
  /** Sum of time blocked on bounded-queue push across reader threads. */
  uint64_t readerPushWaitNs{0};
  uint64_t readerPushCount{0};

  /** Coordinator thread time blocked waiting for queue.pop (wall time on coordinator). */
  uint64_t coordinatorPopWaitNs{0};
  uint64_t coordinatorPopCount{0};
  /** Sum over batches: queuedAt until dispatch (cumulative; may exceed wall-clock). */
  uint64_t batchPreDispatchResidenceNs{0};
  /** Sum over batches held in reorder map until dispatch (cumulative). */
  uint64_t trueReorderHoldNs{0};
  uint64_t trueReorderHeldBatches{0};
  uint64_t coordinatorDispatchedBatches{0};
  uint64_t coordinatorDispatchedTasks{0};
  /** Coordinator thread time blocked on inflight limit (wall time on coordinator). */
  uint64_t coordinatorInflightWaitNs{0};
  uint64_t coordinatorInflightWaitCount{0};

  /** Sum of writer queue pop wait across writer threads. */
  uint64_t writerQueuePopWaitNs{0};
  uint64_t writerQueuePopCount{0};
  /** Sum of per-task processing across writer threads. */
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
  bool hasWallClockInstrumentation{false};
  ParquetWallClockInstrSnapshot wallClockInstrumentation{};
};

/**
 * Cohort execution model for parquet direct import. Selected via
 * `--parquet-cohort-mode={sequential,parallel}`.
 *
 *  - Sequential: cohorts run back-to-back through the shared `TableInfo` /
 *    `ColumnInfo` set. This is the row-range + finalize plumbing established
 *    in the previous turn and the default for N > 1.
 *  - Parallel: cohorts run concurrently, each with its own sidecar
 *    `ColumnInfo` / `Dctnry` / `ColumnBufferManager` set built via
 *    `setupDelayedFileCreation` + on-demand BRM stripe extent allocation.
 *    The driver launches one `std::thread` per cohort, then runs a merged
 *    finalize that walks every cohort's sidecars, feeds them into the master
 *    TableInfo `BRMReporter`, and performs the table-level lock-state /
 *    rollback-meta cleanup. See `TableInfo::finalizeDirectImportSidecars`.
 */
enum class ParquetCohortExecMode
{
  Sequential = 0,
  Parallel = 1,
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
  /**
   * Experimental cohort partition count.
   * 1 (default) preserves existing behavior exactly.
   * N > 1 splits the parquet row groups into N contiguous ranges. The actual
   * threading is governed by `cohortMode` (sequential = back-to-back through
   * the shared TableInfo; parallel = one std::thread per cohort with
   * independent sidecar ColumnInfo, gated off until that work lands).
   * Allowed values: 1, 2, 4.
   */
  int cohorts{1};
  /**
   * Execution mode for N > 1 cohorts. Defaults to Sequential to preserve the
   * existing behavior; Parallel is opt-in and currently gated off (see comment
   * on `ParquetCohortExecMode`).
   */
  ParquetCohortExecMode cohortMode{ParquetCohortExecMode::Sequential};
};

/**
 * Per-cohort context describing a contiguous row-group range to be imported
 * through `importIntoTableDirectRange`. Carries enough information for the
 * range routine to honor only the cohort's portion of the parquet file and
 * for the cohort driver to log/aggregate per-cohort accounting.
 */
struct ParquetCohortContext
{
  /** Zero-based cohort id within this import. */
  int cohortId{0};
  /** Total cohorts in this import (G); 1 means "not a cohort split". */
  int cohortCount{1};
  /** First parquet row-group index (inclusive) processed by this cohort. */
  int rowGroupBegin{0};
  /** Last parquet row-group index (exclusive) processed by this cohort. */
  int rowGroupEnd{0};
  /** First global RID assigned to this cohort (== rowGroupStarts[rowGroupBegin]). */
  int64_t globalRowBase{0};
  /** Total rows expected for this cohort (sum of row group row counts in range). */
  int64_t rowCountInCohort{0};
  /**
   * If true, this cohort is responsible for calling
   * `prepareDirectImportCompletion`/`finalizeDirectImport` at the end of its
   * run. The cohort driver sets this only on the final cohort (or on the sole
   * cohort when `cohortCount == 1`).
   */
  bool driveFinalize{true};
  /**
   * If true, the range function rebases parquet row indices so the cohort's
   * RID stream is **0-based** rather than absolute. Required for the parallel
   * driver: each parallel cohort writes through its own sidecar
   * `ColumnBufferManager`, whose `reserveSection` enforces a strict monotonic
   * RID stream starting at 0 (`fMaxRowId == max-uint -> next must be 0`).
   *
   * For sequential cohorts this must remain `false` so each cohort's first
   * batch lines up with the canonical buffer manager's accumulated `fMaxRowId`.
   * `globalRowBase` is preserved for logging in either mode.
   */
  bool rebaseRowsToZero{false};
  /** Optional human-readable label (used for cohort-aware logging). */
  std::string label;
};

/**
 * Per-cohort runtime telemetry, returned to the cohort driver/caller so it
 * can print a summary line per cohort and aggregate totals.
 *
 * `startWallSec` / `endWallSec` are wall-clock timestamps captured by the
 * parallel driver (steady-clock seconds, monotonic within one import) so the
 * post-run banner can show staggered start/end ordering, the max cohort
 * elapsed, and a sequential-vs-parallel speedup estimate.
 */
struct ParquetCohortStats
{
  int cohortId{0};
  int rowGroupBegin{0};
  int rowGroupEnd{0};
  int64_t globalRowBase{0};
  int64_t rowsImported{0};
  double elapsedSeconds{0.0};
  double dictionaryColumnSeconds{0.0};
  double fixedColumnSeconds{0.0};
  double startWallSec{0.0};
  double endWallSec{0.0};
};

class ParquetReader
{
 public:
  static void setImportRuntimeConfig(const ParquetImportRuntimeConfig& cfg);
  static int readFile(const std::string& filePath, ParquetReadStats& stats, std::string& errMsg);
  static int convertToDelimitedFile(const std::string& parquetFilePath, const std::string& outputFilePath,
                                    ParquetConversionResult& result, std::string& errMsg);
  /**
   * Whole-file direct import. Preserved for the cohorts == 1 path; drives
   * `prepareDirectImportCompletion` + `finalizeDirectImport` internally.
   * Behavior unchanged from the pre-cohort version.
   */
  static int importIntoTableDirect(const std::string& parquetFilePath, TableInfo& tableInfo,
                                   ParquetConversionResult& result, std::string& errMsg);
  /**
   * Cohort-aware direct import (experimental). When `gImportRuntimeConfig.cohorts == 1`
   * this delegates to `importIntoTableDirect`. For N > 1 it splits the parquet
   * file into N contiguous row-group ranges and imports them back-to-back through
   * the same `TableInfo`, calling finalize exactly once after the last cohort.
   *
   * Returns the same `ParquetConversionResult` shape as `importIntoTableDirect`,
   * with per-cohort timing appended to the human-readable summary written to
   * stdout (instrumentation snapshots aggregate the totals).
   */
  static int importIntoTableDirectWithCohorts(const std::string& parquetFilePath, TableInfo& tableInfo,
                                              ParquetConversionResult& result, std::string& errMsg);
  /**
   * Import one cohort's row-group range against an EXPLICIT `ColumnInfo` set.
   *
   * The `cohortColumns` argument identifies the `ColumnInfo` instances the
   * cohort should write to:
   *  - For the sequential and `cohorts == 1` paths, the caller passes
   *    `tableInfo.directImportColumns()` (canonical set from `BulkLoad::preProcess`).
   *  - For the parallel driver, each cohort thread passes its own sidecar
   *    `boost::ptr_vector<ColumnInfo>` built by `buildSidecarColumnInfosForCohort`.
   *
   * `externalStop` is an optional cooperative-cancel flag observed at coarse
   * boundaries (per row group, before main pipeline setup). The parallel
   * driver sets this when one cohort fails so peer cohorts unwind quickly
   * without further BRM allocations. `nullptr` disables cooperative cancel
   * (sequential / single-cohort callers pass nullptr).
   *
   * The function does NOT call `prepareDirectImportCompletion` /
   * `finalizeDirectImport`; the caller is responsible for finalizing exactly
   * once after every cohort returns (so a set of range calls share a single
   * finalize: either `TableInfo::finalizeDirectImport` for the canonical-set
   * paths or `TableInfo::finalizeDirectImportSidecars` for the parallel path).
   * The `tableInfo` parameter is retained for `directImportTableName`,
   * timezone, BRM / RBMeta back-pointers carried on each `ColumnInfo`, and
   * the finalize chain itself.
   */
  static int importIntoTableDirectRange(const std::string& parquetFilePath, TableInfo& tableInfo,
                                        boost::ptr_vector<ColumnInfo>& cohortColumns,
                                        const ParquetCohortContext& cohortCtx,
                                        ParquetConversionResult& result, ParquetCohortStats* cohortStats,
                                        std::atomic<bool>* externalStop, std::string& errMsg);

  /**
   * Build a cohort-local set of `ColumnInfo` objects ("sidecars") that mirror
   * the canonical set on `masterTableInfo`. Each sidecar:
   *  - shares the canonical `JobColumn` (static schema/OIDs/widths only);
   *  - holds its own `ColumnBufferManager`, `Dctnry` / dictionary store,
   *    current segment cursors, ColExtInf CP map, saturated counters;
   *  - points at the master `TableInfo` via `fpTableInfo` so it can use the
   *    shared `ExtentStripeAlloc` (mutex-protected: see
   *    `we_extentstripealloc.cpp`) and the shared `RBMetaWriter` (documented
   *    thread-safe via `fRBChunkDctnryMutex`);
   *  - is initialized in delayed-file-creation mode (`bEmptyPM = true`) so
   *    the first row triggers `createDelayedFileIfNeeded` -> fresh BRM stripe
   *    allocation -> file creation -> `setupInitialColumnExtent`. Sidecars
   *    therefore never share segment files with the canonical columns nor
   *    with peer cohorts.
   *
   * Caller owns the returned `boost::ptr_vector`; pass it to
   * `importIntoTableDirectRange` and then to
   * `TableInfo::finalizeDirectImportSidecars` for the merged finalize.
   */
  static int buildSidecarColumnInfosForCohort(TableInfo& masterTableInfo, int cohortId,
                                              uint32_t targetPartition, uint16_t targetSegment,
                                              boost::ptr_vector<ColumnInfo>& sidecarColumns,
                                              std::string& errMsg);
};

}  // namespace WriteEngine
