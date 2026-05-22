/* Copyright (C) 2014 InfiniDB, Inc.

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

/*******************************************************************************
 * $Id$
 *
 *******************************************************************************/
#pragma once

#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "we_bulkload.h"

#include "we_type.h"

namespace boost::program_options
{
class options_description;
}

namespace WriteEngine
{
class WECmdArgs
{
public:
  enum class InputFormat
  {
    Text,
    Binary,
    Parquet
  };

  WECmdArgs(int argc, char** argv);
  ~WECmdArgs();

  using VecInts = std::vector<unsigned int>;
  using VecArgs = std::vector<std::string>;

  void parseCmdLineArgs(int argc, char** argv);
  void usage() const;
  bool checkForCornerCases();

  void startupError(const std::string& errMsg, bool showHint = false) const;
  void fillParams(BulkLoad& curJob, std::string& sJobIdStr,
                  std::string& sXMLJobDir, std::string& sModuleIDandPID, bool& bLogInfo2ToConsole,
                  std::string& xmlGenSchema, std::string& xmlGenTable, bool& bValidateColumnList);
  bool isParquetMode() const
  {
    return fInputFormat == InputFormat::Parquet;
  }
  std::string getParquetFilePath() const;
  int getParquetReadThreads() const
  {
    return fParquetReadThreads;
  }
  int64_t getParquetQueueBytes() const
  {
    return fParquetQueueBytes;
  }
  /** Parse / writer thread count from `-w` / `--writers` (used as parquet column writer count for direct import). */
  int getNoOfParseThreads() const
  {
    return fNoOfWriteThrds;
  }
  /** 0 = auto cap on coordinator batch pipeline for parquet direct import. */
  int getParquetMaxInflightBatches() const
  {
    return fParquetMaxInflightBatches;
  }
  /** 1 = per-chunk dictionary string dedupe (parquet direct import); 0 = off (default). */
  bool getParquetDictChunkDedupe() const
  {
    return fParquetDictChunkDedupe != 0;
  }
  /** 1 = Arrow parquet FileReader use_threads (parallel decode inside Arrow); 0 = off (default). */
  bool getParquetArrowReaderUseThreads() const
  {
    return fParquetArrowReaderUseThreads != 0;
  }
  /**
   * Number of cohort partitions for parquet direct import (experimental).
   * 1 (default) = unchanged behavior.
   * N > 1       = split parquet row groups into N contiguous ranges and run them
   *               back-to-back through the same TableInfo; a single finalize runs
   *               after the last cohort. This is the sequential cohort mode that
   *               establishes the row-range + RID + finalize plumbing; concurrent
   *               cohorts and per-cohort segment routing are follow-up work.
   * Allowed values: 1, 2, 4.
   */
  int getParquetCohorts() const
  {
    return fParquetCohorts;
  }
  /**
   * Execution mode for `--parquet-cohorts=N` (N > 1). One of:
   *  - "sequential" (default): cohorts run back-to-back through the shared
   *    `TableInfo` / `ColumnInfo` set (the row-range + finalize plumbing
   *    established in the previous turn).
   *  - "parallel": cohorts run concurrently, each with its own sidecar
   *    `ColumnInfo` / `Dctnry` / `ColumnBufferManager` state. Opt-in only;
   *    while the sidecar `ColumnInfo` construction and the merged finalize
   *    chain are still being implemented and validated, the runtime returns
   *    an explicit error if this mode is selected with N > 1. Cohort
   *    partitioning, mode plumbing, and the driver skeleton (threads,
   *    stop atomic, first-error) are present so the wiring can be reviewed
   *    in isolation from sidecar construction.
   * For `--parquet-cohorts=1` the mode value is accepted but has no effect.
   */
  const std::string& getParquetCohortMode() const
  {
    return fParquetCohortMode;
  }

  void setCpimportJobId(uint32_t cpimportJobId)
  {
    fCpimportJobId = cpimportJobId;
  }

private:
  void checkIntArg(const std::string& name, long min, long max, int value) const;
  VecArgs fVecArgs;
  VecInts fPmVec;

  VecArgs fVecJobFiles;         // JobFiles splitter from master JobFile
  int fMultiTableCount{0};      // MultiTable count
  VecArgs fColFldsFromJobFile;  // List of columns from any job file, that
  // represent fields in the import data

  std::string fJobId;         // JobID
  std::string fOrigJobId;     // Original JobID, in case we have to split it
  bool fJobLogOnly{false};    // Job number is only for log filename only
  bool fHelp{false};          // Help mode
  int fMode{BULK_MODE_LOCAL}; // splitter Mode
  int fArgMode{-1};           // Argument mode, dep. on this fMode is decided.
  bool fQuiteMode{true};      // in quite mode or not
  bool fConsoleLog{false};    // Log everything to console - w.r.t cpimport
  std::string fPmFile;        // FileName at PM
  std::string fPmFilePath;    // Path of input file in PM
  std::string fLocFile;       // Local file name
  std::string fBrmRptFile;    // BRM report file
  std::string fJobPath;       // Path to Job File
  std::string fTmpFileDir;    // Temp file directory.
  std::string fBulkRoot;      // Bulk Root path
  std::string fJobFile;       // Job File Name
  std::string fS3Key;         // S3 key
  std::string fS3Secret;      // S3 Secret
  std::string fS3Bucket;      // S3 Bucket
  std::string fS3Host;        // S3 Host
  std::string fS3Region;      // S3 Region

  int fNoOfReadThrds{1};      // No. of read buffers
  int fDebugLvl{0};           // Debug level
  int fMaxErrors{MAX_ERRORS_DEFAULT}; // Max allowable errors
  int fReadBufSize{-1};       // Read buffer size
  int fIOReadBufSize{-1};     // I/O read buffer size
  int fSetBufSize{0};         // Buff size w/setvbuf
  char fColDelim{0};          // column delimiter
  char fEnclosedChar{0};      // enclosed by char
  char fEscChar{0};           // esc char
  int fSkipRows{0};           // skip header
  int fNoOfWriteThrds{3};     // No. of write threads
  int fParquetReadThreads{1}; // Number of parquet reader workers
  int64_t fParquetQueueBytes{134217728}; // Max bytes in parquet batch queue
  /** Max batches between coordinator and column writers (0 = auto). Parquet direct import only. */
  int fParquetMaxInflightBatches{0};
  /** 1 = dedupe repeated strings per chunk before dictionary store (parquet direct import; default 0: hash cost often loses vs Dctnry). */
  int fParquetDictChunkDedupe{0};
  /** 1 = Arrow parquet reader use_threads (testing; default 0). */
  int fParquetArrowReaderUseThreads{0};
  /** Number of cohort partitions for parquet direct import (1=default; range [1, 32]). */
  int fParquetCohorts{1};
  /** Cohort execution mode for `--parquet-cohorts=N` (N>1): "sequential" (default) or "parallel" (gated; not yet implemented). */
  std::string fParquetCohortMode{"sequential"};
  bool fNullStrMode{false};   // set null string mode - treat null as null
  ImportDataMode fImportDataMode{IMPORT_DATA_TEXT};  // Importing text or binary data
  InputFormat fInputFormat{InputFormat::Text};       // Input format type
  std::string fPrgmName;      // argv[0]
  std::string fSchema;        // Schema name - positional parmater
  std::string fTable;         // Table name - table name parameter

  bool fBlockMode3{false};           // Do not allow Mode 3
  bool fbTruncationAsError{false};   // Treat string truncation as error
  std::string fUUID{boost::uuids::to_string(boost::uuids::nil_generator()())};
  bool fConsoleOutput{true};         // If false, no output to console.
  std::string fTimeZone{"SYSTEM"};   // Timezone to use for TIMESTAMP datatype
  std::string fUsername;             // Username of the data files owner
  std::string fErrorDir{MCSLOGDIR "/cpimport"};
  bool fDisableTableLockTimeOut{false};
  bool fSilent{false};
  std::string fModuleIDandPID;

  std::string fReportFilename;
  bool fKeepRollbackMetaData{false};
  bool fAllowMissingColumn{false};

  uint32_t fCpimportJobId{};

  std::unique_ptr<boost::program_options::options_description> fOptions;
  std::unique_ptr<boost::program_options::options_description> fVisibleOptions;
};

}  // namespace WriteEngine
