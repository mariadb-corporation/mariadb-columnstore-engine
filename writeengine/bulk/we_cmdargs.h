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
  WECmdArgs(int argc, char** argv);
  virtual ~WECmdArgs();

  using VecInts = std::vector<unsigned int>;
  using VecArgs = std::vector<std::string>;

  void parseCmdLineArgs(int argc, char** argv);
  void usage() const;
  bool checkForCornerCases();

  void startupError(const std::string& errMsg, bool showHint = false) const;
  void fillParams(BulkLoad& curJob, std::string& sJobIdStr,
                  std::string& sXMLJobDir, std::string& sModuleIDandPID, bool& bLogInfo2ToConsole,
                  std::string& xmlGenSchema, std::string& xmlGenTable, bool& bValidateColumnList);

  void setCpimportJobId(uint32_t cpimportJobId)
  {
    fCpimportJobId = cpimportJobId;
  }

private:
  void checkIntArg(const std::string& name, long min, long max, int value) const;
  VecArgs fVecArgs;
  VecInts fPmVec;

  VecArgs fVecJobFiles;         // JobFiles splitter from master JobFile
  int fMultiTableCount;         // MultiTable count
  VecArgs fColFldsFromJobFile;  // List of columns from any job file, that
  // represent fields in the import data

  std::string fJobId;       // JobID
  std::string fOrigJobId;   // Original JobID, in case we have to split it
  bool fJobLogOnly;         // Job number is only for log filename only
  bool fHelp;               // Help mode
  int fMode;                // splitter Mode
  int fArgMode;             // Argument mode, dep. on this fMode is decided.
  bool fQuiteMode;          // in quite mode or not
  bool fConsoleLog;         // Log everything to console - w.r.t cpimport
  std::string fPmFile;      // FileName at PM
  std::string fPmFilePath;  // Path of input file in PM
  std::string fLocFile;     // Local file name
  std::string fBrmRptFile;  // BRM report file
  std::string fJobPath;     // Path to Job File
  std::string fTmpFileDir;  // Temp file directory.
  std::string fBulkRoot;    // Bulk Root path
  std::string fJobFile;     // Job File Name
  std::string fS3Key;       // S3 key
  std::string fS3Secret;    // S3 Secret
  std::string fS3Bucket;    // S3 Bucket
  std::string fS3Host;      // S3 Host
  std::string fS3Region;    // S3 Region

  int fNoOfReadThrds;      // No. of read buffers
  int fDebugLvl;                   // Debug level
  int fMaxErrors;                  // Max allowable errors
  int fReadBufSize;                // Read buffer size
  int fIOReadBufSize;              // I/O read buffer size
  int fSetBufSize;                 // Buff size w/setvbuf
  char fColDelim;                  // column delimiter
  char fEnclosedChar;              // enclosed by char
  char fEscChar;                   // esc char
  int fSkipRows;                   // skip header
  int fNoOfWriteThrds;             // No. of write threads
  bool fNullStrMode;               // set null string mode - treat null as null
  ImportDataMode fImportDataMode;  // Importing text or binary data
  std::string fPrgmName;           // argv[0]
  std::string fSchema;             // Schema name - positional parmater
  std::string fTable;              // Table name - table name parameter

  bool fBlockMode3;          // Do not allow Mode 3
  bool fbTruncationAsError;  // Treat string truncation as error
  std::string fUUID;
  bool fConsoleOutput;    // If false, no output to console.
  std::string fTimeZone;  // Timezone to use for TIMESTAMP datatype
  std::string fUsername;  // Username of the data files owner
  std::string fErrorDir;
  bool fDisableTableLockTimeOut;
  bool fSilent;
  std::string fModuleIDandPID;

  std::string fReportFilename;
  bool fKeepRollbackMetaData;
  bool fAllowMissingColumn;

  uint32_t fCpimportJobId;

  std::unique_ptr<boost::program_options::options_description> fOptions;
  std::unique_ptr<boost::program_options::options_description> fVisibleOptions;
};

}  // namespace WriteEngine
