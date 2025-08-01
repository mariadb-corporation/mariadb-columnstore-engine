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

#include <set>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/nil_generator.hpp>

#include "we_xmlgetter.h"
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

  typedef std::vector<unsigned int> VecInts;
  typedef std::vector<std::string> VecArgs;

  void appTestFunction();
  void parseCmdLineArgs(int argc, char** argv);
  std::string getCpImportCmdLine(bool skipRows);
  void setSchemaAndTableFromJobFile(std::string& JobName);
  void setEnclByAndEscCharFromJobFile(std::string& JobName);
  void usage();
  bool checkForCornerCases();
  void checkForBulkLogDir(const std::string& BulkRoot);

  void addJobFilesToVector(std::string& JobName);
  void splitConfigFilePerTable(std::string& ConfigName, int tblCount);
  void write2ConfigFiles(std::vector<std::ofstream*>& Files, char* pBuff, int FileIdx);
  void updateWithJobFile(int Idx);

  std::string getJobFileName();
  std::string getBrmRptFileName();
  std::string getTmpFileDir();
  std::string getBulkRootDir();
  unsigned int getBatchQuantity();
  void checkJobIdCase();
  std::string getFileNameFromPath(const std::string& Path) const;
  std::string getModuleID();
  std::string replaceCharInStr(const std::string& Str, char C, char R);

  std::string getJobId() const
  {
    return fJobId;
  }
  std::string getOrigJobId() const
  {
    return fOrigJobId;
  }
  std::string getLocFile() const
  {
    return fLocFile;
  }
  int getReadBufSize() const
  {
    return fReadBufSize;
  }
  int getMode() const
  {
    return fMode;
  }
  int getArgMode() const
  {
    return fArgMode;
  }
  bool isHelpMode() const
  {
    return fHelp;
  }
  int getDebugLvl() const
  {
    return fDebugLvl;
  }
  char getEnclChar() const
  {
    return fEnclosedChar;
  }
  char getEscChar() const
  {
    return fEscChar;
  }
  char getDelimChar() const
  {
    return fColDelim;
  }
  int getSkipRows() const
  {
    return fSkipRows;
  }
  ImportDataMode getImportDataMode() const
  {
    return fImportDataMode;
  }
  bool getConsoleLog() const
  {
    return fConsoleLog;
  }

  bool isCpimportInvokeMode() const
  {
    return (fBlockMode3) ? false : fCpiInvoke;
  }
  bool isQuiteMode() const
  {
    return fQuiteMode;
  }
  void setJobId(const std::string& fJobId)
  {
    this->fJobId = fJobId;
  }
  void setOrigJobId()
  {
    this->fOrigJobId = fJobId;
  }
  void setLocFile(const std::string& fLocFile)
  {
    this->fLocFile = fLocFile;
  }
  void setMode(int fMode)
  {
    this->fMode = fMode;
  }
  void setArgMode(int ArgMode)
  {
    this->fArgMode = ArgMode;
  }
  void setPmFile(const std::string& fPmFile)
  {
    this->fPmFile = fPmFile;
  }
  void setQuiteMode(bool fQuiteMode)
  {
    this->fQuiteMode = fQuiteMode;
  }
  void setVerbose(int fVerbose)
  {
    this->fVerbose = fVerbose;
  }
  void setJobFileName(std::string& JobFileName)
  {
    fJobFile = JobFileName;
  }
  void setBrmRptFileName(std::string& BrmFileName)
  {
    this->fBrmRptFile = BrmFileName;
  }
  void setCpiInvoke(bool CpiInvoke = true)
  {
    this->fCpiInvoke = CpiInvoke;
  }
  void setBlockMode3(bool Block)
  {
    this->fBlockMode3 = Block;
  }
  void setTruncationAsError(bool bTruncationAsError)
  {
    fbTruncationAsError = bTruncationAsError;
  }
  void setUsername(const std::string& username);

  bool isJobLogOnly() const
  {
    return fJobLogOnly;
  }
  void setJobUUID(const boost::uuids::uuid& jobUUID)
  {
    fUUID = jobUUID;
  }
  bool getConsoleOutput() const
  {
    return fConsoleOutput;
  }
  const std::string& getTimeZone() const
  {
    return fTimeZone;
  }

  bool getPmStatus(int Id);
  bool str2PmList(std::string& PmList, VecInts& V);
  size_t getPmVecSize() const
  {
    return fPmVec.size();
  }
  void add2PmVec(int PmId)
  {
    fPmVec.push_back(PmId);
  }

  WECmdArgs::VecInts& getPmVec()
  {
    return fPmVec;
  }
  std::string getPmFile() const
  {
    return fPmFile;
  }
  int getVerbose() const
  {
    return fVerbose;
  }
  std::string getTableName() const
  {
    return fTable;
  }
  std::string getSchemaName() const
  {
    return fSchema;
  }
  bool getTruncationAsError() const
  {
    return fbTruncationAsError;
  }

  int getMultiTableCount() const
  {
    return fMultiTableCount;
  }
  void setMultiTableCount(int Count)
  {
    fMultiTableCount = Count;
  }

  bool isS3Import() const
  {
    return !fS3Key.empty();
  }
  std::string getS3Key() const
  {
    return fS3Key;
  }
  std::string getS3Bucket() const
  {
    return fS3Bucket;
  }
  std::string getS3Host() const
  {
    return fS3Host;
  }
  std::string getS3Secret() const
  {
    return fS3Secret;
  }
  std::string getS3Region() const
  {
    return fS3Region;
  }
  std::string getErrorDir() const
  {
    return fErrorDir;
  }
  void setErrorDir(const std::string& fErrorDir)
  {
    this->fErrorDir = fErrorDir;
  }
  std::string& getUsername();
  std::string PrepMode2ListOfFiles(std::string& FileName);  // Bug 4342
  void getColumnList(std::set<std::string>& columnList) const;

 private:
  static void checkIntArg(const std::string& name, long min, long max, int value);
 private:  // variables for SplitterApp
  VecArgs fVecArgs;
  VecInts fPmVec;

  VecArgs fVecJobFiles;         // JobFiles splitter from master JobFile
  int fMultiTableCount{0};      // MultiTable count
  VecArgs fColFldsFromJobFile;  // List of columns from any job file, that
                                // represent fields in the import data

  std::string fJobId;       // JobID
  std::string fOrigJobId;   // Original JobID, in case we have to split it
  bool fJobLogOnly{false};  // Job number is only for log filename only
  bool fHelp{false};        // Help mode
  int fMode{1};             // splitter Mode
  int fArgMode{-1};         // Argument mode, dep. on this fMode is decided.
  bool fQuiteMode{true};    // in quite mode or not
  bool fConsoleLog{false};  // Log everything to console - w.r.t cpimport
  int fVerbose{0};          // how many v's
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

  int fBatchQty{10000};     // No. of batch Qty.
  int fNoOfReadThrds{0};    // No. of read buffers
  int fDebugLvl{0};         // Debug level
  int fMaxErrors{MAX_ERRORS_DEFAULT}; // Max allowable errors
  int fReadBufSize{0};      // Read buffer size
  int fIOReadBufSize{0};    // I/O read buffer size
  int fSetBufSize{0};       // Buff size w/setvbuf
  char fColDelim{'|'};      // column delimiter
  char fEnclosedChar{0};    // enclosed by char
  char fEscChar{0};         // esc char
  int fSkipRows{0};         // skip header
  int fNoOfWriteThrds{0};   // No. of write threads
  bool fNullStrMode{false}; // set null string mode - treat null as null
  ImportDataMode fImportDataMode{IMPORT_DATA_TEXT};  // Importing text or binary data
  std::string fPrgmName;    // argv[0]
  std::string fSchema;      // Schema name - positional parmater
  std::string fTable;       // Table name - table name parameter

  bool fCpiInvoke{false};            // invoke cpimport in mode 3
  bool fBlockMode3{false};           // Do not allow Mode 3
  bool fbTruncationAsError{false};   // Treat string truncation as error
  boost::uuids::uuid fUUID{boost::uuids::nil_generator()()};
  bool fConsoleOutput{true};         // If false, no output to console.
  std::string fTimeZone{"SYSTEM"};   // Timezone to use for TIMESTAMP datatype
  std::string fUsername;             // Username of the data files owner
  std::string fErrorDir{MCSLOGDIR "/cpimport/"};
  std::unique_ptr<boost::program_options::options_description> fOptions;
};
//----------------------------------------------------------------------

inline void WECmdArgs::setUsername(const std::string& username)
{
  fUsername = username;
}
inline std::string& WECmdArgs::getUsername()
{
  return fUsername;
}

}  // namespace WriteEngine
