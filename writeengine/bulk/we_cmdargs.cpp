/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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

#include "we_simplesyslog.h"

#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>

#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <exception>
#include <stdexcept>
#include <cerrno>
#include <boost/program_options.hpp>
namespace po = boost::program_options;
using namespace std;

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/filesystem.hpp>

#include "dataconvert.h"
#include "liboamcpp.h"
using namespace oam;

#include "we_cmdargs.h"

#include "mcsconfig.h"

namespace WriteEngine
{
//----------------------------------------------------------------------
//----------------------------------------------------------------------
WECmdArgs::WECmdArgs(int argc, char** argv)
{
  try
  {
    fOptions = std::make_unique<po::options_description>();
    fVisibleOptions = std::make_unique<po::options_description>();
#define DECLARE_INT_ARG(name, stor, min, max, desc) \
    (name,\
      po::value<int>(&stor)\
        ->notifier([this](auto&& value) { checkIntArg(name, min, max, value); }),\
      desc)

    fVisibleOptions->add_options()
      ("help,h", "Print this message.")
      DECLARE_INT_ARG("read-buffer,b", fIOReadBufSize, 1, INT_MAX, "Number of read buffers.")
      DECLARE_INT_ARG("read-buffer-size,c", fReadBufSize, 1, INT_MAX,
        "Application read buffer size (in bytes)")
      DECLARE_INT_ARG("debug,d", fDebugLvl, 1, 3, "Print different level(1-3) debug message")
      DECLARE_INT_ARG("max-errors,e", fMaxErrors, 0, INT_MAX,
          "Maximum number of allowable error per table per PM")
      ("file-path,f", po::value<string>(&fPmFilePath),
        "Data file directory path. Default is current working directory.\n"
        "\tIn Mode 1, represents the local input file path.\n"
        "\tIn Mode 2, represents the PM based input file path.\n"
        "\tIn Mode 3, represents the local input file path.")
      DECLARE_INT_ARG("mode,m", fArgMode, 1, 3,
        "\t1 - rows will be loaded in a distributed manner acress PMs.\n"
        "\t2 - PM based input files loaded into their respective PM.\n"
        "\t3 - input files will be loaded on the local PM.")
      ("filename,l", po::value<string>(&fPmFile),
        "Name of import file to be loaded, relative to 'file-path'")
      ("console-log,i", po::bool_switch(&fConsoleLog),
        "Print extended info to console in Mode 3.")
      ("job-id,j", po::value<string>(),
        "Job ID. In simple usage, default is the table OID unless a fully qualified input "
        "file name is given.")
      ("null-strings,n", po::value(&fNullStrMode)->implicit_value(true),
        "NullOption (0-treat the string NULL as data (default);\n"
        "1-treat the string NULL as a NULL value)")
      ("xml-job-path,p", po::value<string>(&fJobPath), "Path for the XML job description file.")
      DECLARE_INT_ARG("readers,r", fNoOfReadThrds, 1, INT_MAX, "Number of readers.")
      ("separator,s", po::value<string>(), "Delimiter between column values.")
      DECLARE_INT_ARG("io-buffer-size,B", fSetBufSize, 1, INT_MAX,
        "I/O library read buffer size (in bytes)")
      DECLARE_INT_ARG("writers,w", fNoOfWriteThrds, 1, INT_MAX, "Number of parsers.")
      ("enclosed-by,E", po::value<char>(&fEnclosedChar),
        "Enclosed by character if field values are enclosed.")
      ("escape-char,C", po::value<char>(&fEscChar)->default_value('\\'),
        "Escape character used in conjunction with 'enclosed-by'"
        "character, or as a part of NULL escape sequence ('\\N');\n"
        "default is '\\'")
      ("headers,O",
        po::value<int>(&fSkipRows)->implicit_value(1)
          ->notifier([this](auto&& value) { checkIntArg("headers,O", 0, INT_MAX, value); }),
        "Number of header rows to skip.")
      ("binary-mode,I", po::value<int>(),
        "Import binary data; how to treat NULL values:\n"
        "\t1 - import NULL values\n"
        "\t2 - saturate NULL values\n")
      ("calling-module,P", po::value<string>(&fModuleIDandPID), "Calling module ID and PID.")
      ("truncation-as-error,S", po::bool_switch(&fbTruncationAsError),
        "Treat string truncations as errors.")
      ("tz,T", po::value<string>(),
        "Timezone used for TIMESTAMP datatype. Possible values:\n"
        "\t\"SYSTEM\" (default)\n"
        "\tOffset in the form +/-HH:MM")
      ("disable-tablelock-timeout,D", po::bool_switch(&fDisableTableLockTimeOut),
        "Disable timeout when waiting for table lock.")
      ("silent,N", po::bool_switch(&fSilent), "Disable console output.")
      ("s3-key,y", po::value<string>(&fS3Key),
        "S3 Authentication Key (for S3 imports)")
      ("s3-secret,K", po::value<string>(&fS3Secret),
        "S3 Authentication Secret (for S3 imports)")
      ("s3-bucket,t", po::value<string>(&fS3Bucket),
        "S3 Bucket (for S3 imports)")
      ("s3-hostname,H", po::value<string>(&fS3Host),
        "S3 Hostname (for S3 imports, Amazon's S3 default)")
      ("s3-region,g", po::value<string>(&fS3Region),
        "S3 Region (for S3 imports)")
      ("errors-dir,L", po::value<string>(&fErrorDir)->default_value(fErrorDir),
        "Directory for the output .err and .bad files")
      ("job-uuid,u", po::value<string>(&fUUID), "import job UUID")
      ("username,U", po::value<string>(&fUsername), "Username of the files owner.")
      ("dbname", po::value<string>(), "Name of the database to load")
      ("table", po::value<string>(), "Name of table to load")
      ("load-file", po::value<string>(),
        "Optional input file name in current directory, "
        "unless a fully qualified name is given. If not given, input read from STDIN.");

    po::options_description hidden("Hidden options");
    hidden.add_options()
      ("keep-rollback-metadata,k", po::bool_switch(&fKeepRollbackMetaData),
        "Keep rollback metadata.")
      ("report-file,R", po::value<string>(&fReportFilename), "Report file name.")
      ("allow-missing-columns,X", po::value<string>(), "Allow missing columns.");

    fOptions->add(*fVisibleOptions).add(hidden);

#undef DECLARE_INT_ARG
    parseCmdLineArgs(argc, argv);
  }
  catch (std::exception& exp)
  {
    startupError(exp.what(), true);
  }
}

WECmdArgs::~WECmdArgs() = default;

//----------------------------------------------------------------------

void WECmdArgs::checkIntArg(const std::string& name, long min, long max, int value) const
{
  if (value < min || value > max)
  {
    ostringstream oss;
    oss << "Argument " << name << " is out of range [" << min << ", " << max << "]";
    startupError(oss.str(), true);
  }
}

//----------------------------------------------------------------------

void WECmdArgs::usage() const
{
  cout << endl
       << "Simple usage using positional parameters "
          "(no XML job file):"
       << endl
       << "    " << fPrgmName << " dbName tblName [loadFile] [-j jobID] " << endl
       << "    [-h] [-r readers] [-w parsers] [-s c] [-f path] [-b readBufs] " << endl
       << "    [-c readBufSize] [-e maxErrs] [-B libBufSize] [-n NullOption] " << endl
       << "    [-E encloseChar] [-C escapeChar] [-I binaryOpt] [-S] "
          "[-d debugLevel] [-i] "
       << endl
       << "     [-D] [-N] [-L rejectDir] [-T timeZone]" << endl
       << "    [-U username]" << endl
       << endl;

  cout << endl
       << "Traditional usage without positional parameters "
          "(XML job file required):"
       << endl
       << "    " << fPrgmName << " -j jobID " << endl
       << "    [-h] [-r readers] [-w parsers] [-s c] [-f path] [-b readBufs] " << endl
       << "    [-c readBufSize] [-e maxErrs] [-B libBufSize] [-n NullOption] " << endl
       << "    [-E encloseChar] [-C escapeChar] [-I binaryOpt] [-S] "
          "[-d debugLevel] [-i] "
       << endl
       << "    [-p path] [-l loadFile]" << endl
       << "     [-D] [-N] [-L rejectDir] [-T timeZone]" << endl
       << "    [-U username]" << endl
       << endl;

  cout << "\n\n" << (*fVisibleOptions) << endl;

  cout << "    Example1:" << endl
       << "        " << fPrgmName << " -j 1234" << endl
       << "    Example2: Some column values are enclosed within double quotes." << endl
       << "        " << fPrgmName << " -j 3000 -E '\"'" << endl
       << "    Example3: Import a nation table without a Job XML file" << endl
       << "        " << fPrgmName << " -j 301 tpch nation nation.tbl" << endl;

  exit(1);
}

//-----------------------------------------------------------------------------

void WECmdArgs::parseCmdLineArgs(int argc, char** argv)
{
  std::string importPath;

  if (argc > 0)
    fPrgmName = string(MCSBINDIR) + "/" + "cpimport.bin";  // argv[0] is splitter but we need cpimport

  po::positional_options_description pos_opt;
  pos_opt.add("dbname", 1)
    .add("table", 1)
    .add("load-file", 1);

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(*fOptions).positional(pos_opt).run(), vm);
  po::notify(vm);

  if (vm.contains("help"))
  {
    fHelp = true;
    usage();
    return;
  }
  if (vm.contains("separator"))
  {
    auto value = vm["separator"].as<std::string>();
    if (value == "\\t")
    {
      fColDelim = '\t';
    }
    else
    {
      fColDelim = value[0];
    }
  }
  if (vm.contains("binary-mode"))
  {
    int value = vm["binary-mode"].as<int>();
    if (value == 1)
    {
      fImportDataMode = IMPORT_DATA_BIN_ACCEPT_NULL;
    }
    else if (value == 2)
    {
      fImportDataMode = IMPORT_DATA_BIN_SAT_NULL;
    }
    else
    {
      startupError("Invalid Binary mode; value can be 1 or 2");
    }
  }
  if (vm.contains("tz"))
  {
    auto tz = vm["tz"].as<std::string>();
    long offset;
    if (tz != "SYSTEM" && dataconvert::timeZoneToOffset(tz.c_str(), tz.size(), &offset))
    {
      startupError("Value for option --tz/-T is invalid");
    }
    fTimeZone = tz;
  }
  if (vm.contains("job-id"))
  {
    errno = 0;
    string optarg = vm["job-id"].as<std::string>();
    long lValue = strtol(optarg.c_str(), nullptr, 10);
    if (errno != 0 || lValue < 0 || lValue > INT_MAX)
    {
      startupError("Option --job-id/-j is invalid or outof range");
    }
    fJobId = optarg;
    fOrigJobId = fJobId;

    if (0 == fJobId.length())
    {
      startupError("Wrong JobID Value");
    }
  }
  if (vm.contains("allow-missing-columns"))
  {
    if (vm["allow-missing-columns"].as<string>() == "AllowMissingColumn")
    {
      fAllowMissingColumn = true;
    }
  }

  if (fArgMode != -1)
    fMode = fArgMode;  // BUG 4210

  if (2 == fArgMode && fPmFilePath.empty())
    throw runtime_error("-f option is mandatory with mode 2.");

  if (vm.contains("dbname"))
  {
    fSchema = vm["dbname"].as<std::string>();
  }
  if (vm.contains("table"))
  {
    fTable = vm["table"].as<std::string>();
  }
  if (vm.contains("load-file"))
  {
    fLocFile = vm["load-file"].as<std::string>();
  }
}

void WECmdArgs::fillParams(BulkLoad& curJob, std::string& sJobIdStr, std::string& sXMLJobDir,
                           std::string& sModuleIDandPID, bool& bLogInfo2ToConsole, std::string& xmlGenSchema,
                           std::string& xmlGenTable, bool& bValidateColumnList)
{
  std::string importPath;
  std::string rptFileName;
  bool bImportFileArg = false;
  BulkModeType bulkMode = BULK_MODE_LOCAL;
  std::string jobUUID;

  curJob.setReadBufferCount(fIOReadBufSize);
  curJob.setReadBufferSize(fReadBufSize);
  if (fMaxErrors >= 0)
  {
    curJob.setMaxErrorCount(fMaxErrors);
  }
  if (!fPmFilePath.empty())
  {
    importPath = fPmFilePath;
    string setAltErrMsg;
    if (curJob.setAlternateImportDir(importPath, setAltErrMsg) != NO_ERROR)
    {
      startupError(setAltErrMsg, false);
    }
  }
  bLogInfo2ToConsole = fConsoleLog;
  sJobIdStr = fJobId;
  curJob.setKeepRbMetaFiles(fKeepRollbackMetaData);
  bulkMode = static_cast<BulkModeType>(fMode);
  curJob.setNullStringMode(fNullStrMode);
  sXMLJobDir = fJobPath;
  curJob.setNoOfReadThreads(fNoOfReadThrds);
  curJob.setColDelimiter(fColDelim);
  curJob.setJobUUID(fUUID);
  curJob.setNoOfParseThreads(fNoOfWriteThrds);
  curJob.setVbufReadSize(fReadBufSize);
  if (fEscChar != -1)
  {
    curJob.setEscapeChar(fEscChar);
  }
  if (fEnclosedChar != -1)
  {
    curJob.setEnclosedByChar(fEnclosedChar);
  }
  curJob.setImportDataMode(fImportDataMode);
  curJob.setErrorDir(fErrorDir);
  sModuleIDandPID = fModuleIDandPID;
  rptFileName = fReportFilename;
  curJob.setTruncationAsError(fbTruncationAsError);
  if (!fTimeZone.empty())
  {
    long offset;
    if (dataconvert::timeZoneToOffset(fTimeZone.c_str(), fTimeZone.size(), &offset))
    {
      startupError("Invalid timezone specified");
    }
    curJob.setTimeZone(offset);
  }
  bValidateColumnList = !fAllowMissingColumn;
  curJob.disableTimeOut(fDisableTableLockTimeOut);
  curJob.disableConsoleOutput(fSilent);
  curJob.setS3Key(fS3Key);
  curJob.setS3Bucket(fS3Bucket);
  curJob.setS3Secret(fS3Secret);
  curJob.setS3Region(fS3Region);
  curJob.setS3Host(fS3Host);
  if (!fUsername.empty())
  {
    curJob.setUsername(fUsername);
  }
  curJob.setSkipRows(fSkipRows);

  curJob.setDefaultJobUUID();

  // Inconsistent to specify -f STDIN with -l importFile
  if (bImportFileArg && importPath == "STDIN")
  {
    startupError(std::string("-f STDIN is invalid with -l importFile."), true);
  }

  // If distributed mode, make sure report filename is specified and that we
  // can create the file using the specified path.
  if (bulkMode == BULK_MODE_REMOTE_SINGLE_SRC || bulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC)
  {
    if (rptFileName.empty())
    {
      startupError(std::string("Bulk modes 1 and 2 require -R rptFileName."), true);
    }
    else
    {
      std::ofstream rptFile(rptFileName.c_str());

      if (rptFile.fail())
      {
        std::ostringstream oss;
        oss << "Unable to open report file " << rptFileName;
        startupError(oss.str(), false);
      }

      rptFile.close();
    }

    curJob.setBulkLoadMode(bulkMode, rptFileName);
  }

  // Get positional arguments, User can provide:
  // 1. no positional parameters
  // 2. Two positional parameters (schema and table names)
  // 3. Three positional parameters (schema, table, and import file name)
  if (!fSchema.empty())
  {
    xmlGenSchema = fSchema;

    if (!fTable.empty())
    {
      // Validate invalid options in conjunction with 2-3 positional
      // parameter mode, which means we are using temp Job XML file.
      if (bImportFileArg)
      {
        startupError(std::string("-l importFile is invalid with positional parameters"), true);
      }

      if (!sXMLJobDir.empty())
      {
        startupError(std::string("-p path is invalid with positional parameters."), true);
      }

      if (importPath == "STDIN")
      {
        startupError(std::string("-f STDIN is invalid with positional parameters."), true);
      }

      xmlGenTable = fTable;

      if (!fLocFile.empty())
      {
        // 3rd pos parm
        curJob.addToCmdLineImportFileList(fLocFile);

        // Default to CWD if loadfile name given w/o -f path
        if (importPath.empty())
        {
          std::string setAltErrMsg;

          if (curJob.setAlternateImportDir(std::string("."), setAltErrMsg) != NO_ERROR)
            startupError(setAltErrMsg, false);
        }
      }
      else
      {
        // Invalid to specify -f if no load file name given
        if (!importPath.empty())
        {
          startupError(std::string("-f requires 3rd positional parameter (load file name)."), true);
        }

        // Default to STDIN if no import file name given
        std::string setAltErrMsg;

        if (curJob.setAlternateImportDir(std::string("STDIN"), setAltErrMsg) != NO_ERROR)
          startupError(setAltErrMsg, false);
      }
    }
    else
    {
      startupError(std::string("No table name specified with schema."), true);
    }
  }
  else
  {
    // JobID is a required parameter with no positional parm mode,
    // because we need the jobid to identify the input job xml file.
    if (sJobIdStr.empty())
    {
      startupError(std::string("No JobID specified."), true);
    }
  }

  // Dump some configuration info
  if (!fSilent)
  {
    if (fDebugLvl != 0)
    {
      cout << "Debug level is set to " << fDebugLvl << endl;
    }
    if (fNoOfReadThrds != 0)
    {
      cout << "number of read threads : " << fNoOfReadThrds << endl;
    }
    if (fColDelim != 0)
    {
      cout << "Column delimiter : " << (fColDelim == '\t' ? "\\t" : string{fColDelim}) << endl;
    }
    if (fNoOfWriteThrds != 0)
    {
      cout << "number of parse threads : " << fNoOfWriteThrds << endl;
    }
    if (fEscChar != 0)
    {
      cout << "Escape Character : " << fEscChar << endl;
    }
    if (fEnclosedChar != 0)
    {
      cout << "Enclosed by Character : " << fEnclosedChar << endl;
    }
  }
}

void WECmdArgs::startupError(const std::string& errMsg, bool showHint) const
{
  BRMWrapper::getInstance()->finishCpimportJob(fCpimportJobId);
  // Log to console
  if (!BulkLoad::disableConsoleOutput())
    cerr << errMsg << endl;

  if (showHint && !fSilent)
  {
    cerr << "Try '" << fPrgmName << " -h' for more information." << endl;
  }

  // Log to syslog
  logging::Message::Args errMsgArgs;
  errMsgArgs.add(errMsg);
  SimpleSysLog::instance()->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0087);

  std::string jobIdStr("0");
  logging::Message::Args endMsgArgs;
  endMsgArgs.add(jobIdStr);
  endMsgArgs.add("FAILED");
  SimpleSysLog::instance()->logMsg(endMsgArgs, logging::LOG_TYPE_INFO, logging::M0082);

  exit(EXIT_FAILURE);
}

} /* namespace WriteEngine */
