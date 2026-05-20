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
 * $Id: cpimport.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *******************************************************************************/

#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <clocale>
#include <algorithm>
#include <numeric>

#include <sys/types.h>
#include <unistd.h>
#include <csignal>
#include <cstring>
#include <string>
#include <cerrno>
#include <cstdlib>
#include <sys/time.h>
#include <sys/resource.h>
#include <boost/filesystem/path.hpp>
#include "idberrorinfo.h"
#include "we_simplesyslog.h"
#include "we_bulkload.h"
#include "we_bulkstatus.h"
#include "we_config.h"
#include "we_xmljob.h"
#include "we_xmlgenproc.h"
#include "we_tempxmlgendata.h"
#include "liboamcpp.h"
#include "IDBPolicy.h"
#include "MonitorProcMem.h"
#include "dataconvert.h"
#include "mcsconfig.h"
#include "mariadb_my_sys.h"
#include "we_cmdargs.h"
#include "we_parquet_reader.h"

using namespace std;
using namespace WriteEngine;
using namespace execplan;

namespace
{
const std::string IMPORT_PATH_CWD(".");
unique_ptr<WECmdArgs> cmdArgs;
bool bDebug = false;
uint32_t cpimportJobId = 0;

//@bug 4643: cpimport job ended during setup w/o any err msg.
//           Added a try/catch with logging to main() in case
//           the process was dying with an uncaught exception.
enum TASK
{
  TASK_CMD_LINE_PARSING = 1,
  TASK_INIT_CONFIG_CACHE = 2,
  TASK_BRM_STATE_READY = 3,
  TASK_BRM_STATE_READ_WRITE = 4,
  TASK_SHUTDOWN_PENDING = 5,
  TASK_SUSPEND_PENDING = 6,
  TASK_ESTABLISH_JOBFILE = 7,
  TASK_LOAD_JOBFILE = 8,
  TASK_PROCESS_DATA = 9
};
const char* taskLabels[] = {"",
                            "parsing command line options",
                            "initializing config cache",
                            "checking BRM Ready state",
                            "checking BRM Read/Write state",
                            "checking for pending shutdown",
                            "checking for pending suspend",
                            "establishing job file",
                            "loading job file",
                            "processing data"};

int configureParquetDirectImport(BulkLoad& curJob, ParquetConversionResult& result, std::string& errMsg)
{
  errMsg.clear();
  ParquetImportRuntimeConfig importCfg;
  importCfg.readThreads = cmdArgs->getParquetReadThreads();
  importCfg.queueBytes = cmdArgs->getParquetQueueBytes();
  importCfg.columnWriteThreads = cmdArgs->getNoOfParseThreads();
  importCfg.maxParquetInflightBatches = cmdArgs->getParquetMaxInflightBatches();
  importCfg.dictChunkDedupe = cmdArgs->getParquetDictChunkDedupe();
  importCfg.arrowReaderUseThreads = cmdArgs->getParquetArrowReaderUseThreads();
  ParquetReader::setImportRuntimeConfig(importCfg);

  const std::string inputFile = cmdArgs->getParquetFilePath();
  if (inputFile.empty())
  {
    errMsg = "Parquet input format requires a single load-file argument.";
    return ERR_INVALID_PARAM;
  }

  if (!BulkLoad::disableConsoleOutput())
  {
    cout << "Input format: parquet" << endl;
    cout << "Reading parquet file: " << inputFile << endl;
  }

  curJob.enableParquetDirectImport(inputFile, &result, &errMsg);
  return NO_ERROR;
}
}  // namespace

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigTerm(int /*i*/)
{
  BRMWrapper::getInstance()->finishCpimportJob(cpimportJobId);
  std::cout << "Received SIGTERM to terminate the process..." << std::endl;
  BulkStatus::setJobStatus(EXIT_FAILURE);
}

//------------------------------------------------------------------------------
// Signal handler to catch Control-C signal to terminate the process
//------------------------------------------------------------------------------
void handleControlC(int /*i*/)
{
  BRMWrapper::getInstance()->finishCpimportJob(cpimportJobId);
  if (!BulkLoad::disableConsoleOutput())
    std::cout << "Received Control-C to terminate the process..." << std::endl;

  BulkStatus::setJobStatus(EXIT_FAILURE);
}

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigSegv(int /*i*/)
{
  BRMWrapper::getInstance()->finishCpimportJob(cpimportJobId);
  std::cout << "Received SIGSEGV to terminate the process..." << std::endl;
  BulkStatus::setJobStatus(EXIT_FAILURE);
}

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigAbrt(int /*i*/)
{
  BRMWrapper::getInstance()->finishCpimportJob(cpimportJobId);
  std::cout << "Received SIGABRT to terminate the process..." << std::endl;
  BulkStatus::setJobStatus(EXIT_FAILURE);
}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------
void setupSignalHandlers()
{
  struct sigaction ign;

  // Ignore SIGPIPE signal
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &ign, 0);

  // Ignore SIGHUP signals
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = SIG_IGN;
  sigaction(SIGHUP, &ign, 0);

  // @bug 4344 enable Control-C by disabling this section of code
  // Ignore SIGINT (Control-C) signal
  // memset(&ign, 0, sizeof(ign));
  // ign.sa_handler = SIG_IGN;
  // sigaction(SIGINT, &ign, 0);

  // @bug 4344 enable Control-C by adding this section of code
  // catch Control-C signal to terminate the program
  struct sigaction act;
  memset(&act, 0, sizeof(act));
  act.sa_handler = handleControlC;
  sigaction(SIGINT, &act, 0);

  // catch SIGTERM signal to terminate the program
  memset(&act, 0, sizeof(act));
  act.sa_handler = handleSigTerm;
  sigaction(SIGTERM, &act, 0);

  // catch SIGSEGV signal to terminate the program
  memset(&act, 0, sizeof(act));
  act.sa_handler = handleSigSegv;
  sigaction(SIGSEGV, &act, 0);

  // catch SIGABRT signal to terminate the program
  memset(&act, 0, sizeof(act));
  act.sa_handler = handleSigAbrt;
  sigaction(SIGABRT, &act, 0);
}

//------------------------------------------------------------------------------
// Print the path of the input load file(s), and the name of the job xml file.
//------------------------------------------------------------------------------
void printInputSource(const std::string& alternateImportDir, const std::string& jobDescFile,
                      const std::string& S3Bucket)
{
  if (!S3Bucket.empty())
  {
    cout << "Input file will be read from S3 Bucket : " << S3Bucket << ", file/object : " << jobDescFile
         << endl;
  }
  else if (alternateImportDir.size() > 0)
  {
    if (alternateImportDir == IMPORT_PATH_CWD)
    {
      char cwdBuf[4096];
      char* bufPtr = ::getcwd(cwdBuf, sizeof(cwdBuf));

      if (!(BulkLoad::disableConsoleOutput()))
        cout << "Input file(s) will be read from : " << bufPtr << endl;
    }
    else
    {
      if (!(BulkLoad::disableConsoleOutput()))
        cout << "Input file(s) will be read from : " << alternateImportDir << endl;
    }
  }
  else
  {
    if (!(BulkLoad::disableConsoleOutput()))
      cout << "Input file(s) will be read from Bulkload root directory : " << Config::getBulkRoot() << endl;
  }

  if (!(BulkLoad::disableConsoleOutput()))
    cout << "Job description file : " << jobDescFile << endl;
}

//------------------------------------------------------------------------------
// Get TableOID string for the specified db and table name.
//------------------------------------------------------------------------------
void getTableOID(const std::string& xmlGenSchema, const std::string& xmlGenTable, std::string& tableOIDStr)
{
  OID tableOID = 0;

  execplan::CalpontSystemCatalog::TableName tbl(xmlGenSchema, xmlGenTable);

  try
  {
    boost::shared_ptr<CalpontSystemCatalog> cat =
        CalpontSystemCatalog::makeCalpontSystemCatalog(BULK_SYSCAT_SESSION_ID);
    cat->identity(CalpontSystemCatalog::EC);
    tableOID = cat->tableRID(tbl).objnum;
  }
  catch (std::exception& ex)
  {
    std::ostringstream oss;
    oss << "Unable to set default JobID; " << "Error getting OID for table " << tbl.schema << '.' << tbl.table
        << ": " << ex.what();
    cmdArgs->startupError(oss.str(), false);
  }
  catch (...)
  {
    std::ostringstream oss;
    oss << "Unable to set default JobID; " << "Unknown error getting OID for table " << tbl.schema << '.'
        << tbl.table;
    cmdArgs->startupError(oss.str(), false);
  }

  std::ostringstream oss;
  oss << tableOID;
  tableOIDStr = oss.str();
}
//------------------------------------------------------------------------------
// Construct temporary Job XML file if user provided schema, job, and
// optional load filename.
// tempJobDir   - directory used to store temporary job xml file
// sJobIdStr    - job id (-j) specified by user
// xmlGenSchema - db schema name specified by user (1st positional parm)
// xmlGenTable  - db table name specified by user  (2nd positional parm)
// alternateImportDir - alternate directory for input data files
// sFileName(out)-filename path for temporary job xml file that is created
//------------------------------------------------------------------------------
void constructTempXmlFile(const std::string& tempJobDir, const std::string& sJobIdStr,
                          const std::string& xmlGenSchema, const std::string& xmlGenTable,
                          const std::string& alternateImportDir, const std::string& S3Bucket,
                          const std::string& tableOIDStr, boost::filesystem::path& sFileName)
{
  // Construct the job description file name
  std::string xmlErrMsg;
  int rc = 0;
  std::string localTableOIDStr;
  if (tableOIDStr.empty())
  {
    getTableOID(xmlGenSchema, xmlGenTable, localTableOIDStr);
  }
  else
  {
    localTableOIDStr = tableOIDStr;
  }

  rc = XMLJob::genJobXMLFileName(std::string(), tempJobDir, sJobIdStr,
                                 true,  // using temp job xml file
                                 xmlGenSchema, xmlGenTable, sFileName, xmlErrMsg, localTableOIDStr);

  if (rc != NO_ERROR)
  {
    std::ostringstream oss;
    oss << "cpimport.bin error creating temporary Job XML file name: " << xmlErrMsg;
    cmdArgs->startupError(oss.str(), false);
  }

  printInputSource(alternateImportDir, sFileName.string(), S3Bucket);

  TempXMLGenData genData(sJobIdStr, xmlGenSchema, xmlGenTable);
  XMLGenProc genProc(&genData,
                     false,   // don't log to Jobxml_nnn.log
                     false);  // generate XML file (not a syscat report)

  try
  {
    genProc.startXMLFile();
    execplan::CalpontSystemCatalog::TableName tbl(xmlGenSchema, xmlGenTable);
    genProc.makeTableData(tbl, localTableOIDStr);

    if (!genProc.makeColumnData(tbl))
    {
      std::ostringstream oss;
      oss << "No columns for " << xmlGenSchema << '.' << xmlGenTable;
      cmdArgs->startupError(oss.str(), false);
    }
  }
  catch (runtime_error& ex)
  {
    std::ostringstream oss;
    oss << "cpimport.bin runtime exception constructing temporary "
           "Job XML file: "
        << ex.what();
    cmdArgs->startupError(oss.str(), false);
  }
  catch (exception& ex)
  {
    std::ostringstream oss;
    oss << "cpimport.bin exception constructing temporary "
           "Job XML file: "
        << ex.what();
    cmdArgs->startupError(oss.str(), false);
  }
  catch (...)
  {
    cmdArgs->startupError(std::string("cpimport.bin "
                                      "unknown exception constructing temporary Job XML file"),
                          false);
  }

  genProc.writeXMLFile(sFileName.string());
}

//------------------------------------------------------------------------------
// Verify we are running from a PM node.
//------------------------------------------------------------------------------
void verifyNode()
{
  std::string localModuleType = Config::getLocalModuleType();

  // Validate running on a PM
  if (localModuleType != "pm")
  {
    cmdArgs->startupError(std::string("Exiting, "
                                      "cpimport.bin can only be run on a PM node"),
                          true);
  }
}

//------------------------------------------------------------------------------
// Log initiate message
//------------------------------------------------------------------------------
void logInitiateMsg(const char* initText)
{
  logging::Message::Args initMsgArgs;
  initMsgArgs.add(initText);
  SimpleSysLog::instance()->logMsg(initMsgArgs, logging::LOG_TYPE_INFO, logging::M0086);
}

//------------------------------------------------------------------------------
// Main entry point into the cpimport.bin program
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  setupSignalHandlers();

  // Initialize the charset library
  MY_INIT(argv[0]);

  // Set locale language
  const char* pLoc = setlocale(LC_ALL, "");
  if (pLoc)
  {
    // Log one line
    cout << "Locale = " << pLoc;
  }
  else
  {
    cout << "Failed to set locale ";
  }
  setlocale(LC_NUMERIC, "C");

  // Initialize singleton instance of syslogging
  logging::IDBErrorInfo::instance();
  SimpleSysLog::instance()->setLoggingID(logging::LoggingID(SUBSYSTEM_ID_WE_BULK));

  // Log job initiation unless user is asking for help
  cmdArgs = make_unique<WECmdArgs>(argc, argv);
  std::ostringstream ossArgList;

  for (int m = 1; m < argc; m++)
  {
    if (!strcmp(argv[m], "\t"))  // special case to print a <TAB>
      ossArgList << "'\\t'" << ' ';
    else
      ossArgList << argv[m] << ' ';
  }

  logInitiateMsg(ossArgList.str().c_str());

  BulkLoad curJob;
  string sJobIdStr;
  string sXMLJobDir;
  string sModuleIDandPID;
  bool bLogInfo2ToConsole = false;
  bool bValidateColumnList = true;
  bool bRollback = false;
  bool bForce = false;
  const bool parquetMode = cmdArgs->isParquetMode();
  ParquetConversionResult parquetConversion;
  std::string parquetPrepErrMsg;
  int rc = NO_ERROR;
  std::string exceptionMsg;
  TASK task;  // track tasks being performed
  // set this upfront
  curJob.setErrorDir(string(MCSLOGDIR) + "/cpimport/");
  try
  {
    //--------------------------------------------------------------------------
    // Parse the command line arguments
    //--------------------------------------------------------------------------
    task = TASK_CMD_LINE_PARSING;
    string xmlGenSchema;
    string xmlGenTable;
    cmdArgs->fillParams(curJob, sJobIdStr, sXMLJobDir, sModuleIDandPID, bLogInfo2ToConsole, xmlGenSchema,
                        xmlGenTable, bValidateColumnList);

    //--------------------------------------------------------------------------
    // Save basename portion of program path from argv[0]
    //--------------------------------------------------------------------------
    string base;
    string::size_type startBase = string(argv[0]).rfind('/');

    if (startBase == string::npos)
      base.assign(argv[0]);
    else
      base.assign(argv[0] + startBase + 1);

    curJob.setProcessName(base);

    if (bDebug)
      logInitiateMsg("Command line arguments parsed");

    if (parquetMode)
    {
      task = TASK_PROCESS_DATA;
      rc = configureParquetDirectImport(curJob, parquetConversion, parquetPrepErrMsg);
      if (rc != NO_ERROR)
      {
        cmdArgs->startupError(parquetPrepErrMsg, false);
      }
    }

    //--------------------------------------------------------------------------
    // Init singleton classes (other than syslogging that we already setup)
    //--------------------------------------------------------------------------
    task = TASK_INIT_CONFIG_CACHE;

    // Initialize cache used to store configuration parms from Columnstore.xml
    Config::initConfigCache();

    // Setup signal handlers "again" because HDFS plugin seems to be
    // changing our settings to ignore ctrl-C and sigterm
    setupSignalHandlers();

    // initialize singleton BRM Wrapper.  Also init ExtentRows (in dbrm) from
    // main thread, since ExtentMap::getExtentRows is not thread safe.
    BRMWrapper::getInstance()->getInstance()->getExtentRows();

    //--------------------------------------------------------------------------
    // Validate running on valid node
    //--------------------------------------------------------------------------
    verifyNode();

    //--------------------------------------------------------------------------
    // Set scheduling priority for this cpimport.bin process
    //--------------------------------------------------------------------------
    setpriority(PRIO_PROCESS, 0, Config::getBulkProcessPriority());

    if (bDebug)
      logInitiateMsg("Config cache initialized");

    //--------------------------------------------------------------------------
    // Make sure DMLProc startup has completed before running a cpimport.bin job
    //--------------------------------------------------------------------------
    task = TASK_BRM_STATE_READY;

    if (!BRMWrapper::getInstance()->isSystemReady())
    {
      cmdArgs->startupError(std::string("System is not ready.  Verify that ColumnStore is up and ready "
                                        "before running cpimport."),
                            false);
    }

    if (bDebug)
      logInitiateMsg("BRM state verified: state is Ready");

    //--------------------------------------------------------------------------
    // Verify that the state of BRM is read/write
    //--------------------------------------------------------------------------
    task = TASK_BRM_STATE_READ_WRITE;
    int brmReadWriteStatus = BRMWrapper::getInstance()->isReadWrite();

    if (brmReadWriteStatus != NO_ERROR)
    {
      WErrorCodes ec;
      std::ostringstream oss;
      oss << ec.errorString(brmReadWriteStatus) << "  cpimport.bin is terminating.";
      cmdArgs->startupError(oss.str(), false);
    }

    if (bDebug)
      logInitiateMsg("BRM state is Read/Write");

    //--------------------------------------------------------------------------
    // Make sure we're not about to shutdown
    //--------------------------------------------------------------------------
    task = TASK_SHUTDOWN_PENDING;
    int brmShutdownPending = BRMWrapper::getInstance()->isShutdownPending(bRollback, bForce);

    if (brmShutdownPending != NO_ERROR)
    {
      WErrorCodes ec;
      std::ostringstream oss;
      oss << ec.errorString(brmShutdownPending) << "  cpimport.bin is terminating.";
      cmdArgs->startupError(oss.str(), false);
    }

    if (bDebug)
      logInitiateMsg("Verified no shutdown operation is pending");

    //--------------------------------------------------------------------------
    // Make sure we're not write suspended
    //--------------------------------------------------------------------------
    task = TASK_SUSPEND_PENDING;
    int brmSuspendPending = BRMWrapper::getInstance()->isSuspendPending();

    if (brmSuspendPending != NO_ERROR)
    {
      WErrorCodes ec;
      std::ostringstream oss;
      oss << ec.errorString(brmSuspendPending) << "  cpimport.bin is terminating.";
      cmdArgs->startupError(oss.str(), false);
    }

    if (bDebug)
      logInitiateMsg("Verified no suspend operation is pending");

    //--------------------------------------------------------------------------
    // Set some flags
    //--------------------------------------------------------------------------
    task = TASK_ESTABLISH_JOBFILE;
    BRMWrapper::setUseVb(false);
    Cache::setUseCache(false);

    //--------------------------------------------------------------------------
    // Construct temporary Job XML file if user provided schema, job, and
    // optional load filename.
    //--------------------------------------------------------------------------
    boost::filesystem::path sFileName;
    bool bUseTempJobFile = false;

    if (!BulkLoad::disableConsoleOutput())
      cout << std::endl;  // print blank line before we start

    // Start tracking time to create/load jobfile;
    // The elapsed time for this step is logged at the end of loadJobInfo()
    curJob.startTimer();

    if (!xmlGenSchema.empty())  // create temporary job file name
    {
      // If JobID is not provided, then default to the table OID
      std::string tableOIDStr{""};
      if (sJobIdStr.empty())
      {
        getTableOID(xmlGenSchema, xmlGenTable, tableOIDStr);

        if (!(BulkLoad::disableConsoleOutput()))
          cout << "Using table OID " << tableOIDStr << " as the default JOB ID" << std::endl;

        sJobIdStr = tableOIDStr;
      }

      // No need to validate column list in job XML file for user errors,
      // if cpimport.bin just generated the job XML file on-the-fly.
      bValidateColumnList = false;

      bUseTempJobFile = true;
      constructTempXmlFile(curJob.getTempJobDir(), sJobIdStr, xmlGenSchema, xmlGenTable,
                           curJob.getAlternateImportDir(), curJob.getS3Bucket(), tableOIDStr, sFileName);
    }
    else  // create user's persistent job file name
    {
      // Construct the job description file name
      std::string xmlErrMsg;
      std::string tableOIdStr("");
      rc = XMLJob::genJobXMLFileName(sXMLJobDir, curJob.getJobDir(), sJobIdStr, bUseTempJobFile,
                                     std::string(), std::string(), sFileName, xmlErrMsg, tableOIdStr);

      if (rc != NO_ERROR)
      {
        std::ostringstream oss;
        oss << "cpimport.bin error creating Job XML file name: " << xmlErrMsg;
        cmdArgs->startupError(oss.str(), false);
      }

      printInputSource(curJob.getAlternateImportDir(), sFileName.string(), curJob.getS3Bucket());
    }

    if (bDebug)
      logInitiateMsg("Job xml file is established");

    //-------------------------------------------------------------------------
    // Bug 5415 Add HDFS MemBuffer vs. FileBuffer decision logic.
    // MemoryCheckPercent. This controls at what percent of total memory be
    // consumed by all processes before we switch from HdfsRdwrMemBuffer to
    // HdfsRdwrFileBuffer. This is only used in Hdfs installations.
    //-------------------------------------------------------------------------
    config::Config* cf = config::Config::makeConfig();
    int checkPct = 95;
    string strCheckPct = cf->getConfig("SystemConfig", "MemoryCheckPercent");

    if (strCheckPct.length() != 0)
      checkPct = cf->uFromText(strCheckPct);

    //--------------------------------------------------------------------------
    // If we're HDFS, start the monitor thread.
    // Otherwise, we don't need it, so don't waste the resources.
    //--------------------------------------------------------------------------
    if (idbdatafile::IDBPolicy::useHdfs())
    {
      new boost::thread(utils::MonitorProcMem(0, checkPct, SUBSYSTEM_ID_WE_BULK));
    }

    rc = BRMWrapper::getInstance()->newCpimportJob(cpimportJobId);
    cmdArgs->setCpimportJobId(cpimportJobId);
    // TODO kemm: pass cpimportJobId to WECmdArgs
    if (rc != NO_ERROR)
    {
      WErrorCodes ec;
      std::ostringstream oss;
      oss << "Error in creating new cpimport job on Controller node; " << ec.errorString(rc)
          << "; cpimport is terminating.";
      cmdArgs->startupError(oss.str(), false);
    }

    //--------------------------------------------------------------------------
    // This is the real business
    //--------------------------------------------------------------------------
    task = TASK_LOAD_JOBFILE;
    rc = curJob.loadJobInfo(sFileName.string(), bUseTempJobFile, argc, argv, bLogInfo2ToConsole,
                            bValidateColumnList);

    if (rc != NO_ERROR)
    {
      WErrorCodes ec;
      std::ostringstream oss;
      oss << "Error in loading job information; " << ec.errorString(rc) << "; cpimport.bin is terminating.";
      cmdArgs->startupError(oss.str(), false);
    }

    if (bDebug)
      logInitiateMsg("Job xml file is loaded");

    task = TASK_PROCESS_DATA;

    // Log start of job to INFO log
    logging::Message::Args startMsgArgs;
    startMsgArgs.add(sJobIdStr);
    startMsgArgs.add(curJob.getSchema());
    SimpleSysLog::instance()->logMsg(startMsgArgs, logging::LOG_TYPE_INFO, logging::M0081);

    curJob.printJob();

    rc = curJob.processJob();

    if (rc != NO_ERROR)
    {
      if (!BulkLoad::disableConsoleOutput())
      {
        cerr << endl << "Error in loading job data" << endl;
        if (parquetMode && !parquetPrepErrMsg.empty())
          cerr << parquetPrepErrMsg << endl;
      }
    }

    if (parquetMode && rc == NO_ERROR && !BulkLoad::disableConsoleOutput())
    {
      cout << "Parquet import conversion summary:" << endl;
      cout << "  rows       : " << parquetConversion.stats.totalRows << endl;
      cout << "  columns    : " << parquetConversion.stats.columnCount << endl;
      cout << "  row groups : " << parquetConversion.stats.rowGroupCount << endl;
      cout << "  batches    : " << parquetConversion.stats.batchCount << endl;
      cout << "  elapsed(s) : " << parquetConversion.stats.elapsedSeconds << endl;
      const char* instrEnv = std::getenv("COLUMNSTORE_PARQUET_IMPORT_INSTR");
      const char* instrVerboseEnv = std::getenv("COLUMNSTORE_PARQUET_IMPORT_INSTR_VERBOSE");
      const bool instrVerbose =
          instrVerboseEnv && instrVerboseEnv[0] != '\0' && instrVerboseEnv[0] != '0';
      if (instrEnv && instrEnv[0] != '\0' && instrEnv[0] != '0' &&
          (!parquetConversion.columnInstrumentation.empty() || parquetConversion.hasPipelineInstrumentation ||
           parquetConversion.hasWallClockInstrumentation))
      {
        auto nsToSec = [](uint64_t ns) -> double { return static_cast<double>(ns) / 1e9; };
        auto nsToMs = [](uint64_t ns) -> double { return static_cast<double>(ns) / 1e6; };
        auto pctOf = [](uint64_t partNs, uint64_t totalNs) -> double {
          return totalNs ? (100.0 * static_cast<double>(partNs) / static_cast<double>(totalNs)) : 0.0;
        };

        cout << std::fixed << std::setprecision(3);
        cout << "Parquet import instrumentation (COLUMNSTORE_PARQUET_IMPORT_INSTR):" << endl;
        cout << "  Timing categories:" << endl;
        cout << "    wall-clock stage budget     : single-thread elapsed time per import stage" << endl;
        cout << "    aggregate worker time       : summed across parallel reader/writer threads;"
             << " may exceed wall-clock" << endl;
        cout << "    cumulative batch residence  : summed across all batches;"
             << " may greatly exceed wall-clock" << endl;

        if (parquetConversion.hasWallClockInstrumentation)
        {
          const ParquetWallClockInstrSnapshot& w = parquetConversion.wallClockInstrumentation;
          const uint64_t wallTotal = w.totalNs ? w.totalNs : 1;
          cout << endl << "  Parquet wall-clock stage budget:" << endl;
          cout << "    total parquet elapsed              : " << nsToSec(w.totalNs) << "s  100.0%" << endl;
          cout << "    setup/schema/bindings              : " << nsToSec(w.setupNs) << "s  "
               << pctOf(w.setupNs, wallTotal) << "%" << endl;
          cout << "    start writer threads               : " << nsToSec(w.startWritersNs) << "s  "
               << pctOf(w.startWritersNs, wallTotal) << "%" << endl;
          cout << "    start reader threads               : " << nsToSec(w.startReadersNs) << "s  "
               << pctOf(w.startReadersNs, wallTotal) << "%" << endl;
          cout << "    coordinator dispatch loop          : " << nsToSec(w.coordinatorLoopNs) << "s  "
               << pctOf(w.coordinatorLoopNs, wallTotal) << "%" << endl;
          cout << "    reader join                        : " << nsToSec(w.readerJoinNs) << "s  "
               << pctOf(w.readerJoinNs, wallTotal) << "%" << endl;
          cout << "    writer drain wait                  : " << nsToSec(w.writerDrainNs) << "s  "
               << pctOf(w.writerDrainNs, wallTotal) << "%" << endl;
          cout << "    writer join                        : " << nsToSec(w.writerJoinNs) << "s  "
               << pctOf(w.writerJoinNs, wallTotal) << "%" << endl;
          cout << "    finalize                           : " << nsToSec(w.finalizeNs) << "s  "
               << pctOf(w.finalizeNs, wallTotal) << "%" << endl;
        }

        if (parquetConversion.hasPipelineInstrumentation)
        {
          const ParquetPipelineInstrSnapshot& p = parquetConversion.pipelineInstrumentation;
          cout << endl << "  Parquet aggregate worker time:" << endl;
          cout << "    reader decode aggregate            : " << nsToSec(p.readerDecodeNs) << "s" << endl;
          cout << "    reader push wait aggregate         : " << nsToSec(p.readerPushWaitNs) << "s";
          if (p.readerPushCount > 0)
            cout << "  avg " << nsToMs(p.readerPushWaitNs / p.readerPushCount) << "ms/push";
          cout << endl;
          cout << "    writer task process aggregate      : " << nsToSec(p.writerTaskProcessNs) << "s"
               << "  tasks=" << p.writerTasks << endl;
          cout << "    fixed column aggregate             : " << nsToSec(p.fixedColumnNs) << "s"
               << "  calls=" << p.fixedColumnCalls << endl;
          cout << "    dictionary column aggregate        : " << nsToSec(p.dictionaryColumnNs) << "s"
               << "  calls=" << p.dictionaryColumnCalls << endl;

          cout << endl << "  Parquet queue/backpressure:" << endl;
          cout << "    coordinator pop wait (wall)        : " << nsToSec(p.coordinatorPopWaitNs) << "s";
          if (p.coordinatorPopCount > 0)
            cout << "  avg " << nsToMs(p.coordinatorPopWaitNs / p.coordinatorPopCount) << "ms/pop";
          cout << endl;
          cout << "    coordinator inflight wait (wall)   : " << nsToSec(p.coordinatorInflightWaitNs) << "s"
               << "  episodes=" << p.coordinatorInflightWaitCount << endl;
          cout << "    max inflight observed              : " << p.maxInflightBatchesObserved << endl;
          cout << "    max queue bytes observed           : " << p.maxQueueBytesObserved << endl;
          cout << "    reader batches / rows              : " << p.readerBatches << " / " << p.readerRows
               << endl;
          cout << "    dispatched batches / tasks         : " << p.coordinatorDispatchedBatches << " / "
               << p.coordinatorDispatchedTasks << endl;
          if (p.coordinatorDispatchedBatches > 0)
          {
            cout << "    batch pre-dispatch residence total : " << nsToSec(p.batchPreDispatchResidenceNs)
                 << "s cumulative" << endl;
            cout << "    batch pre-dispatch residence avg   : "
                 << nsToMs(p.batchPreDispatchResidenceNs / p.coordinatorDispatchedBatches) << "ms per batch"
                 << endl;
          }
          else
          {
            cout << "    batch pre-dispatch residence total : 0s cumulative" << endl;
            cout << "    batch pre-dispatch residence avg   : n/a" << endl;
          }
          cout << "    true reorder hold total            : " << nsToSec(p.trueReorderHoldNs) << "s cumulative"
               << endl;
          cout << "    true reorder held batches          : " << p.trueReorderHeldBatches << endl;
          if (p.trueReorderHeldBatches > 0)
            cout << "    true reorder hold avg              : "
                 << nsToMs(p.trueReorderHoldNs / p.trueReorderHeldBatches) << "ms per held batch" << endl;
          else
            cout << "    true reorder hold avg              : n/a" << endl;
          cout << "    writer queue pop wait aggregate    : " << nsToSec(p.writerQueuePopWaitNs) << "s";
          if (p.writerQueuePopCount > 0)
            cout << "  avg " << nsToMs(p.writerQueuePopWaitNs / p.writerQueuePopCount) << "ms/pop";
          cout << endl;
        }

        if (!parquetConversion.columnInstrumentation.empty())
        {
          cout << endl << "  Column conversion counters:" << endl;
          cout << "    dict-chunk dedupe: " << (parquetConversion.dictChunkDedupeEnabled ? "enabled" : "disabled")
               << endl;
          uint64_t sumDictRows = 0;
          uint64_t sumDictCanonHits = 0;
          uint64_t sumDictChunkDistinct = 0;
          uint64_t sumDictChunks = 0;
          uint64_t sumTemporalFast = 0;
          uint64_t sumTemporalFallback = 0;
          for (const auto& s : parquetConversion.columnInstrumentation)
          {
            sumDictRows += s.dictRows;
            sumDictCanonHits += s.dictCanonHits;
            sumDictChunkDistinct += s.dictChunkDistinctSum;
            sumDictChunks += s.dictChunks;
            sumTemporalFast += s.temporalArrowFast;
            sumTemporalFallback += s.temporalScalarFallback;
          }
          cout << "    dictRows=" << sumDictRows << " dictCanonHits=" << sumDictCanonHits
               << " dictChunkDistinctSum=" << sumDictChunkDistinct << " dictChunks=" << sumDictChunks;
          if (sumDictChunks > 0)
            cout << " avgDistinct/chunk="
                 << (static_cast<double>(sumDictChunkDistinct) / static_cast<double>(sumDictChunks));
          else if (!parquetConversion.dictChunkDedupeEnabled)
            cout << " avgDistinct/chunk=n/a (dedupe off)";
          cout << endl;
          cout << "    temporalArrowFast=" << sumTemporalFast << " temporalScalarFallback=" << sumTemporalFallback
               << endl;

          const uint64_t writerTaskAggNs =
              parquetConversion.hasPipelineInstrumentation
                  ? parquetConversion.pipelineInstrumentation.writerTaskProcessNs
                  : 0;
          std::vector<size_t> colOrder(parquetConversion.columnInstrumentation.size());
          std::iota(colOrder.begin(), colOrder.end(), 0);
          std::sort(colOrder.begin(), colOrder.end(), [&](size_t a, size_t b) {
            const auto& ca = parquetConversion.columnInstrumentation[a];
            const auto& cb = parquetConversion.columnInstrumentation[b];
            const uint64_t ta = ca.fixedColumnNs + ca.dictionaryColumnNs;
            const uint64_t tb = cb.fixedColumnNs + cb.dictionaryColumnNs;
            return ta > tb;
          });
          size_t printedSlow = 0;
          for (size_t k = 0; k < colOrder.size() && printedSlow < 5; ++k)
          {
            const size_t idx = colOrder[k];
            const auto& s = parquetConversion.columnInstrumentation[idx];
            const uint64_t tsum = s.fixedColumnNs + s.dictionaryColumnNs;
            if (tsum == 0)
              continue;
            if (printedSlow == 0)
              cout << endl << "  Slowest Parquet columns by aggregate writer time:" << endl;
            const std::string& cname =
                (idx < parquetConversion.columnNames.size()) ? parquetConversion.columnNames[idx] : std::string("?");
            const double colPct =
                writerTaskAggNs ? (100.0 * static_cast<double>(tsum) / static_cast<double>(writerTaskAggNs)) : 0.0;
            cout << "    column[" << idx << "] " << cname << "      : " << nsToSec(tsum) << "s  " << colPct
                 << "% of writer task aggregate" << endl;
            ++printedSlow;
          }

          if (instrVerbose)
          {
            for (size_t i = 0; i < parquetConversion.columnInstrumentation.size(); ++i)
            {
              const std::string& cname =
                  (i < parquetConversion.columnNames.size()) ? parquetConversion.columnNames[i] : std::string("?");
              const auto& s = parquetConversion.columnInstrumentation[i];
              const uint64_t colTotal = s.fixedColumnNs + s.dictionaryColumnNs;
              cout << endl << "  column[" << i << "] " << cname << ":" << endl;
              if (colTotal > 0)
              {
                cout << "    writer total aggregate             : " << nsToSec(colTotal) << "s" << endl;
                cout << "    fixed aggregate                    : " << nsToSec(s.fixedColumnNs) << "s"
                     << "  calls=" << s.fixedColumnCalls << endl;
                cout << "    dictionary aggregate               : " << nsToSec(s.dictionaryColumnNs) << "s"
                     << "  calls=" << s.dictionaryColumnCalls << endl;
              }
              cout << "    dictRows=" << s.dictRows << " dictNulls=" << s.dictNulls
                   << " dictDctnryCalls=" << s.dictDctnryCalls << " dictCanonHits=" << s.dictCanonHits << endl;
              cout << "    dictChunkDistinctSum=" << s.dictChunkDistinctSum << " dictChunks=" << s.dictChunks;
              if (s.dictChunks > 0)
                cout << " avgDistinct/chunk=" << (static_cast<double>(s.dictChunkDistinctSum) /
                                                      static_cast<double>(s.dictChunks));
              cout << endl;
              cout << "    temporalArrowFast=" << s.temporalArrowFast
                   << " temporalScalarFallback=" << s.temporalScalarFallback << endl;
            }
          }
        }

        cout << endl << "  Interpretation hints:" << endl;
        cout << "    high coordinator pop wait:" << endl;
        cout << "      readers are not feeding batches fast enough" << endl;
        cout << "    high reader push wait or max queue bytes reached:" << endl;
        cout << "      readers are producing faster than downstream can consume" << endl;
        cout << "    high coordinator inflight wait:" << endl;
        cout << "      writer side is the limiting stage or inflight limit is too small" << endl;
        cout << "    high writer task process aggregate:" << endl;
        cout << "      writer-side conversion/dictionary/write work dominates" << endl;
        cout << "    high true reorder hold:" << endl;
        cout << "      multiple readers are producing out-of-order batches" << endl;
        cout << "    aggregate worker times may exceed wall time:" << endl;
        cout << "      values are summed across parallel threads" << endl;
        cout << "    cumulative batch residence may exceed wall time:" << endl;
        cout << "      values are summed across all batches" << endl;
        cout << std::defaultfloat << std::setprecision(6);
      }
    }
  }
  catch (std::exception& ex)
  {
    std::ostringstream oss;
    oss << "Uncaught exception caught in cpimport.bin main() while " << taskLabels[task] << "; " << ex.what();
    exceptionMsg = oss.str();

    if (task != TASK_PROCESS_DATA)
    {
      cmdArgs->startupError(exceptionMsg, false);
    }

    rc = ERR_UNKNOWN;
  }

  BRMWrapper::getInstance()->finishCpimportJob(cpimportJobId);
  // Free up resources allocated by MY_INIT() above.
  my_end(0);

  //--------------------------------------------------------------------------
  // Log end of job to INFO log
  //--------------------------------------------------------------------------
  logging::Message::Args endMsgArgs;
  endMsgArgs.add(sJobIdStr);

  if (rc != NO_ERROR)
  {
    std::string failMsg("FAILED");

    if (exceptionMsg.length() > 0)
    {
      failMsg += "; ";
      failMsg += exceptionMsg;
    }

    endMsgArgs.add(failMsg);
  }
  else
  {
    endMsgArgs.add("SUCCESS");
  }

  SimpleSysLog::instance()->logMsg(endMsgArgs, logging::LOG_TYPE_INFO, logging::M0082);

  if (rc != NO_ERROR)
    return (EXIT_FAILURE);
  else
    return (EXIT_SUCCESS);
}
