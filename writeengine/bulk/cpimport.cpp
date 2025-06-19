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
#include <clocale>

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
        cerr << endl << "Error in loading job data" << endl;
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
