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

/******************************************************************************************
******************************************************************************************/
#include <string>
#include <unistd.h>
#include <signal.h>
#include <clocale>
using namespace std;

#include "ddlproc.h"
#include "ddlprocessor.h"
#include "messageobj.h"
#include "messagelog.h"
#include "configcpp.h"
using namespace logging;

using namespace config;

#include "liboamcpp.h"
using namespace oam;

#include "distributedenginecomm.h"
using namespace joblist;

//#include "boost/filesystem/operations.hpp"
//#include "boost/filesystem/path.hpp"
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>

using namespace boost;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "writeengine.h"
#include "cacheutils.h"
#include "../writeengine/client/we_clients.h"
#include "dbrm.h"
#include "IDBPolicy.h"

#include "crashtrace.h"
#include "installdir.h"

#include "mariadb_my_sys.h"

#include "service.h"

namespace
{
class Opt
{
 public:
  int m_debug;
  bool m_fg;
  Opt(int argc, char* argv[]) : m_debug(0), m_fg(false)
  {
    int c;
    while ((c = getopt(argc, argv, "df")) != EOF)
    {
      switch (c)
      {
        case 'd':
          m_debug++;  // TODO: not really used yes
          break;
        case 'f': m_fg = true; break;
        case '?':
        default: break;
      }
    }
  }
};

class ServiceDDLProc : public Service, public Opt
{
 protected:
  void setupChildSignalHandlers();

  void log(logging::LOG_TYPE type, const std::string& str)
  {
    LoggingID logid(23, 0, 0);
    Message::Args args;
    Message message(8);
    args.add(str);
    message.format(args);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, message, logid);
  }

 public:
  ServiceDDLProc(const Opt& opt) : Service("DDLProc"), Opt(opt)
  {
  }
  void LogErrno() override
  {
    log(LOG_TYPE_CRITICAL, std::string(strerror(errno)));
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    log(LOG_TYPE_INFO, str);
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
};

DistributedEngineComm* Dec;

int8_t setupCwd()
{
  string workdir = startup::StartUp::tmpDir();

  if (workdir.length() == 0)
    workdir = ".";

  int8_t rc = chdir(workdir.c_str());
  return rc;
}

void added_a_pm(int)
{
  LoggingID logid(23, 0, 0);
  logging::Message::Args args1;
  logging::Message msg(1);
  args1.add("DDLProc caught SIGHUP. Resetting connections");
  msg.format(args1);
  logging::Logger logger(logid.fSubsysID);
  logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
  Dec->Setup();
}
}  // namespace

void ServiceDDLProc::setupChildSignalHandlers()
{
#ifndef _MSC_VER
  /* set up some signal handlers */
  struct sigaction ign;
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = added_a_pm;
  sigaction(SIGHUP, &ign, 0);
  ign.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &ign, 0);
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &ign, 0);
  sigaction(SIGABRT, &ign, 0);
  sigaction(SIGFPE, &ign, 0);
#endif
}

int ServiceDDLProc::Child()
{
  if (setupCwd() < 0)
  {
    LoggingID logid(23, 0, 0);
    logging::Message::Args args1;
    logging::Message msg(9);
    args1.add("DDLProc could not set working directory ");
    msg.format(args1);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, msg, logid);
    NotifyServiceInitializationFailed();
    return 1;
  }

  WriteEngine::WriteEngineWrapper::init(WriteEngine::SUBSYSTEM_ID_DDLPROC);
#ifdef _MSC_VER
  // In windows, initializing the wrapper (A dll) does not set the static variables
  // in the main program
  idbdatafile::IDBPolicy::configIDBPolicy();
#endif

  ResourceManager* rm = ResourceManager::instance();
  Dec = DistributedEngineComm::instance(rm);

  setupChildSignalHandlers();

  ddlprocessor::DDLProcessor ddlprocessor(1, 20);

  NotifyServiceStarted();

  try
  {
    ddlprocessor.process();
  }
  catch (std::exception& ex)
  {
    cerr << ex.what() << endl;
    LoggingID logid(23, 0, 0);
    Message::Args args;
    Message message(8);
    args.add("DDLProc failed on: ");
    args.add(ex.what());
    message.format(args);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, message, logid);
    return 1;
  }
  catch (...)
  {
    cerr << "Caught unknown exception!" << endl;
    LoggingID logid(23, 0, 0);
    Message::Args args;
    Message message(8);
    args.add("DDLProc failed on: ");
    args.add("receiving DDLPackage (unknown exception)");
    message.format(args);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, message, logid);
    return 1;
  }

  return 0;
}

int main(int argc, char** argv)
{
  Opt opt(argc, argv);
  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");
  // This is unset due to the way we start it
  program_invocation_short_name = const_cast<char*>("DDLProc");
  // Initialize the charset library
  MY_INIT(argv[0]);

  return ServiceDDLProc(opt).Run();
}
// vim:ts=4 sw=4:
