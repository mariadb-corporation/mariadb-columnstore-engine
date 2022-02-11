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
 * $Id: we_server.cpp 4700 2013-07-08 16:43:49Z bpaul $
 *
 *******************************************************************************/

#include <unistd.h>
#include <iostream>
#include <string>
#include <sstream>
#ifndef _MSC_VER
#include <signal.h>
#include <stdexcept>
#include "logger.h"
#endif
#ifndef _MSC_VER
#include <sys/resource.h>
#endif
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;

#include "threadpool.h"
using namespace threadpool;

#include "we_readthread.h"

#include "liboamcpp.h"
using namespace oam;

#include "distributedenginecomm.h"
#include "IDBPolicy.h"
#include "dbrm.h"

#include "crashtrace.h"

#include "mariadb_my_sys.h"

#include "service.h"

using namespace WriteEngine;

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
        case 'd': m_debug++; break;
        case 'f': m_fg = true; break;
        case '?':
        default: break;
      }
    }
  }
};

class ServiceWriteEngine : public Service, public Opt
{
  void log(logging::LOG_TYPE type, const std::string& str)
  {
    logging::LoggingID logid(SUBSYSTEM_ID_WE_SRV);
    logging::Message::Args args;
    logging::Message msg(1);
    args.add(str);
    msg.format(args);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(type, msg, logid);
  }
  int setupResources();
  void setupChildSignalHandlers();

 public:
  ServiceWriteEngine(const Opt& opt) : Service("WriteEngine"), Opt(opt)
  {
  }
  void LogErrno() override
  {
    log(logging::LOG_TYPE_CRITICAL, strerror(errno));
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    log(logging::LOG_TYPE_INFO, str);
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
};

void added_a_pm(int)
{
  logging::LoggingID logid(21, 0, 0);
  logging::Message::Args args1;
  logging::Message msg(1);
  args1.add("we_server caught SIGHUP. Resetting connections");
  msg.format(args1);
  logging::Logger logger(logid.fSubsysID);
  logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);
  joblist::DistributedEngineComm::reset();
}
}  // namespace

int ServiceWriteEngine::setupResources()
{
#ifndef _MSC_VER
  struct rlimit rlim;

  if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
  {
    return -1;
  }

  rlim.rlim_cur = rlim.rlim_max = 65536;

  if (setrlimit(RLIMIT_NOFILE, &rlim) != 0)
  {
    return -2;
  }

  if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
  {
    return -3;
  }

  if (rlim.rlim_cur != 65536)
  {
    return -4;
  }

#endif
  return 0;
}

void ServiceWriteEngine::setupChildSignalHandlers()
{
#ifndef _MSC_VER
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = added_a_pm;
  sigaction(SIGHUP, &sa, 0);
  sa.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &sa, 0);

  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &sa, 0);
  sigaction(SIGABRT, &sa, 0);
  sigaction(SIGFPE, &sa, 0);
#endif
}

int ServiceWriteEngine::Child()
{
  setupChildSignalHandlers();

  // Init WriteEngine Wrapper (including Config Columnstore.xml cache)
  WriteEngine::WriteEngineWrapper::init(WriteEngine::SUBSYSTEM_ID_WE_SRV);

#ifdef _MSC_VER
  // In windows, initializing the wrapper (A dll) does not set the static variables
  // in the main program
  idbdatafile::IDBPolicy::configIDBPolicy();
#endif
  Config weConfig;

  ostringstream serverParms;
  serverParms << "pm" << weConfig.getLocalModuleID() << "_WriteEngineServer";

  // Create MessageQueueServer, with one retry in case the call to bind the
  // known port fails with "Address already in use".
  boost::scoped_ptr<MessageQueueServer> mqs;
  bool tellUser = true;

  for (;;)
  {
    try
    {
      mqs.reset(new MessageQueueServer(serverParms.str()));
      break;
    }
    // @bug4393 Error Handling for MessageQueueServer constructor exception
    catch (runtime_error& re)
    {
      string what = re.what();

      if (what.find("Address already in use") != string::npos)
      {
        if (tellUser)
        {
          cerr << "Address already in use, retrying..." << endl;
          tellUser = false;
        }

        sleep(5);
      }
      else
      {
        // If/when a common logging class or function is added to the
        // WriteEngineServer, we should use that.  In the mean time,
        // I will log this errmsg with inline calls to the logging.
        logging::Message::Args args;
        logging::Message message;
        string errMsg("WriteEngineServer failed to initiate: ");
        errMsg += what;
        args.add(errMsg);
        message.format(args);
        logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
        logging::MessageLog ml(lid);
        ml.logCriticalMessage(message);
        NotifyServiceInitializationFailed();
        return 2;
      }
    }
  }

  int err = 0;
  if (!m_debug)
    err = setupResources();
  string errMsg;

  switch (err)
  {
    case -1:
    case -3: errMsg = "Error getting file limits, please see non-root install documentation"; break;

    case -2: errMsg = "Error setting file limits, please see non-root install documentation"; break;

    case -4:
      errMsg = "Could not install file limits to required value, please see non-root install documentation";
      break;

    default: break;
  }

  if (err < 0)
  {
    Oam oam;
    logging::Message::Args args;
    logging::Message message;
    args.add(errMsg);
    message.format(args);
    logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
    logging::MessageLog ml(lid);
    ml.logCriticalMessage(message);
    cerr << errMsg << endl;

    NotifyServiceInitializationFailed();
    return 2;
  }

  IOSocket ios;
  size_t mt = 20;
  size_t qs = mt * 100;
  ThreadPool tp(mt, qs);

  cout << "WriteEngineServer is ready" << endl;
  NotifyServiceStarted();

  BRM::DBRM dbrm;

  for (;;)
  {
    try  // BUG 4834 -
    {
      ios = mqs->accept();
      // tp.invoke(ReadThread(ios));
      ReadThreadFactory::CreateReadThread(tp, ios, dbrm);
      {
        /*				logging::Message::Args args;
                                        logging::Message message;
                                        string aMsg("WriteEngineServer : New incoming connection");
                                        args.add(aMsg);
                                        message.format(args);
                                        logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
                                        logging::MessageLog ml(lid);
                                        ml.logInfoMessage( message ); */
      }
    }
    catch (std::exception& ex)  // BUG 4834 - log the exception
    {
      logging::Message::Args args;
      logging::Message message;
      string errMsg("WriteEngineServer : Exception caught on accept(): ");
      errMsg += ex.what();
      args.add(errMsg);
      message.format(args);
      logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
      logging::MessageLog ml(lid);
      ml.logCriticalMessage(message);
      break;
    }
  }

  // It is an error to reach here...
  return 1;
}

int main(int argc, char** argv)
{
  Opt opt(argc, argv);

  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");
  // This is unset due to the way we start it
  program_invocation_short_name = const_cast<char*>("WriteEngineServ");
  // Initialize the charset library
  MY_INIT(argv[0]);

  return ServiceWriteEngine(opt).Run();
}
