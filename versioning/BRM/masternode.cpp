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

/*****************************************************************************
 * $Id: masternode.cpp 1931 2013-07-08 16:53:02Z bpaul $
 *
 ****************************************************************************/

/*
 * The executable source that runs a Master DBRM Node.
 */

#include "masterdbrmnode.h"
#include "liboamcpp.h"
#include <unistd.h>
#include <signal.h>
#include <exception>
#include <string>
#include <clocale>
#include "IDBPolicy.h"
#include "brmtypes.h"
#include "service.h"
#include "jobstep.h"

#include "crashtrace.h"

#define MAX_RETRIES 10

BRM::MasterDBRMNode* m;
bool die = false;

using namespace std;
using namespace BRM;

class Opt
{
 protected:
  const char* m_progname;
  bool m_fg;

 public:
  Opt(int argc, char** argv) : m_progname(argv[0]), m_fg(argc >= 2 && string(argv[1]) == "fg")
  {
  }
};

class ServiceControllerNode : public Service, public Opt
{
 protected:
  void setupChildSignalHandlers();

 public:
  ServiceControllerNode(const Opt& opt) : Service("ControllerNode"), Opt(opt)
  {
  }
  void LogErrno() override
  {
    perror(m_progname);
    log_errno(std::string(m_progname));
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    log(str, logging::LOG_TYPE_INFO);
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
};

void stop(int num)
{
#ifdef BRM_VERBOSE
  std::cerr << "stopping..." << std::endl;
#endif
  die = true;

  if (m != NULL)
    m->stop();
}

void restart(int num)
{
#ifdef BRM_VERBOSE
  std::cerr << "stopping this instance..." << std::endl;
#endif

  if (m != NULL)
    m->stop();
}

/*  doesn't quite work yet...
void reload(int num)
{
#ifdef BRM_VERBOSE
        std::cerr << "reloading the config file" << std::endl;
#endif
        m->lock();
        try {
                m->reload();
        }
        catch (std::exception &e) {
                m->setReadOnly(true);
                std::cerr << "Reload failed.  Check the config file.  Reverting to read-only mode."
                        << std::endl;
        }
        m->unlock();
}
*/

void ServiceControllerNode::setupChildSignalHandlers()
{
  /* XXXPAT: we might want to install signal handlers for every signal */

  signal(SIGINT, stop);
  signal(SIGTERM, stop);
#ifndef _MSC_VER
  signal(SIGHUP, SIG_IGN);
  signal(SIGUSR1, restart);
  signal(SIGPIPE, SIG_IGN);
#endif
  struct sigaction ign;

  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &ign, 0);
  sigaction(SIGABRT, &ign, 0);
  sigaction(SIGFPE, &ign, 0);
}

int ServiceControllerNode::Child()
{
  int retries = 0;

  (void)config::Config::makeConfig();

  setupChildSignalHandlers();

  idbdatafile::IDBPolicy::configIDBPolicy();

  m = NULL;

  while (retries < MAX_RETRIES && !die)
  {
    try
    {
      if (m != NULL)
        delete m;

      m = new BRM::MasterDBRMNode();

      NotifyServiceStarted();

      m->run();
      retries = 0;
      delete m;
      m = NULL;
    }
    catch (std::exception& e)
    {
      ostringstream os;
      os << e.what();
      os << "... attempt #" << retries + 1 << "/" << MAX_RETRIES << " to restart the  DBRM controller node";
      cerr << os.str() << endl;
      log(os.str());
      sleep(5);
    }

    retries++;
  }

  if (retries == MAX_RETRIES)
  {
    NotifyServiceInitializationFailed();
    log(string("Exiting after too many errors"));
    return 1;
  }

  std::cerr << "Exiting..." << std::endl;
  return 0;
}

int main(int argc, char** argv)
{
  Opt opt(argc, argv);
  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");

  BRM::logInit(BRM::SubSystemLogId_controllerNode);

  /*
    Need to shutdown TheadPool before fork(),
    otherwise it would get stuck when trying to join fPruneThread.
  */
  joblist::JobStep::jobstepThreadPool.stop();

  return ServiceControllerNode(opt).Run();
}
