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
 * $Id: slavenode.cpp 1931 2013-07-08 16:53:02Z bpaul $
 *
 ****************************************************************************/

#include <iostream>
#include <signal.h>
#include <string>
#include <clocale>
#ifdef _MSC_VER
#include <process.h>
#include <winsock2.h>
#endif
#include "slavedbrmnode.h"
#include "slavecomm.h"
#include "liboamcpp.h"
#include "brmtypes.h"
#include "rwlockmonitor.h"

#include "IDBPolicy.h"

#include "crashtrace.h"
#include "service.h"
#include "jobstep.h"

using namespace BRM;
using namespace std;

SlaveComm* comm;
bool die = false;
boost::thread_group monitorThreads;

class Opt
{
 protected:
  const char* m_progname;
  const char* m_nodename;
  bool m_fg;

 public:
  Opt(int argc, char** argv)
   : m_progname(argv[0]), m_nodename(argc > 1 ? argv[1] : nullptr), m_fg(argc > 2 && string(argv[2]) == "fg")
  {
  }
};

class ServiceWorkerNode : public Service, public Opt
{
 protected:
  void setupChildSignalHandlers();

 public:
  ServiceWorkerNode(const Opt& opt) : Service("WorkerNode"), Opt(opt)
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

void stop(int sig)
{
  if (!die)
  {
    die = true;
    comm->stop();
    monitorThreads.interrupt_all();
  }
}

void reset(int sig)
{
  comm->reset();
}

void ServiceWorkerNode::setupChildSignalHandlers()
{
#ifdef SIGHUP
  signal(SIGHUP, reset);
#endif
  signal(SIGINT, stop);
  signal(SIGTERM, stop);
#ifdef SIGPIPE
  signal(SIGPIPE, SIG_IGN);
#endif

  struct sigaction ign;
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &ign, 0);
  sigaction(SIGABRT, &ign, 0);
  sigaction(SIGFPE, &ign, 0);
}

int ServiceWorkerNode::Child()
{
  setupChildSignalHandlers();

  SlaveDBRMNode slave;
  ShmKeys keys;

  try
  {
    comm = new SlaveComm(std::string(m_nodename), &slave);
    NotifyServiceStarted();
  }
  catch (exception& e)
  {
    ostringstream os;
    os << "An error occured: " << e.what();
    cerr << os.str() << endl;
    log(os.str());
    NotifyServiceInitializationFailed();
    return 1;
  }

  /* Start 4 threads to monitor write lock state */
  monitorThreads.create_thread(RWLockMonitor(&die, slave.getEMFLLockStatus(), keys.KEYRANGE_EMFREELIST_BASE));
  monitorThreads.create_thread(RWLockMonitor(&die, slave.getEMLockStatus(), keys.KEYRANGE_EXTENTMAP_BASE));
  monitorThreads.create_thread(RWLockMonitor(&die, slave.getVBBMLockStatus(), keys.KEYRANGE_VBBM_BASE));
  monitorThreads.create_thread(RWLockMonitor(&die, slave.getVSSLockStatus(), keys.KEYRANGE_VSS_BASE));

  try
  {
    comm->run();
  }
  catch (exception& e)
  {
    ostringstream os;
    os << "An error occurred: " << e.what();
    cerr << os.str() << endl;
    log(os.str());
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

  BRM::logInit(BRM::SubSystemLogId_workerNode);

  /*
    Need to shutdown TheadPool before fork(),
    otherwise it would get stuck when trying to join fPruneThread.
  */
  joblist::JobStep::jobstepThreadPool.stop();

  if (argc < 2)
  {
    ostringstream os;
    os << "Usage: " << argv[0] << " DBRM_WorkerN";
    cerr << os.str() << endl;
    log(os.str());
    exit(1);
  }

  // TODO: this should move to child() probably, like in masternode.cpp
  idbdatafile::IDBPolicy::configIDBPolicy();

  return ServiceWorkerNode(opt).Run();
}
