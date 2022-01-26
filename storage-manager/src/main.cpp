/* Copyright (C) 2019 MariaDB Corporation

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

#include <unistd.h>
#include <string>
#include <iostream>
#include <stdio.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>

using namespace std;

#include "SMLogging.h"
#include "SessionManager.h"
#include "IOCoordinator.h"
#include "Cache.h"
#include "Synchronizer.h"
#include "Replicator.h"
#include "crashtrace.h"
#include "service.h"

using namespace storagemanager;

class Opt
{
 public:
  const char* m_progname;
  bool m_fg;
  Opt(int argc, char** argv) : m_progname(argv[0]), m_fg(argc >= 2 && string(argv[1]) == "fg")
  {
  }
};

class ServiceStorageManager : public Service, public Opt
{
 protected:
  void setupChildSignalHandlers();

 public:
  ServiceStorageManager(const Opt& opt) : Service("StorageManager"), Opt(opt)
  {
  }
  void LogErrno() override
  {
    SMLogging::get()->log(LOG_ERR, "%s", strerror(errno));
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    SMLogging::get()->log(LOG_INFO, "%.*s", (int)str.length(), str.data());
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
};

bool signalCaught = false;

void printCacheUsage(int sig)
{
  Cache::get()->validateCacheSize();
  cout << "Current cache size = " << Cache::get()->getCurrentCacheSize() << endl;
  cout << "Cache element count = " << Cache::get()->getCurrentCacheElementCount() << endl;
}

void printKPIs(int sig)
{
  IOCoordinator::get()->printKPIs();
  Cache::get()->printKPIs();
  Synchronizer::get()->printKPIs();
  CloudStorage::get()->printKPIs();
  Replicator::get()->printKPIs();
}

void shutdownSM(int sig)
{
  if (!signalCaught)
  {
    (SessionManager::get())->shutdownSM(sig);
  }
  signalCaught = true;
}

void coreSM(int sig)
{
  if (!signalCaught)
  {
    fatalHandler(sig);
  }
  signalCaught = true;
}

void ServiceStorageManager::setupChildSignalHandlers()
{
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));

  std::vector<int> shutdownSignals{SIGALRM, SIGHUP, SIGINT, SIGPOLL, SIGPROF, SIGPWR, SIGTERM, SIGVTALRM};

  std::vector<int> coreSignals{SIGABRT, SIGBUS, SIGFPE,  SIGILL,  SIGQUIT,
                               SIGSEGV, SIGSYS, SIGTRAP, SIGXCPU, SIGXFSZ};

  sa.sa_handler = shutdownSM;
  for (int sig : shutdownSignals)
    sigaction(sig, &sa, NULL);

  sa.sa_handler = coreSM;
  for (int sig : coreSignals)
    sigaction(sig, &sa, NULL);

  sa.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &sa, NULL);

  sa.sa_handler = printCacheUsage;
  sigaction(SIGUSR1, &sa, NULL);

  sa.sa_handler = printKPIs;
  sigaction(SIGUSR2, &sa, NULL);
}

int ServiceStorageManager::Child()
{
  SMLogging* logger = SMLogging::get();
  IOCoordinator* ioc = NULL;
  Cache* cache = NULL;
  Synchronizer* sync = NULL;
  Replicator* rep = NULL;

  /* Instantiate objects to have them verify config settings before continuing */
  try
  {
    ioc = IOCoordinator::get();
    cache = Cache::get();
    sync = Synchronizer::get();
    rep = Replicator::get();
  }
  catch (exception& e)
  {
    logger->log(LOG_INFO, "StorageManager init FAIL: %s", e.what());
    return -1;
  }

  setupChildSignalHandlers();

  int ret = 0;

  logger->log(LOG_NOTICE, "StorageManager started.");

  SessionManager* sm = SessionManager::get();

  NotifyServiceStarted();

  ret = sm->start();

  cache->shutdown();

  delete sync;
  delete cache;
  delete ioc;
  delete rep;
  logger->log(LOG_INFO, "StorageManager Shutdown Complete.");

  return ret;
}

int main(int argc, char** argv)
{
  return ServiceStorageManager(Opt(argc, argv)).Run();
}
