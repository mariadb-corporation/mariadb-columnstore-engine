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

/******************************************************************************
 * $Id: MonitorProcMem.cpp 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *****************************************************************************/

#include <sys/types.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <stdint.h>
using namespace std;

#include "messageids.h"
#include "messageobj.h"
#include "loggingid.h"
#include "logger.h"
using namespace logging;

#include "MonitorProcMem.h"
#include "threadnaming.h"

namespace utils
{
uint64_t MonitorProcMem::fMemTotal = 1;
uint64_t MonitorProcMem::fMemFree = 0;
int MonitorProcMem::fMemPctCheck = 0;

//------------------------------------------------------------------------------
// Thread entry point function, that drives a thread to periodically (every
// 1 seconds by default) monitor the memory usage for the process we are
// running in. Call getAvailablePct() or memUsedPct() to access the current value.
//------------------------------------------------------------------------------
void MonitorProcMem::operator()() const
{
  utils::setThreadName("MonitorProcMem");
  while (true)
  {
    if (fMaxPct > 0)
    {
      size_t pct = rss() * 100 / fMemTotal;

      if (pct > fMaxPct)
      {
        cerr << "PrimProc: Too much memory allocated!" << endl;

        LoggingID logid(fSubsystemID);
        logging::Message msg(logging::M0045);
        logging::Message::Args args;
        msg.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(LOG_TYPE_CRITICAL, msg, logid);
        exit(1);
      }
    }

    fMemFree = cg.getFreeMemory();
    // calculateFreeMem();
    pause_();
  }
}

//------------------------------------------------------------------------------
// Returns the maximum memory available on the current host this process is
// running on.
//------------------------------------------------------------------------------
size_t MonitorProcMem::memTotal() const
{
  return cg.getTotalMemory();
}

//------------------------------------------------------------------------------
// Returns the RSS memory usage for the current process by reading the pid's
// statm file.
//------------------------------------------------------------------------------
size_t MonitorProcMem::rss() const
{
  uint64_t rss;

#if   defined(__FreeBSD__)
  ostringstream cmd;
  cmd << "ps -a -o rss -p " << getpid() << " | tail +2";
  FILE* cmdPipe;
  char input[80];
  cmdPipe = popen(cmd.str().c_str(), "r");
  input[0] = '\0';
  fgets(input, 80, cmdPipe);
  input[79] = '\0';
  pclose(cmdPipe);
  rss = atoi(input) * 1024LL;
#else
  ostringstream pstat;
  pstat << "/proc/" << fPid << "/statm";
  ifstream in(pstat.str().c_str());
  size_t x;

  in >> x;
  in >> rss;

  // rss is now in pages, convert to bytes
  rss *= fPageSize;
#endif

  return static_cast<size_t>(rss);
}

//------------------------------------------------------------------------------
// Stays in "sleep" state till fSleepSec seconds have elapsed.
//------------------------------------------------------------------------------
void MonitorProcMem::pause_() const
{
  struct timespec req;
  struct timespec rem;

  req.tv_sec = fSleepSec;
  req.tv_nsec = 0;

  rem.tv_sec = 0;
  rem.tv_nsec = 0;

  while (true)
  {

    if (nanosleep(&req, &rem) != 0)
    {
      if (rem.tv_sec > 0 || rem.tv_nsec > 0)
      {
        req = rem;
        continue;
      }
    }

    break;
  }
}

//------------------------------------------------------------------------------
// Returns the system used memory %
//------------------------------------------------------------------------------
unsigned MonitorProcMem::memUsedPct()
{
  return ((100 * (fMemTotal - fMemFree)) / fMemTotal);
}

//------------------------------------------------------------------------------
// Returns if used memory (including new request) < configured memory check
//------------------------------------------------------------------------------
bool MonitorProcMem::isMemAvailable(size_t memRequest)
{
  int memAvailPct = ((100 * (fMemFree - memRequest)) / fMemTotal);
  return (memAvailPct < fMemPctCheck);
}

}  // namespace utils
