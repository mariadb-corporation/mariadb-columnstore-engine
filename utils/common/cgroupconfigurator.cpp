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

#include "cgroupconfigurator.h"
#include "configcpp.h"
#include "logger.h"
#include <charconv>
#include <fstream>
#include <iostream>
#include <limits>
#include <sys/sysinfo.h>

using namespace boost;
using namespace std;

// minor space-savers
#define RETURN_NO_GROUP(err)                                                   \
  do                                                                           \
  {                                                                            \
    if (!printedWarning)                                                       \
    {                                                                          \
      printedWarning = true;                                                   \
      ostringstream os;                                                        \
      os << "CGroup warning!  The group " << cGroupName << " does not exist."; \
      cerr << os.str() << endl;                                                \
      log(logging::LOG_TYPE_WARNING, os.str());                                \
    }                                                                          \
    return err;                                                                \
  } while (0)

#define RETURN_READ_ERROR(err)                                              \
  do                                                                        \
  {                                                                         \
    if (!printedWarning)                                                    \
    {                                                                       \
      printedWarning = true;                                                \
      ostringstream os;                                                     \
      os << "CGroup warning!  Could not read the file " << filename << "."; \
      cerr << os.str() << endl;                                             \
      log(logging::LOG_TYPE_WARNING, os.str());                             \
    }                                                                       \
    return err;                                                             \
  } while (0)

namespace
{
void log(logging::LOG_TYPE whichLogFile, const string& msg)
{
  logging::Logger logger(12);  // 12 = configcpp
  logger.logMessage(whichLogFile, msg, logging::LoggingID(12));
}

}  // namespace

namespace utils
{
CGroupConfigurator::CGroupConfigurator()
{
  config = config::Config::makeConfig();

  cGroupName = config->getConfig("SystemConfig", "CGroup");

  if (cGroupName.empty())
    cGroupDefined = false;
  else
    cGroupDefined = true;
  cout << __func__ << " cGroupDefined is  " << cGroupDefined << endl;

  ifstream v2Check("/sys/fs/cgroup/cgroup.controllers");
  cGroupVersion_ = (v2Check) ? v2 : v1;

  string cGroupVersion_str="";
  switch(cGroupVersion_){
    case v1:
      cGroupVersion_str="v1";
      break;
    case v2:
      cGroupVersion_str="v2";
      break;
  }
  cout << __func__<< " cGroupVersion_str is  " <<  cGroupVersion_str  << endl;
}

CGroupConfigurator::~CGroupConfigurator()
{
}

uint32_t CGroupConfigurator::getNumCoresFromCGroup()
{
  ostringstream filenameOs;
  if (cGroupVersion_ == v1)
  {
    filenameOs << "/sys/fs/cgroup/cpuset/" << cGroupName << "/cpuset.cpus";
  }
  else
  {
    filenameOs << "/sys/fs/cgroup/" << cGroupName << "/cpuset.cpus";
  }
  string filename = filenameOs.str();

  ifstream in(filename.c_str());
  string cpusString;
  uint32_t cpus = 0;

  if (!in)
    RETURN_NO_GROUP(0);

  try
  {
    // Need to parse & count how many CPUs we have access to
    in >> cpusString;
  }
  catch (...)
  {
    RETURN_READ_ERROR(0);
  }

  // the file has comma-deliminted CPU ranges like "0-7,9,11-12".
  size_t first = 0, last;
  bool lastRange = false;

  while (!lastRange)
  {
    size_t dash;
    string oneRange;

    last = cpusString.find(',', first);

    if (last == string::npos)
    {
      lastRange = true;
      oneRange = cpusString.substr(first);
    }
    else
      oneRange = cpusString.substr(first, last - first - 1);

    if ((dash = oneRange.find('-')) == string::npos)  // single-cpu range
      cpus++;
    else
    {
      const char* data = oneRange.c_str();
      uint32_t firstCPU = strtol(data, NULL, 10);
      uint32_t lastCPU = strtol(&data[dash + 1], NULL, 10);
      cpus += lastCPU - firstCPU + 1;
    }

    first = last + 1;
  }

  return cpus;
}

uint32_t CGroupConfigurator::getNumCoresFromProc()
{
  uint32_t nc = sysconf(_SC_NPROCESSORS_ONLN);

  return nc;
}

uint32_t CGroupConfigurator::getNumCores()
{
  /*
      Detect if MCS is in a C-Group
          - get the group ID
      If not, get the number of cores from /proc
  */
  uint32_t ret;

  if (!cGroupDefined)
    ret = getNumCoresFromProc();
  else
  {
    ret = getNumCoresFromCGroup();

    if (ret == 0)
      ret = getNumCoresFromProc();
  }

  return ret;
}

uint64_t CGroupConfigurator::getTotalMemory()
{
  uint64_t ret;

  if (totalMemory != 0)
    return totalMemory;

  if (!cGroupDefined)
    ret = getTotalMemoryFromProc();
  else
  {
    ret = getTotalMemoryFromCGroup();

    if (ret == 0 || ret == std::numeric_limits<uint64_t>::max())
      ret = getTotalMemoryFromProc();
  }

  cout << "Total mem available is " << ret << endl;
  totalMemory = ret;
  return totalMemory;
}

uint64_t CGroupConfigurator::getTotalMemoryFromProc()
{
  size_t memTot;
  cout << __func__ << "  reading /proc/meminfo "  << endl;
  ifstream in("/proc/meminfo");
  string x;

  in >> x;
  in >> memTot;

  // memTot is now in KB, convert to bytes
  memTot *= 1024;

  return memTot;
}

uint64_t CGroupConfigurator::getTotalMemoryFromCGroup()
{
  std::string memLimitStr;
  uint64_t memLimit = std::numeric_limits<uint64_t>::max();
  ostringstream os;

  if (cGroupVersion_ == v1)
  {
    os << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.limit_in_bytes";
  }
  else
  {
    os << "/sys/fs/cgroup/" << cGroupName << "/memory.max";
  }

  string filename = os.str();
  cout << __func__ <<" reading " << filename   << endl;

  ifstream in(filename.c_str());

  if (!in)
    RETURN_NO_GROUP(0);

  try
  {
    in >> memLimitStr;
  }
  catch (...)
  {
    RETURN_READ_ERROR(0);
  }
  cout << __func__<<  " read into memLimitStr " <<  memLimitStr  << endl;

  if (cGroupVersion_ == v2 && memLimitStr == "max")
  {
    return std::numeric_limits<uint64_t>::max();
  }

  auto [ch, ec] = std::from_chars(memLimitStr.c_str(), memLimitStr.c_str() + memLimitStr.size(), memLimit);
  if (ec != std::errc())
  {
    return std::numeric_limits<uint64_t>::max();
  }

  if (cGroupVersion_ == v1)
  {
    return std::min(getTotalMemoryFromProc(), memLimit);
  }
  return memLimit;
}

uint64_t CGroupConfigurator::getFreeMemory()
{
  uint64_t ret;
  if (!cGroupDefined)
    ret = getFreeMemoryFromProc();
  else
  {
    uint64_t usage = getMemUsageFromCGroup();
    cout << "usage " << usage << endl;

    if (usage == 0)
      ret = getFreeMemoryFromProc();
    else
      ret = getTotalMemory() - usage;
  }

  return ret;
}

uint64_t CGroupConfigurator::getMemUsageFromCGroup()
{
  char oneline[80];
  if (memUsageFilename.empty())
  {
    ostringstream filename;
    if (cGroupVersion_ == v1)
    {
      memStatePrefix = "rss ";
      filename << "/sys/fs/cgroup/memory/" << cGroupName << "/memory.stat";
    }
    else
    {
      memStatePrefix = "anon ";
      filename << "/sys/fs/cgroup/" << cGroupName << "/memory.stat";
    }
    memUsageFilename = filename.str();
  }

  ifstream in(memUsageFilename.c_str());
  string& filename = memUsageFilename;

  if (!in)
    RETURN_NO_GROUP(0);

  try
  {
    while (in)
    {
      in.getline(oneline, 80);

      if (strncmp(oneline, memStatePrefix.c_str(), memStatePrefix.size() - 1) == 0)
      {
        return atoll(&oneline[memStatePrefix.size()]);
      }
    }
  }
  catch (...)
  {
    RETURN_READ_ERROR(0);
  }

  return 0;
}

uint64_t CGroupConfigurator::getFreeMemoryFromProc()
{
  uint64_t memFree = 0;
  uint64_t buffers = 0;
  uint64_t cached = 0;
  uint64_t memTotal = 0;
  uint64_t memAvailable = 0;

  ifstream in("/proc/meminfo");
  string x;

  in >> x;  // MemTotal:
  in >> memTotal;
  in >> x;  // kB

  in >> x;  // MemFree:
  in >> memFree;
  in >> x;  // kB

  // check if available or buffers is passed
  in >> x;

  if (x == "MemAvailable:")
  {
    in >> memAvailable;  // MemAvailable
  }
  else
  {
    // centos 6 and older OSs
    in >> buffers;
    in >> x;  // kB

    in >> x;  // Cached:
    in >> cached;

    memAvailable = memFree + buffers + cached;
  }

  // amount available for application
  memAvailable *= 1024;
  return memAvailable;
}

}  // namespace utils
