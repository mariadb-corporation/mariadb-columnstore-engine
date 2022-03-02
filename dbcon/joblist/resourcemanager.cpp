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

/******************************************************************************************
 * $Id: resourcemanager.cpp 9655 2013-06-25 23:08:13Z xlou $
 *
 ******************************************************************************************/

#include <unistd.h>
#include <string>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <fstream>
#include <sys/time.h>
using namespace std;

#include <boost/regex.hpp>
using namespace boost;

#include "resourcemanager.h"

#include "jl_logger.h"
#include "cgroupconfigurator.h"
#include "liboamcpp.h"
#include "secrets.h"

using namespace config;

namespace joblist
{
// const string ResourceManager::fExeMgrStr("ExeMgr1");
const string ResourceManager::fHashJoinStr("HashJoin");
const string ResourceManager::fJobListStr("JobList");
const string ResourceManager::fPrimitiveServersStr("PrimitiveServers");
// const string ResourceManager::fSystemConfigStr("SystemConfig");
const string ResourceManager::fExtentMapStr("ExtentMap");
// const string ResourceManager::fDMLProcStr("DMLProc");
// const string ResourceManager::fBatchInsertStr("BatchInsert");
const string ResourceManager::fOrderByLimitStr("OrderByLimit");
const string ResourceManager::fRowAggregationStr("RowAggregation");

ResourceManager* ResourceManager::fInstance = NULL;
boost::mutex mx;

ResourceManager* ResourceManager::instance(bool runningInExeMgr)
{
  boost::mutex::scoped_lock lk(mx);

  if (!fInstance)
    fInstance = new ResourceManager(runningInExeMgr);

  return fInstance;
}

ResourceManager::ResourceManager(bool runningInExeMgr)
 : fExeMgrStr("ExeMgr1")
 , fSystemConfigStr("SystemConfig")
 , fDMLProcStr("DMLProc")
 , fBatchInsertStr("BatchInsert")
 , fConfig(Config::makeConfig())
 , fNumCores(8)
 , fHjNumThreads(defaultNumThreads)
 , fJlProcessorThreadsPerScan(defaultProcessorThreadsPerScan)
 , fJlNumScanReceiveThreads(defaultScanReceiveThreads)
 , fJlMaxOutstandingRequests(defaultMaxOutstandingRequests)
 , fHJUmMaxMemorySmallSideDistributor(
       fHashJoinStr, "UmMaxMemorySmallSide",
       getUintVal(fHashJoinStr, "TotalUmMaxMemorySmallSide", defaultTotalUmMemory),
       getUintVal(fHashJoinStr, "UmMaxMemorySmallSide", defaultHJUmMaxMemorySmallSide), 0)
 , fHJPmMaxMemorySmallSideSessionMap(
       getUintVal(fHashJoinStr, "PmMaxMemorySmallSide", defaultHJPmMaxMemorySmallSide))
 , isExeMgr(runningInExeMgr)
{
  int temp;
  int configNumCores = -1;

  fTraceFlags = 0;
  // See if we want to override the calculated #cores
  temp = getIntVal(fJobListStr, "NumCores", -1);

  if (temp > 0)
    configNumCores = temp;

  if (configNumCores <= 0)
  {
    // count the actual #cores
    utils::CGroupConfigurator cg;
    fNumCores = cg.getNumCores();

    if (fNumCores <= 0)
      fNumCores = 8;
  }
  else
    fNumCores = configNumCores;

  // based on the #cores, calculate some thread parms
  if (fNumCores > 0)
  {
    fHjNumThreads = fNumCores;
    fJlNumScanReceiveThreads = fNumCores;
  }

  // possibly override any calculated values
  temp = getIntVal(fHashJoinStr, "NumThreads", -1);

  if (temp > 0)
    fHjNumThreads = temp;

  temp = getIntVal(fJobListStr, "ProcessorThreadsPerScan", -1);

  if (temp > 0)
    fJlProcessorThreadsPerScan = temp;

  temp = getIntVal(fJobListStr, "MaxOutstandingRequests", -1);

  if (temp > 0)
    fJlMaxOutstandingRequests = temp;
  else
  {
    oam::Oam oam;
    oam::ModuleTypeConfig moduletypeconfig;
    oam.getSystemConfig("pm", moduletypeconfig);
    const uint temp = moduletypeconfig.ModuleCount * fNumCores * 4 / fJlProcessorThreadsPerScan;
    const uint minMaxOutstandingRequests =
        std::max(static_cast<uint>(moduletypeconfig.ModuleCount * 2), defaultMaxOutstandingRequests);
    fJlMaxOutstandingRequests = temp > minMaxOutstandingRequests ? temp : minMaxOutstandingRequests;
  }

  temp = getIntVal(fJobListStr, "NumScanReceiveThreads", -1);

  if (temp > 0)
    fJlNumScanReceiveThreads = temp;

  fDECConnectionsPerQuery = getUintVal(fJobListStr, "DECConnectionsPerQuery", 0);
  fDECConnectionsPerQuery =
      (fDECConnectionsPerQuery) ? fDECConnectionsPerQuery : getPsConnectionsPerPrimProc();

  pmJoinMemLimit = getUintVal(fHashJoinStr, "PmMaxMemorySmallSide", defaultHJPmMaxMemorySmallSide);

  // Need to use different limits if this instance isn't running on the UM,
  // or if it's an ExeMgr running on a PM node
  if (!isExeMgr)
    totalUmMemLimit = pmJoinMemLimit;
  else
  {
    // Installation.PMwithUM = y by default so RM prefers HashJoin.TotalPmUmMemory
    // if it exists.
    string whichLimit = "TotalUmMemory";
    string pmWithUM = fConfig->getConfig("Installation", "PMwithUM");

    if (pmWithUM == "y" || pmWithUM == "Y")
    {
      oam::Oam OAM;
      oam::oamModuleInfo_t moduleInfo = OAM.getModuleInfo();
      string& moduleType = boost::get<1>(moduleInfo);

      if (moduleType == "pm" || moduleType == "PM")
      {
        string doesItExist = fConfig->getConfig(fHashJoinStr, "TotalPmUmMemory");

        if (!doesItExist.empty())
          whichLimit = "TotalPmUmMemory";
      }
    }

    string umtxt = fConfig->getConfig(fHashJoinStr, whichLimit);

    if (umtxt.empty())
      totalUmMemLimit = defaultTotalUmMemory;
    else
    {
      // is it an absolute or a percentage?
      if (umtxt.find('%') != string::npos)
      {
        utils::CGroupConfigurator cg;
        uint64_t totalMem = cg.getTotalMemory();
        totalUmMemLimit = atoll(umtxt.c_str()) / 100.0 * (double)totalMem;

        if (totalUmMemLimit == 0 || totalUmMemLimit == LLONG_MIN ||
            totalUmMemLimit == LLONG_MAX)  // some garbage in the xml entry
          totalUmMemLimit = defaultTotalUmMemory;
      }
      else  // an absolute; use the existing converter
      {
        totalUmMemLimit = getIntVal(fHashJoinStr, whichLimit, defaultTotalUmMemory);
      }
    }
  }

  configuredUmMemLimit = totalUmMemLimit;
  // cout << "RM: total UM memory = " << totalUmMemLimit << endl;

  // multi-thread aggregate
  string nt, nb, nr;
  nt = fConfig->getConfig("RowAggregation", "RowAggrThreads");

  if (nt.empty())
  {
    if (numCores() > 0)
      fAggNumThreads = numCores();
    else
      fAggNumThreads = 1;
  }
  else
    fAggNumThreads = fConfig->uFromText(nt);

  nb = fConfig->getConfig("RowAggregation", "RowAggrBuckets");

  if (nb.empty())
    fAggNumBuckets = fAggNumThreads * 4;
  else
    fAggNumBuckets = fConfig->uFromText(nb);

  nr = fConfig->getConfig("RowAggregation", "RowAggrRowGroupsPerThread");

  if (nr.empty())
    fAggNumRowGroups = 20;
  else
    fAggNumRowGroups = fConfig->uFromText(nr);

  // window function
  string wt = fConfig->getConfig("WindowFunction", "WorkThreads");

  if (wt.empty())
    fWindowFunctionThreads = numCores();
  else
    fWindowFunctionThreads = fConfig->uFromText(wt);

  // hdfs info
  string hdfs = fConfig->getConfig("SystemConfig", "DataFilePlugin");

  if (hdfs.find("hdfs") != string::npos)
    fUseHdfs = true;
  else
    fUseHdfs = false;

  fAllowedDiskAggregation =
      getBoolVal(fRowAggregationStr, "AllowDiskBasedAggregation", defaultAllowDiskAggregation);

  fMaxBPPSendQueue = getUintVal(fPrimitiveServersStr, "MaxBPPSendQueue", defaultMaxBPPSendQueue);

  if (!load_encryption_keys())
  {
    Logger log;
    log.logMessage(logging::LOG_TYPE_ERROR, "Error loading CEJ password encryption keys");
  }
}

// Used only for WIndows
int ResourceManager::getEmPriority() const
{
  int temp = getIntVal(fExeMgrStr, "Priority", defaultEMPriority);
  // config file priority is 40..1 (highest..lowest)
  // convert to  -20..19 (highest..lowest, defaults to -1)
  int val;

  // @Bug3385 - the ExeMgr priority was being set backwards with 1 being the highest instead of the lowest.
  if (temp < 1)
    val = 19;
  else if (temp > 40)
    val = -20;
  else
    val = 20 - temp;

  return val;
}

void ResourceManager::addHJPmMaxSmallSideMap(uint32_t sessionID, uint64_t mem)
{
  if (fHJPmMaxMemorySmallSideSessionMap.addSession(sessionID, mem,
                                                   fHJUmMaxMemorySmallSideDistributor.getTotalResource()))
    logResourceChangeMessage(logging::LOG_TYPE_INFO, sessionID, mem, defaultHJPmMaxMemorySmallSide,
                             "PmMaxMemorySmallSide", LogRMResourceChange);
  else
  {
    logResourceChangeMessage(logging::LOG_TYPE_WARNING, sessionID, mem,
                             fHJUmMaxMemorySmallSideDistributor.getTotalResource(), "PmMaxMemorySmallSide",
                             LogRMResourceChangeError);

    logResourceChangeMessage(logging::LOG_TYPE_INFO, sessionID, mem,
                             fHJUmMaxMemorySmallSideDistributor.getTotalResource(), "PmMaxMemorySmallSide",
                             LogRMResourceChangeError);
  }
}

void ResourceManager::addHJUmMaxSmallSideMap(uint32_t sessionID, uint64_t mem)
{
  if (fHJUmMaxMemorySmallSideDistributor.addSession(sessionID, mem))
    logResourceChangeMessage(logging::LOG_TYPE_INFO, sessionID, mem, defaultHJUmMaxMemorySmallSide,
                             "UmMaxMemorySmallSide", LogRMResourceChange);
  else
  {
    logResourceChangeMessage(logging::LOG_TYPE_WARNING, sessionID, mem,
                             fHJUmMaxMemorySmallSideDistributor.getTotalResource(), "UmMaxMemorySmallSide",
                             LogRMResourceChangeError);

    logResourceChangeMessage(logging::LOG_TYPE_INFO, sessionID, mem,
                             fHJUmMaxMemorySmallSideDistributor.getTotalResource(), "UmMaxMemorySmallSide",
                             LogRMResourceChangeError);
  }
}

void ResourceManager::logResourceChangeMessage(logging::LOG_TYPE logType, uint32_t sessionID,
                                               uint64_t newvalue, uint64_t value, const string& source,
                                               logging::Message::MessageID mid)
{
  logging::Message::Args args;
  args.add(source);
  args.add(newvalue);
  args.add(value);
  Logger log;
  log.logMessage(logType, mid, args, logging::LoggingID(5, sessionID));
}

bool ResourceManager::getMysqldInfo(std::string& h, std::string& u, std::string& w, unsigned int& p) const
{
  static const std::string hostUserUnassignedValue("unassigned");
  // MCS will read username and pass from disk if the config changed.
  u = getStringVal("CrossEngineSupport", "User", hostUserUnassignedValue);
  std::string encryptedPW = getStringVal("CrossEngineSupport", "Password", "");
  // This will return back the plaintext password if there is no MCSDATADIR/.secrets file present
  w = decrypt_password(encryptedPW);
  // MCS will not read username and pass from disk if the config changed.
  h = getStringVal("CrossEngineSupport", "Host", hostUserUnassignedValue);
  p = getUintVal("CrossEngineSupport", "Port", 0);
  u = getStringVal("CrossEngineSupport", "User", "unassigned");

  bool rc = true;

  if ((h.compare("unassigned") == 0) || (u.compare("unassigned") == 0) || (p == 0))
    rc = false;

  return rc;
}

bool ResourceManager::queryStatsEnabled() const
{
  std::string val(getStringVal("QueryStats", "Enabled", "N"));
  boost::to_upper(val);
  return "Y" == val;
}

bool ResourceManager::userPriorityEnabled() const
{
  std::string val(getStringVal("UserPriority", "Enabled", "N"));
  boost::to_upper(val);
  return "Y" == val;
}

// Counts memory. This funtion doesn't actually malloc, just counts against two limits
// totalUmMemLimit for overall UM counting and (optional) sessionLimit for a single session.
// If both have space, return true.
bool ResourceManager::getMemory(int64_t amount, boost::shared_ptr<int64_t>& sessionLimit, bool patience)
{
  bool ret1 = (atomicops::atomicSub(&totalUmMemLimit, amount) >= 0);
  bool ret2 = sessionLimit ? (atomicops::atomicSub(sessionLimit.get(), amount) >= 0) : ret1;

  uint32_t retryCounter = 0, maxRetries = 20;  // 10s delay

  while (patience && !(ret1 && ret2) && retryCounter++ < maxRetries)
  {
    atomicops::atomicAdd(&totalUmMemLimit, amount);
    sessionLimit ? atomicops::atomicAdd(sessionLimit.get(), amount) : 0;
    usleep(500000);
    ret1 = (atomicops::atomicSub(&totalUmMemLimit, amount) >= 0);
    ret2 = sessionLimit ? (atomicops::atomicSub(sessionLimit.get(), amount) >= 0) : ret1;
  }
  if (!(ret1 && ret2))
  {
    // If  we  didn't  get any memory, restore the counters.
    atomicops::atomicAdd(&totalUmMemLimit, amount);
    sessionLimit ? atomicops::atomicAdd(sessionLimit.get(), amount) : 0;
  }
  return (ret1 && ret2);
}
// Don't care about session memory
bool ResourceManager::getMemory(int64_t amount, bool patience)
{
  bool ret1 = (atomicops::atomicSub(&totalUmMemLimit, amount) >= 0);

  uint32_t retryCounter = 0, maxRetries = 20;  // 10s delay

  while (patience && !ret1 && retryCounter++ < maxRetries)
  {
    atomicops::atomicAdd(&totalUmMemLimit, amount);
    usleep(500000);
    ret1 = (atomicops::atomicSub(&totalUmMemLimit, amount) >= 0);
  }
  if (!ret1)
  {
    // If  we  didn't  get any memory, restore the counters.
    atomicops::atomicAdd(&totalUmMemLimit, amount);
  }
  return ret1;
}

}  // namespace joblist
