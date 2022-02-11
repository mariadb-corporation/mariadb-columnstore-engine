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
 * $Id: configure.cpp 64 2006-10-12 22:21:51Z dhill $
 *
 *
 * List of files being updated by configure:
 *		Columnstore/etc/Columnstore.xml
 *
 *
 ******************************************************************************************/
/**
 * @file
 */

#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include "stdio.h"
#include "ctype.h"
#include <netdb.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "installdir.h"

#include "mcsconfig.h"

using namespace std;
using namespace oam;
using namespace config;

typedef struct Performance_Module_struct
{
  std::string moduleIP1;
  std::string moduleIP2;
  std::string moduleIP3;
  std::string moduleIP4;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

int main(int argc, char* argv[])
{
  setenv("CALPONT_HOME", "./", 1);

  Oam oam;
  string systemParentOAMModuleName;
  string parentOAMModuleIPAddr;
  PerformanceModuleList performancemodulelist;
  int pmNumber = 0;
  string prompt;
  int DBRMworkernodeID = 0;
  string remote_installer_debug = "0";
  Config* sysConfigOld;
  Config* sysConfigNew;
  string systemName;
  bool extextMapCheckOnly = false;

  if (argc > 1)
  {
    string arg1 = argv[1];

    if (arg1 == "-e")
      extextMapCheckOnly = true;
    else
      systemName = arg1;
  }
  else
    systemName = oam::UnassignedName;

  try
  {
    std::string configFilePathOld = std::string(MCSSYSCONFDIR) + std::string("/columnstore/Columnstore.xml");
    std::string configFilePathNew =
        std::string(MCSSYSCONFDIR) + std::string("/columnstore/Columnstore.xml.new");
    sysConfigOld = Config::makeConfig(configFilePathOld);  // system version
    sysConfigNew = Config::makeConfig(configFilePathNew);  // released version
  }
  catch (...)
  {
    cout << "ERROR: Problem reading Columnstore.xml files\n";
    exit(-1);
  }

  string SystemSection = "SystemConfig";
  string InstallSection = "Installation";

  // read cloud parameter to see if OLD is a 3.0+ or pre-3.0 build being installed
  string cloud;

  try
  {
    cloud = sysConfigOld->getConfig(InstallSection, "Cloud");
  }
  catch (...)
  {
  }

  // build3 is for 3 and above 4
  bool OLDbuild3 = false;

  if (!cloud.empty())
    OLDbuild3 = true;

  // read cloud parameter to see if NEW is a 3.0+ or pre-3.0 build being installed
  try
  {
    cloud = sysConfigNew->getConfig(InstallSection, "Cloud");
  }
  catch (...)
  {
  }

  // build3 is for 3 and above 4
  bool build3 = false;

  if (!cloud.empty())
    build3 = true;

  bool build40 = false;
  bool build401 = true;

  // check and update PMwithUM
  try
  {
    string PMwithUM = sysConfigOld->getConfig(InstallSection, "PMwithUM");

    if (!PMwithUM.empty())
    {
      try
      {
        sysConfigNew->setConfig(InstallSection, "PMwithUM", PMwithUM);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting PMwithUM in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  // check and update PMwithUM
  try
  {
    string MySQLRep = sysConfigOld->getConfig(InstallSection, "MySQLRep");

    if (!MySQLRep.empty())
    {
      try
      {
        sysConfigNew->setConfig(InstallSection, "MySQLRep", MySQLRep);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting MySQLRep in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  // set gluster flag if it exists
  string DataRedundancyConfig;
  string DataRedundancyCopies;
  string DataRedundancyStorageType;
  string DataRedundancyNetworkType;

  try
  {
    DataRedundancyConfig = sysConfigOld->getConfig(InstallSection, "DataRedundancyConfig");
    DataRedundancyCopies = sysConfigOld->getConfig(InstallSection, "DataRedundancyCopies");
    DataRedundancyStorageType = sysConfigOld->getConfig(InstallSection, "DataRedundancyStorageType");
    DataRedundancyNetworkType = sysConfigOld->getConfig(InstallSection, "DataRedundancyNetworkType");
  }
  catch (...)
  {
  }

  if (!DataRedundancyConfig.empty())
  {
    try
    {
      sysConfigNew->setConfig(InstallSection, "DataRedundancyConfig", DataRedundancyConfig);
      sysConfigNew->setConfig(InstallSection, "DataRedundancyCopies", DataRedundancyCopies);
      sysConfigNew->setConfig(InstallSection, "DataRedundancyStorageType", DataRedundancyStorageType);
      sysConfigNew->setConfig(InstallSection, "DataRedundancyNetworkType", DataRedundancyNetworkType);
    }
    catch (...)
    {
    }
  }

  // check and make sure the ExtentMap variables don't get changed at install
  string oldFilesPerColumnPartition;
  string oldExtentsPerSegmentFile;
  string newFilesPerColumnPartition;
  string newExtentsPerSegmentFile;

  try
  {
    oldFilesPerColumnPartition = sysConfigOld->getConfig("ExtentMap", "FilesPerColumnPartition");
    oldExtentsPerSegmentFile = sysConfigOld->getConfig("ExtentMap", "ExtentsPerSegmentFile");

    newFilesPerColumnPartition = sysConfigNew->getConfig("ExtentMap", "FilesPerColumnPartition");
    newExtentsPerSegmentFile = sysConfigNew->getConfig("ExtentMap", "ExtentsPerSegmentFile");

    if (oldFilesPerColumnPartition != newFilesPerColumnPartition)
    {
      try
      {
        sysConfigNew->setConfig("ExtentMap", "FilesPerColumnPartition", oldFilesPerColumnPartition);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting FilesPerColumnPartition in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }

    if (oldExtentsPerSegmentFile != newExtentsPerSegmentFile)
    {
      try
      {
        sysConfigNew->setConfig("ExtentMap", "ExtentsPerSegmentFile", oldExtentsPerSegmentFile);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting ExtentsPerSegmentFile in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  sysConfigNew->write();

  if (extextMapCheckOnly)
    exit(0);

  systemParentOAMModuleName = "pm1";

  // check if systemParentOAMModuleName (pm1) is configured, if not set to 'pm2'
  //	string IPaddr = sysConfigOld->getConfig("pm1_ProcessMonitor", "IPAddr");
  //	if ( IPaddr == "0.0.0.0" )
  //		systemParentOAMModuleName = "pm2";

  // set Parent OAM Module Name
  try
  {
    sysConfigNew->setConfig(SystemSection, "ParentOAMModuleName", systemParentOAMModuleName);
  }
  catch (...)
  {
    cout << "ERROR: Problem updating the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  // setup System Name
  string oldSystemName;

  try
  {
    oldSystemName = sysConfigOld->getConfig(SystemSection, "SystemName");
  }
  catch (...)
  {
  }

  if (!oldSystemName.empty())
    systemName = oldSystemName;

  try
  {
    sysConfigNew->setConfig(SystemSection, "SystemName", systemName);
  }
  catch (...)
  {
    cout << "ERROR: Problem setting SystemName from the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  // WaitPeriod
  try
  {
    string waitPeriod = sysConfigOld->getConfig(SystemSection, "WaitPeriod");
    if (waitPeriod.length() > 0)
    {
      sysConfigNew->setConfig(SystemSection, "WaitPeriod", waitPeriod);
    }
  }
  catch (...)
  {
  }

  // setup HA IP Address
  string HA_IPadd;

  try
  {
    HA_IPadd = sysConfigOld->getConfig("ProcMgr_HA", "IPAddr");
  }
  catch (...)
  {
  }

  if (!HA_IPadd.empty())
  {
    try
    {
      sysConfigNew->setConfig("ProcMgr_HA", "IPAddr", HA_IPadd);
    }
    catch (...)
    {
      cout << "ERROR: Problem setting ProcMgr_HA from the Columnstore System Configuration file" << endl;
      exit(-1);
    }
  }

  // setup CMP IP Addresses
  string CMP_IPadd;
  string CMP_port;

  for (int id = 1;; id++)
  {
    string cmpName = "CMP" + oam.itoa(id);

    try
    {
      CMP_IPadd = sysConfigOld->getConfig(cmpName, "IPAddr");
    }
    catch (...)
    {
    }

    if (!CMP_IPadd.empty())
    {
      try
      {
        CMP_port = sysConfigOld->getConfig(cmpName, "Port");
      }
      catch (...)
      {
      }

      try
      {
        sysConfigNew->setConfig(cmpName, "IPAddr", CMP_IPadd);
        sysConfigNew->setConfig(cmpName, "Port", CMP_port);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting CMP from the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
    else
      break;
  }

  // make DBRM backwards compatiable for pre 1.0.0.157 load
  string dbrmMainProc = "DBRM_Controller";
  string dbrmSubProc = "DBRM_Worker";
  string numSubProc = "NumWorkers";

  // CrossEngineSupport
  string Host = "";
  string Port = "3306";
  string User = "";
  string Password = "";
  string TLSCA = "";
  string TLSClientCert = "";
  string TLSClientKey = "";

  try
  {
    Host = sysConfigOld->getConfig("CrossEngineSupport", "Host");
    Port = sysConfigOld->getConfig("CrossEngineSupport", "Port");
    User = sysConfigOld->getConfig("CrossEngineSupport", "User");
    Password = sysConfigOld->getConfig("CrossEngineSupport", "Password");
  }
  catch (...)
  {
    Host = "";
    Port = "3306";
    User = "";
    Password = "";
  }

  try
  {
    sysConfigNew->setConfig("CrossEngineSupport", "Host", Host);
    sysConfigNew->setConfig("CrossEngineSupport", "Port", Port);
    sysConfigNew->setConfig("CrossEngineSupport", "User", User);
    sysConfigNew->setConfig("CrossEngineSupport", "Password", Password);
  }
  catch (...)
  {
  }

  try
  {
    TLSCA = sysConfigOld->getConfig("CrossEngineSupport", "TLSCA");
    TLSClientCert = sysConfigOld->getConfig("CrossEngineSupport", "TLSClientCert");
    TLSClientKey = sysConfigOld->getConfig("CrossEngineSupport", "TLSClientKey");
  }
  catch (...)
  {
    TLSCA = "";
    TLSClientCert = "";
    TLSClientKey = "";
  }

  try
  {
    sysConfigNew->setConfig("CrossEngineSupport", "TLSCA", TLSCA);
    sysConfigNew->setConfig("CrossEngineSupport", "TLSClientCert", TLSClientCert);
    sysConfigNew->setConfig("CrossEngineSupport", "TLSClientKey", TLSClientKey);
  }
  catch (...)
  {
  }

  // QueryStats and UserPriority
  string QueryStats = "N";
  string UserPriority = "N";

  try
  {
    QueryStats = sysConfigOld->getConfig("QueryStats", "Enabled");
    UserPriority = sysConfigOld->getConfig("UserPriority", "Enabled");
  }
  catch (...)
  {
    QueryStats = "N";
    UserPriority = "N";
  }

  try
  {
    sysConfigNew->setConfig("QueryStats", "Enabled", QueryStats);
    sysConfigNew->setConfig("UserPriority", "Enabled", UserPriority);
  }
  catch (...)
  {
  }

  // @bug4598, DirectIO setting
  string directIO = "y";

  try
  {
    directIO = sysConfigOld->getConfig("PrimitiveServers", "DirectIO");
  }
  catch (...)
  {
    directIO = "y";
  }

  try
  {
    sysConfigNew->setConfig("PrimitiveServers", "DirectIO", directIO);
  }
  catch (...)
  {
  }

  // @bug4507, configurable pm MemoryCheck
  string memCheck;

  try
  {
    memCheck = sysConfigOld->getConfig("SystemConfig", "MemoryCheckPercent");

    if (!(memCheck.empty() || memCheck == ""))
    {
      sysConfigNew->setConfig("SystemConfig", "MemoryCheckPercent", memCheck);
    }
  }
  catch (...)
  {
  }

  // Priority Settings
  string HighPriorityPercentage;

  try
  {
    HighPriorityPercentage = sysConfigOld->getConfig("PrimitiveServers", "HighPriorityPercentage");
    sysConfigNew->setConfig("PrimitiveServers", "HighPriorityPercentage", HighPriorityPercentage);
  }
  catch (...)
  {
  }

  string MediumPriorityPercentage;

  try
  {
    MediumPriorityPercentage = sysConfigOld->getConfig("PrimitiveServers", "MediumPriorityPercentage");
    sysConfigNew->setConfig("PrimitiveServers", "MediumPriorityPercentage", MediumPriorityPercentage);
  }
  catch (...)
  {
  }

  string LowPriorityPercentage;

  try
  {
    LowPriorityPercentage = sysConfigOld->getConfig("PrimitiveServers", "LowPriorityPercentage");
    sysConfigNew->setConfig("PrimitiveServers", "LowPriorityPercentage", LowPriorityPercentage);
  }
  catch (...)
  {
  }

  // default to single-server install type
  string OserverTypeInstall = oam.itoa(oam::INSTALL_COMBINE_DM_UM_PM);
  ;
  string NserverTypeInstall;
  string OSingleServerInstall = "y";
  int IserverTypeInstall;

  try
  {
    OserverTypeInstall = sysConfigOld->getConfig(InstallSection, "ServerTypeInstall");
    OSingleServerInstall = sysConfigOld->getConfig(InstallSection, "SingleServerInstall");
  }
  catch (...)
  {
    // default to Normal mult-server install type
    OserverTypeInstall = oam.itoa(oam::INSTALL_COMBINE_DM_UM_PM);
    OSingleServerInstall = "y";
  }

  // set Server Installation Type
  try
  {
    sysConfigNew->setConfig(InstallSection, "ServerTypeInstall", OserverTypeInstall);
    sysConfigNew->setConfig(InstallSection, "SingleServerInstall", OSingleServerInstall);
  }
  catch (...)
  {
  }

  NserverTypeInstall = OserverTypeInstall;

  IserverTypeInstall = atoi(NserverTypeInstall.c_str());

  // set RotatingDestination
  switch (IserverTypeInstall)
  {
    case (oam::INSTALL_COMBINE_DM_UM_PM):  // combined #1 - dm/um/pm on a single server
    {
      try
      {
        sysConfigNew->setConfig("PrimitiveServers", "RotatingDestination", "n");
      }
      catch (...)
      {
        cout << "ERROR: Problem setting RotatingDestination in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }

      break;
    }
  }

  string parentOAMModuleType = systemParentOAMModuleName.substr(0, MAX_MODULE_TYPE_SIZE);

  //
  // get Data storage Mount
  //

  string DBRootStorageType;

  int DBRootCount;
  string deviceName;

  try
  {
    DBRootStorageType = sysConfigOld->getConfig(InstallSection, "DBRootStorageType");
    DBRootCount = strtol(sysConfigOld->getConfig(SystemSection, "DBRootCount").c_str(), 0, 0);
  }
  catch (...)
  {
    cout << "ERROR: Problem getting DB Storage Data from the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  // 2.2 to 3.x+ DBRootStorageTypeconversion
  if (DBRootStorageType == "local")
    DBRootStorageType = "internal";

  if (DBRootStorageType == "storage")
    DBRootStorageType = "external";

  try
  {
    sysConfigNew->setConfig(InstallSection, "DBRootStorageType", DBRootStorageType);
  }
  catch (...)
  {
    cout << "ERROR: Problem setting DBRootStorageType in the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  try
  {
    sysConfigNew->setConfig(SystemSection, "DBRootCount", oam.itoa(DBRootCount));
  }
  catch (...)
  {
    cout << "ERROR: Problem setting DBRoot Count in the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  //
  // Update memory and cache settings
  //

  string NumBlocksPct;

  try
  {
    NumBlocksPct = sysConfigOld->getConfig("DBBC", "NumBlocksPct");
  }
  catch (...)
  {
  }

  if ((NumBlocksPct.empty() || NumBlocksPct == "") && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM)
  {
    NumBlocksPct = "50";
  }

  if (!(NumBlocksPct.empty() || NumBlocksPct == ""))
  {
    try
    {
      sysConfigNew->setConfig("DBBC", "NumBlocksPct", NumBlocksPct);
    }
    catch (...)
    {
    }
  }

  string TotalUmMemory;

  try
  {
    TotalUmMemory = sysConfigOld->getConfig("HashJoin", "TotalUmMemory");
  }
  catch (...)
  {
  }

  try
  {
    sysConfigNew->setConfig("HashJoin", "TotalUmMemory", TotalUmMemory);
  }
  catch (...)
  {
  }

  string TotalPmUmMemory;

  try
  {
    TotalPmUmMemory = sysConfigOld->getConfig("HashJoin", "TotalPmUmMemory");
  }
  catch (...)
  {
  }

  try
  {
    sysConfigNew->setConfig("HashJoin", "TotalPmUmMemory", TotalPmUmMemory);
  }
  catch (...)
  {
  }

  string strNumThreads;

  try
  {
    strNumThreads = sysConfigOld->getConfig("DBBC", "NumThreads");
  }
  catch (...)
  {
  }

  if (!(strNumThreads.empty() || strNumThreads == ""))
  {
    try
    {
      sysConfigNew->setConfig("DBBC", "NumThreads", strNumThreads);
    }
    catch (...)
    {
    }
  }

  string MySQLPort = "3306";

  try
  {
    MySQLPort = sysConfigOld->getConfig("Installation", "MySQLPort");
  }
  catch (...)
  {
    MySQLPort = "3306";
  }

  try
  {
    sysConfigNew->setConfig("Installation", "MySQLPort", MySQLPort);
  }
  catch (...)
  {
  }

  sysConfigNew->write();

  // Get list of configured system modules
  SystemModuleTypeConfig sysModuleTypeConfig;

  try
  {
    oam.getSystemConfig(sysModuleTypeConfig);
  }
  catch (...)
  {
    cout << "ERROR: Problem reading the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  //
  // Module Configuration
  //
  string ModuleSection = "SystemModuleConfig";
  unsigned int maxPMNicCount = 1;

  for (unsigned int i = 0; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
  {
    string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
    string moduleDesc = sysModuleTypeConfig.moduletypeconfig[i].ModuleDesc;
    int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

    // verify and setup of modules count
    switch (IserverTypeInstall)
    {
      case (oam::INSTALL_COMBINE_DM_UM_PM):
      {
        if (moduleType == "um")
        {
          moduleCount = 0;

          try
          {
            string ModuleCountParm = "ModuleCount" + oam.itoa(i + 1);
            sysConfigNew->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
            continue;
          }
          catch (...)
          {
            cout << "ERROR: Problem setting Module Count in the Columnstore System Configuration file"
                 << endl;
            exit(-1);
          }
        }
        else
        {
          try
          {
            string ModuleCountParm = "ModuleCount" + oam.itoa(i + 1);
            sysConfigNew->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
          }
          catch (...)
          {
            cout << "ERROR: Problem setting Module Count in the Columnstore System Configuration file"
                 << endl;
            exit(-1);
          }
        }

        break;
      }

      default:
      {
        try
        {
          string ModuleCountParm = "ModuleCount" + oam.itoa(i + 1);
          sysConfigNew->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
        }
        catch (...)
        {
          cout << "ERROR: Problem setting Module Count in the Columnstore System Configuration file" << endl;
          exit(-1);
        }

        break;
      }
    }

    if (moduleCount == 0)
      // no modules equipped for this Module Type, skip
      continue;

    if (moduleType == "pm")
      pmNumber = moduleCount;

    // for 2.x to 3.x upgrade dbroot assignments
    int dbrootNum = 0;
    int systemDBRootCount = 0;
    int dbrootCountPerModule = 0;

    if (moduleType == "pm" && !OLDbuild3)
    {
      dbrootNum = 1;
      systemDBRootCount = DBRootCount;

      if (pmNumber > 0)
        dbrootCountPerModule = DBRootCount / pmNumber;

      if (dbrootCountPerModule == 0)
        dbrootCountPerModule = 1;
    }

    // get Module Name IP addresses and Host Names
    DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

    for (; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end(); listPT++)
    {
      PerformanceModule performancemodule;
      string moduleName = (*listPT).DeviceName;
      int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE).c_str());

      string moduleDisableState = (*listPT).DisableState;

      // set Module Disable State
      string moduleDisableStateParm = "ModuleDisableState" + oam.itoa(moduleID) + "-" + oam.itoa(i + 1);

      try
      {
        sysConfigNew->setConfig(ModuleSection, moduleDisableStateParm, moduleDisableState);
      }
      catch (...)
      {
        cout
            << "ERROR: Problem setting ModuleDisableState in the Columnstore System Configuration file for " +
                   moduleName
            << endl;
        exit(-1);
      }

      for (unsigned int nicID = 1; nicID < MAX_NIC + 1; nicID++)
      {
        string moduleIPAddr = oam::UnassignedIpAddr;
        string moduleHostName = oam::UnassignedName;

        HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();

        for (; pt1 != (*listPT).hostConfigList.end(); pt1++)
        {
          if (moduleName == (*listPT).DeviceName && (*pt1).NicID == nicID)
          {
            moduleIPAddr = (*pt1).IPAddr;
            moduleHostName = (*pt1).HostName;
            break;
          }
        }

        if (moduleHostName.empty() || (moduleHostName == oam::UnassignedName))
          // exit out to next module ID
          break;

        if (moduleIPAddr.empty())
          moduleIPAddr = oam::UnassignedIpAddr;

        string moduleNameDesc = moduleDesc + " #" + oam.itoa(moduleID);

        // set New Module Host Name
        string moduleHostNameParm =
            "ModuleHostName" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i + 1);

        try
        {
          sysConfigNew->setConfig(ModuleSection, moduleHostNameParm, moduleHostName);
        }
        catch (...)
        {
          cout << "ERROR: Problem setting Host Name in the Columnstore System Configuration file" << endl;
          exit(-1);
        }

        // set Module IP address
        string moduleIPAddrNameParm =
            "ModuleIPAddr" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i + 1);

        try
        {
          sysConfigNew->setConfig(ModuleSection, moduleIPAddrNameParm, moduleIPAddr);
        }
        catch (...)
        {
          cout << "ERROR: Problem setting IP address in the Columnstore System Configuration file" << endl;
          exit(-1);
        }

        if (moduleType == "pm")
        {
          switch (nicID)
          {
            case 1: performancemodule.moduleIP1 = moduleIPAddr; break;

            case 2: performancemodule.moduleIP2 = moduleIPAddr; break;

            case 3: performancemodule.moduleIP3 = moduleIPAddr; break;

            case 4: performancemodule.moduleIP4 = moduleIPAddr; break;
          }

          if (maxPMNicCount < nicID)
            maxPMNicCount = nicID;
        }

        if (nicID > 1)
          continue;

        // set port addresses
        if (moduleName == systemParentOAMModuleName)
        {
          parentOAMModuleIPAddr = moduleIPAddr;

          // exit out if parentOAMModuleIPAddr is NOT set, this means the System Columnstore.xml isn't
          // configured
          if (parentOAMModuleIPAddr == "0.0.0.0")
          {
            cout << "ERROR: System Columnstore.xml not configured" << endl;
            exit(-1);
          }

          // set Parent Processes Port IP Address
          string parentProcessMonitor = systemParentOAMModuleName + "_ProcessMonitor";
          sysConfigNew->setConfig(parentProcessMonitor, "IPAddr", parentOAMModuleIPAddr);
          sysConfigNew->setConfig(parentProcessMonitor, "Port", "8800");
          sysConfigNew->setConfig("ProcMgr", "IPAddr", parentOAMModuleIPAddr);
          sysConfigNew->setConfig("ProcMgr_Alarm", "IPAddr", parentOAMModuleIPAddr);
          sysConfigNew->setConfig("ProcStatusControl", "IPAddr", parentOAMModuleIPAddr);
          string parentServerMonitor = systemParentOAMModuleName + "_ServerMonitor";
          sysConfigNew->setConfig(parentServerMonitor, "IPAddr", parentOAMModuleIPAddr);
          sysConfigNew->setConfig(parentServerMonitor, "Port", "8622");

          if (build3)
          {
            string portName = systemParentOAMModuleName + "_WriteEngineServer";
            sysConfigNew->setConfig(portName, "IPAddr", parentOAMModuleIPAddr);
            sysConfigNew->setConfig(portName, "Port", "8630");
          }
          else
          {
            sysConfigNew->setConfig("DDLProc", "IPAddr", parentOAMModuleIPAddr);
            sysConfigNew->setConfig("DMLProc", "IPAddr", parentOAMModuleIPAddr);
          }

          if (IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM)
          {
            // set User Module's IP Addresses
            string Section = "ExeMgr" + oam.itoa(moduleID);

            sysConfigNew->setConfig(Section, "IPAddr", moduleIPAddr);
            sysConfigNew->setConfig(Section, "Port", "8601");

            // set Performance Module's IP's to first NIC IP entered
            sysConfigNew->setConfig("DDLProc", "IPAddr", moduleIPAddr);
            sysConfigNew->setConfig("DMLProc", "IPAddr", moduleIPAddr);
          }
        }
        else
        {
          // set child Process Monitor Port IP Address
          string portName = moduleName + "_ProcessMonitor";
          sysConfigNew->setConfig(portName, "IPAddr", moduleIPAddr);
          sysConfigNew->setConfig(portName, "Port", "8800");

          // set child Server Monitor Port IP Address
          portName = moduleName + "_ServerMonitor";
          sysConfigNew->setConfig(portName, "IPAddr", moduleIPAddr);
          sysConfigNew->setConfig(portName, "Port", "8622");

          // set Performance Module WriteEngineServer Port IP Address
          if (moduleType == "pm" && build3)
          {
            portName = moduleName + "_WriteEngineServer";
            sysConfigNew->setConfig(portName, "IPAddr", moduleIPAddr);
            sysConfigNew->setConfig(portName, "Port", "8630");
          }

          // set User Module's IP Addresses
          if (moduleType == "um" ||
              (moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM))
          {
            string Section = "ExeMgr" + oam.itoa(moduleID);

            sysConfigNew->setConfig(Section, "IPAddr", moduleIPAddr);
            sysConfigNew->setConfig(Section, "Port", "8601");
          }
        }

        // set Performance Module's IP's to first NIC IP entered
        if (moduleName == "um1" && build3)
        {
          sysConfigNew->setConfig("DDLProc", "IPAddr", moduleIPAddr);
          sysConfigNew->setConfig("DMLProc", "IPAddr", moduleIPAddr);
        }

        // setup DBRM processes
        if (moduleName == systemParentOAMModuleName)
          sysConfigNew->setConfig(dbrmMainProc, "IPAddr", moduleIPAddr);

        // if ( moduleDisableState == oam::ENABLEDSTATE )
        //{
        DBRMworkernodeID++;
        string DBRMSection = dbrmSubProc + oam.itoa(DBRMworkernodeID);
        sysConfigNew->setConfig(DBRMSection, "IPAddr", moduleIPAddr);
        sysConfigNew->setConfig(DBRMSection, "Module", moduleName);
        //}
      }  // end of nicID loop

      // set dbroot assigments
      DeviceDBRootList::iterator pt3 = sysModuleTypeConfig.moduletypeconfig[i].ModuleDBRootList.begin();

      // this will be empty if upgrading from 2.2
      if (sysModuleTypeConfig.moduletypeconfig[i].ModuleDBRootList.size() == 0)
      {
        if (!OLDbuild3 && moduleType == "pm")
        {
          int dbrootCount = dbrootCountPerModule;
          string moduleCountParm = "ModuleDBRootCount" + oam.itoa(moduleID) + "-" + oam.itoa(i + 1);

          try
          {
            sysConfigNew->setConfig(ModuleSection, moduleCountParm, oam.itoa(dbrootCount));
          }
          catch (...)
          {
            cout << "ERROR: Problem setting Host Name in the Columnstore System Configuration file" << endl;
            exit(-1);
          }

          int entry = 1;

          for (; entry < dbrootCountPerModule + 1; entry++)
          {
            int dbrootid = dbrootNum;

            if (dbrootNum > systemDBRootCount)
              dbrootid = 0;
            else
              dbrootNum++;

            string moduleDBRootIDParm =
                "ModuleDBRootID" + oam.itoa(moduleID) + "-" + oam.itoa(entry) + "-" + oam.itoa(i + 1);

            try
            {
              sysConfigNew->setConfig(ModuleSection, moduleDBRootIDParm, oam.itoa(dbrootid));
            }
            catch (...)
            {
              cout << "ERROR: Problem setting Host Name in the Columnstore System Configuration file" << endl;
              exit(-1);
            }
          }
        }
      }
      else
      {
        for (; pt3 != sysModuleTypeConfig.moduletypeconfig[i].ModuleDBRootList.end(); pt3++)
        {
          if ((*pt3).dbrootConfigList.size() > 0)
          {
            int moduleID = (*pt3).DeviceID;

            DBRootConfigList::iterator pt4 = (*pt3).dbrootConfigList.begin();

            int dbrootCount = (*pt3).dbrootConfigList.size();
            string moduleCountParm = "ModuleDBRootCount" + oam.itoa(moduleID) + "-" + oam.itoa(i + 1);

            try
            {
              sysConfigNew->setConfig(ModuleSection, moduleCountParm, oam.itoa(dbrootCount));
            }
            catch (...)
            {
              cout << "ERROR: Problem setting Host Name in the Columnstore System Configuration file" << endl;
              exit(-1);
            }

            int entry = 1;

            for (; pt4 != (*pt3).dbrootConfigList.end(); pt4++, entry++)
            {
              int dbrootid = *pt4;

              string moduleDBRootIDParm =
                  "ModuleDBRootID" + oam.itoa(moduleID) + "-" + oam.itoa(entry) + "-" + oam.itoa(i + 1);

              try
              {
                sysConfigNew->setConfig(ModuleSection, moduleDBRootIDParm, oam.itoa(dbrootid));
              }
              catch (...)
              {
                cout << "ERROR: Problem setting Host Name in the Columnstore System Configuration file"
                     << endl;
                exit(-1);
              }
            }
          }
        }
      }

      if ((moduleType == "pm") || (IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM))
        performancemodulelist.push_back(performancemodule);

    }  // end of module loop

    sysConfigNew->write();

  }  // end of i for loop

  if (performancemodulelist.size() == 0)
  {
    cout << "ERROR: performancemodulelist is empty, exiting..." << endl;
    exit(-1);
  }

  // set dm count to 0 always
  try
  {
    sysConfigNew->setConfig(ModuleSection, "ModuleCount1", "0");
  }
  catch (...)
  {
    cout << "ERROR: Problem setting Module Count in the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  // setup DBRM Controller
  sysConfigNew->setConfig(dbrmMainProc, numSubProc, oam.itoa(DBRMworkernodeID));

  // setup PrimitiveServers parameters
  try
  {
    sysConfigNew->setConfig("PrimitiveServers", "ConnectionsPerPrimProc", oam.itoa(maxPMNicCount * 2));
  }
  catch (...)
  {
    cout << "ERROR: Problem setting ConnectionsPerPrimProc in the Columnstore System Configuration file"
         << endl;
    exit(-1);
  }

  // set the PM Ports based on Number of PM modules equipped, if any equipped
  int minPmPorts = 32;
  sysConfigNew->setConfig("PrimitiveServers", "Count", oam.itoa(pmNumber));

  int pmPorts = pmNumber * (maxPMNicCount * 2);

  if (pmPorts < minPmPorts)
    pmPorts = minPmPorts;

  if (pmNumber > 0 || (IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM))
  {
    const string PM = "PMS";

    for (int pmsID = 1; pmsID < pmPorts + 1;)
    {
      for (unsigned int j = 1; j < maxPMNicCount + 1; j++)
      {
        PerformanceModuleList::iterator list1 = performancemodulelist.begin();

        for (; list1 != performancemodulelist.end(); list1++)
        {
          string pmName = PM + oam.itoa(pmsID);
          string IpAddr;

          switch (j)
          {
            case 1: IpAddr = (*list1).moduleIP1; break;

            case 2: IpAddr = (*list1).moduleIP2; break;

            case 3: IpAddr = (*list1).moduleIP3; break;

            case 4: IpAddr = (*list1).moduleIP4; break;
          }

          if (!IpAddr.empty() && IpAddr != oam::UnassignedIpAddr)
          {
            sysConfigNew->setConfig(pmName, "IPAddr", IpAddr);
            pmsID++;

            if (pmsID > pmPorts)
              break;
          }
        }

        if (pmsID > pmPorts)
          break;
      }
    }
  }

  sysConfigNew->write();

  //
  // Configure NMS Addresses
  //

  string NMSIPAddress;

  try
  {
    NMSIPAddress = sysConfigOld->getConfig(SystemSection, "NMSIPAddress");
  }
  catch (...)
  {
    cout << "ERROR: Problem getting NMSIPAddress from Columnstore System Configuration file" << endl;
    exit(-1);
  }

  try
  {
    sysConfigNew->setConfig(SystemSection, "NMSIPAddress", NMSIPAddress);
  }
  catch (...)
  {
    cout << "ERROR: Problem setting NMSIPAddress in the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  //
  // setup TransactionArchivePeriod
  //

  string transactionArchivePeriod;

  try
  {
    transactionArchivePeriod = sysConfigOld->getConfig(SystemSection, "TransactionArchivePeriod");
  }
  catch (...)
  {
    cout << "ERROR: Problem getting transactionArchivePeriod from Columnstore System Configuration file"
         << endl;
    exit(-1);
  }

  try
  {
    sysConfigNew->setConfig(SystemSection, "TransactionArchivePeriod", transactionArchivePeriod);
  }
  catch (...)
  {
    cout << "ERROR: Problem setting IP address in the Columnstore System Configuration file" << endl;
    exit(-1);
  }

  //
  // 3 and above configuration items
  //
  if (build3)
  {
    // setup cloud parameters
    string UMStorageType;
    string PMInstanceType;
    string UMInstanceType;
    string UMSecurityGroup;
    string UMVolumeSize;
    string PMVolumeSize;
    string AmazonAutoTagging;
    string AmazonVPCNextPrivateIP;
    string AmazonDeviceName;
    string UMVolumeType;
    string UMVolumeIOPS;
    string PMVolumeType;
    string PMVolumeIOPS;

    try
    {
      cloud = sysConfigOld->getConfig(InstallSection, "Cloud");
      UMStorageType = sysConfigOld->getConfig(InstallSection, "UMStorageType");
      PMInstanceType = sysConfigOld->getConfig(InstallSection, "PMInstanceType");
      UMInstanceType = sysConfigOld->getConfig(InstallSection, "UMInstanceType");
      UMSecurityGroup = sysConfigOld->getConfig(InstallSection, "UMSecurityGroup");
      UMVolumeSize = sysConfigOld->getConfig(InstallSection, "UMVolumeSize");
      PMVolumeSize = sysConfigOld->getConfig(InstallSection, "PMVolumeSize");
      AmazonAutoTagging = sysConfigOld->getConfig(InstallSection, "AmazonAutoTagging");
      AmazonVPCNextPrivateIP = sysConfigOld->getConfig(InstallSection, "AmazonVPCNextPrivateIP");
      AmazonDeviceName = sysConfigOld->getConfig(InstallSection, "AmazonDeviceName");
      UMVolumeType = sysConfigOld->getConfig(InstallSection, "UMVolumeType");
      UMVolumeIOPS = sysConfigOld->getConfig(InstallSection, "UMVolumeIOPS");
      PMVolumeType = sysConfigOld->getConfig(InstallSection, "PMVolumeType");
      PMVolumeIOPS = sysConfigOld->getConfig(InstallSection, "PMVolumeIOPS");
    }
    catch (...)
    {
    }

    // this is for 2.2 to 4.x builds
    if (UMStorageType.empty() || UMStorageType == "")
      UMStorageType = "internal";

    // 3.x upgrade
    if (build3 && !build40 && !build401)
    {
      if (cloud == "no" || cloud == oam::UnassignedName)
        cloud = "n";

      if (cloud == "amazon-ec2" || cloud == "amazon-vpc")
        cloud = "amazon";
    }

    // 4.0 upgrade
    if (build40 && !build401)
    {
      if (cloud == "no" || cloud == "n")
        cloud = oam::UnassignedName;
    }

    // 4.0.1+ upgrade
    if (build401)
    {
      if (cloud == "no" || cloud == "n")
        cloud = oam::UnassignedName;

      if (cloud == "amazon")
        cloud = "amazon-ec2";

      if (AmazonVPCNextPrivateIP.empty())
        AmazonVPCNextPrivateIP = oam::UnassignedName;

      try
      {
        sysConfigNew->setConfig(InstallSection, "AmazonVPCNextPrivateIP", AmazonVPCNextPrivateIP);
      }
      catch (...)
      {
        //		cout << "ERROR: Problem setting Cloud Parameters from the Columnstore System
        //Configuration file" << endl; 		exit(-1);
      }
    }

    try
    {
      sysConfigNew->setConfig(InstallSection, "Cloud", cloud);
      sysConfigNew->setConfig(InstallSection, "UMStorageType", UMStorageType);
      sysConfigNew->setConfig(InstallSection, "PMInstanceType", PMInstanceType);
      sysConfigNew->setConfig(InstallSection, "UMInstanceType", UMInstanceType);
      sysConfigNew->setConfig(InstallSection, "UMSecurityGroup", UMSecurityGroup);
      sysConfigNew->setConfig(InstallSection, "UMVolumeSize", UMVolumeSize);
      sysConfigNew->setConfig(InstallSection, "PMVolumeSize", PMVolumeSize);
      sysConfigNew->setConfig(InstallSection, "AmazonAutoTagging", AmazonAutoTagging);
      sysConfigNew->setConfig(InstallSection, "AmazonDeviceName", AmazonDeviceName);

      sysConfigNew->setConfig(InstallSection, "UMVolumeType", UMVolumeType);
      sysConfigNew->setConfig(InstallSection, "UMVolumeIOPS", UMVolumeIOPS);
      sysConfigNew->setConfig(InstallSection, "PMVolumeType", PMVolumeType);
      sysConfigNew->setConfig(InstallSection, "PMVolumeIOPS", PMVolumeIOPS);
    }
    catch (...)
    {
      //		cout << "ERROR: Problem setting Cloud Parameters from the Columnstore System
      //Configuration file" << endl; 		exit(-1);
    }

    if (cloud == "amazon-ec2" || cloud == "amazon-vpc")
      cloud = "amazon";

    // setup um storage
    if (cloud == "amazon" && UMStorageType == "external")
    {
      try
      {
        systemStorageInfo_t t;
        t = oam.getStorageConfig();

        ModuleTypeConfig moduletypeconfig;
        oam.getSystemConfig("um", moduletypeconfig);

        for (int id = 1; id < moduletypeconfig.ModuleCount + 1; id++)
        {
          string volumeNameID = "UMVolumeName" + oam.itoa(id);
          string volumeName = oam::UnassignedName;
          string deviceNameID = "UMVolumeDeviceName" + oam.itoa(id);
          string deviceName = oam::UnassignedName;

          try
          {
            volumeName = sysConfigOld->getConfig(InstallSection, volumeNameID);
            deviceName = sysConfigOld->getConfig(InstallSection, deviceNameID);
          }
          catch (...)
          {
          }

          try
          {
            sysConfigNew->setConfig(InstallSection, volumeNameID, volumeName);
            sysConfigNew->setConfig(InstallSection, deviceNameID, deviceName);
          }
          catch (...)
          {
          }
        }
      }
      catch (exception& e)
      {
        cout << endl << "**** getStorageConfig Failed :  " << e.what() << endl;
      }
    }

    // setup dbroot storage
    try
    {
      DBRootConfigList dbrootConfigList;
      oam.getSystemDbrootConfig(dbrootConfigList);

      DBRootConfigList::iterator pt = dbrootConfigList.begin();

      for (; pt != dbrootConfigList.end(); pt++)
      {
        int id = *pt;
        string DBrootID = "DBRoot" + oam.itoa(id);
        ;
        string pathID = "/var/lib/columnstore/data" + oam.itoa(id);

        try
        {
          sysConfigNew->setConfig(SystemSection, DBrootID, pathID);
        }
        catch (...)
        {
          cout << "ERROR: Problem setting DBRoot in the Columnstore System Configuration file" << endl;
          exit(-1);
        }

        if (cloud == "amazon" && DBRootStorageType == "external")
        {
          string volumeNameID = "PMVolumeName" + oam.itoa(id);
          string volumeName = oam::UnassignedName;
          string deviceNameID = "PMVolumeDeviceName" + oam.itoa(id);
          string deviceName = oam::UnassignedName;
          string amazondeviceNameID = "PMVolumeAmazonDeviceName" + oam.itoa(id);
          string amazondeviceName = oam::UnassignedName;

          try
          {
            volumeName = sysConfigOld->getConfig(InstallSection, volumeNameID);
            deviceName = sysConfigOld->getConfig(InstallSection, deviceNameID);
            amazondeviceName = sysConfigOld->getConfig(InstallSection, amazondeviceNameID);
          }
          catch (...)
          {
          }

          try
          {
            sysConfigNew->setConfig(InstallSection, volumeNameID, volumeName);
            sysConfigNew->setConfig(InstallSection, deviceNameID, deviceName);
            sysConfigNew->setConfig(InstallSection, amazondeviceNameID, amazondeviceName);
          }
          catch (...)
          {
          }

          string UMVolumeSize = oam::UnassignedName;
          string PMVolumeSize = oam::UnassignedName;

          try
          {
            UMVolumeSize = sysConfigOld->getConfig(InstallSection, "UMVolumeSize");
            PMVolumeSize = sysConfigOld->getConfig(InstallSection, "PMVolumeSize");
          }
          catch (...)
          {
          }

          try
          {
            sysConfigNew->setConfig(InstallSection, "UMVolumeSize", UMVolumeSize);
            sysConfigNew->setConfig(InstallSection, "PMVolumeSize", PMVolumeSize);
          }
          catch (...)
          {
          }
        }

        if (!DataRedundancyConfig.empty())
        {
          try
          {
            string dbrootPMsID = "DBRoot" + oam.itoa(id) + "PMs";
            string dbrootPMs = sysConfigOld->getConfig("DataRedundancyConfig", dbrootPMsID);

            try
            {
              sysConfigNew->setConfig("DataRedundancyConfig", dbrootPMsID, dbrootPMs);
            }
            catch (...)
            {
            }
          }
          catch (...)
          {
          }
        }
      }
    }
    catch (exception& e)
    {
      cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
    }
  }
  else
  {
    // pre 3.0 only

    string DBRootStorageLoc;

    for (int i = 1; i < DBRootCount + 1; i++)
    {
      if (DBRootStorageType != "local")
      {
        string DBRootStorageLocID = "DBRootStorageLoc" + oam.itoa(i);

        try
        {
          DBRootStorageLoc = sysConfigOld->getConfig(InstallSection, DBRootStorageLocID);
        }
        catch (...)
        {
          cout << "ERROR: Problem getting '" + DBRootStorageLocID +
                      "' from the Columnstore System Configuration file"
               << endl;
          exit(-1);
        }

        try
        {
          sysConfigNew->setConfig(InstallSection, DBRootStorageLocID, DBRootStorageLoc);
        }
        catch (...)
        {
          cout << "ERROR: Problem setting '" + DBRootStorageLocID +
                      "' in the Columnstore System Configuration file"
               << endl;
          exit(-1);
        }
      }

      string DBrootID = "DBRoot" + oam.itoa(i);
      string pathID = "/var/lib/columnstore/data" + oam.itoa(i);

      try
      {
        sysConfigNew->setConfig(SystemSection, DBrootID, pathID);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting DBRoot in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }

  // do elastic IP configuration
  int AmazonElasticIPCount = 0;

  try
  {
    AmazonElasticIPCount = atoi(sysConfigOld->getConfig(InstallSection, "AmazonElasticIPCount").c_str());

    if (AmazonElasticIPCount > 0)
    {
      for (int id = 1; id < AmazonElasticIPCount + 1; id++)
      {
        string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
        string ELmoduleName;
        string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
        string ELIPaddress;

        ELmoduleName = sysConfigOld->getConfig(InstallSection, AmazonElasticModule);
        ELIPaddress = sysConfigOld->getConfig(InstallSection, AmazonElasticIPAddr);

        try
        {
          sysConfigNew->setConfig(InstallSection, "AmazonElasticIPCount", oam.itoa(AmazonElasticIPCount));
          sysConfigNew->setConfig(InstallSection, AmazonElasticModule, ELmoduleName);
          sysConfigNew->setConfig(InstallSection, AmazonElasticIPAddr, ELIPaddress);
        }
        catch (...)
        {
        }
      }
    }
  }
  catch (...)
  {
  }

  try
  {
    oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
  }
  catch (...)
  {
    AmazonElasticIPCount = 0;
  }

  // ConcurrentTransactions
  string ConcurrentTransactions;

  try
  {
    ConcurrentTransactions = sysConfigOld->getConfig(SystemSection, "ConcurrentTransactions");

    if (!ConcurrentTransactions.empty())
    {
      try
      {
        sysConfigNew->setConfig(SystemSection, "ConcurrentTransactions", ConcurrentTransactions);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting ConcurrentTransactions in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  // NetworkCompression Enabled
  string NetworkCompression;

  try
  {
    NetworkCompression = sysConfigOld->getConfig("NetworkCompression", "Enabled");

    if (!NetworkCompression.empty())
    {
      try
      {
        sysConfigNew->setConfig("NetworkCompression", "Enabled", NetworkCompression);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting NetworkCompression in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  // hadoop
  string DataFilePlugin;

  try
  {
    DataFilePlugin = sysConfigOld->getConfig(SystemSection, "DataFilePlugin");

    if (!DataFilePlugin.empty())
    {
      try
      {
        sysConfigNew->setConfig(SystemSection, "DataFilePlugin", DataFilePlugin);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting DataFilePlugin in the Columnstore System Configuration file" << endl;
        exit(-1);
      }

      string ExtentsPerSegmentFile;

      try
      {
        ExtentsPerSegmentFile = sysConfigOld->getConfig("ExtentMap", "ExtentsPerSegmentFile");

        try
        {
          sysConfigNew->setConfig("ExtentMap", "ExtentsPerSegmentFile", ExtentsPerSegmentFile);
        }
        catch (...)
        {
          cout << "ERROR: Problem setting ExtentsPerSegmentFile in the Columnstore System Configuration file"
               << endl;
          exit(-1);
        }
      }
      catch (...)
      {
      }
    }
  }
  catch (...)
  {
  }

  string DataFileLog;

  try
  {
    DataFileLog = sysConfigOld->getConfig(SystemSection, "DataFileLog");

    if (!DataFileLog.empty())
    {
      try
      {
        sysConfigNew->setConfig(SystemSection, "DataFileLog", DataFileLog);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting DataFileLog in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  string AllowDiskBasedJoin;
  string TempFileCompression;

  try
  {
    AllowDiskBasedJoin = sysConfigOld->getConfig("HashJoin", "AllowDiskBasedJoin");

    if (!AllowDiskBasedJoin.empty())
    {
      TempFileCompression = sysConfigOld->getConfig("HashJoin", "TempFileCompression");

      try
      {
        sysConfigNew->setConfig("HashJoin", "AllowDiskBasedJoin", AllowDiskBasedJoin);
        sysConfigNew->setConfig("HashJoin", "TempFileCompression", TempFileCompression);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting AllowDiskBasedJoin in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  string AllowDiskBasedAggregation;
  //    string TempFileCompression;

  try
  {
    AllowDiskBasedAggregation = sysConfigOld->getConfig("RowAggregation", "AllowDiskBasedAggregation");

    if (!AllowDiskBasedAggregation.empty())
    {
      try
      {
        sysConfigNew->setConfig("RowAggregation", "AllowDiskBasedAggregation", AllowDiskBasedAggregation);
      }
      catch (...)
      {
        cout
            << "ERROR: Problem setting AllowDiskBasedAggregation in the Columnstore System Configuration file"
            << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  string SystemTempFileDir;

  try
  {
    SystemTempFileDir = sysConfigOld->getConfig("SystemConfig", "SystemTempFileDir");

    if (!SystemTempFileDir.empty())
    {
      SystemTempFileDir = sysConfigOld->getConfig("SystemConfig", "SystemTempFileDir");

      try
      {
        sysConfigNew->setConfig("SystemConfig", "SystemTempFileDir", SystemTempFileDir);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting SystemTempFileDir in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  try
  {
    Host = sysConfigOld->getConfig("QueryTele", "Host");

    if (!Host.empty())
    {
      Port = sysConfigOld->getConfig("QueryTele", "Port");

      try
      {
        sysConfigNew->setConfig("QueryTele", "Host", Host);
        sysConfigNew->setConfig("QueryTele", "Port", Port);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting QueryTele in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  try
  {
    string AmazonAccessKey = sysConfigOld->getConfig("Installation", "AmazonAccessKey");

    if (!AmazonAccessKey.empty())
    {
      try
      {
        sysConfigNew->setConfig("Installation", "AmazonAccessKey", AmazonAccessKey);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting AmazonAccessKey in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  try
  {
    string AmazonSecretKey = sysConfigOld->getConfig("Installation", "AmazonSecretKey");

    if (!AmazonSecretKey.empty())
    {
      try
      {
        sysConfigNew->setConfig("Installation", "AmazonSecretKey", AmazonSecretKey);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting AmazonSecretKey in the Columnstore System Configuration file" << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  try
  {
    string LockFileDirectory = sysConfigOld->getConfig("Installation", "LockFileDirectory");

    if (!LockFileDirectory.empty())
    {
      try
      {
        sysConfigNew->setConfig("Installation", "LockFileDirectory", LockFileDirectory);
      }
      catch (...)
      {
        cout << "ERROR: Problem setting LockFileDirectory in the Columnstore System Configuration file"
             << endl;
        exit(-1);
      }
    }
  }
  catch (...)
  {
  }

  // add entries from tuning guide

  string ColScanReadAheadBlocks;
  string PrefetchThreshold;
  string MaxOutstandingRequests;
  string PmMaxMemorySmallSide;
  string ThreadPoolSize;

  try
  {
    ColScanReadAheadBlocks = sysConfigOld->getConfig("PrimitiveServers", "ColScanReadAheadBlocks");
    PrefetchThreshold = sysConfigOld->getConfig("PrimitiveServers", "PrefetchThreshold");
    PmMaxMemorySmallSide = sysConfigOld->getConfig("HashJoin", "PmMaxMemorySmallSide");
    ThreadPoolSize = sysConfigOld->getConfig("JobList", "ThreadPoolSize");
  }
  catch (...)
  {
  }

  try
  {
    sysConfigNew->setConfig("PrimitiveServers", "ColScanReadAheadBlocks", ColScanReadAheadBlocks);
    sysConfigNew->setConfig("PrimitiveServers", "PrefetchThreshold", PrefetchThreshold);
    sysConfigNew->setConfig("HashJoin", "PmMaxMemorySmallSide", PmMaxMemorySmallSide);
    sysConfigNew->setConfig("JobList", "ThreadPoolSize", ThreadPoolSize);
  }
  catch (...)
  {
  }

  // ExeMgr Optional settings
  try
  {
    // threadpool size. This is the size the pool will idle down to if it grows bigger
    string threadPoolSize;
    // Time between checks to see if memory is exausted
    string secondsBetweenMemChecks;
    // Max percent of total memory used by everything before we kill ourself
    string maxPct;
    // Maximum number of concurrent queries. Any more will have to wait.
    string execQueueSize;
    threadPoolSize = sysConfigOld->getConfig("ExeMgr1", "ThreadPoolSize");
    if (!threadPoolSize.empty())
    {
      sysConfigNew->setConfig("ExeMgr1", "ThreadPoolSize", threadPoolSize);
    }

    secondsBetweenMemChecks = sysConfigOld->getConfig("ExeMgr1", "SecondsBetweenMemChecks");
    if (!secondsBetweenMemChecks.empty())
    {
      sysConfigNew->setConfig("ExeMgr1", "SecondsBetweenMemChecks", secondsBetweenMemChecks);
    }

    maxPct = sysConfigOld->getConfig("ExeMgr1", "MaxPct");
    if (!maxPct.empty())
    {
      sysConfigNew->setConfig("ExeMgr1", "MaxPct", maxPct);
    }

    execQueueSize = sysConfigOld->getConfig("ExeMgr1", "ExecQueueSize");
    if (!execQueueSize.empty())
    {
      sysConfigNew->setConfig("ExeMgr1", "ExecQueueSize", execQueueSize);
    }
  }
  catch (...)
  {
  }

  // PrimProc optional parameters
  // Max percent of total memory used by everything before we kill the current query
  // For 5.6.1, this setting uses the same mechanism that ExeMgr uses to kill itself
  try
  {
    string maxPct;
    maxPct = sysConfigOld->getConfig("PrimitiveServers", "MaxPct");
    if (!maxPct.empty())
    {
      sysConfigNew->setConfig("PrimitiveServers", "MaxPct", maxPct);
    }
  }
  catch (...)
  {
  }

  // Write out Updated System Configuration File
  sysConfigNew->write();
}
