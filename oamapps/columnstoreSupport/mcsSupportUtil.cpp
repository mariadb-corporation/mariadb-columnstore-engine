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

#include "mcsSupportUtil.h"

using namespace std;
using namespace oam;
using namespace config;

void getSystemNetworkConfig(FILE* pOutputFile)
{
  Oam oam;
  // get and display Module Network Config
  SystemModuleTypeConfig systemmoduletypeconfig;
  systemmoduletypeconfig.moduletypeconfig.clear();

  // get max length of a host name for header formatting

  int maxSize = 9;

  try
  {
    oam.getSystemConfig(systemmoduletypeconfig);

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
      string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

      if (moduleCount > 0)
      {
        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
        {
          HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

          for (; pt1 != (*pt).hostConfigList.end(); pt1++)
          {
            if (maxSize < (int)(*pt1).HostName.size())
              maxSize = (*pt1).HostName.size();
          }
        }
      }
    }
  }
  catch (exception& e)
  {
    fprintf(pOutputFile, "**** getNetworkConfig Failed =  %s\n\n", e.what());
  }

  fprintf(pOutputFile, "%-15s%-30s%-10s%-14s%-20s\n", "Module Name", "Module Description", "NIC ID",
          "Host Name", "IP Address");
  fprintf(pOutputFile, "%-15s%-30s%-10s%-14s%-20s\n", "-----------", "-------------------------", "------",
          "---------", "---------------");

  try
  {
    oam.getSystemConfig(systemmoduletypeconfig);

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
      string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

      if (moduleCount > 0)
      {
        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
        {
          string modulename = (*pt).DeviceName;
          string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);
          string modulenamedesc = moduletypedesc + " #" + moduleID;

          fprintf(pOutputFile, "%-15s%-30s", modulename.c_str(), modulenamedesc.c_str());

          HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

          for (; pt1 != (*pt).hostConfigList.end(); pt1++)
          {
            /* MCOL-1607.  IPAddr may be a host name here b/c it is read straight
            from the config file. */
            string tmphost = getIPAddress(pt1->IPAddr);
            string ipAddr;
            if (tmphost.empty())
              ipAddr = pt1->IPAddr;
            else
              ipAddr = tmphost;
            string hostname = (*pt1).HostName;
            string nicID = oam.itoa((*pt1).NicID);

            if (nicID != "1")
            {
              fprintf(pOutputFile, "%-45s", "");
            }
            fprintf(pOutputFile, "%-13s%-14s%-20s\n", nicID.c_str(), hostname.c_str(), ipAddr.c_str());
          }
        }
      }
    }
  }
  catch (exception& e)
  {
    fprintf(pOutputFile, "**** getNetworkConfig Failed =  %s\n\n", e.what());
  }
}

void getModuleTypeConfig(FILE* pOutputFile)
{
  Oam oam;
  SystemModuleTypeConfig systemmoduletypeconfig;
  ModuleTypeConfig moduletypeconfig;
  ModuleConfig moduleconfig;
  systemmoduletypeconfig.moduletypeconfig.clear();

  try
  {
    oam.getSystemConfig(systemmoduletypeconfig);

    fprintf(pOutputFile, "Module Type Configuration\n\n");

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

      if (moduleCount < 1)
        continue;

      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

      fprintf(pOutputFile, "ModuleType '%s' Configuration information\n", moduletype.c_str());
      fprintf(pOutputFile, "ModuleDesc = %s\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc.c_str());
      fprintf(pOutputFile, "ModuleCount = %i\n", moduleCount);

      if (moduleCount > 0)
      {
        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
        {
          string modulename = (*pt).DeviceName;
          HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

          for (; pt1 != (*pt).hostConfigList.end(); pt1++)
          {
            string ipAddr = (*pt1).IPAddr;
            string servername = (*pt1).HostName;
            fprintf(pOutputFile, "ModuleHostName and ModuleIPAddr for NIC ID %u on module '%s' = %s ,  %s\n",
                    (*pt1).NicID, modulename.c_str(), servername.c_str(), ipAddr.c_str());
          }
        }
      }

      DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();

      for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end(); pt++)
      {
        if ((*pt).dbrootConfigList.size() > 0)
        {
          fprintf(pOutputFile, "DBRootIDs assigned to module 'pm%u' = ", (*pt).DeviceID);
          DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

          for (; pt1 != (*pt).dbrootConfigList.end();)
          {
            fprintf(pOutputFile, "%u", *pt1);
            pt1++;

            if (pt1 != (*pt).dbrootConfigList.end())
              fprintf(pOutputFile, ", ");
          }
        }
        fprintf(pOutputFile, "\n");
      }

      fprintf(pOutputFile, "ModuleCPUCriticalThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUCriticalThreshold);
      fprintf(pOutputFile, "ModuleCPUMajorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMajorThreshold);
      fprintf(pOutputFile, "ModuleCPUMinorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorThreshold);
      fprintf(pOutputFile, "ModuleCPUMinorClearThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorClearThreshold);
      fprintf(pOutputFile, "ModuleDiskCriticalThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskCriticalThreshold);
      fprintf(pOutputFile, "ModuleDiskMajorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMajorThreshold);
      fprintf(pOutputFile, "ModuleDiskMinorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMinorThreshold);
      fprintf(pOutputFile, "ModuleMemCriticalThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleMemCriticalThreshold);
      fprintf(pOutputFile, "ModuleMemMajorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMajorThreshold);
      fprintf(pOutputFile, "ModuleMemMinorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMinorThreshold);
      fprintf(pOutputFile, "ModuleSwapCriticalThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapCriticalThreshold);
      fprintf(pOutputFile, "ModuleSwapMajorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMajorThreshold);
      fprintf(pOutputFile, "ModuleSwapMinorThreshold = %u%%\n",
              systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMinorThreshold);

      DiskMonitorFileSystems::iterator pt2 = systemmoduletypeconfig.moduletypeconfig[i].FileSystems.begin();
      int id = 1;

      for (; pt2 != systemmoduletypeconfig.moduletypeconfig[i].FileSystems.end(); pt2++)
      {
        string fs = *pt2;
        fprintf(pOutputFile, "ModuleDiskMonitorFileSystem#%i =  %s\n", id, fs.c_str());
        ++id;
      }
      fprintf(pOutputFile, "\n");
    }
  }
  catch (exception& e)
  {
    cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
  }
}

void getStorageConfig(FILE* pOutputFile)
{
  Oam oam;
  try
  {
    systemStorageInfo_t t;
    t = oam.getStorageConfig();

    string cloud;

    try
    {
      oam.getSystemConfig("Cloud", cloud);
    }
    catch (...)
    {
    }

    string::size_type pos = cloud.find("amazon", 0);

    if (pos != string::npos)
      cloud = "amazon";

    fprintf(pOutputFile, "System Storage Configuration\n");

    fprintf(pOutputFile, "Performance Module (DBRoot) Storage Type = %s\n", boost::get<0>(t).c_str());

    if (cloud == "amazon")
      fprintf(pOutputFile, "User Module Storage Type = %s\n", boost::get<3>(t).c_str());

    fprintf(pOutputFile, "System Assigned DBRoot Count = %i\n", boost::get<1>(t));

    DeviceDBRootList moduledbrootlist = boost::get<2>(t);

    typedef std::vector<int> dbrootList;
    dbrootList dbrootlist;

    DeviceDBRootList::iterator pt = moduledbrootlist.begin();

    for (; pt != moduledbrootlist.end(); pt++)
    {
      fprintf(pOutputFile, "DBRoot IDs assigned to 'pm%u' = ", (*pt).DeviceID);
      DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

      for (; pt1 != (*pt).dbrootConfigList.end();)
      {
        fprintf(pOutputFile, "%u", *pt1);
        dbrootlist.push_back(*pt1);
        pt1++;

        if (pt1 != (*pt).dbrootConfigList.end())
          fprintf(pOutputFile, ", ");
      }

      fprintf(pOutputFile, "\n");
    }

    // get any unassigned DBRoots
    /*DBRootConfigList undbrootlist;

    try
    {
        oam.getUnassignedDbroot(undbrootlist);
    }
    catch (...) {}

    if ( !undbrootlist.empty() )
    {
        fprintf(pOutputFile,"DBRoot IDs unassigned = ");
        DBRootConfigList::iterator pt1 = undbrootlist.begin();

        for ( ; pt1 != undbrootlist.end() ;)
        {
            fprintf(pOutputFile,"%u",*pt1);
            pt1++;

            if (pt1 != undbrootlist.end())
                fprintf(pOutputFile,", ");
        }

        fprintf(pOutputFile,"\n");
    }*/

    fprintf(pOutputFile, "\n");

    // um volumes
    if (cloud == "amazon" && boost::get<3>(t) == "external")
    {
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
          oam.getSystemConfig(volumeNameID, volumeName);
          oam.getSystemConfig(deviceNameID, deviceName);
        }
        catch (...)
        {
        }

        fprintf(pOutputFile, "Amazon EC2 Volume Name/Device Name for 'um%i': %s, %s", id, volumeName.c_str(),
                deviceName.c_str());
      }
    }

    // pm volumes
    if (cloud == "amazon" && boost::get<0>(t) == "external")
    {
      fprintf(pOutputFile, "\n");

      DBRootConfigList dbrootConfigList;

      try
      {
        oam.getSystemDbrootConfig(dbrootConfigList);

        DBRootConfigList::iterator pt = dbrootConfigList.begin();

        for (; pt != dbrootConfigList.end(); pt++)
        {
          string volumeNameID = "PMVolumeName" + oam.itoa(*pt);
          string volumeName = oam::UnassignedName;
          string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt);
          string deviceName = oam::UnassignedName;

          try
          {
            oam.getSystemConfig(volumeNameID, volumeName);
            oam.getSystemConfig(deviceNameID, deviceName);
          }
          catch (...)
          {
            continue;
          }
        }
      }
      catch (exception& e)
      {
        cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
      }

      // print un-assigned dbroots
      /*DBRootConfigList::iterator pt1 = undbrootlist.begin();

      for ( ; pt1 != undbrootlist.end() ; pt1++)
      {
          string volumeNameID = "PMVolumeName" + oam.itoa(*pt1);
          string volumeName = oam::UnassignedName;
          string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt1);
          string deviceName = oam::UnassignedName;

          try
          {
              oam.getSystemConfig( volumeNameID, volumeName);
              oam.getSystemConfig( deviceNameID, deviceName);
          }
          catch (...)
          {
              continue;
          }
      }*/
    }

    string DataRedundancyConfig;
    int DataRedundancyCopies;
    string DataRedundancyStorageType;

    try
    {
      oam.getSystemConfig("DataRedundancyConfig", DataRedundancyConfig);
      oam.getSystemConfig("DataRedundancyCopies", DataRedundancyCopies);
      oam.getSystemConfig("DataRedundancyStorageType", DataRedundancyStorageType);
    }
    catch (...)
    {
    }

    if (DataRedundancyConfig == "y")
    {
      fprintf(pOutputFile, "\nData Redundant Configuration\n\n");
      fprintf(pOutputFile, "Copies Per DBroot = %i", DataRedundancyCopies);

      oamModuleInfo_t st;
      string moduleType;

      try
      {
        st = oam.getModuleInfo();
        moduleType = boost::get<1>(st);
      }
      catch (...)
      {
      }

      if (moduleType != "pm")
        return;

      try
      {
        DBRootConfigList dbrootConfigList;
        oam.getSystemDbrootConfig(dbrootConfigList);

        DBRootConfigList::iterator pt = dbrootConfigList.begin();

        for (; pt != dbrootConfigList.end(); pt++)
        {
          fprintf(pOutputFile, "DBRoot #%u has copies on PMs = ", *pt);

          string pmList = "";

          try
          {
            string errmsg;
            // oam.glusterctl(oam::GLUSTER_WHOHAS, oam.itoa(*pt), pmList, errmsg);
          }
          catch (...)
          {
          }

          boost::char_separator<char> sep(" ");
          boost::tokenizer<boost::char_separator<char> > tokens(pmList, sep);

          for (boost::tokenizer<boost::char_separator<char> >::iterator it = tokens.begin();
               it != tokens.end(); ++it)
          {
            fprintf(pOutputFile, "%s ", (*it).c_str());
          }

          fprintf(pOutputFile, "\n");
        }

        fprintf(pOutputFile, "\n");
      }
      catch (exception& e)
      {
        cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
      }
    }
  }
  catch (exception& e)
  {
    cout << endl << "**** getStorageConfig Failed :  " << e.what() << endl;
  }
}

void getStorageStatus(FILE* pOutputFile)
{
  Oam oam;

  fprintf(pOutputFile, "System External DBRoot Storage Statuses\n\n");
  fprintf(pOutputFile, "Component     Status                       Last Status Change\n");
  fprintf(pOutputFile, "------------  --------------------------   ------------------------\n");

  /*try
  {
      oam.getSystemStatus(systemstatus, false);

      if ( systemstatus.systemdbrootstatus.dbrootstatus.size() == 0 )
      {
          fprintf(pOutputFile," No External DBRoot Storage Configured\n\n");
          return;
      }

      for ( unsigned int i = 0 ; i < systemstatus.systemdbrootstatus.dbrootstatus.size(); i++)
      {
          if ( systemstatus.systemdbrootstatus.dbrootstatus[i].Name.empty() )
              // end of list
              break;

          int state = systemstatus.systemdbrootstatus.dbrootstatus[i].OpState;
          string stime = systemstatus.systemdbrootstatus.dbrootstatus[i].StateChangeDate ;
          stime = stime.substr (0, 24);
          fprintf(pOutputFile,"DBRoot%s%-29s%-24s\n",
                              systemstatus.systemdbrootstatus.dbrootstatus[i].Name.c_str(),
                              oamState[state].c_str(),
                              stime.c_str());
      }
      fprintf(pOutputFile,"\n");
  }
  catch (exception& e)
  {
      cout << endl << "**** getSystemStatus Failed =  " << e.what() << endl;
  }*/

  string DataRedundancyConfig;
  int DataRedundancyCopies;
  string DataRedundancyStorageType;

  try
  {
    oam.getSystemConfig("DataRedundancyConfig", DataRedundancyConfig);
    oam.getSystemConfig("DataRedundancyCopies", DataRedundancyCopies);
    oam.getSystemConfig("DataRedundancyStorageType", DataRedundancyStorageType);
  }
  catch (...)
  {
  }
}

/********************************************************************
 *
 * checkLogStatus - Check for a phrase in a log file and return status
 *
 ********************************************************************/
bool checkLogStatus(std::string fileName, std::string phrase)
{
  ifstream file(fileName.c_str());

  if (!file.is_open())
  {
    return false;
  }

  string buf;

  while (getline(file, buf))
  {
    string::size_type pos = buf.find(phrase, 0);

    if (pos != string::npos)
      // found phrase
      return true;
  }

  if (file.bad())
  {
    return false;
  }

  file.close();
  return false;
}

/******************************************************************************************
 * @brief   Get Network IP Address for Host Name
 *
 * purpose: Get Network IP Address for Host Name
 *
 ******************************************************************************************/
string getIPAddress(string hostName)
{
  static uint32_t my_bind_addr;
  struct hostent* ent;
  string IPAddr = "";
  Oam oam;

  ent = gethostbyname(hostName.c_str());

  if (ent != 0)
  {
    my_bind_addr = (uint32_t)((in_addr*)ent->h_addr_list[0])->s_addr;

    uint8_t split[4];
    uint32_t ip = my_bind_addr;
    split[0] = (ip & 0xff000000) >> 24;
    split[1] = (ip & 0x00ff0000) >> 16;
    split[2] = (ip & 0x0000ff00) >> 8;
    split[3] = (ip & 0x000000ff);

    IPAddr =
        oam.itoa(split[3]) + "." + oam.itoa(split[2]) + "." + oam.itoa(split[1]) + "." + oam.itoa(split[0]);
  }

  return IPAddr;
}
