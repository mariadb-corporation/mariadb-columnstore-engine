/* Copyright (C) 2013 Calpont Corp. */
/* Copyright (C) 2016 MariaDB Corporation */

/******************************************************************************************
 * $Id: columnstoreSupport.cpp 64 2006-10-12 22:21:51Z dhill $
 *
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
#include <readline.h>
#include <boost/filesystem.hpp>

#include "mcsconfig.h"
#include "liboamcpp.h"
#include "configcpp.h"
#include "installdir.h"
#include "mcsSupportUtil.h"
#include "columnstoreversion.h"

using namespace std;
using namespace oam;
using namespace config;

typedef struct Child_Module_struct
{
  std::string moduleName;
  std::string moduleIP;
  std::string hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

string currentDate;
string systemName;
string localModule;
string localModuleHostName;
ChildModuleList childmodulelist;
ChildModuleList parentmodulelist;
ChildModule childmodule;

string rootPassword = "";
string debug_flag = "0";
string mysqlpw = " ";
string tmpDir;

int runningThreads = 0;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

typedef boost::tuple<ChildModuleList::iterator, string> threadInfo_t;

bool LOCAL = false;

void* childReportThread(threadInfo_t* st)
{
  assert(st);
  ChildModuleList::iterator& list = boost::get<0>(*st);
  string reportType = boost::get<1>(*st);

  string remoteModuleName = (*list).moduleName;
  string remoteModuleIP = (*list).moduleIP;
  string remoteHostName = (*list).hostName;

  pthread_mutex_lock(&mutex1);
  runningThreads++;
  // cout << "++ " << runningThreads << endl;
  pthread_mutex_unlock(&mutex1);

  string outputFile;

  if (reportType == "log")
  {
    outputFile = remoteModuleName + "_" + reportType + "Report.tar.gz";
  }
  else
  {
    outputFile = remoteModuleName + "_" + reportType + "Report.txt";

    FILE* pOutputFile = fopen(outputFile.c_str(), "a");
    if (pOutputFile == NULL)
    {
      printf("Could not open file: %s", outputFile.c_str());
      exit(1);
    }

    fprintf(pOutputFile,
            "********************************************************************************\n"
            "\n"
            " System %s\n"
            " columnstoreSupportReport script ran from Module %s on %s\n"
            " SoftwareVersion = %s-%s"
            "\n"
            "********************************************************************************\n"
            "\n"
            "                     %s report\n"
            "\n"
            "********************************************************************************\n",
            systemName.c_str(), localModule.c_str(), currentDate.c_str(), columnstore_version.c_str(),
            columnstore_release.c_str(), reportType.c_str());
  }

  cout << "Get " + reportType + " report data for " + remoteModuleName + "      " << endl;

  string cmd = "remote_command.sh " + remoteModuleIP + " " + rootPassword + ";" + reportType + "Report.sh " +
               remoteModuleName + "' " + debug_flag + " - forcetty";

  int rtnCode = system(cmd.c_str());

  if (WEXITSTATUS(rtnCode) != 0)
  {
    cout << "Error with running remote_command.sh, exiting..." << endl;
  }

  cmd = "remote_scp_get.sh " + remoteModuleIP + " " + rootPassword + " " + tmpDir + "/" + outputFile +
        " > /dev/null 2>&1";
  rtnCode = system(cmd.c_str());

  if (WEXITSTATUS(rtnCode) != 0)
    cout << "ERROR: failed to retrieve " << tmpDir << "/" << outputFile << " from " + remoteHostName << endl;

  pthread_mutex_lock(&mutex1);
  runningThreads--;
  // cout << "-- " << runningThreads << endl;
  pthread_mutex_unlock(&mutex1);

  // exit thread
  pthread_exit(0);
}

void* reportThread(string* reporttype)
{
  assert(reporttype);
  string reportType = *reporttype;

  Oam oam;

  pthread_mutex_lock(&mutex1);
  runningThreads++;
  // cout << "++ " << runningThreads << endl;
  pthread_mutex_unlock(&mutex1);

  string outputFile = localModule + "_" + reportType + "Report.txt";

  FILE* pOutputFile = fopen(outputFile.c_str(), "a");
  if (pOutputFile == NULL)
  {
    printf("Could not open file: %s", outputFile.c_str());
    exit(1);
  }
  // get local report
  fprintf(pOutputFile,
          "********************************************************************************\n"
          "\n"
          " System %s\n"
          " columnstoreSupportReport script ran from Module %s on %s\n"
          " SoftwareVersion = %s-%s"
          "\n"
          "********************************************************************************\n"
          "\n"
          "                     %s report\n"
          "\n"
          "********************************************************************************\n",
          systemName.c_str(), localModule.c_str(), currentDate.c_str(), columnstore_version.c_str(),
          columnstore_release.c_str(), reportType.c_str());

  fclose(pOutputFile);
  // run on child servers and get report
  if (!LOCAL)
  {
    ChildModuleList::iterator list1 = childmodulelist.begin();

    for (; list1 != childmodulelist.end(); list1++)
    {
      threadInfo_t* st = new threadInfo_t;
      *st = boost::make_tuple(list1, reportType);

      pthread_t childreportthread;
      int status = pthread_create(&childreportthread, NULL, (void* (*)(void*)) & childReportThread, st);

      if (status != 0)
      {
        cout << "ERROR: childreportthread: pthread_create failed, return status = " + oam.itoa(status)
             << endl;
      }

      sleep(1);
    }
  }

  if (reportType == "log")
  {
    // run log config on local server
    cout << "Get log config data for " + localModule << endl;

    string cmd = "logReport.sh " + localModule + " " + outputFile;
    system(cmd.c_str());
  }
  else
  {
    string cmd = reportType + "Report.sh " + localModule + " " + outputFile;
    system(cmd.c_str());

    if (reportType == "config")
    {
      pOutputFile = fopen(outputFile.c_str(), "a");
      if (pOutputFile == NULL)
      {
        printf("Could not open file: %s", outputFile.c_str());
        exit(1);
      }

      fprintf(pOutputFile,
              "\n******************** System Network Configuration ******************************\n\n");
      getSystemNetworkConfig(pOutputFile);

      fprintf(pOutputFile,
              "\n******************** System Module Configure  **********************************\n\n");
      getModuleTypeConfig(pOutputFile);

      fprintf(pOutputFile,
              "\n******************** System Storage Configuration  *****************************\n\n");
      getStorageConfig(pOutputFile);

      fprintf(pOutputFile,
              "\n******************** System Storage Status  ************************************\n\n");
      getStorageStatus(pOutputFile);

      // BT: most of this is tedious to collect and can be manually looked up in the debug.log file
      // fprintf(pOutputFile,"\n******************** System Status
      // ********************************************\n\n"); printSystemStatus(pOutputFile);
      // printProcessStatus(pOutputFile);
      // printAlarmSummary(pOutputFile);
      //
      // fprintf(pOutputFile,"\n******************** System Directories
      // ***************************************\n\n"); getSystemDirectories(pOutputFile);

      boost::filesystem::path configFile =
          std::string(MCSSYSCONFDIR) + std::string("/columnstore/Columnstore.xml");
      boost::filesystem::copy_file(configFile, "./Columnstore.xml",
                                   boost::filesystem::copy_option::overwrite_if_exists);
      boost::filesystem::path SMconfigFile =
          std::string(MCSSYSCONFDIR) + std::string("/columnstore/storagemanager.cnf");
      boost::filesystem::copy_file(SMconfigFile, "./storagemanager.cnf",
                                   boost::filesystem::copy_option::overwrite_if_exists);
      system("sed -i 's/.*aws_access_key_id.*/aws_access_key_id={PRIVATE}/' ./storagemanager.cnf");
      system("sed -i 's/.*aws_secret_access_key.*/aws_secret_access_key={PRIVATE}/' ./storagemanager.cnf");
      fclose(pOutputFile);
    }

    /*
    // TODO: This can be ported from mcsadmin if needed most info included does not seem useful at this time
    if (reportType == "resource" )
    {
        if (LOCAL)
        {
            fprintf(pOutputFile,"\n******************** mcsadmin getModuleResourceUsage
    **************************\n\n"); string cmd = "mcsadmin getModuleResourceUsage " + localModule + " >> " +
    outputFile; system(cmd.c_str());
        }
        else
        {
            fprintf(pOutputFile,"\n******************** mcsadmin getSystemResourceUsage
    **************************\n\n"); string cmd = "mcsadmin getSystemResourceUsage >> " + outputFile;
            system(cmd.c_str());
        }
    }*/
  }

  // exit thread
  pthread_mutex_lock(&mutex1);
  runningThreads--;
  // cout << "-- " << runningThreads << endl;
  pthread_mutex_unlock(&mutex1);

  pthread_exit(0);
}

int main(int argc, char* argv[])
{
  Oam oam;

  Config* sysConfig = Config::makeConfig();
  string SystemSection = "SystemConfig";
  string InstallSection = "Installation";

  bool HARDWARE = false;
  bool CONFIG = false;
  bool DBMS = false;
  bool RESOURCE = false;
  bool LOG = false;
  bool BULKLOG = false;
  bool HADOOP = false;

  // get current time and date
  time_t now;
  now = time(NULL);
  struct tm tm;
  localtime_r(&now, &tm);
  char timestamp[200];
  strftime(timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
  currentDate = timestamp;

  char helpArg[3] = "-h";

  // Get System Name
  try
  {
    oam.getSystemConfig("SystemName", systemName);
  }
  catch (...)
  {
    systemName = "unassigned";
  }

  // get Local Module Name and Server Install Indicator
  string singleServerInstall = "n";

  oamModuleInfo_t st;

  try
  {
    st = oam.getModuleInfo();
    localModule = boost::get<0>(st);
  }
  catch (...)
  {
    cout << endl << "**** Failed : Failed to read Local Module Name" << endl;
    exit(-1);
  }

  if (argc == 1)
  {
    argv[1] = &helpArg[0];
    argc = 2;
  }

  string DataFilePlugin;

  try
  {
    DataFilePlugin = sysConfig->getConfig(SystemSection, "DataFilePlugin");
  }
  catch (...)
  {
    cout << "ERROR: Problem accessing Columnstore configuration file" << endl;
    exit(-1);
  }

  tmpDir = startup::StartUp::tmpDir();

  for (int i = 1; i < argc; i++)
  {
    if (string("-h") == argv[i])
    {
      cout << endl;
      cout << "'columnstoreSupport' generates a Set of System Support Report Files in a tar file" << endl;
      cout << "called columnstoreSupportReport.'system-name'.tar.gz in the local directory." << endl;
      cout << "It should be run on the server with the DBRM front-end." << endl;
      cout << "Check the Admin Guide for additional information." << endl;
      cout << endl;
      cout << "Usage: columnstoreSupport [-h][-a][-hw][-s][-c][-db][-r][-l][-bl][-lc][-p "
              "'root-password'][-de]";

      cout << endl;
      cout << "			-h  help" << endl;
      cout << "			-a  Output all Reports (excluding Bulk Logs Reports)" << endl;
      cout << "			-hw Output Hardware Reports only" << endl;
      cout << "			-c  Output Configuration/Status Reports only" << endl;
      cout << "			-db Output DBMS Reports only" << endl;
      cout << "			-r  Output Resource Reports only" << endl;
      cout << "			-l  Output Columnstore Log/Alarms Reports only" << endl;
      cout << "			-bl Output Columnstore Bulk Log Reports only" << endl;
      cout << "			-lc Output Reports for Local Server only" << endl;
      cout << "			-p  password (multi-server systems), root-password or 'ssh' to use 'ssh keys'"
           << endl;
      cout << "			-de Debug Flag" << endl;

      exit(0);
    }
    else
    {
      if (string("-a") == argv[i])
      {
        HARDWARE = true;
        CONFIG = true;
        DBMS = true;
        RESOURCE = true;
        LOG = true;
        HADOOP = (DataFilePlugin.empty() ? false : true);
      }
      else if (string("-hw") == argv[i])
        HARDWARE = true;
      else if (string("-c") == argv[i])
        CONFIG = true;
      else if (string("-db") == argv[i])
        DBMS = true;
      else if (string("-r") == argv[i])
        RESOURCE = true;
      else if (string("-l") == argv[i])
        LOG = true;
      else if (string("-bl") == argv[i])
        BULKLOG = true;
      else if (string("-lc") == argv[i])
        LOCAL = true;
      else if (string("-p") == argv[i])
      {
        i++;

        if (argc == i)
        {
          cout << "ERROR: missing root password argument" << endl;
          exit(-1);
        }

        rootPassword = argv[i];

        // add single quote for special characters
        if (rootPassword != "ssh")
        {
          rootPassword = "'" + rootPassword + "'";
        }
      }
      else if (string("-mp") == argv[i])
      {
        i++;

        if (argc == i)
        {
          cout << "ERROR: missing MariaDB Columnstore root user password argument" << endl;
          exit(-1);
        }

        mysqlpw = argv[i];
        mysqlpw = "'" + mysqlpw + "'";
      }
      else if (string("-de") == argv[i])
        debug_flag = "1";
      else if (string("-hd") == argv[i])
      {
        HADOOP = (DataFilePlugin.empty() ? false : true);
      }
      else
      {
        cout << "Invalid Option of '" << argv[i] << "', run with '-h' for help" << endl;
        exit(1);
      }
    }
  }

  // default to -a if nothing is set
  if (!HARDWARE && !CONFIG && !DBMS && !RESOURCE && !LOG && !BULKLOG && !HADOOP)
  {
    HARDWARE = true;
    CONFIG = true;
    DBMS = true;
    RESOURCE = true;
    LOG = true;
    HADOOP = (DataFilePlugin.empty() ? false : true);
  }

  // get Parent OAM Module Name and setup of it's Custom OS files
  string PrimaryUMModuleName;

  try
  {
    PrimaryUMModuleName = sysConfig->getConfig(SystemSection, "PrimaryUMModuleName");
  }
  catch (...)
  {
    cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
    exit(-1);
  }

  if (PrimaryUMModuleName == "unassigned")
    PrimaryUMModuleName = localModule;

  if ((localModule != PrimaryUMModuleName) && DBMS)
  {
    char* pcommand = 0;
    char* p;
    string argument = "n";

    while (true)
    {
      cout << endl << "You selected to get the DBMS data." << endl;
      cout << "You need to run the columnstoreSupport command on module '" << PrimaryUMModuleName
           << "' to get that information." << endl;
      cout << "Or you can proceed on to get all data except the DBMS." << endl;

      pcommand = readline("           Do you want to proceed: (y or n) [n]: ");

      if (pcommand && *pcommand)
      {
        p = strtok(pcommand, " ");
        argument = p;
        free(pcommand);
        pcommand = 0;
      }

      if (pcommand)
      {
        free(pcommand);
        pcommand = 0;
      }

      if (argument == "y")
      {
        cout << endl;
        break;
      }
      else if (argument == "n")
        exit(1);
    }
  }

  // get number of worker-nodes, will tell us if a single server system
  // get Parent OAM Module Name and setup of it's Custom OS files
  try
  {
    string NumWorkers = sysConfig->getConfig("DBRM_Controller", "NumWorkers");

    if (NumWorkers == "1")
      singleServerInstall = "y";
  }
  catch (...)
  {
  }

  if (singleServerInstall == "n" && !LOCAL)
    if (HARDWARE || CONFIG || RESOURCE || LOG || HADOOP)
      if (rootPassword.empty())
      {
        cout << "ERROR: Multi-Module System, Password Argument required or use '-lc' option, check help for "
                "more information"
             << endl;
        exit(-1);
      }

  // get Parent OAM Module Name and setup of it's Custom OS files
  // string parentOAMModuleName;
  ChildModule parentOAMModule;

  try
  {
    parentOAMModule.moduleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
  }
  catch (...)
  {
    cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
    exit(-1);
  }

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

  string ModuleSection = "SystemModuleConfig";

  for (unsigned int i = 0; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
  {
    string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
    int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

    if (moduleCount == 0)
      // no modules equipped for this Module Type, skip
      continue;

    // get IP addresses and Host Names
    DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

    for (; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end(); listPT++)
    {
      string moduleName = (*listPT).DeviceName;
      HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
      string moduleIPAddr = (*pt1).IPAddr;
      string moduleHostName = (*pt1).HostName;

      if (moduleName == localModule)
      {
        localModuleHostName = moduleHostName;
      }

      // save Child modules
      if (moduleName != localModule && moduleType != "xm")
      {
        childmodule.moduleName = moduleName;
        childmodule.moduleIP = moduleIPAddr;
        childmodule.hostName = moduleHostName;
        childmodulelist.push_back(childmodule);
      }

      if (moduleName == parentOAMModule.moduleName)
      {
        parentOAMModule.moduleIP = moduleIPAddr;
        parentOAMModule.hostName = moduleHostName;
        parentOAMModule.moduleName = moduleName;
      }
    }
  }  // end of i for loop

  // create a clean Columnstore Support Report
  system("rm -f *_configReport.txt");
  system("rm -f *_dbmsReport.txt");
  system("rm -f *_hardwareReport.txt");
  system("rm -f *_logReport.txt");
  system("rm -f *_bulklogReport.txt");
  system("rm -f *_resourceReport.txt");

  //
  // Configuration
  //
  if (CONFIG)
  {
    string reportType = "config";
    cout << "Get " + reportType + " report data for " + localModule << endl;
    pthread_t reportthread;
    int status = pthread_create(&reportthread, NULL, (void* (*)(void*)) & reportThread, &reportType);
    if (status != 0)
    {
      cout << "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
    }
    sleep(1);
  }

  //
  // Alarms and Columnstore Logs
  //
  if (LOG)
  {
    string reportType = "log";
    cout << "Get " + reportType + " report data for " + localModule << endl;
    pthread_t reportthread;
    int status = pthread_create(&reportthread, NULL, (void* (*)(void*)) & reportThread, &reportType);
    if (status != 0)
    {
      cout << "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
    }
    sleep(1);
  }

  //
  // Bulk Logs
  //
  if (BULKLOG)
  {
    string reportType = "bulklog";
    cout << "Get " + reportType + " report data for " + localModule << endl;
    pthread_t reportthread;
    int status = pthread_create(&reportthread, NULL, (void* (*)(void*)) & reportThread, &reportType);
    if (status != 0)
    {
      cout << "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
    }
    sleep(1);
  }

  //
  // Hardware
  //
  if (HARDWARE)
  {
    string reportType = "hardware";
    cout << "Get " + reportType + " report data for " + localModule << endl;
    pthread_t reportthread;
    int status = pthread_create(&reportthread, NULL, (void* (*)(void*)) & reportThread, &reportType);
    if (status != 0)
    {
      cout << "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
    }
    sleep(1);
  }

  //
  // Resources
  //
  if (RESOURCE)
  {
    string reportType = "resource";
    cout << "Get " + reportType + " report data for " + localModule << endl;
    pthread_t reportthread;
    int status = pthread_create(&reportthread, NULL, (void* (*)(void*)) & reportThread, &reportType);
    if (status != 0)
    {
      cout << "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
    }
    sleep(1);
  }

  //
  // DBMS
  //
  if (DBMS)
  {
    cout << "Get dbms report data for " << localModule << endl;

    string outputFile = localModule + "_dbmsReport.txt";

    FILE* pOutputFile = fopen(outputFile.c_str(), "w");
    if (pOutputFile == NULL)
    {
      cout << "Could not open file: " + outputFile << endl;
      exit(1);
    }

    fprintf(pOutputFile,
            "********************************************************************************\n"
            "\n"
            " System %s\n"
            " columnstoreSupportReport script ran from Module %s on %s\n"
            " SoftwareVersion = %s-%s"
            "\n"
            "********************************************************************************\n"
            "\n"
            "                     DBMS report\n"
            "\n"
            "********************************************************************************\n",
            systemName.c_str(), localModule.c_str(), currentDate.c_str(), columnstore_version.c_str(),
            columnstore_release.c_str());

    fclose(pOutputFile);

    // run DBMS report on local server
    bool FAILED = false;

    if (localModule != PrimaryUMModuleName)
    {
      cout << "     FAILED: run columnstoreSupport on '" << PrimaryUMModuleName << "' to get the dbrm report"
           << endl;
      FAILED = true;
    }
    else
    {
      // check if mysql is supported and get info
      string logFile = tmpDir + "/idbmysql.log";
      string columnstoreMysql = "mysql -u root ";
      string cmd = columnstoreMysql + " -e 'status' > " + logFile + " 2>&1";
      system(cmd.c_str());

      // check for mysql password set
      string pwprompt = " ";

      if (checkLogStatus(logFile, "ERROR 1045"))
      {
        cout << "NOTE: MariaDB Columnstore root user password is set" << endl;

        // needs a password, was password entered on command line
        if (mysqlpw == " ")
        {
          // go check columnstore.cnf
          string file = std::string(MCSMYCNFDIR) + "/columnstore.cnf";
          ifstream oldFile(file.c_str());

          vector<string> lines;
          char line[200];
          string buf;

          while (oldFile.getline(line, 200))
          {
            buf = line;
            string::size_type pos = buf.find("password", 0);

            if (pos != string::npos)
            {
              string::size_type pos1 = buf.find("=", 0);

              if (pos1 != string::npos)
              {
                pos = buf.find("#", 0);

                if (pos == string::npos)
                {
                  // password arg in columnstore.cnf, go get password
                  cout << "NOTE: Using password from columnstore.cnf" << endl;
                  mysqlpw = buf.substr(pos1 + 1, 80);
                  cout << mysqlpw << endl;
                  break;
                }
              }
            }
          }

          oldFile.close();

          if (mysqlpw == " ")
          {
            cout << "NOTE: No password provide on command line or found uncommented in columnstore.cnf"
                 << endl;
            cout << endl;
            string prompt = " *** Enter MariaDB Columnstore password > ";
            mysqlpw = getpass(prompt.c_str());
          }
        }

        // check for mysql password set
        pwprompt = "--password=" + mysqlpw;

        string cmd = columnstoreMysql + pwprompt + " -e 'status' > " + logFile + " 2>&1";
        system(cmd.c_str());

        if (checkLogStatus(logFile, "ERROR 1045"))
        {
          cout << "FAILED: Failed login using MariaDB Columnstore root user password '" << mysqlpw << "'"
               << endl;
          FAILED = true;
        }
      }

      if (!FAILED)
      {
        string cmd = "dbmsReport.sh " + localModule + " " + outputFile + " " + std::string(MCSSUPPORTDIR) +
                     " " + pwprompt;
        system(cmd.c_str());
      }
    }

    /*
      BT: This doesn't appear to do anything
    fprintf(pOutputFile,"\n******************** Database Size Report
    *************************************\n\n"); getStorageStatus(pOutputFile);

    string file = "databaseSizeReport";
    ifstream File (file.c_str());

    if (File)
    {
        string cmd = "databaseSizeReport >> " + outputFile;
        system(cmd.c_str());
    }
     */

    boost::filesystem::path configFile = std::string(MCSMYCNFDIR) + "/columnstore.cnf";
    boost::filesystem::copy_file(configFile, "./columnstore.cnf",
                                 boost::filesystem::copy_option::overwrite_if_exists);
  }

  int wait = 0;

  while (true)
  {
    // cout << "check " << runningThreads << endl;
    if (runningThreads < 1)
      break;

    sleep(2);
    wait++;

    // give it 60 minutes to complete
    if (wait >= 3600 * 5)
    {
      cout << "Timed out (60 minutes) waiting for Requests to complete" << endl;
    }
  }

  system("unix2dos *Report.txt > /dev/null 2>&1");
  system(
      "rm -rf columnstoreSupportReport;"
      "mkdir columnstoreSupportReport;"
      "mv *Report.txt columnstoreSupportReport/. > /dev/null 2>&1;"
      "mv Columnstore.xml columnstoreSupportReport/. > /dev/null 2>&1;"
      "mv columnstore.cnf columnstoreSupportReport/. > /dev/null 2>&1;"
      "mv storagemanager.cnf columnstoreSupportReport/. > /dev/null 2>&1;"
      "mv *Report.tar.gz columnstoreSupportReport/. > /dev/null 2>&1");
  string cmd = "tar -zcf columnstoreSupportReport." + systemName + ".tar.gz columnstoreSupportReport/*";
  system(cmd.c_str());

  cout << endl
       << "Columnstore Support Script Successfully completed, files located in columnstoreSupportReport." +
              systemName + ".tar.gz"
       << endl;
}
