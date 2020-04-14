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
* $Id: postConfigure.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by post-install configure:
*		mariadb/columnstore/etc/Columnstore.xml
*		mariadb/columnstore/etc/ProcessConfig.xml
*		/etc/rc.d/rc.local
*
******************************************************************************************/
/**
 * @file
 */

#include <unistd.h>
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
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include <climits>
#include <cstring>
#include <glob.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>

#include <string.h> /* for strncpy */
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>

#include <readline.h>
#include <history.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/tokenizer.hpp>

#include "mcsconfig.h"
#include "columnstoreversion.h"
#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace oam;
using namespace config;

#include "helpers.h"
using namespace installer;

#include "installdir.h"

typedef struct DBRoot_Module_struct
{
    std::string		moduleName;
    std::string     dbrootID;
} DBRootModule;

typedef std::vector<DBRootModule> DBRootList;

typedef struct Performance_Module_struct
{
    std::string     moduleIP1;
    std::string     moduleIP2;
    std::string     moduleIP3;
    std::string     moduleIP4;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

typedef struct ModuleIP_struct
{
	std::string     IPaddress;
	std::string     moduleName;
} ModuleIP;

typedef std::vector<ModuleIP> ModuleIpList;

void offLineAppCheck();
bool setOSFiles(string parentOAMModuleName, int serverTypeInstall);
bool checkSaveConfigFile();
bool updateBash();
bool makeModuleFile(string moduleName, string parentOAMModuleName);
bool updateProcessConfig();
bool uncommentCalpontXml( string entry);
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall);
bool createDbrootDirs(string DBRootStorageType, bool amazonInstall);
bool pkgCheck(std::string columnstorePackage);
bool storageSetup(bool amazonInstall);
void setSystemName();
bool singleServerDBrootSetup();
bool copyFstab(string moduleName);
bool attachVolume(string instanceName, string volumeName, string deviceName, string dbrootPath);
void singleServerConfigSetup(Config* sysConfig);
std::string resolveHostNameToReverseDNSName(std::string hostname);

void remoteInstallThread(void*);

bool glusterSetup(string password, bool doNotResolveHostNames);

std::string launchInstance(ModuleIP moduleip);

string columnstorePackage;
//string calpontPackage2;
//string calpontPackage3;
//string mysqlPackage;
//string mysqldPackage;

string parentOAMModuleName;
int pmNumber = 0;
int umNumber = 0;

string DBRootStorageLoc;
string DBRootStorageType;
string UMStorageType;

string PMVolumeSize = oam::UnassignedName;
string UMVolumeSize = oam::UnassignedName;
string UMVolumeType = "standard";
string PMVolumeType = "standard";
string PMVolumeIOPS = oam::UnassignedName;
string UMVolumeIOPS = oam::UnassignedName;

int DBRootCount;
string deviceName;

Config* sysConfig = Config::makeConfig();
string SystemSection = "SystemConfig";
string InstallSection = "Installation";
string ModuleSection = "SystemModuleConfig";
string serverTypeInstall;
int    IserverTypeInstall;
string parentOAMModuleIPAddr;
string remote_installer_debug = "1";
bool thread_remote_installer = true;

string singleServerInstall = "1";
string reuseConfig = "n";
string oldFileName = oam::UnassignedName;
string glusterCopies;
string glusterInstalled = "n";
string hadoopInstalled = "n";
string mysqlPort = oam::UnassignedName;
string systemName;

bool noPrompting = true;
bool rootUser = true;
string USER = "root";
bool hdfs = false;
bool DataRedundancy = false;
bool pmwithum = false;
bool mysqlRep = false;
string MySQLRep = "y";
string PMwithUM = "n";
bool amazonInstall = false;
bool single_server_quick_install = false;
bool multi_server_quick_install = false;
bool amazon_quick_install = false;
bool doNotResolveHostNames = false;
bool resolveHostNamesToReverseDNSNames = false;

string tmpDir;
string HOME = "/root";
string SUDO = "";
extern string pwprompt;

extern const char* pcommand;
extern string prompt;


/* create thread argument struct for thr_func() */
typedef struct _thread_data_t
{
    std::string command;
} thread_data_t;


int main(int argc, char* argv[])
{
    Oam oam;
    string parentOAMModuleHostName;
    ChildModuleList childmodulelist;
    ChildModuleList niclist;
    ChildModule childmodule;
    DBRootModule dbrootmodule;
    DBRootList dbrootlist;
    PerformanceModuleList performancemodulelist;
    string nodeps = "-h";
    bool startOfflinePrompt = false;
    noPrompting = false;
    string password;
    string cmd;
	string pmIpAddrs = "";
	string umIpAddrs = "";
    string numBlocksPctParam = "";
    string totalUmMemoryParam = "";

//  	struct sysinfo myinfo;

    // hidden options
    // -f for force use nodeps on rpm install
    // -o to prompt for process to start offline

    //check if root-user
    int user;
    user = getuid();

	string SUDO = "";
    if (user != 0)
    {
        rootUser = false;
		SUDO = "sudo ";
	}

    char* p = getenv("USER");

    if (p && *p)
        USER = p;

    if (!rootUser)
    {
        char* p = getenv("HOME");

        if (p && *p)
            HOME = p;
    }

	tmpDir = startup::StartUp::tmpDir();

    if ( oldFileName == oam::UnassignedName )
        oldFileName = std::string(MCSSYSCONFDIR) + "/columnstore/Columnstore.xml.rpmsave";

    string ProfileFile;
    try
    {
        ProfileFile = sysConfig->getConfig(InstallSection, "ProfileFile");
    }
    catch (...)
    {}

	//get current time and date
	time_t now;
	now = time(NULL);
	struct tm tm;
	localtime_r(&now, &tm);
	char timestamp[200];
	strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
	string currentDate = timestamp;

	string postConfigureLog = "/var/log/columnstore-postconfigure-" + currentDate;

    //determine package type
    string EEPackageType;

	if (!rootUser)
			EEPackageType = "binary";
	else
	{
		string cmd = "rpm -qi mariadb-columnstore-platform > " + tmpDir + "/columnstore.txt 2>&1";
		int rtnCode = system(cmd.c_str());

		if (WEXITSTATUS(rtnCode) == 0)
				EEPackageType = "rpm";
		else {
			string cmd = "dpkg -s mariadb-columnstore-platform > " + tmpDir + "/columnstore.txt 2>&1";
			int rtnCode = system(cmd.c_str());

			if (WEXITSTATUS(rtnCode) == 0)
						EEPackageType = "deb";
				else
						EEPackageType = "binary";
		}
	}

	try {
			sysConfig->setConfig(InstallSection, "EEPackageType", EEPackageType);
	}
	catch(...)
	{
			cout << "ERROR: Problem setting EEPackageType from the MariaDB ColumnStore System Configuration file" << endl;
			exit(1);
	}

    if ( !writeConfig(sysConfig) )
    {
        cout << "ERROR: Failed trying to update MariaDB ColumnStore System Configuration file" << endl;
        exit(1);
    }

	// perform single server install
	cout << endl << "Performing the Single Server Install." << endl << endl;

	//setup to Columnstore.xml file for single server
    //singleServerConfigSetup(sysConfig);

	// setup storage
	if (!storageSetup(false))
	{
		cout << "ERROR: Problem setting up storage" << endl;
		exit(1);
	}

	if (hdfs || !rootUser)
		if ( !updateBash() )
			cout << "updateBash error" << endl;

	//check if dbrm data resides in older directory path and inform user if it does
	dbrmDirCheck();

	if (startOfflinePrompt)
		offLineAppCheck();

	checkMysqlPort(mysqlPort, sysConfig);
	if ( !writeConfig(sysConfig) )
	{
		cout << "ERROR: Failed trying to update MariaDB ColumnStore System Configuration file" << endl;
		exit(1);
	}

    if (numBlocksPctParam.empty()) {
        numBlocksPctParam = "-";
    }
    if (totalUmMemoryParam.empty()) {
        totalUmMemoryParam = "-";
    }

	cmd = "columnstore_installer dummy.rpm dummy.rpm dummy.rpm dummy.rpm dummy.rpm initial dummy " + reuseConfig + " --nodeps ' ' 1 " + numBlocksPctParam + " " + totalUmMemoryParam;
	system(cmd.c_str());
	exit(0);
}

/*
 * Setup OS Files by appending the Calpont versions
 */

// /etc OS Files to be updated
string files[] =
{
    " "
};

bool setOSFiles(string parentOAMModuleName, int serverTypeInstall)
{
    bool allfound = true;

    //update /etc files
    for ( int i = 0;; ++i)
    {
        if ( files[i] == " ")
            //end of list
            break;

        string fileName = "/etc/" + files[i];

        //make a backup copy before changing
        string cmd = "rm -f " + fileName + ".columnstoreSave";

        system(cmd.c_str());

        cmd = "cp " + fileName + " " + fileName + ".columnstoreSave > /dev/null 2>&1";

        system(cmd.c_str());

        cmd = "cat /var/lib/columnstore/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont >> " + fileName;

        int rtnCode = system(cmd.c_str());

        if (WEXITSTATUS(rtnCode) != 0)
            cout << "Error Updating " + files[i] << endl;

        cmd = "rm -f /var/lib/columnstore/local/ " + files[i] + "*.calpont > /dev/null 2>&1";
        system(cmd.c_str());

        cmd = "cp /var/lib/columnstore/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont /var/lib/columnstore/local/. > /dev/null 2>&1";
        system(cmd.c_str());
    }

    //check and do the amazon credentials file
    string fileName = HOME + "/.aws/credentials";
    ifstream oldFile (fileName.c_str());

    if (!oldFile)
        return allfound;

    string cmd = "cp " + fileName + " /var/lib/columnstore/local/etc/. > /dev/null 2>&1";
    system(cmd.c_str());
    return allfound;
}

/*
 * Uncomment entry in Columnstore.xml
 */
bool uncommentCalpontXml( string entry)
{
    string fileName = std::string(MCSSYSCONFDIR) + "/columnstore/Columnstore.xml";

    ifstream oldFile (fileName.c_str());

    if (!oldFile) return true;

    vector <string> lines;
    char line[200];
    string buf;
    string newLine;

    string firstComment = "<!--";
    string lastComment = "--> ";

    while (oldFile.getline(line, 200))
    {
        buf = line;

        string::size_type pos = buf.find(entry, 0);

        if (pos != string::npos)
        {
            pos = buf.find(firstComment, 0);

            if (pos == string::npos)
            {
                return true;
            }
            else
            {
                buf = buf.substr(pos + 4, 80);

                pos = buf.find(lastComment, 0);

                if (pos == string::npos)
                {
                    return true;
                }
                else
                {
                    buf = buf.substr(0, pos);
                }
            }
        }

        //output to temp file
        lines.push_back(buf);
    }

    oldFile.close();
    unlink (fileName.c_str());
    ofstream newFile (fileName.c_str());

    //create new file
    int fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0664);

    copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
    newFile.close();

    close(fd);
    return true;
}

/*
 * Make makeRClocal to set mount scheduler
 */
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall)
{

    return true;

    vector <string> lines;

    string mount1;
    string mount2
    ;

    switch ( IserverTypeInstall )
    {
        case (oam::INSTALL_NORMAL):	// normal
        {
            if ( moduleType == "um" )
                mount1 = "/mnt\\/tmp/";
            else if ( moduleType == "pm" )
                mount1 = "/columnstore\\/data/";
            else
                return true;

            break;
        }

        case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm
        {
            if ( moduleType == "pm" )
            {
                mount1 = "/mnt\\/tmp/";
                mount2 = "/columnstore\\/data/";
            }
            else
                return true;

            break;
        }
    }

    if ( !mount1.empty() )
    {
        string line1 = "for scsi_dev in `mount | awk '" + mount1 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
        string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
        string line3 = "done";

        lines.push_back(line1);
        lines.push_back(line2);
        lines.push_back(line3);
    }
    else
    {
        if ( !mount2.empty() )
        {
            string line1 = "for scsi_dev in `mount | awk '" + mount2 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
            string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
            string line3 = "done";

            lines.push_back(line1);
            lines.push_back(line2);
            lines.push_back(line3);
        }
    }

    if ( lines.begin() == lines.end())
        return true;

    string RCfileName = "/etc/rc.d/rc.local";
    std::ofstream file;

    file.open(RCfileName.c_str(), std::ios::out | std::ios::app);

    if (file.fail())
    {
        RCfileName = "/etc/rc.local";

        file.open(RCfileName.c_str(), std::ios::out | std::ios::app);

        if (file.fail())
            return true;
    }

    file.exceptions(file.exceptions() | std::ios::failbit | std::ifstream::badbit);

    copy(lines.begin(), lines.end(), ostream_iterator<string>(file, "\n"));

    file.close();

    return true;
}

/*
 * createDbrootDirs
 */
bool createDbrootDirs(string DBRootStorageType, bool amazonInstall)
{
    int rtnCode;
    string cmd;

    // mount data1 and create directories if configured with storage
    if ( DBRootStorageType == "external" )
    {
        string cmd = "mount /var/lib/columnstore/data1 > " + tmpDir + "/mount.log 2>&1";
        system(cmd.c_str());

        if ( !rootUser)
        {
			//if Amazon, Non-Root, and External EBS, use sudo
			string SUDO = "";
			if ( amazonInstall )
				SUDO = "sudo ";

            cmd = SUDO + "chown -R " + USER + ":" + USER + " /var/lib/columnstore/data1 > /dev/null";
            system(cmd.c_str());
        }

        // create system file directories
        cmd = "mkdir -p /var/lib/columnstore/data1/systemFiles/dbrm > /dev/null 2>&1";
        rtnCode = system(cmd.c_str());

        if (WEXITSTATUS(rtnCode) != 0)
        {
            cout << endl << "Error: failed to make mount dbrm dir" << endl;
            return false;
        }
    }

    cmd = "chmod 755 -R /var/lib/columnstore/data1/systemFiles/dbrm > /dev/null 2>&1";
    system(cmd.c_str());

    return true;
}

/*
 * pkgCheck
 */
bool pkgCheck(string columnstorePackage)
{
    while (true)
    {
        string fileName = tmpDir + "/calpontpkgs";

        string cmd = "ls " + columnstorePackage + " > " + fileName;
        system(cmd.c_str());

        string pkg = columnstorePackage;
        ifstream oldFile (fileName.c_str());

        if (oldFile)
        {
            oldFile.seekg(0, std::ios::end);
            int size = oldFile.tellg();

            if ( size != 0 )
            {
                oldFile.close();
                unlink (fileName.c_str());
                // pkgs found
                return true;
            }
        }

        cout << endl << " Error: can't locate " + pkg + " Package in directory " + HOME << endl << endl;

        if ( noPrompting )
            exit(1);

        while (true)
        {
            pcommand = callReadline("Please place a copy of the MariaDB ColumnStore Packages in directory " + HOME + " and press <enter> to continue or enter 'exit' to exit the install > ");

            if (pcommand)
            {
                if (strcmp(pcommand, "exit") == 0)
                {
                    callFree(pcommand);
                    return false;
                }

                if (strlen(pcommand) == 0)
                {
                    callFree(pcommand);
                    break;
                }

                callFree(pcommand);
                cout << "Invalid entry, please re-enter" << endl;

                if ( noPrompting )
                    exit(1);

                continue;
            }

            break;
        }
    }

    return true;
}

bool storageSetup(bool amazonInstall)
{
    Oam oam;

    try
    {
        DBRootStorageType = sysConfig->getConfig(InstallSection, "DBRootStorageType");
    }
    catch (...)
    {
        cout << "ERROR: Problem getting DB Storage Data from the MariaDB ColumnStore System Configuration file" << endl;
        return false;
    }

    if ( DBRootStorageType == "hdfs")
        hdfs = true;

    if ( DBRootStorageType == "DataRedundancy")
        DataRedundancy = true;

    sysConfig->setConfig("StorageManager", "Enabled", "N");
    if ( reuseConfig == "y" )
    {
        cout << "===== Storage Configuration = " + DBRootStorageType + " =====" << endl << endl;

        if (DBRootStorageType == "storagemanager")
        {
            hdfs = false;
            sysConfig->setConfig("StorageManager", "Enabled", "Y");
            sysConfig->setConfig("SystemConfig", "DataFilePlugin", "libcloudio.so");
        }

        return true;
    }

    string storageType;

    try
    {
        sysConfig->setConfig(InstallSection, "UMStorageType", "internal");
    }
    catch (...)
    {
        cout << "ERROR: Problem setting UMStorageType in the MariaDB ColumnStore System Configuration file" << endl;
        return false;
    }

    //check if gluster is installed
    int rtnCode = 1;

	rtnCode = system("gluster --version > /tmp/gluster.log 2>&1");

    if (rtnCode == 0)
    {
        glusterInstalled = "y";
    }
    else
    {
        glusterInstalled = "n";
    }

    //check if hadoop is installed
    string hadoopLog = tmpDir + "/hadoop.log";

	string cmd = "which hadoop > " + hadoopLog + " 2>&1";
    system(cmd.c_str());

    ifstream in(hadoopLog.c_str());

    in.seekg(0, std::ios::end);
    int size = in.tellg();

    if ( size == 0 || oam.checkLogStatus(hadoopLog, "no hadoop"))
        // no hadoop
        size = 0;
    else
        hadoopInstalled = "y";

    // check whether StorageManager is installed
    Config *processConfig = Config::makeConfig((std::string(MCSSYSCONFDIR) + "/columnstore/ProcessConfig.xml").c_str());
    string storageManagerLocation;
    bool storageManagerInstalled = false;
    // search the 'PROCESSCONFIG#' entries for the StorageManager entry
    for (uint i = 1; i <= MAX_PROCESS; i++)
    {
        char blah[20];
        sprintf(blah, "PROCESSCONFIG%d", i);
        if (processConfig->getConfig(blah, "ProcessName") == "StorageManager")
        {
            storageManagerLocation = processConfig->getConfig(blah, "ProcessLocation");
            break;
        }
    }
    storageManagerInstalled = boost::filesystem::exists(storageManagerLocation);

    //
    // get Backend Data storage type
    //

    // default to internal
    storageType = "1";

    if ( DBRootStorageType == "external" )
        storageType = "2";

    if ( DBRootStorageType == "DataRedundancy" )
        storageType = "3";

    if (DBRootStorageType == "storagemanager")
        storageType = "4";

    if (storageType != "3" && DataRedundancy)
    {
        cout << "WARNING: This system was previously configured with ColumnStore DataRedundancy storage." << endl;
        cout << "         Before changing from DataRedundancy to another storage type," << endl;
        cout << "         existing data should be migrated to the targeted storage." << endl;
        cout << "         Please refer to the ColumnStore documentation for more information." << endl;

        cout << endl;
        string continueInstall = "y";

        while (true)
        {
            pcommand = callReadline("Would you like to continue with this storage setting? [y,n] (" + continueInstall + ") > ");

            if (pcommand)
            {
                if (strlen(pcommand) > 0) continueInstall = pcommand;

                callFree(pcommand);
            }

            if ( continueInstall == "y" || continueInstall == "n" )
                break;
            else
                cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;

            continueInstall = "y";

            if ( noPrompting )
                exit(1);
        }

        if ( continueInstall == "n")
            exit(1);
    }

    switch ( atoi(storageType.c_str()) )
    {
        case (1):
        {
            DBRootStorageType = "internal";
            break;
        }

        case (2):
        {
            DBRootStorageType = "external";
            break;
        }

        case (3):
        {
            DBRootStorageType = "DataRedundancy";
            break;
        }

        case 4:
            DBRootStorageType = "storagemanager";
            break;
    }

    //set DBRootStorageType
    try
    {
        sysConfig->setConfig(InstallSection, "DBRootStorageType", DBRootStorageType);
    }
    catch (...)
    {
        cout << "ERROR: Problem setting DBRootStorageType in the MariaDB ColumnStore System Configuration file" << endl;
        return false;
    }

    // if external and not amazon, print fstab note
    if ( storageType == "2" && !amazonInstall)
    {
        cout << endl << "NOTE: For External configuration, the /etc/fstab should have been manually updated for the" << endl;
        cout <<         "      DBRoot mounts. Check the Installation Guide for further details" << endl << endl;

    }

    // if external and amazon, prompt for storage size
    if ( storageType == "2" && amazonInstall)
    {
        cout << endl << "NOTE: The volume type. This can be gp2 for General Purpose  SSD,  io1  for" << endl;
        cout << "      Provisioned IOPS SSD, st1 for Throughput Optimized HDD, sc1 for Cold" << endl;
        cout << "      HDD, or standard for Magnetic volumes." << endl;

        cout << endl;

        try
        {
            oam.getSystemConfig("PMVolumeType", PMVolumeType);
        }
        catch (...)
        {}

        if ( PMVolumeType.empty() || PMVolumeType == "" || PMVolumeType == oam::UnassignedName)
            PMVolumeType = "gp2";

        while (true)
        {
            string prompt = "Enter EBS Volume Type (gp2, io1, sc1, st1, standard) : (" + PMVolumeType + ") > ";
            pcommand = callReadline(prompt);

            if (pcommand)
            {
                if (strlen(pcommand) > 0) PMVolumeType = pcommand;

                callFree(pcommand);
            }

            if ( PMVolumeType == "standard" || PMVolumeType == "gp2" || PMVolumeType == "io1" || PMVolumeType == "sc1" || PMVolumeType == "st1")
                break;
            else
            {
                cout << endl << "Invalid Entry, please re-enter" << endl << endl;

                if ( noPrompting )
                    exit(1);
            }
        }

        //set PMVolumeType
        try
        {
            sysConfig->setConfig(InstallSection, "PMVolumeType", PMVolumeType);
        }
        catch (...)
        {
            cout << "ERROR: Problem setting PMVolumeType in the MariaDB ColumnStore System Configuration file" << endl;
            return false;
        }

        cout << endl;

        string minSize = "1";
        string maxSize = "16384";

        if (PMVolumeType == "io1")
            minSize = "4";

        if (PMVolumeType == "sc1" || PMVolumeType == "st1")
            minSize = "500";

        if (PMVolumeType == "standard")
            maxSize = "1024";

        try
        {
            oam.getSystemConfig("PMVolumeSize", PMVolumeSize);
        }
        catch (...)
        {}

        if ( PMVolumeSize.empty() || PMVolumeSize == "" || PMVolumeSize == oam::UnassignedName)
            PMVolumeSize = minSize;

        while (true)
        {
            string prompt = "Enter EBS Volume storage size in GB: [" + minSize + "," + maxSize + "] (" + PMVolumeSize + ") > ";
            pcommand = callReadline(prompt);

            if (pcommand)
            {
                if (strlen(pcommand) > 0) PMVolumeSize = pcommand;

                callFree(pcommand);
            }

            if ( atoi(PMVolumeSize.c_str()) < atoi(minSize.c_str()) || atoi(PMVolumeSize.c_str()) > atoi(maxSize.c_str()) )
            {
                cout << endl << "Invalid Entry, please re-enter" << endl << endl;

                if ( noPrompting )
                    exit(1);
            }
            else
                break;
        }

        //set PMVolumeSize
        try
        {
            sysConfig->setConfig(InstallSection, "PMVolumeSize", PMVolumeSize);
        }
        catch (...)
        {
            cout << "ERROR: Problem setting PMVolumeSize in the MariaDB ColumnStore System Configuration file" << endl;
            return false;
        }


        if ( PMVolumeType == "io1" )
        {
            string minIOPS = PMVolumeSize;
            string maxIOPS = oam.itoa(atoi(PMVolumeSize.c_str()) * 30);

            cout << endl;

            try
            {
                oam.getSystemConfig("PMVolumeIOPS", PMVolumeIOPS);
            }
            catch (...)
            {}

            if ( PMVolumeIOPS.empty() || PMVolumeIOPS == "" || PMVolumeIOPS == oam::UnassignedName)
                PMVolumeIOPS = maxIOPS;

            while (true)
            {
                string prompt = "Enter EBS Volume IOPS: [" + minIOPS + "," + maxIOPS + "] (" + PMVolumeIOPS + ") > ";
                pcommand = callReadline(prompt);

                if (pcommand)
                {
                    if (strlen(pcommand) > 0) PMVolumeIOPS = pcommand;

                    callFree(pcommand);
                }

                if ( atoi(PMVolumeIOPS.c_str()) < atoi(minIOPS.c_str()) || atoi(PMVolumeIOPS.c_str()) > atoi(maxIOPS.c_str()) )
                {
                    cout << endl << "Invalid Entry, please re-enter" << endl << endl;

                    if ( noPrompting )
                        exit(1);
                }
                else
                    break;
            }

            //set PMVolumeIOPS
            try
            {
                sysConfig->setConfig(InstallSection, "PMVolumeIOPS", PMVolumeIOPS);
            }
            catch (...)
            {
                cout << "ERROR: Problem setting PMVolumeIOPS in the MariaDB ColumnStore System Configuration file" << endl;
                return false;
            }
        }

        //set DBRootStorageType
        try
        {
            sysConfig->setConfig(InstallSection, "PMVolumeSize", PMVolumeSize);
        }
        catch (...)
        {
            cout << "ERROR: Problem setting PMVolumeSize in the MariaDB ColumnStore System Configuration file" << endl;
            return false;
        }
    }

    // if gluster
    if ( storageType == "3" )
    {
        string command = "stat /var/run/glusterd.pid > /dev/null 2>&1";
        int status = system(command.c_str());

        if (WEXITSTATUS(status) != 0 )
        {
            cout << "ERROR: No glusterd process detected. " << endl;
            cout << "       Start and enable glusterd on all PMs and run postConfigure again." << endl;
            return false;
        }

        DataRedundancy = true;
        sysConfig->setConfig(InstallSection, "DataRedundancyConfig", "y");
        sysConfig->setConfig("PrimitiveServers", "DirectIO", "n");
    }
    else
    {
        DataRedundancy = false;
        sysConfig->setConfig(InstallSection, "DataRedundancyConfig", "n");
        sysConfig->setConfig("PrimitiveServers", "DirectIO", "y");
    }

    if (storageType == "4")
    {
        hdfs = false;
        sysConfig->setConfig("StorageManager", "Enabled", "Y");
        sysConfig->setConfig("SystemConfig", "DataFilePlugin", "libcloudio.so");
    }
    else
    {
        hdfs = false;

        try
        {
            sysConfig->setConfig("SystemConfig", "DataFilePlugin", "");
        }
        catch (...)
        {
            cout << "ERROR: Problem setting DataFilePlugin in the MariaDB ColumnStore System Configuration file" << endl;
            return false;
        }

        if ( !writeConfig(sysConfig) )
        {
            cout << "ERROR: Failed trying to update MariaDB ColumnStore System Configuration file" << endl;
            return false;
        }
    }

    if ( !writeConfig(sysConfig) )
    {
        cout << "ERROR: Failed trying to update MariaDB ColumnStore System Configuration file" << endl;
        return false;
    }

    return true;
}

/*
 * Copy fstab file
 */
bool copyFstab(string moduleName)
{
    string cmd;
    cmd = "/bin/cp -f /etc/fstab /var/lib/columnstore/local/etc/" + moduleName + "/. > /dev/null 2>&1";

    system(cmd.c_str());

    return true;
}


/*
 * Create a module file
 */
bool makeModuleFile(string moduleName, string parentOAMModuleName)
{
    string fileName;

    if ( moduleName == parentOAMModuleName)
        fileName = "/var/lib/columnstore/local/module";
    else
        fileName = "/var/lib/columnstore/local/etc/" + moduleName + "/module";

    unlink (fileName.c_str());
    ofstream newFile (fileName.c_str());

    string cmd = "echo " + moduleName + " > " + fileName;
    system(cmd.c_str());

    newFile.close();

    return true;
}

/*
 * Create a module file
 */
bool updateBash()
{
    string fileName = HOME + "/.bashrc";

    ifstream newFile (fileName.c_str());

    newFile.close();

    return true;

}

void offLineAppCheck()
{
    //check for system startup type, process offline or online option
    cout << endl << "===== Setup Process Startup offline Configuration =====" << endl << endl;

    string systemStartupOffline;

    try
    {
        systemStartupOffline = sysConfig->getConfig(InstallSection, "SystemStartupOffline");
    }
    catch (...)
    {
        cout << "ERROR: Problem getting systemStartupOffline from the MariaDB ColumnStore System Configuration file" << endl;
        exit(1);
    }

    cout << endl;
    string temp = "y";

    while (true)
    {
        prompt = "Do you want the Database Processes started automatically at system startup [y,n] (y) > ";
        pcommand = callReadline(prompt.c_str());

        if (pcommand)
        {
            if (strlen(pcommand) > 0) temp = pcommand;

            callFree(pcommand);

            if ( temp == "n" || temp == "y")
            {
                break;
            }

            cout << "Invalid Option, please re-enter" << endl;

            if ( noPrompting )
                exit(1);
        }
        else
            break;
    }

    if ( temp == "y" )
        systemStartupOffline = "n";
    else
        systemStartupOffline = "y";

    try
    {
        sysConfig->setConfig(InstallSection, "SystemStartupOffline", systemStartupOffline);
    }
    catch (...)
    {
        cout << "ERROR: Problem setting systemStartupOffline in the MariaDB ColumnStore System Configuration file" << endl;
        exit(1);
    }
}

bool attachVolume(string instanceName, string volumeName, string deviceName, string dbrootPath)
{
    Oam oam;

//	cout << "Checking if Volume " << volumeName << " is attached , please wait..." << endl;

    string status = oam.getEC2VolumeStatus(volumeName);

    if ( status == "attached" )
    {
        cout << "  Volume " << volumeName << " is attached " << endl;
//		cout << "Make sure it's device " << deviceName << " is mounted to DBRoot directory " << dbrootPath << endl;
        return true;
    }

    if ( status != "available" )
    {
        cout << "  ERROR: Volume " << volumeName << " status is " << status << endl;
        cout << "  Please resolve and re-run postConfigure" << endl;
        return false;
    }
    else
    {
        /*		cout << endl;
        		string temp = "y";
        		while(true)
        		{
        			prompt = "Volume is unattached and available, do you want to attach it? [y,n] (y) > ";
        			pcommand = callReadline(prompt.c_str());
        			if (pcommand) {
        				if (strlen(pcommand) > 0) temp = pcommand;
        				callFree(pcommand);
        				if ( temp == "n" || temp == "y") {
        					break;
        				}
        				cout << "Invalid Option, please re-enter" << endl;
        				if ( noPrompting )
        					exit(1);
        			}
        			else
        				break;
        		}

        		if ( temp == "y" ) {
        */			cout << "  Attaching, please wait..." << endl;

        if (oam.attachEC2Volume(volumeName, deviceName, instanceName))
        {
            cout << "  Volume " << volumeName << " is now attached " << endl;
//				cout << "Make sure it's device " << deviceName << " is mounted to DBRoot directory " << dbrootPath << endl;
            return true;
        }
        else
        {
            cout << "  ERROR: Volume " << volumeName << " failed to attach" << endl;
            cout << "  Please resolve and re-run postConfigure" << endl;
            return false;
        }

        /*		}
        		else
        		{
        			cout << "Volume " << volumeName << " will need to be attached before completing the install" << endl;
        			cout << "Please resolve and re-run postConfigure" << endl;
        			return false;
        		}
        */
    }
}

pthread_mutex_t THREAD_LOCK;

void remoteInstallThread(void* arg)
{
    thread_data_t* data = (thread_data_t*)arg;

    for ( int retry = 0 ; retry < 5 ; retry++ )
    {
        int rtnCode = system((data->command).c_str());

        if (WEXITSTATUS(rtnCode) == 0)
        {
            //success
            pthread_exit(0);
        }
        else
        {
            if (WEXITSTATUS(rtnCode) == 2)
            {
                //timeout retry
                continue;
            }
            else
            {
                //failure
                pthread_mutex_lock(&THREAD_LOCK);
                cout << endl << "Failure with a remote module install, check install log files in " << tmpDir << endl;
                exit(1);
            }
        }
    }

    pthread_mutex_lock(&THREAD_LOCK);
    cout << endl << "Failure with a remote module install, check install log files in " << tmpDir << endl;
    exit(1);
}

std::string launchInstance(ModuleIP moduleip)
{
    Oam oam;

    //get module info
    string moduleName = moduleip.moduleName;
    string IPAddress = moduleip.IPaddress;
    string instanceName = oam::UnassignedName;

    //due to bad instances getting launched causing scp failures
    //have retry login around fail scp command where a instance will be relaunched
    int instanceRetry = 0;

    for ( ; instanceRetry < 5 ; instanceRetry ++ )
    {
        if ( moduleName.find("um") == 0 )
        {
            string UserModuleInstanceType;

            try
            {
                oam.getSystemConfig("UMInstanceType", UserModuleInstanceType);
            }
            catch (...) {}

            string UserModuleSecurityGroup;

            try
            {
                oam.getSystemConfig("UMSecurityGroup", UserModuleSecurityGroup);
            }
            catch (...) {}

            instanceName = oam.launchEC2Instance(moduleName, IPAddress, UserModuleInstanceType, UserModuleSecurityGroup);
        }
        else
            instanceName = oam.launchEC2Instance(moduleName, IPAddress);

        if ( instanceName == "failed" )
        {
            cout << " *** Failed to Launch an Instance for " + moduleName << ", will retry up to 5 times" << endl;
            continue;
        }

        cout << endl << "Launched Instance for " << moduleName << ": " << instanceName << endl;

        //give time for instance to startup
        sleep(60);

        string ipAddress = oam::UnassignedName;

        bool pass = false;

        for ( int retry = 0 ; retry < 60 ; retry++ )
        {
            //get IP Address of pm instance
            if ( ipAddress == oam::UnassignedName || ipAddress == "stopped" || ipAddress == "No-IPAddress" )
            {
                ipAddress = oam.getEC2InstanceIpAddress(instanceName);

                if (ipAddress == "stopped" || ipAddress == "No-IPAddress" )
                {
                    sleep(5);
                    continue;
                }
            }

            pass = true;
            break;
        }

        if (!pass)
        {
            oam.terminateEC2Instance( instanceName );
            continue;
        }

        string autoTagging;
        oam.getSystemConfig("AmazonAutoTagging", autoTagging);

        if ( autoTagging == "y" )
        {
            string tagValue = systemName + "-" + moduleName;
            oam.createEC2tag( instanceName, "Name", tagValue );
        }

        break;
    }

    if ( instanceRetry >= 5 )
    {
        cout << " *** Failed to Successfully Launch Instance for " + moduleName << endl;
        return oam::UnassignedName;
    }

    return instanceName;
}

bool glusterSetup(string password, bool doNotResolveHostNames)
{
    Oam oam;
    int dataRedundancyCopies = 0;
    int dataRedundancyNetwork = 0;
    int numberDBRootsPerPM = DBRootCount / pmNumber;
    int numberBricksPM = 0;
    std::vector<int> dbrootPms[DBRootCount];
    DataRedundancySetup DataRedundancyConfigs[pmNumber];
    string command = "";
    string remoteCommand = "remote_command.sh ";

    // how many copies?
    if (pmNumber > 2)
    {
        cout << endl;
        cout << endl << "----- Setup Data Redundancy Copy Count Configuration -----" << endl << endl;
        cout << "Setup the Number of Copies: This is the total number of copies of the data" << endl;
        cout << "in the system. At least 2, but not more than the number of PMs(" + oam.itoa(pmNumber) + "), are required." << endl << endl;

        while (dataRedundancyCopies < 2 || dataRedundancyCopies > pmNumber)
        {
            dataRedundancyCopies = 2;  //minimum 2 copies
            prompt = "Enter Number of Copies [2-" + oam.itoa(pmNumber) + "] (" + oam.itoa(dataRedundancyCopies) + ") >  ";
            pcommand = callReadline(prompt.c_str());

            if (pcommand)
            {
                if (strlen(pcommand) > 0) dataRedundancyCopies = atoi(pcommand);

                callFree(pcommand);
            }

            if ( dataRedundancyCopies < 2 || dataRedundancyCopies > pmNumber )
            {
                cout << endl << "ERROR: Invalid Copy Count '" + oam.itoa(dataRedundancyCopies) + "', please re-enter" << endl << endl;

                if ( noPrompting )
                    exit(1);

                continue;
            }
        }
    }
    else if (pmNumber == 2)
    {
        dataRedundancyCopies = 2;  //minimum 2 copies
        cout << endl;
        cout << "Only 2 PMs configured. Setting number of copies at 2." << endl;
    }
    else
    {
        // This should never happen
        cout << endl;
        cout << "ERROR: Invalid value for pm count Data Redundancy could not be configured." << endl;
        exit(1);
    }

    //update count
    try
    {
        sysConfig->setConfig(InstallSection, "DataRedundancyCopies", oam.itoa(dataRedundancyCopies));
    }
    catch (...)
    {
        cout << "ERROR: Problem setting DataRedundancyCopies in the MariaDB ColumnStore System Configuration file" << endl;
        exit(1);
    }

    numberBricksPM = numberDBRootsPerPM * dataRedundancyCopies;


    cout << endl << "----- Setup Data Redundancy Network Configuration -----" << endl << endl;


    cout << "  'existing'  -   This is specified when using previously configured network devices. (NIC Interface #1)" << endl;
    cout << "                  No additional network configuration is required with this option." << endl << endl;
    cout << "  'dedicated' -   This is specified when it is desired for Data Redundancy traffic to use" << endl;
    cout << "                  a separate network than one previously configured for ColumnStore."  << endl;
    cout << "                  You will be prompted to provide Hostname and IP information for each PM." << endl << endl;
    cout << endl;

    while ( dataRedundancyNetwork != 1 && dataRedundancyNetwork != 2 )
    {
        dataRedundancyNetwork = 1;
        prompt =  "Select the data redundancy network [1=existing, 2=dedicated] (" + oam.itoa(dataRedundancyNetwork) + ") > ";
        pcommand = callReadline(prompt.c_str());

        if (pcommand)
        {
            if (strlen(pcommand) > 0) dataRedundancyNetwork = atoi(pcommand);;

            callFree(pcommand);
        }

        if ( dataRedundancyNetwork != 1 && dataRedundancyNetwork != 2 )
        {
            cout << "Invalid Entry, please re-enter" << endl << endl;
            dataRedundancyNetwork = 0;

            if ( noPrompting )
                exit(1);

            continue;
        }

        //update network type
        try
        {
            sysConfig->setConfig(InstallSection, "DataRedundancyNetworkType", oam.itoa(dataRedundancyNetwork));
        }
        catch (...)
        {
            cout << "ERROR: Problem setting DataRedundancyNetworkType in the MariaDB ColumnStore System Configuration file" << endl;
            exit(1);
        }
    }

    if (dataRedundancyNetwork == 2)
    {
        //loop through pms and get hostname and IP address for each
        for (int pm = 0; pm < pmNumber; pm++)
        {
            DataRedundancyConfigs[pm].pmID = pm + 1;
            string dataDupIPaddr = "ModuleIPAddr" + oam.itoa(DataRedundancyConfigs[pm].pmID) + "-1-3";
            string dataDupHostName = "ModuleHostName" + oam.itoa(DataRedundancyConfigs[pm].pmID) + "-1-3";
            string moduleIPAddr = sysConfig->getConfig("DataRedundancyConfig", dataDupIPaddr);
            string moduleHostName = sysConfig->getConfig("DataRedundancyConfig", dataDupHostName);

            if ( moduleIPAddr.empty() )
            {
                moduleIPAddr = oam::UnassignedIpAddr;
            }

            if ( moduleHostName.empty() )
            {
                moduleHostName = oam::UnassignedName;
            }

            prompt = "Enter PM #" + oam.itoa(DataRedundancyConfigs[pm].pmID) + " Host Name (" + moduleHostName + ") > ";
            pcommand = callReadline(prompt.c_str());

            if (pcommand)
            {
                if (strlen(pcommand) > 0)
                {
                    moduleHostName = pcommand;
                    moduleIPAddr = oam::UnassignedIpAddr;
                }

                callFree(pcommand);
            }

            if ( moduleIPAddr == oam::UnassignedIpAddr)
            {
                //get IP Address
                string IPAddress;
                if (doNotResolveHostNames)
                    if (resolveHostNamesToReverseDNSNames) {
                        IPAddress = resolveHostNameToReverseDNSName(moduleHostName);
                    }
                    else {
                        IPAddress = moduleHostName;
                    }
                else
                    IPAddress = oam.getIPAddress( moduleHostName);

                if ( !IPAddress.empty() )
                    moduleIPAddr = IPAddress;
                else
                    moduleIPAddr = oam::UnassignedIpAddr;
            }

            if ( moduleIPAddr == "127.0.0.1")
                moduleIPAddr = "unassigned";

            //prompt for IP address
            while (true)
            {
                prompt = "Enter PM #" + oam.itoa(DataRedundancyConfigs[pm].pmID) + " IP Address or hostname of " + moduleHostName + " (" + moduleIPAddr + ") > ";
                pcommand = callReadline(prompt.c_str());

                if (pcommand)
                {
                    if (strlen(pcommand) > 0) moduleIPAddr = pcommand;

                    callFree(pcommand);
                }

                if (!doNotResolveHostNames)
                {
                    string ugh = oam.getIPAddress(moduleIPAddr);
                    if (ugh.length() > 0)
                        moduleIPAddr = ugh;
                }

                if (moduleIPAddr == "127.0.0.1" || moduleIPAddr == "0.0.0.0" || moduleIPAddr == "128.0.0.1")
                {
                    cout << endl << moduleIPAddr + " is an Invalid IP Address for a multi-server system, please re-enter" << endl << endl;
                    moduleIPAddr = "unassigned";

                    if ( noPrompting )
                        exit(1);

                    continue;
                }

                if (oam.isValidIP(moduleIPAddr) || doNotResolveHostNames)
                {

                    // run ping test to validate
                    string cmdLine = "ping ";
                    string cmdOption = " -c 1 -w 5 >> /dev/null";
                    string cmd = cmdLine + moduleIPAddr + cmdOption;
                    int rtnCode = system(cmd.c_str());

                    if ( WEXITSTATUS(rtnCode) != 0 )
                    {
                        //NIC failed to respond to ping
                        string temp = "2";

                        while (true)
                        {
                            cout << endl;
                            prompt = "IP Address of '" + moduleIPAddr + "' failed ping test, please validate. Do you want to continue or re-enter [1=continue, 2=re-enter] (2) > ";

                            if ( noPrompting )
                                exit(1);

                            pcommand = callReadline(prompt.c_str());

                            if (pcommand)
                            {
                                if (strlen(pcommand) > 0) temp = pcommand;

                                callFree(pcommand);
                            }

                            if ( temp == "1" || temp == "2")
                                break;
                            else
                            {
                                temp = "2";
                                cout << endl << "Invalid entry, please re-enter" << endl;

                                if ( noPrompting )
                                    exit(1);
                            }
                        }

                        cout << endl;

                        if ( temp == "1")
                            break;
                    }
                    else	// good ping
                        break;
                }
                else
                {
                    cout << endl << "Invalid IP Address format, xxx.xxx.xxx.xxx, please re-enter" << endl << endl;

                    if ( noPrompting )
                        exit(1);
                }
            }

            //set Host Name and IP
            try
            {
                sysConfig->setConfig("DataRedundancyConfig", dataDupHostName, moduleHostName);
                sysConfig->setConfig("DataRedundancyConfig", dataDupIPaddr, moduleIPAddr);
                DataRedundancyConfigs[pm].pmID = pm + 1;
                DataRedundancyConfigs[pm].pmIpAddr = moduleIPAddr;
                DataRedundancyConfigs[pm].pmHostname = moduleHostName;
            }
            catch (...)
            {
                cout << "ERROR: Problem setting Host Name in the MariaDB ColumnStore Data Redundancy Configuration" << endl;
                exit(1);
            }
        }
    }
    else
    {
        for (int pm = 0; pm < pmNumber; pm++)
        {
            string pmIPaddr = "ModuleIPAddr" + oam.itoa(pm + 1) + "-1-3";
            string pmHostName = "ModuleHostName" + oam.itoa(pm + 1) + "-1-3";
            DataRedundancyConfigs[pm].pmID = pm + 1;
            DataRedundancyConfigs[pm].pmIpAddr = sysConfig->getConfig("SystemModuleConfig", pmIPaddr);
            DataRedundancyConfigs[pm].pmHostname = sysConfig->getConfig("SystemModuleConfig", pmHostName);
        }
    }

    /*
    	cout << endl;
    	cout << "OK. You have " + oam.itoa(pmNumber) + " PMs, " + oam.itoa(DBRootCount) + " DBRoots, and you have chosen to keep " + oam.itoa(dataRedundancyCopies) << endl;
    	cout << "copies of the data. You can choose to place the copies in " << endl;
    	cout << "/gluster or you can specify individual, mountable device names. " << endl;
    	/// [directory,storage] (directory) >
    	while( dataRedundancyStorage != 1 && dataRedundancyStorage != 2 )
    	{
    		dataRedundancyStorage = 1;
    		prompt =  "Select the data redundancy storage device [1=directory, 2=storage] (" + oam.itoa(dataRedundancyStorage) + ") > ";
    		pcommand = callReadline(prompt.c_str());
    		if (pcommand)
    		{
    			if (strlen(pcommand) > 0) dataRedundancyStorage = atoi(pcommand);;
    			callFree(pcommand);
    		}

    		if ( dataRedundancyStorage != 1 && dataRedundancyStorage != 2 ) {
    			cout << "Invalid Entry, please re-enter" << endl << endl;
    			dataRedundancyStorage = 0;
    			if ( noPrompting )
    				exit(1);
    			continue;
    		}
    		//update network type
    		try {
    			sysConfig->setConfig(InstallSection, "DataRedundancyStorageType", oam.itoa(dataRedundancyStorage));
    		}
    		catch(...)
    		{
    			cout << "ERROR: Problem setting DataRedundancyStorageType in the MariaDB ColumnStore System Configuration file" << endl;
    			exit(1);
    		}
    	}

    	if (dataRedundancyStorage == 2)
    	{
    		int numberStorageLocations = DBRootCount*dataRedundancyCopies;
    		cout << endl;
    		cout << "You will need " + oam.itoa(numberStorageLocations) + "total storage locations," << endl;
    		cout << "and " + oam.itoa(numberBricksPM) + " storage locations per PM. You will now " << endl;
    		cout << "be asked to enter the device names for the storage locations." << endl;

    		//loop through pms and get storage locations for each
    		for (int pm=0; pm < pmNumber; pm++)
    		{
    			vector<int>::iterator dbrootID = DataRedundancyConfigs[pm].dbrootCopies.begin();
    			for (; dbrootID < DataRedundancyConfigs[pm].dbrootCopies.end(); dbrootID++ )
    			{
    				int brick = (*dbrootID);
    				cout << "PM#" + oam.itoa(DataRedundancyConfigs[pm].pmID) + " brick#" + oam.itoa(brick) + " : " << endl;
    			}
    			for (int brick=0; brick < numberBricksPM; brick++)
    			{
    				prompt = "Enter a storage locations for PM#" + oam.itoa(DataRedundancyConfigs[pm].pmID) + " brick#" + oam.itoa(brick) + " : ";
    				pcommand = callReadline(prompt.c_str());
    				if (pcommand)
    				{
    					if (strlen(pcommand) > 0)
    					{
    						DataRedundancyStorageSetup dataredundancyStorageItem;
    						dataredundancyStorageItem.brickID = brick;
    						dataredundancyStorageItem.storageLocation = pcommand;
    						//get filesystem type
    						prompt = "Enter a filesystem type for storage location " + dataredundancyStorageItem.storageLocation + " (ext2,ext3,xfs,etc): ";
    						pcommand = callReadline(prompt.c_str());
    						if (pcommand)
    						{
    							if (strlen(pcommand) > 0)
    							{
    								dataredundancyStorageItem.storageFilesytemType = pcommand;
    							}
    							callFree(pcommand);
    						}
    						DataRedundancyConfigs[pm].storageLocations.push_back(dataredundancyStorageItem);
    					}
    					callFree(pcommand);
    				}
    			}
    		}
    	}
    */
    // User config complete setup the gluster bricks
    cout << endl << "----- Performing Data Redundancy Configuration -----" << endl << endl;

    // This will distribute DBRootCopies evenly across PMs
    for (int pm = 0; pm < pmNumber; pm++)
    {
        int dbrootCopy = DataRedundancyConfigs[pm].pmID;
        int nextPM = DataRedundancyConfigs[pm].pmID;

        if (nextPM >= pmNumber)
        {
            nextPM = 0;
        }

        for ( int i = 0; i < numberDBRootsPerPM; i++)
        {
            DataRedundancyConfigs[pm].dbrootCopies.push_back(dbrootCopy);

            for ( int copies = 0; copies < (dataRedundancyCopies - 1); copies++)
            {
                DataRedundancyConfigs[nextPM].dbrootCopies.push_back(dbrootCopy);
                nextPM++;

                if (nextPM >= pmNumber)
                {
                    nextPM = 0;
                }

                if (nextPM == pm)
                {
                    nextPM++;
                }

                if (nextPM >= pmNumber)
                {
                    nextPM = 0;
                }
            }

            dbrootCopy += pmNumber;
        }
    }

    // Store to config and distribute so that GLUSTER_WHOHAS will work
    for (int db = 0; db < DBRootCount; db++)
    {
        int dbrootID = db + 1;
        string dbrootIDPMs = "";
        string moduleDBRootIDPMs = "DBRoot" + oam.itoa(dbrootID) + "PMs";

        for (int pm = 0; pm < pmNumber; pm++)
        {
            if (find(DataRedundancyConfigs[pm].dbrootCopies.begin(), DataRedundancyConfigs[pm].dbrootCopies.end(), dbrootID) != DataRedundancyConfigs[pm].dbrootCopies.end())
            {
                //dbrootPms for each dbroot there is a list of PMs for building the gluster volume commands below
                dbrootPms[db].push_back(DataRedundancyConfigs[pm].pmID);
                dbrootIDPMs += oam.itoa(DataRedundancyConfigs[pm].pmID);
                dbrootIDPMs += " ";
            }
        }

        // Store to config and distribute so that GLUSTER_WHOHAS will work
        sysConfig->setConfig("DataRedundancyConfig", moduleDBRootIDPMs, dbrootIDPMs);
    }

    int status;

    for (int pm = 0; pm < pmNumber; pm++)
    {
        for ( int brick = 1; brick <= numberBricksPM; brick++)
        {
            command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password + " 'mkdir -p /var/lib/columnstore/gluster/brick" + oam.itoa(brick) + "'";
            status = system(command.c_str());

            if (WEXITSTATUS(status) != 0 )
            {
                cout << "ERROR: failed to make directory(" << DataRedundancyConfigs[pm].pmIpAddr  << "): '" << command << "'" << endl;
                exit(1);
            }

            /*
            			if (dataRedundancyStorage == 2)
            			{
            				//walk data storage locations and modify fstab to reflect the storage locations entered by user
            				vector<DataRedundancyStorageSetup>::iterator storageSetupIter=DataRedundancyConfigs[pm].storageLocations.begin();
            				for (; storageSetupIter < DataRedundancyConfigs[pm].storageLocations.end(); storageSetupIter++ )
            				{
            					if (rootUser)
            					{
            						command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password +
            								" 'echo " + (*storageSetupIter).storageLocation + " " +
            								installDir + "/gluster/brick" + oam.itoa(brick) + " " +
            								(*storageSetupIter).storageFilesytemType + " defaults 1 2 >> /etc/fstab'";
            					}
            					else
            					{
            						command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password +
            								" 'sudo bash -c `sudo echo " + (*storageSetupIter).storageLocation + " " +
            								installDir + "/gluster/brick" + oam.itoa(brick) + " " +
            								(*storageSetupIter).storageFilesytemType + " defaults 1 2 >> /etc/fstab`'";
            					}
            					status = system(command.c_str());
            					if (WEXITSTATUS(status) != 0 )
            					{
            						cout << "ERROR: command failed: " << command << endl;
            						exit(1);
            					}
            					if (rootUser)
            					{
            						command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password +
            								" 'mount " + installDir + "/gluster/brick" + oam.itoa(brick) + "'";
            					}
            					else
            					{
            						command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password +
            								" 'sudo bash -c `sudo mount " + installDir + "/gluster/brick" + oam.itoa(brick) + "`'";
            					}
            					status = system(command.c_str());
            					if (WEXITSTATUS(status) != 0 )
            					{
            						cout << "ERROR: command failed: " << command << endl;
            						exit(1);
            					}
            					if (!rootUser)
            					{
            						int user;
            						user = getuid();
            						command = remoteCommand + DataRedundancyConfigs[pm].pmIpAddr + " " + password +
            								"'sudo bash -c `sudo chown -R " + oam.itoa(user) + ":" + oam.itoa(user) + " " + installDir + "/gluster/brick" + oam.itoa(brick) + "`'";
            						status = system(command.c_str());
            						if (WEXITSTATUS(status) != 0 )
            						{
            							cout << "ERROR(" << status <<"): command failed: " << command << endl;
            						}
            					}

            				}
            			}
            */
        }

        string errmsg1;
        string errmsg2;
        int ret = oam.glusterctl(oam::GLUSTER_PEERPROBE, DataRedundancyConfigs[pm].pmIpAddr, password, errmsg2);

        if ( ret != 0 )
        {
            exit(1);
        }
    }

    sleep(5);

    if (rootUser)
    {
        command = "gluster peer status >> /tmp/glusterCommands.txt 2>&1";
    }
    else
    {
        command = "sudo gluster peer status >> /tmp/glusterCommands.txt 2>&1";
    }

    status = system(command.c_str());

    if (WEXITSTATUS(status) != 0 )
    {
        cout << "ERROR: peer status command failed." << endl;
        exit(1);
    }

    //Need to wait since peer probe success does not always mean it is ready for volume create command
    //TODO: figureout a cleaner way to do this.
    sleep(10);
    // Build the gluster volumes and start them for each dbroot
    int pmnextbrick[pmNumber];

    for (int pm = 0; pm < pmNumber; pm++)
    {
        pmnextbrick[pm] = 1;
    }

    for (int db = 0; db < DBRootCount; db++)
    {
        int dbrootID = db + 1;

        if (rootUser)
        {
            command = "gluster volume create dbroot" + oam.itoa(dbrootID) + " transport tcp replica " + oam.itoa(dataRedundancyCopies) + " ";
        }
        else
        {
            command = "sudo gluster volume create dbroot" + oam.itoa(dbrootID) + " transport tcp replica " + oam.itoa(dataRedundancyCopies) + " ";
        }

        vector<int>::iterator dbrootPmIter = dbrootPms[db].begin();

        for (; dbrootPmIter < dbrootPms[db].end(); dbrootPmIter++ )
        {
            int pm = (*dbrootPmIter) - 1;
            command += DataRedundancyConfigs[pm].pmIpAddr + ":/var/lib/columnstore/gluster/brick" + oam.itoa(pmnextbrick[pm]) + " ";
            pmnextbrick[pm]++;
        }

        command += "force >> /tmp/glusterCommands.txt 2>&1";
        cout << "Gluster create and start volume dbroot" << oam.itoa(dbrootID) << "...";
        status = system(command.c_str());

        if (WEXITSTATUS(status) != 0 )
        {
            if (oam.checkLogStatus("/tmp/glusterCommands.txt", "dbroot" + oam.itoa(dbrootID) + " already exists" ))
            {
                string errmsg1;
                string errmsg2;
                int ret = oam.glusterctl(oam::GLUSTER_DELETE, oam.itoa(dbrootID), errmsg1, errmsg2);

                if ( ret != 0 )
                {
                    cerr << "FAILURE: Error replacing existing gluster dbroot# " + oam.itoa(dbrootID) + ", error: " + errmsg1 << endl;
                    exit(1);
                }

                status = system(command.c_str());

                if (WEXITSTATUS(status) != 0 )
                {
                    cout << "ERROR: Failed to create volume dbroot" << oam.itoa(dbrootID) << endl;
                    exit(1);
                }
            }
            else
            {
                cout << "ERROR: Failed to create volume dbroot" << oam.itoa(dbrootID) << endl;
                exit(1);
            }
        }

        if (rootUser)
        {
            command = "gluster volume start dbroot" + oam.itoa(dbrootID) + " >> /tmp/glusterCommands.txt 2>&1";
            status = system(command.c_str());

            if (WEXITSTATUS(status) != 0 )
            {
                cout << "ERROR: Failed to start dbroot" << oam.itoa(dbrootID) << endl;
                exit(1);
            }
        }
        else
        {
            int user = getuid();
            int group = getgid();
            command = "sudo gluster volume set dbroot" + oam.itoa(dbrootID) + " storage.owner-uid " + oam.itoa(user) + " >> /tmp/glusterCommands.txt 2>&1";;
            status = system(command.c_str());

if (WEXITSTATUS(status) != 0)
{
    cout << "ERROR: Failed to start dbroot" << oam.itoa(dbrootID) << endl;
    exit(1);
}

command = "sudo gluster volume set dbroot" + oam.itoa(dbrootID) + " storage.owner-gid " + oam.itoa(group) + " >> /tmp/glusterCommands.txt 2>&1";;
status = system(command.c_str());

if (WEXITSTATUS(status) != 0)
{
    cout << "ERROR: Failed to start dbroot" << oam.itoa(dbrootID) << endl;
    exit(1);
}

command = "sudo gluster volume start dbroot" + oam.itoa(dbrootID) + " >> /tmp/glusterCommands.txt 2>&1";
status = system(command.c_str());

if (WEXITSTATUS(status) != 0)
{
    cout << "ERROR: Failed to start dbroot" << oam.itoa(dbrootID) << endl;
    exit(1);
}
        }

        cout << "DONE" << endl;
    }

    cout << endl << "----- Data Redundancy Configuration Complete -----" << endl << endl;

    return true;
}

void singleServerConfigSetup(Config* sysConfig)
{

    try
    {
        sysConfig->setConfig("ExeMgr1", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("ExeMgr1", "Module", "pm1");
        sysConfig->setConfig("ProcMgr", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("ProcMgr_Alarm", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("ProcStatusControl", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("pm1_ProcessMonitor", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("pm1_ServerMonitor", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("pm1_WriteEngineServer", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("DDLProc", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("DMLProc", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS1", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS2", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS3", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS4", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS5", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS6", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS7", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS8", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS9", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS10", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS11", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS12", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS13", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS14", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS15", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS16", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS17", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS18", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS19", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS20", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS21", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS22", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS23", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS24", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS25", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS26", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS27", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS28", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS29", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS30", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS31", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("PMS32", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("SystemModuleConfig", "ModuleCount2", "0");
        sysConfig->setConfig("SystemModuleConfig", "ModuleIPAddr1-1-3", "127.0.0.1");
        sysConfig->setConfig("SystemModuleConfig", "ModuleHostName1-1-3", "localhost");
        sysConfig->setConfig("DBRM_Controller", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("DBRM_Worker1", "IPAddr", "127.0.0.1");
        sysConfig->setConfig("DBRM_Worker1", "Module", "pm1");
        sysConfig->setConfig("DBBC", "NumBlocksPct", "50");
        sysConfig->setConfig("Installation", "InitialInstallFlag", "y");
        sysConfig->setConfig("Installation", "SingleServerInstall", "y");
        sysConfig->setConfig("HashJoin", "TotalUmMemory", "25%");
    }
    catch (...)
    {
        cout << "ERROR: Problem setting for Single Server in the MariaDB ColumnStore System Configuration file" << endl;
        exit(1);
    }

    return;
}

/**
    Resolves the given hostname into its reverse DNS name.

    @param hostname the hostname to resolve.
    @return the reverse dns name of given hostname or an empty string in case the hostname could not be resolved.
*/
std::string resolveHostNameToReverseDNSName(std::string hostname) {
    struct hostent *hp = gethostbyname(hostname.c_str());
    if (hp == NULL) {
        std::cout << "Error: Couldn't resolve hostname " << hostname << " to ip address" << std::endl;
        return "";
    }
    struct hostent *rl = gethostbyaddr(hp->h_addr_list[0], sizeof hp->h_addr_list[0], AF_INET);
    if (rl == NULL) {
        std::cout << "Error: Couldn't resolve ip address of hostname " << hostname << " back to a hostname" << std::endl;
        return "";
    }
    hostname = rl->h_name;
    return hostname;
}

// vim:ts=4 sw=4:
