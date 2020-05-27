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

#include <string>
#include <vector>
#include <unistd.h>
#include <fstream>
#include <algorithm>
#include <iterator>
#include <stdio.h>

#include <readline.h>

#include "mcsconfig.h"
#include "configcpp.h"
using namespace config;

using namespace std;

#include "liboamcpp.h"
using namespace oam;

#include "helpers.h"

using namespace installer;

#include "installdir.h"

string pwprompt = " ";

string masterLogFile = oam::UnassignedName;
string masterLogPos = oam::UnassignedName;
string prompt;

const char* pcommand = 0;

bool noPrompting = false;

namespace installer
{

const char* callReadline(string prompt)
{
    if ( !noPrompting )
    {
        const char* ret = readline(prompt.c_str());

        if ( ret == 0 )
            exit(1);

        return ret;
    }
    else
    {
        cout << prompt << endl;
        return "";
    }
}

void callFree(const char* )
{
    if ( !noPrompting )
        free((void*)pcommand);

    pcommand = 0;
}


bool waitForActive()
{
    Oam oam;

    try
    {
        oam.waitForActive();
        return true;
    }
    catch (...)
    {}

    return false;
}


void dbrmDirCheck()
{

    const string fname = std::string(MCSSYSCONFDIR) + "/columnstore/Columnstore.xml.rpmsave";
    ifstream oldFile (fname.c_str());

    if (!oldFile) return;

    string SystemSection = "SystemConfig";
    Config* sysConfig = Config::makeConfig();
    Config* sysConfigPrev = Config::makeConfig(fname);

    string dbrmroot = "";
    string dbrmrootPrev = "";

    try
    {
        dbrmroot = sysConfig->getConfig(SystemSection, "DBRMRoot");
        dbrmrootPrev = sysConfigPrev->getConfig(SystemSection, "DBRMRoot");
    }
    catch (const std::exception& exc)
    {
        std::cerr << exc.what() << std::endl;
    }

    if ( dbrmrootPrev.empty() )
        return;

    if ( dbrmroot == dbrmrootPrev )
        return;

    string dbrmrootDir = "";
    string dbrmrootPrevDir = "";

    string::size_type pos = dbrmroot.find("/BRM_saves", 0);

    if (pos != string::npos)
        //get directory path
        dbrmrootDir = dbrmroot.substr(0, pos);
    else
    {
        return;
    }

    pos = dbrmrootPrev.find("/BRM_saves", 0);

    if (pos != string::npos)
        //get directory path
        dbrmrootPrevDir = dbrmrootPrev.substr(0, pos);
    else
    {
        return;
    }

    // return if prev directory doesn't exist
    ifstream File (dbrmrootPrevDir.c_str());

    if (!File)
        return;

    string dbrmrootCurrent = dbrmroot + "_current";
    string dbrmrootCurrentPrev = dbrmrootPrev + "_current";

    // return if prev current file doesn't exist
    ifstream File1 (dbrmrootCurrentPrev.c_str());

    if (!File1)
        return;

    string cmd;
    // check if current file does't exist
    // if not, copy prev files to current directory
    ifstream File2 (dbrmrootCurrent.c_str());

    if (!File2)
    {
        cout << endl << "===== DBRM Data File Directory Check  =====" << endl << endl;
        cmd = "/bin/cp -rpf " + dbrmrootPrevDir + "/* " + dbrmrootDir + "/.";
        system(cmd.c_str());

        //update the current file hardcoded path
        ifstream oldFile (dbrmrootCurrent.c_str());

        if (oldFile)
        {
            char line[200];
            oldFile.getline(line, 200);
            string dbrmFile = line;

            string::size_type pos = dbrmFile.find("/BRM_saves", 0);

            if (pos != string::npos)
                dbrmFile = dbrmrootDir + dbrmFile.substr(pos, 80);

            unlink (dbrmrootCurrent.c_str());
            ofstream newFile (dbrmrootCurrent.c_str());

            string cmd = "echo " + dbrmFile + " > " + dbrmrootCurrent;
            system(cmd.c_str());

            newFile.close();
        }

        cmd = "mv -f " + dbrmrootPrevDir + " " + dbrmrootPrevDir + ".old";
        system(cmd.c_str());
        cout << endl << "DBRM data files were copied from dbrm directory" << endl;
        cout << dbrmrootPrevDir << " to current directory of " << dbrmrootDir << "." << endl;
        cout << "The old dbrm directory was renamed to " << dbrmrootPrevDir << ".old ." << endl;
    }
    else
    {
        string start = "y";
        cout << endl << "===== DBRM Data File Directory Check  =====" << endl << endl;
        cout << endl << "DBRM data files were found in " << dbrmrootPrevDir << endl;
        cout << "and in the new location " << dbrmrootDir << "." << endl << endl;
        cout << "Make sure that the correct set of files are in the new location." << endl;
        cout << "Then rename the directory " << dbrmrootPrevDir << " to " << dbrmrootPrevDir << ".old" << endl;
        cout << "If the files were copied from " << dbrmrootPrevDir << " to " << dbrmrootDir << endl;
        cout << "you will need to edit the file BRM_saves_current to contain the current path of" << endl;
        cout << dbrmrootDir << endl << endl;
        cout << "Please reference the MariaDB Columnstore Installation Guide on Upgrade Installs for" << endl;
        cout << "addition information, if needed." << endl << endl;

        while (true)
        {
            string answer = "n";
            prompt = "Enter 'y' when you are ready to continue > ";
            pcommand = callReadline(prompt.c_str());

            if (pcommand)
            {
                if (strlen(pcommand) > 0) answer = pcommand;

                callFree(pcommand);
                pcommand = 0;
            }

            if ( answer == "y" )
                break;
            else
                cout << "Invalid Entry, please enter 'y' for yes" << endl;
        }
    }

    cmd = "chmod 755 -R /var/lib/columnstore/data1/systemFiles/dbrm > /dev/null 2>&1";
    system(cmd.c_str());

    return;
}

void mysqlSetup()
{
    Oam oam;
    string cmd;
    string tmpDir = startup::StartUp::tmpDir();
    string mysqlpw = oam.getMySQLPassword();
    string passwordOption = "";
    
    if ( mysqlpw != oam::UnassignedName )
        passwordOption = " --password=" + mysqlpw;

    cmd = "post-mysqld-install " + passwordOption + " --tmpdir=" + tmpDir + " > " + tmpDir + "/post-mysqld-install.log 2>&1";
    int rtnCode = system(cmd.c_str());

    if (WEXITSTATUS(rtnCode) != 0)
    {
        cout << "Error running post-mysqld-install, check " << tmpDir << "/post-mysqld-install.log" << endl;
        cout << "Exiting..." << endl;
        exit (1);
    }
    else
        cout << "post-mysqld-install Successfully Completed" << endl;

    int user;
    bool rootUser = true;
    user = getuid();

    if (user != 0)
        rootUser = false;

    string HOME = "/root";

    if (!rootUser)
    {
        char* p = getenv("HOME");

        if (p && *p)
            HOME = p;
    }
    
    cmd = "post-mysql-install --tmpdir=" + tmpDir + " > " + tmpDir + "/post-mysql-install.log";
    rtnCode = system(cmd.c_str());

    if (WEXITSTATUS(rtnCode) == 2)
    {
        cout << "Error running post-mysql-install, password is needed. check " + HOME + "/.my.cnf " << endl;
        cout << "Exiting..." << endl;
        exit (1);
    }
    else if (WEXITSTATUS(rtnCode) == 1)
    {
        cout << "Error running post-mysql-install, " + tmpDir + "/post-mysql-install.log" << endl;
        cout << "Exiting..." << endl;
        exit (1);
    }
    else
        cout << "post-mysql-install Successfully Completed" << endl;

    return;
}

/******************************************************************************************
* @brief	sendReplicationRequest
*
* purpose:	send Upgrade Request Msg to all ACTIVE UMs
*
*
******************************************************************************************/
int sendReplicationRequest(int IserverTypeInstall, std::string password, bool pmwithum)
{
    Oam oam;

    SystemModuleTypeConfig systemmoduletypeconfig;

	string tmpDir = startup::StartUp::tmpDir();

    try
    {
        oam.getSystemConfig(systemmoduletypeconfig);
    }
    catch (const std::exception& exc)
    {
        std::cerr << exc.what() << std::endl;
    }

    //get Primary (Master) UM
    string masterModule = oam::UnassignedName;

    try
    {
        oam.getSystemConfig("PrimaryUMModuleName", masterModule);
    }
    catch (...)
    {
        masterModule = oam::UnassignedName;
    }

    if ( masterModule == oam::UnassignedName )
    {
        // use default setting
        masterModule = "um1";

        if ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM )
            masterModule = "pm1";
    }

    int returnStatus = oam::API_SUCCESS;

    bool masterDone = false;

    for ( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
        int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

        if ( moduleCount == 0)
            continue;

        string moduleType = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); )
        {
            // we want to do master first
            if ( ( (*pt).DeviceName == masterModule && !masterDone ) ||
                    ( (*pt).DeviceName != masterModule && masterDone ) )
            {
                int opState = oam::ACTIVE;
                bool degraded;

                try
                {
                    oam.getModuleStatus((*pt).DeviceName, opState, degraded);

                    if (opState == oam::ACTIVE ||
                            opState == oam::DEGRADED)
                    {
                        if ( (*pt).DeviceName == masterModule )
                        {
                            // set for Master MySQL DB distrubution to slaves
                            messageqcpp::ByteStream msg1;
                            messageqcpp::ByteStream::byte requestID = oam::MASTERDIST;
                            msg1 << requestID;
                            msg1 << password;
                            msg1 << "all";	// dist to all slave modules

                            returnStatus = sendMsgProcMon( (*pt).DeviceName, msg1, requestID, 600 );

                            if ( returnStatus != API_SUCCESS)
                            {
                                cout << endl << "ERROR: Error return in running the MariaDB ColumnStore Master DB Distribute, check " + tmpDir + "/master-dist*.logs on " << masterModule << endl;
                                return returnStatus;
                            }

                            // set for master repl request
                            messageqcpp::ByteStream msg;
                            requestID = oam::MASTERREP;
                            msg << requestID;

                            returnStatus = sendMsgProcMon( (*pt).DeviceName, msg, requestID, 30 );

                            if ( returnStatus != API_SUCCESS)
                            {
                                cout << endl << "ERROR: Error return in running the MariaDB ColumnStore Master replication, check " + tmpDir + "/master-rep*.logs on " << masterModule << endl;
                                return returnStatus;
                            }

                            masterDone = true;
                            pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                        }
                        else
                        {
                            // set for slave repl request
                            // don't do PMs unless PMwithUM flag is set
                            string moduleType = (*pt).DeviceName.substr(0, MAX_MODULE_TYPE_SIZE);

                            if ( ( moduleType == "pm" && !pmwithum ) &&
                                    ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM ) )
                            {
                                pt++;
                                continue;
                            }

                            messageqcpp::ByteStream msg;
                            messageqcpp::ByteStream::byte requestID = oam::SLAVEREP;
                            msg << requestID;

                            if ( masterLogFile == oam::UnassignedName ||
                                    masterLogPos == oam::UnassignedName )
                                return API_FAILURE;

                            msg << masterLogFile;
                            msg << masterLogPos;

                            returnStatus = sendMsgProcMon( (*pt).DeviceName, msg, requestID, 30 );

                            if ( returnStatus != API_SUCCESS)
                            {
                                cout << endl << "ERROR: Error return in running the MariaDB ColumnStore Slave replication, check " + tmpDir + "/slave-rep*.logs on " << (*pt).DeviceName << endl;
                                return returnStatus;
                            }

                            pt++;
                        }
                    }
                    else
                    {
                        pt++;
                    }
                }
                catch (const std::exception& exc)
                {
                    std::cerr << exc.what() << std::endl;
                }
            }
            else
                pt++;
        }
    }

    return returnStatus;
}


/******************************************************************************************
* @brief	sendMsgProcMon
*
* purpose:	Sends a Msg to ProcMon
*
******************************************************************************************/
int sendMsgProcMon( std::string module, messageqcpp::ByteStream msg, int requestID, int timeout )
{
    string msgPort = module + "_ProcessMonitor";
    int returnStatus = API_FAILURE;
    Oam oam;

    // do a ping test to determine a quick failure
    Config* sysConfig = Config::makeConfig();

    string IPAddr = sysConfig->getConfig(msgPort, "IPAddr");

    if ( IPAddr == oam::UnassignedIpAddr )
    {
        return returnStatus;
    }

    string cmdLine = "ping ";
    string cmdOption = " -w 1 >> /dev/null";
    string cmd = cmdLine + IPAddr + cmdOption;

    if ( system(cmd.c_str()) != 0)
    {
        //ping failure
        return returnStatus;
    }

    try
    {
        messageqcpp::MessageQueueClient mqRequest(msgPort);
        mqRequest.write(msg);

        if ( timeout > 0 )
        {
            // wait for response
            messageqcpp::ByteStream::byte returnACK;
            messageqcpp::ByteStream::byte returnRequestID;
            messageqcpp::ByteStream::byte requestStatus;
            messageqcpp::ByteStream receivedMSG;

            struct timespec ts = { timeout, 0 };

            // get current time in seconds
            time_t startTimeSec;
            time (&startTimeSec);

            while (true)
            {
                try
                {
                    receivedMSG = mqRequest.read(&ts);
                }
                catch (const std::exception& exc)
                {
                    std::cerr << exc.what() << std::endl;
                    return returnStatus;
                }

                if (receivedMSG.length() > 0)
                {
                    receivedMSG >> returnACK;
                    receivedMSG >> returnRequestID;
                    receivedMSG >> requestStatus;

                    if ( requestID == oam::MASTERREP )
                    {
                        receivedMSG >> masterLogFile;
                        receivedMSG >> masterLogPos;
                    }

                    if ( returnACK == oam::ACK &&  returnRequestID == requestID)
                    {
                        // ACK for this request
                        returnStatus = requestStatus;
                        break;
                    }
                }
                else
                {
                    //api timeout occurred, check if retry should be done
                    // get current time in seconds
                    time_t endTimeSec;
                    time (&endTimeSec);

                    if ( timeout <= (endTimeSec - startTimeSec) )
                    {
                        break;
                    }
                }
            }
        }
        else
            returnStatus = oam::API_SUCCESS;

        mqRequest.shutdown();
    }
    catch (const std::exception& exc)
    {
        std::cerr << exc.what() << std::endl;
    }

    return returnStatus;
}

void checkFilesPerPartion(int DBRootCount, Config* sysConfig)
{
    // check 'files per parition' with number of dbroots
    // 'files per parition' need to be a multiple of dbroots
    // update if no database already exist
    // issue warning if database exist

    Oam oam;
    string SystemSection = "SystemConfig";

    string dbRoot = "/var/lib/columnstore/data1";

    try
    {
        dbRoot = sysConfig->getConfig(SystemSection, "DBRoot1");
    }
    catch (const std::exception& exc)
    {
        std::cerr << exc.what() << std::endl;
    }

    dbRoot = dbRoot + "/000.dir";

    float FilesPerColumnPartition = 4;

    try
    {
        string tmp = sysConfig->getConfig("ExtentMap", "FilesPerColumnPartition");
        FilesPerColumnPartition = atoi(tmp.c_str());
    }
    catch (const std::exception& exc)
    {
        std::cerr << exc.what() << std::endl;
    }

    if ( fmod(FilesPerColumnPartition, (float) DBRootCount) != 0 )
    {
        ifstream oldFile (dbRoot.c_str());

        if (!oldFile)
        {
            //set FilesPerColumnPartition == DBRootCount
            sysConfig->setConfig("ExtentMap", "FilesPerColumnPartition", oam.itoa(DBRootCount));

            cout << endl << "***************************************************************************" << endl;
            cout <<         "NOTE: Mismatch between FilesPerColumnPartition (" + oam.itoa((int)FilesPerColumnPartition) + ") and number of DBRoots (" + oam.itoa(DBRootCount) + ")" << endl;
            cout <<         "      Setting FilesPerColumnPartition = number of DBRoots"  << endl;
            cout <<         "***************************************************************************" << endl;
        }
        else
        {
            cout << endl << "***************************************************************************" << endl;
            cout <<         "WARNING: Mismatch between FilesPerColumnPartition (" + oam.itoa((int)FilesPerColumnPartition) + ") and number of DBRoots (" + oam.itoa(DBRootCount) + ")" << endl;
            cout <<         "         Database already exist, going forward could corrupt the database"  << endl;
            cout <<         "         Please Contact Customer Support"  << endl;
            cout <<         "***************************************************************************" << endl;
            exit (1);
        }
    }

    return;
}

void checkMysqlPort(std::string& mysqlPort)
{
	string tmpDir = startup::StartUp::tmpDir();

    while (true)
    {
        string cmd = "netstat -na | grep -e :" + mysqlPort + "[[:space:]] | grep LISTEN > " + tmpDir + "/mysqlport";

        system(cmd.c_str());
        string fileName = tmpDir + "/mysqlport";
        ifstream oldFile (fileName.c_str());

        if (oldFile)
        {
            oldFile.seekg(0, std::ios::end);
            int size = oldFile.tellg();

            if (size != 0)
            {
                cout << endl << "The MariaDB ColumnStore port of '" + mysqlPort + "' is already in-use" << endl;
                cout << "Please stop the process that is using port '" + mysqlPort + "'" << endl;
                exit(1);
            }
            else
                break;
        }
        else
            break;
    }

}

void checkSystemMySQLPort(std::string& mysqlPort, Config* sysConfig, std::string USER, std::string password, ChildModuleList childmodulelist, int IserverTypeInstall, bool pmwithum)
{
    Oam oam;

    bool inUse = false;

	string tmpDir = startup::StartUp::tmpDir();

    while (true)
    {
        string localnetstat = "netstat -na | grep -e :" + mysqlPort + "[[:space:]] | grep LISTEN > " + tmpDir + "/mysqlport";
        string remotenetstat = "netstat -na | grep -e :" + mysqlPort + "[[:space:]] | grep LISTEN";

        //first check local mysql, if needed
        if ( ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
                ( ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM ) && pmwithum ) )
        {
            system(localnetstat.c_str());
            string fileName = tmpDir + "/mysqlport";
            ifstream oldFile (fileName.c_str());

            if (oldFile)
            {
                oldFile.seekg(0, std::ios::end);
                int size = oldFile.tellg();

                if ( size != 0 )
                {
                    if ( noPrompting )
                    {
                        cout << endl << "The MariaDB ColumnStore port of '" + mysqlPort + "' is already in-use" << endl;
                        cout << "Either use the command line argument of 'port' to enter a different number" << endl;
                        cout << "or stop the process that is using port '" + mysqlPort + "'" << endl;
                        cout << "For No-prompt install, exiting" << endl;
                        exit(1);
                    }
                    else
                        inUse = true;
                }
            }
        }

        // if not inuse by local, go check remote servers
        string inUseServer = "";

        if ( !inUse )
        {
            ChildModuleList::iterator list1 = childmodulelist.begin();

            for (; list1 != childmodulelist.end() ; list1++)
            {
                string remoteModuleName = (*list1).moduleName;
                string remoteModuleIP = (*list1).moduleIP;
                string remoteHostName = (*list1).hostName;
                string remoteModuleType = remoteModuleName.substr(0, MAX_MODULE_TYPE_SIZE);

                if ( remoteModuleType == "um" ||
                        (remoteModuleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) ||
                        (remoteModuleType == "pm" && pmwithum) )
                {

                    string cmd = "remote_command_verify.sh " + remoteModuleIP + " " + " " + USER + " " + password + " '" + remotenetstat + "' tcp error 5 0 > /dev/null 2>&1";
                    int rtnCode = system(cmd.c_str());

                    if (WEXITSTATUS(rtnCode) == 0)
                    {
                        if ( noPrompting )
                        {
                            cout << endl << "The MariaDB ColumnStore port of '" + mysqlPort + "' is already in-use on " << remoteModuleName << endl;
                            cout << "Either use the command line argument of 'port' to enter a different number" << endl;
                            cout << "or stop the process that is using port '" + mysqlPort + "'" << endl;
                            cout << "For No-prompt install, exiting" << endl;
                            cout << "exiting..." << endl;
                            exit(1);
                        }
                        else
                        {
                            inUseServer = remoteModuleName;
                            inUse = true;
                            break;
                        }
                    }
                }
            }
        }

        if ( inUse )
        {
            cout << endl << "The MariaDB ColumnStore port of '" + mysqlPort + "' is already in-use on " << inUseServer << endl;
            cout << "Either enter a different port to use" << endl;
            cout << "or stop the process that is using port '" + mysqlPort + "' and enter '" + mysqlPort + "' to continue" << endl;

            while (true)
            {
                prompt = "Enter a port number > ";
                pcommand = callReadline(prompt.c_str());

                if (pcommand)
                {
                    if (strlen(pcommand) > 0) mysqlPort = pcommand;

                    callFree(pcommand);
                    pcommand = 0;
                }

                if ( atoi(mysqlPort.c_str()) < 1000 || atoi(mysqlPort.c_str()) > 9999)
                {
                    cout << "   ERROR: Invalid MariaDB ColumnStore Port ID supplied, must be between 1000-9999" << endl;
                }
                else
                    break;
            }

            inUse = false;
        }
        else
        {
            cout << endl;

            try
            {
                sysConfig->setConfig("Installation", "MySQLPort", mysqlPort);
            }
            catch (const std::exception& exc)
            {
                std::cerr << exc.what() << std::endl;
            }

            if ( !writeConfig(sysConfig) )
            {
                cout << "ERROR: Failed trying to update MariaDB Columnstore System Configuration file" << endl;
                exit(1);
            }

            break;
        }
    }

    // set mysql password
    oam.changeMyCnf( "port", mysqlPort );

    return;
}

/*
 * writeConfig
 */
bool writeConfig( Config* sysConfig )
{
    for ( int i = 0 ; i < 3 ; i++ )
    {
        try
        {
            sysConfig->write();
            return true;
        }
        catch (const std::exception& exc)
        {
            std::cerr << exc.what() << std::endl;
        }
    }

    return false;
}

}


