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
 * $Id: mcsadmin.cpp 3110 2013-06-20 18:09:12Z dhill $
 *
 ******************************************************************************************/

#include <clocale>
#include <netdb.h>
extern int h_errno;

#include "columnstoreversion.h"
#include "mcsadmin.h"
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/tokenizer.hpp>

#include "mcsconfig.h"
#include "sessionmanager.h"
#include "dbrm.h"
#include "messagequeue.h"
#include "we_messages.h"
#include "../../writeengine/redistribute/we_redistributedef.h"
#include "we_config.h" // for findObjectFile
#include "we_fileop.h" // for findObjectFile
namespace fs = boost::filesystem;

using namespace alarmmanager;
using namespace std;
using namespace oam;
using namespace config;
using namespace messageqcpp;
using namespace redistribute;
using namespace execplan;

#include "installdir.h"

// Variables shared in both main and functions

Config* fConfig = 0;
string Section;
int CmdID = 0;
string CmdList[cmdNum];
int CmdListID[cmdNum];
string cmdName;
const string SECTION_NAME = "Cmd";
int serverInstallType;
string systemName;
string parentOAMModule;
string localModule;
bool rootUser = true;
string HOME = "/root";
string SingleServerInstall;
string tmpDir;

bool repeatStop;

static void checkPromptThread();
bool connectToDBRoot1PM(Oam& oam, boost::scoped_ptr<MessageQueueClient>&  msgQueueClient);
bool SendToWES(Oam& oam, ByteStream bs);

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

bool waitForStop()
{
    Oam oam;
    SystemStatus systemstatus;
    SystemProcessStatus systemprocessstatus;

    for (int i = 0 ; i < 1200 ; i ++)
    {
        sleep (3);

        try
        {
            oam.getSystemStatus(systemstatus);

            if (systemstatus.SystemOpState == MAN_OFFLINE)
            {
                return true;
            }

            if (systemstatus.SystemOpState == FAILED)
            {
                return false;
            }

            cout << "." << flush;
        }
        catch (...)
        {
            // At some point, we need to give up, ProcMgr just isn't going to respond.
            if (i > 60) // 3 minutes
            {
                cout << "ProcMgr not responding while waiting for system to start";
                break;
            }
        }
    }

    return false;
}

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigTerm(int i)
{
    std::cout << "Received SIGTERM to terminate MariaDB ColumnStore Console..." << std::endl;

}

//------------------------------------------------------------------------------
// Signal handler to catch Control-C signal to terminate the process
//------------------------------------------------------------------------------
void handleControlC(int i)
{
    std::cout << "Received Control-C to terminate the console..." << std::endl;
    exit(0);
}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------
void setupSignalHandlers()
{
#ifdef _MSC_VER
    //FIXME
#else
    // Control-C signal to terminate a command
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = handleControlC;
    sigaction(SIGINT, &act, 0);

    // catch SIGTERM signal to terminate the program
//    memset(&act, 0, sizeof(act));
//    act.sa_handler = handleSigTerm;
//    sigaction(SIGTERM, &act, 0);
#endif
}

int main(int argc, char* argv[])
{

    {
        cout << "SKIP_OAM_INIT is set and legacy OAM is disabled by default" << endl;
        sleep(2);
    }

#ifndef _MSC_VER
    setuid(0); // set effective ID to root; ignore return status
#endif
    setlocale(LC_ALL, "");
    setlocale(LC_NUMERIC, "C");

    Oam oam;
    char* pcommand = 0;
    string arguments[ArgNum];

    const char* p = getenv("HOME");

    if (!p)
        p = "";
    else
        HOME = p;

    string ccHistoryFile = HOME + "/.cc_history";

    tmpDir = startup::StartUp::tmpDir();

    string cf = std::string(MCSSYSCONFDIR) + "/columnstore/" + ConsoleCmdsFile;
    fConfig = Config::makeConfig(cf);

//	setupSignalHandlers();

    // Get System Name
    try
    {
        oam.getSystemConfig("SystemName", systemName);
    }
    catch (...)
    {
        cout << endl << "**** Failed : Failed to read systemName Name" << endl;
        exit(-1);
    }

    //get parentModule Name
    parentOAMModule = getParentOAMModule();

    // get Local Module Name and Single Server Install Indicator
    oamModuleInfo_t st;

    try
    {
        st = oam.getModuleInfo();
        localModule = boost::get<0>(st);
        serverInstallType = boost::get<5>(st);
    }
    catch (...)
    {
        cout << endl << "**** Failed : Failed to read Local Module Name" << endl;
        exit(-1);
    }

    try
    {
        oam.getSystemConfig("SingleServerInstall", SingleServerInstall);
    }
    catch (...) {}

    //check if root-user
    int user;
    user = getuid();

    if (user != 0)
        rootUser = false;

    // create/open command log file if not created

    logFile.open(DEFAULT_LOG_FILE.c_str(), ios::app);

    if (geteuid() == 0 && !logFile)
    {
        cerr << "UI Command log file cannot be opened" << endl;
    }

    writeLog("Start of a command session!!!");

    // get and sort command list for future help display

    for (int i = 0; i < cmdNum ; i++)
    {
        // get cmd name

        Section = SECTION_NAME + oam.itoa(i);

        cmdName = fConfig->getConfig(Section, "Name");

        if (cmdName.empty())
            // no command skip
            continue;

        CmdList[i] = cmdName;
        CmdListID[i] = i;

        // sort

        for (int j = 0; j < i ; j++)
        {
            if ( CmdList[i] < CmdList[j] )
            {
                cmdName = CmdList[i];
                CmdList[i] = CmdList[j];
                CmdList[j] = cmdName;
                CmdID = CmdListID[i];
                CmdListID[i] = CmdListID[j];
                CmdListID[j] = CmdID;
            }
        }
    }

    if ( localModule != parentOAMModule )
    {
        // issue message informing user they aren't logged into Active OAm Parent
        cout << endl;
        cout << "WARNING: running on non Parent OAM Module, can't make configuration changes in this session." << endl;
        cout << "         Access Console from '" << parentOAMModule << "' if you need to make changes." << endl << endl;
    }

    // check for arguments passed in as a request

    if (argc > 1)
    {
        int j = 0;
        string command;

        for (; argc > 1; j++, argc--)
        {
            arguments[j] = argv[j + 1];
            command.append(arguments[j]);
            command.append(" ");
        }

        // add to history and UI command log file
        read_history(ccHistoryFile.c_str());
        add_history (command.c_str());
        writeLog(command.c_str());
        write_history(ccHistoryFile.c_str());

        checkRepeat(arguments, j);
    }
    else
    {
        cout << endl << "MariaDB ColumnStore Admin Console" << endl;
        cout << "   enter 'help' for list of commands" << endl;
        cout << "   enter 'exit' to exit the MariaDB ColumnStore Command Console" << endl;
        cout << "   use up/down arrows to recall commands" << endl << endl;

        // output current active alarm stats
        printAlarmSummary();
        printCriticalAlarms();

        //read readline history file
        read_history(ccHistoryFile.c_str());

        while (true)
        {
            //get parentModule Name
            parentOAMModule = getParentOAMModule();

            // flush agument list
            for (int j = 0; j < ArgNum; j++)
            {
                arguments[j].clear();
            }

            // read input
            pcommand = readline("mcsadmin> ");

            if (!pcommand)                        // user hit <Ctrl>-D
                pcommand = strdup("exit");

            else if (!*pcommand)
            {
                // just an enter-key was entered, ignore and reprompt
                continue;
            }

            // add to history and UI command log file
            add_history (pcommand);
            writeLog(pcommand);
            write_history(ccHistoryFile.c_str());

            string command = pcommand;

            //check if a argument was entered as a set of char with quotes around them
            int commandLoc = 0;
            int numberArgs = 0;
            bool validCMD = true;

            for (int i = 0; i < ArgNum; i++)
            {
                string::size_type pos = command.find(" ", commandLoc);
                string::size_type pos1 = command.find("\"", commandLoc);
                string::size_type pos3 = command.find("\'", commandLoc);

                if ( (pos == string::npos && pos1 == string::npos) ||
                        (pos == string::npos && pos3 == string::npos) )
                {
                    //end of the command
                    string argtemp = command.substr(commandLoc, 80);

                    if ( argtemp != "" )
                    {
                        arguments[numberArgs] = argtemp;
                        numberArgs++;
                    }

                    break;
                }

                if (pos < pos1 && pos < pos3)
                {
                    // hit ' ' first
                    string argtemp = command.substr(commandLoc, pos - commandLoc);

                    if ( argtemp != "" )
                    {
                        arguments[numberArgs] = argtemp;
                        numberArgs++;
                    }

                    commandLoc = pos + 1;
                }
                else
                {
                    if ( pos >= pos1 )
                    {
                        //hit " first
                        string::size_type pos2 = command.find("\"", pos1 + 1);

                        if (pos2 != string::npos)
                        {
                            arguments[numberArgs] = command.substr(pos1 + 1, pos2 - pos1 - 1);
                            numberArgs++;
                            commandLoc = pos2 + 1;
                        }
                        else
                        {
                            cout << "Invalid Command, mismatching use of quotes" << endl;
                            validCMD = false;
                            break;
                        }
                    }
                    else
                    {
                        //hit ' first
                        string::size_type pos2 = command.find("\'", pos3 + 1);

                        if (pos2 != string::npos)
                        {
                            arguments[numberArgs] = command.substr(pos3 + 1, pos2 - pos3 - 1);
                            numberArgs++;
                            commandLoc = pos2 + 1;
                        }
                        else
                        {
                            cout << "Invalid Command, mismatching use of quotes" << endl;
                            validCMD = false;
                            break;
                        }
                    }
                }
            }

            if (validCMD)
                checkRepeat(arguments, numberArgs);

            free (pcommand);
        }
    }
}

void checkRepeat(string* arguments, int argNumber)
{
    Oam oam;
    bool repeat = false;
    int repeatCount = 5;

    for ( int i = 0; i < argNumber ; i++)
    {
        if ( arguments[i].find("-r") == 0)
        {
            // entered
            if ( arguments[i] != "-r")
            {
                //strip report count off
                repeatCount = atoi(arguments[i].substr(2, 10).c_str());

                if ( repeatCount < 1 || repeatCount > 60 )
                {
                    cout << "Failed: incorrect repeat count entered, valid range is 1-60, set to default of 5" << endl;
                    repeatCount = 5;
                }
            }

            repeat = true;
            arguments[i].clear();
            cout << "repeating the command '" << arguments[0] << "' every " << repeatCount << " seconds, enter CTRL-D to stop" << endl;
            sleep(5);
            break;
        }
    }

    bool threadCreate = false;

    if (repeat)
    {
        while (true)
        {
            system("clear");

            if ( processCommand(arguments) )
                return;
            else
            {
                if ( !threadCreate )
                {
                    threadCreate = true;
                    repeatStop = false;
                    pthread_t PromptThread;
                    pthread_create (&PromptThread, NULL, (void* (*)(void*)) &checkPromptThread, NULL);
                }

                for ( int i = 0 ; i < repeatCount ; i ++ )
                {
                    if (repeatStop)
                        break;

                    sleep(1);
                }

                if (repeatStop)
                    break;
            }
        }
    }
    else
        processCommand(arguments);
}

int processCommand(string* arguments)
{
    Oam oam;
    // Possible command line arguments
    GRACEFUL_FLAG gracefulTemp = GRACEFUL;
    ACK_FLAG ackTemp = ACK_YES;
    CC_SUSPEND_ANSWER suspendAnswer = CANCEL;
    bool bNeedsConfirm = true;
    string password;
    string cmd;
    // get command info from Command config file
    CmdID = -1;

    // put inputted command into lowercase
    string inputCmd = arguments[0];
    transform (inputCmd.begin(), inputCmd.end(), inputCmd.begin(), to_lower());

    for (int i = 0; i < cmdNum; i++)
    {
        // put table command into lowercase
        string cmdName_LC = CmdList[i];
        transform (cmdName_LC.begin(), cmdName_LC.end(), cmdName_LC.begin(), to_lower());

        if (cmdName_LC.find(inputCmd) == 0)
        {
            // command found, ECHO command
            cout << cmdName_LC << "   " << oam.getCurrentTime() << endl;
            CmdID = CmdListID[i];
            break;
        }
    }

    if (CmdID == -1)
    {
        // get is command in the Support Command list
        for (int i = 0;; i++)
        {
            if (supportCmds[i] == "")
                // end of list
                break;

            if (supportCmds[i].find(inputCmd) == 0)
            {
                // match found, go process it
                cout << supportCmds[i] << "   " << oam.getCurrentTime() << endl;
                int status = ProcessSupportCommand(i, arguments);

                if ( status == -1 )
                    // didn't process it for some reason
                    break;

                return 1;
            }
        }

        // command not valid
        cout << arguments[0] << ": Unknown Command, type help for list of commands" << endl << endl;
        return 1;
    }
    switch ( CmdID )
    {
        case 0: // help
        case 1: // ?
        {
            const string DESC_NAME = "Desc";
            string desc;
            string descName;
            const string ARG_NAME = "Arg";
            string arg;
            string argName;

            string argument1_LC = arguments[1];
            transform (argument1_LC.begin(), argument1_LC.end(), argument1_LC.begin(), to_lower());

            if (argument1_LC.find("-a") == 0 || argument1_LC == "")
            {
                // list commands and brief description (Desc1)
                cout << endl << "List of commands:" << endl;
                cout << "Note: the command must be the first entry entered on the command line" << endl << endl;
                cout.setf(ios::left);
                cout.width(34);
                cout << "Command" << "Description" << endl;
                cout.setf(ios::left);
                cout.width(34);
                cout << "------------------------------" << "--------------------------------------------------------" << endl;

                for (int i = 0; i < cmdNum ; i++)
                {
                    // get cmd name

                    Section = SECTION_NAME + oam.itoa(CmdListID[i]);

                    cmdName = fConfig->getConfig(Section, "Name");

                    if (cmdName.empty()  || cmdName == "AVAILABLE")
                        // no command skip
                        continue;

                    cout.setf(ios::left);
                    cout.width(34);
                    cout << cmdName << fConfig->getConfig(Section, "Desc1") << endl;
                }

                cout << endl << "For help on a command, enter 'help' followed by command name" << endl;
            }
            else
            {
                if (argument1_LC.find("-v") == 0)
                {
                    // list of commands with their descriptions
                    cout << endl << "List of commands and descriptions:" << endl << endl;

                    for (int k = 0 ; k < cmdNum ; k++)
                    {
                        Section = SECTION_NAME + oam.itoa(CmdListID[k]);
                        cmdName = fConfig->getConfig(Section, "Name");

                        if (cmdName.empty()  || cmdName == "AVAILABLE")
                            //no command skip
                            continue;

                        cout << "Command: " << cmdName << endl << endl;
                        int i = 2;
                        cout << "   Description: " << fConfig->getConfig(Section, "Desc1") << endl;

                        while (true)
                        {
                            desc = DESC_NAME + oam.itoa(i);
                            descName = fConfig->getConfig(Section, desc);

                            if (descName.empty())
                                //end of Desc list
                                break;

                            cout << "                " << descName << endl;
                            i++;
                        }

                        i = 2;
                        cout << endl << "   Arguments:   " << fConfig->getConfig(Section, "Arg1") << endl;

                        while (true)
                        {
                            arg = ARG_NAME + oam.itoa(i);
                            argName = fConfig->getConfig(Section, arg);

                            if (argName.empty())
                                //end of arg list
                                break;

                            cout << "                " << argName << endl;
                            i++;
                        }

                        cout << endl;
                    }
                }
                else
                {
                    // description for a single command
                    int j = 0;

                    for (j = 0; j < cmdNum; j++)
                    {
                        // get cmd description

                        Section = SECTION_NAME + oam.itoa(j);

                        cmdName = fConfig->getConfig(Section, "Name");

                        string cmdName_LC = cmdName;
                        transform (cmdName_LC.begin(), cmdName_LC.end(), cmdName_LC.begin(), to_lower());

                        if (cmdName_LC == argument1_LC)
                        {
                            // command found, output description
                            cout << endl << "   Command:     " << cmdName << endl << endl;
                            int i = 2;
                            cout << "   Description: " << fConfig->getConfig(Section, "Desc1") << endl;

                            while (true)
                            {
                                desc = DESC_NAME + oam.itoa(i);
                                descName = fConfig->getConfig(Section, desc);

                                if (descName.empty())
                                    //end of Desc list
                                    break;

                                cout << "                " << descName << endl;
                                i++;
                            }

                            i = 2;
                            cout << endl << "   Arguments:   " << fConfig->getConfig(Section, "Arg1") << endl;

                            while (true)
                            {
                                arg = ARG_NAME + oam.itoa(i);
                                argName = fConfig->getConfig(Section, arg);

                                if (argName.empty())
                                    //end of arg list
                                    break;

                                cout << "                " << argName << endl;
                                i++;
                            }

                            break;
                        }
                    }

                    if (j == cmdNum)
                    {
                        // command not valid
                        cout << arguments[1] << ": Unknown Command, type help for list of commands" << endl << endl;
                        break;
                    }
                }
            }

            cout << endl;
        }
        break;

        case 2: // exit
        case 3: // quit
        {
            // close the log file
            writeLog("End of a command session!!!");
            logFile.close();
            cout << "Exiting the MariaDB ColumnStore Admin Console" << endl;

            exit (0);
        }
        break;

        case 4: // redistributeData
        {
            set<uint32_t> removeDbroots;    // set of dbroots we want to leave empty
            vector<uint32_t> srcDbroots;    // all of the currently configured dbroots
            vector<uint32_t> destDbroots;   // srcDbroots - removeDbroots
            set<int>::iterator dbiter;
#if _MSC_VER
			if (_strnicmp(arguments[1].c_str(), "start", 5) == 0))
#else
			if (strncasecmp(arguments[1].c_str(), "start", 5) == 0)
#endif
            {
                // Get a list of all the configured dbroots in the xml file.
                DBRootConfigList dbRootConfigList;
                std::set<int> configuredDBRoots;
                oam.getSystemDbrootConfig(dbRootConfigList);

                for (DBRootConfigList::iterator i = dbRootConfigList.begin(); i != dbRootConfigList.end(); ++i)
                    configuredDBRoots.insert(*i);

                // The user may choose to redistribute in such a way as to
                // leave certain dbroots empty, presumably for later removal.
#if _MSC_VER
				if (_strnicmp(arguments[1].c_str(), "remove", 6) == 0))
#else
				if (strncasecmp(arguments[1].c_str(), "remove", 6) == 0)
#endif
                {
                    int dbroot;
                    bool error = false;

                    for (int i = 3; arguments[i] != ""; ++i)
                    {
                        dbroot = atoi(arguments[i].c_str());

                        if (dbroot == 1)
                        {
                            cout << "Not allowed to remove dbroot-1" << endl;
                            error = true;
                        }
                        else
                        {
                            if (configuredDBRoots.find(dbroot) == configuredDBRoots.end())
                            {
                                ostringstream oss;
                                cout << "DBRoot-" << dbroot << " is not configured" << endl;
                                error = true;
                            }
                            else
                            {
                                removeDbroots.insert((uint32_t)dbroot);
                            }
                        }
                    }

                    if (error)
                    {
                        cout << "Errors encountered. Abort" << endl;
                        break;
                    }
                }

                // Create a list of source dbroots -- where the data currently resides.
                for (dbiter = configuredDBRoots.begin(); dbiter != configuredDBRoots.end(); ++dbiter)
                    srcDbroots.push_back((uint32_t)*dbiter);

                // Create a list of destination dbroots -- where the data is to go.
                for (dbiter = configuredDBRoots.begin(); dbiter != configuredDBRoots.end(); ++dbiter)
                {
                    // Only use the dbroots not in the remove list
                    if (removeDbroots.find((uint32_t)*dbiter) == removeDbroots.end())
                    {
                        destDbroots.push_back((uint32_t)*dbiter);
                    }
                }

                // Print out what we're about to do
                cout << "redistributeData START ";

                if (removeDbroots.size() > 0)
                {
                    cout << "    Removing dbroots:";
                    set<uint32_t>::iterator iter;

                    for (iter = removeDbroots.begin(); iter != removeDbroots.end(); ++iter)
                    {
                        cout << " " << *iter;
                    }
                }

                cout << endl;
                cout << "Source dbroots:";
                vector<uint32_t>::iterator iter;

                for (iter = srcDbroots.begin(); iter != srcDbroots.end(); ++iter)
                    cout << " " << *iter;

                cout << endl << "Destination dbroots:";

                for (iter = destDbroots.begin(); iter != destDbroots.end(); ++iter)
                    cout << " " << *iter;

                cout << endl << endl;

                // Connect to PM for dbroot1
                ByteStream bs;
                // message WES ID, sequence #, action id
                uint32_t sequence = time(0);
                bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;

                // Send the CLEAR message to WriteEngineServer (WES). Wipes out previous state.
                RedistributeMsgHeader header(0, 0, sequence, RED_CNTL_CLEAR);
                bs.append((const ByteStream::byte*) &header, sizeof(header));
                SendToWES(oam, bs);

                // Send the START message
                bs.restart();
                sequence = time(0);
                bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
                header.sequenceNum = sequence;
                header.messageId = RED_CNTL_START;
                bs.append((const ByteStream::byte*) &header, sizeof(header));
                uint32_t options = 0;

                if (removeDbroots.size() > 0)
                {
                    options |= RED_OPTN_REMOVE;
                }

                bs << options;

                // source db roots,
                bs << (uint32_t) srcDbroots.size();

                for (uint64_t i = 0; i < srcDbroots.size(); ++i)
                    bs << (uint32_t) srcDbroots[i];

                // destination db roots,
                bs << (uint32_t) destDbroots.size();

                for (uint64_t i = 0; i < destDbroots.size(); ++i)
                    bs << (uint32_t) destDbroots[i];

                SendToWES(oam, bs);
            }
#if _MSC_VER
			else if (_strnicmp(arguments[1].c_str(), "stop", 4) == 0))
#else
			else if (strncasecmp(arguments[1].c_str(), "stop", 4) == 0)
#endif
            {
                ByteStream bs;
                // message WES ID, sequence #, action id
                uint32_t sequence = time(0);
                bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
                RedistributeMsgHeader header(0, 0, sequence, RED_CNTL_STOP);
                bs.append((const ByteStream::byte*) &header, sizeof(header));
                SendToWES(oam, bs);
            }
#if _MSC_VER
			else if (_strnicmp(arguments[1].c_str(), "status", 6) == 0))
#else
			else if (strncasecmp(arguments[1].c_str(), "status", 6) == 0)
#endif
            {
                ByteStream bs;
                // message WES ID, sequence #, action id
                uint32_t sequence = time(0);
                bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
                RedistributeMsgHeader header(0, 0, sequence, RED_CNTL_STATUS);
                bs.append((const ByteStream::byte*) &header, sizeof(header));
                SendToWES(oam, bs);
            }
            else
            {
                cout << "redistributeData must have one of START, STOP or STATUS" << endl;
            }
        }
        break;

        case 5: // findObjectFile
        {
            unsigned maxDBRoot = WriteEngine::Config::DBRootCount();

            if (maxDBRoot < 1)
            {
                cout << endl << "findobjectfile fails because there are no dbroots defined for this server" << endl;
                break;;
            }

            if (arguments[1] == "")
            {
                cout << endl << "findobjectfile requires one of" << endl;
                cout << "a) oid of column for which file name is to be retrieved" << endl;
                cout << "b) schema, table and column for which the file name is to be retrieved" << endl;
                cout << "c) oid of table for which the file name of each column is to be retrieved" << endl;
                cout << "d) schema and table for which the file name of each column is to be retrieved" << endl;
                break;
            }

            char* endchar;
            int oid = 0;
            int tableOid = 0; // If a table report
            int dictOid = 0;  // If a dictionary oid was given
            std::vector<int> columnOids;
            CalpontSystemCatalog::TableName tableName;
            CalpontSystemCatalog::TableColName columnName;
            boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
                execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0);
            systemCatalogPtr->identity(execplan::CalpontSystemCatalog::FE);

            // Try to get a numeric oid from the argument
            oid = strtol(arguments[1].c_str(), &endchar, 0);

            // test to see if not all numeric
            if (endchar < & (*arguments[1].end())) // endchar from above will not point to the end if not numeric
            {
                oid = 0;
            }

            if (oid == 0)  // A table or column name was entered
            {
                // Need to convert the arguments to oid
                columnName.schema = arguments[1];

                if (arguments[2] == "")
                {
                    cout << endl << "findobjectfile requires a table for schema " << arguments[1] << endl;
                    break;
                }

                columnName.table = arguments[2];

                if (arguments[3] == "")
                {
                    // No column was given. Get the list of column oids for the table.
                    tableName.schema = arguments[1];
                    tableName.table = arguments[2];

                    try
                    {
                        tableOid = systemCatalogPtr->lookupTableOID(tableName);

                        if (tableOid)
                        {
                            CalpontSystemCatalog::RIDList rdlist = systemCatalogPtr->columnRIDs(tableName);

                            for (unsigned int i = 0; i < rdlist.size(); ++i)
                            {
                                columnOids.push_back(rdlist[i].objnum);
                            }
                        }
                        else
                        {
                            cout << arguments[1] << "." << arguments[2] << " is not a columnstore table" << endl;
                            break;
                        }
                    }
                    catch ( runtime_error& e )
                    {
                        cout << "error while trying to get the columns for " << tableName.schema << "." << tableName.table << ": " << e.what() << endl;
                        break;
                    }
                    catch (...)
                    {
                        cout << "error while trying to get the columns for " << tableName.schema << "." << tableName.table << endl;
                        break;
                    }
                }
                else // A column name was given
                {
                    columnName.column = arguments[3];
                    oid = systemCatalogPtr->lookupOID(columnName);

                    if (oid < 1)
                    {
                        cout << arguments[1] << "." << arguments[2] << "." << arguments[3] << " is not a columnstore column" << endl;
                        break;
                    }

                    columnOids.push_back(oid);
                }
            }
            else // An oid was given
            {
                try
                {
                    // Is oid a column?
                    columnName = systemCatalogPtr->colName(oid);
                }
                catch (...) { /* Ignore */ }

                if (columnName.schema.size() == 0 || columnName.table.size() == 0 || columnName.column.size() == 0)
                {
                    // Not a column OID
                    // check to see if it's a dictionary oid.
                    try
                    {
                        columnName = systemCatalogPtr->dictColName(oid);
                    }
                    catch (...) { /* Ignore */ }

                    if (columnName.schema.size() == 0 || columnName.table.size() == 0 || columnName.column.size() == 0)
                    {
                        // Not a dictionary oid
                        // Check to see if a table oid was given. If so, get the column oid list.
                        try
                        {
                            tableName = systemCatalogPtr->tableName(oid);
                        }
                        catch (...) { /* Ignore */ }

                        if (tableName.schema.size() == 0 || tableName.table.size() == 0)
                        {
                            // Not a table or a column OID.
                            cout << "OID " << oid << " does not represent a table or column in columnstore" << endl;
                            break;
                        }

                        tableOid = oid;

                        try
                        {
                            CalpontSystemCatalog::RIDList rdlist = systemCatalogPtr->columnRIDs(tableName);

                            for (unsigned int i = 0; i < rdlist.size(); ++i)
                            {
                                columnOids.push_back(rdlist[i].objnum);
                            }
                        }
                        catch (...) { /* Ignore */ }
                    }
                    else
                    {
                        // This is a dictionary oid
                        dictOid = oid;
                        columnOids.push_back(oid);
                    }
                }
                else
                {
                    // This is a column oid
                    columnOids.push_back(oid);
                }
            }

            // Use writeengine code to get the filenames
            WriteEngine::FileOp fileOp;
            char fileName[WriteEngine::FILE_NAME_SIZE];
            memset(fileName, 0, WriteEngine::FILE_NAME_SIZE);
            int rc;

            if (tableOid)
            {
                cout << "for table OID " << tableOid << " "
                     << tableName.schema << "." << tableName.table << ":" << endl;
            }

            for (unsigned int i = 0; i < columnOids.size(); ++i)
            {
                oid = columnOids[i];

                if (oid < 1000)
                {
                    rc = fileOp.getVBFileName(oid, fileName);
                }
                else
                {
                    rc = fileOp.oid2DirName(oid, fileName);
                }

                if (oid == dictOid)
                {
                    columnName = systemCatalogPtr->dictColName(oid);
                    cout << "dictionary OID " << oid << " ";
                }
                else
                {
                    columnName = systemCatalogPtr->colName(oid);
                    cout << "column OID " << oid << " ";
                }

                if (!tableOid)
                {
                    cout << columnName.schema << "." << columnName.table << ".";
                }

                cout << columnName.column << "\t";

                if (strlen(fileName) > 0)
                {
                    cout << fileName;
                }

                if (rc == WriteEngine::NO_ERROR)
                {
                    // Success. No more output.
                    cout << endl;
                }
                else if (rc == WriteEngine::ERR_FILE_NOT_EXIST)
                {
                    if (strlen(fileName) == 0)
                    {
                        // We couldn't get a name
                        cout << "Error: Filename could not be determined" << endl;
                    }
                    else
                    {
                        // We got a name, but the file doesn't exist
                        cout << " (directory does not exist on this server)" << endl;
                    }
                }
                else
                {
                    // Something broke
                    cerr << "WriteEngine::FileOp::oid2DirName() error. rc=" << rc << endl;
                }
            }
        }
        break;

        case 6: // getModuleTypeConfig
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            ModuleConfig moduleconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();
            string returnValue;
            string Argument;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Module config parameters

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl << "Module Type Configuration" << endl << endl;

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

                        int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                        if ( moduleCount < 1 )
                            continue;

                        string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                        cout << "ModuleType '" << moduletype	<< "' Configuration information" << endl << endl;

                        cout << "ModuleDesc = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc << endl;
                        cout << "RunType = " << systemmoduletypeconfig.moduletypeconfig[i].RunType << endl;
                        cout << "ModuleCount = " << moduleCount << endl;

                        if ( moduleCount > 0 )
                        {
                            DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                            for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                            {
                                string modulename = (*pt).DeviceName;
                                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                                {
                                    string ipAddr = (*pt1).IPAddr;
                                    string servername = (*pt1).HostName;
                                    cout << "ModuleHostName and ModuleIPAddr for NIC ID " + oam.itoa((*pt1).NicID) + " on  module '" << modulename << "' = " << servername  << " , " << ipAddr << endl;
                                }
                            }
                        }

                        DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt++)
                        {
                            if ( (*pt).dbrootConfigList.size() > 0 )
                            {
                                cout << "DBRootIDs assigned to module 'pm" << (*pt).DeviceID << "' = ";
                                DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                                for ( ; pt1 != (*pt).dbrootConfigList.end() ; )
                                {
                                    cout << *pt1;
                                    pt1++;

                                    if (pt1 != (*pt).dbrootConfigList.end())
                                        cout << ", ";
                                }
                            }

                            cout << endl;
                        }

                        cout << "ModuleCPUCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUCriticalThreshold << endl;
                        cout << "ModuleCPUMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMajorThreshold << endl;
                        cout << "ModuleCPUMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorThreshold << endl;
                        cout << "ModuleCPUMinorClearThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorClearThreshold << endl;
                        cout << "ModuleDiskCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskCriticalThreshold << endl;
                        cout << "ModuleDiskMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMajorThreshold << endl;
                        cout << "ModuleDiskMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMinorThreshold << endl;
                        cout << "ModuleMemCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemCriticalThreshold << endl;
                        cout << "ModuleMemMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMajorThreshold << endl;
                        cout << "ModuleMemMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMinorThreshold << endl;
                        cout << "ModuleSwapCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapCriticalThreshold << endl;
                        cout << "ModuleSwapMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMajorThreshold << endl;
                        cout << "ModuleSwapMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMinorThreshold << endl;

                        DiskMonitorFileSystems::iterator pt2 = systemmoduletypeconfig.moduletypeconfig[i].FileSystems.begin();
                        int id = 1;

                        for ( ; pt2 != systemmoduletypeconfig.moduletypeconfig[i].FileSystems.end() ; pt2++)
                        {
                            string fs = *pt2;
                            cout << "ModuleDiskMonitorFileSystem#" << id << " = " << fs << endl;
                            ++id;
                        }

                        cout << endl;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                }
            }
            else
            {
                // get a single module type config
                if (arguments[2] == "")
                {
                    try
                    {
                        oam.getSystemConfig(arguments[1], moduletypeconfig);

                        cout << endl << "Module Type Configuration for " << arguments[1] << endl << endl;

                        int moduleCount = moduletypeconfig.ModuleCount;
                        string moduletype = moduletypeconfig.ModuleType;

                        cout << "ModuleDesc = " << moduletypeconfig.ModuleDesc << endl;
                        cout << "ModuleCount = " << moduleCount << endl;
                        cout << "RunType = " << moduletypeconfig.RunType << endl;

                        if ( moduleCount > 0 )
                        {
                            DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

                            for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
                            {
                                string modulename = (*pt).DeviceName;
                                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                                {
                                    string ipAddr = (*pt1).IPAddr;
                                    string servername = (*pt1).HostName;
                                    cout << "ModuleHostName and ModuleIPAddr for NIC ID " + oam.itoa((*pt1).NicID) + " on  module " << modulename << " = " << servername  << " , " << ipAddr << endl;
                                }
                            }
                        }

                        int dbrootCount = moduletypeconfig.ModuleDBRootList.size();

                        cout << "DBRootCount = " << dbrootCount << endl;

                        if ( dbrootCount > 0 )
                        {
                            DeviceDBRootList::iterator pt = moduletypeconfig.ModuleDBRootList.begin();

                            for ( ; pt != moduletypeconfig.ModuleDBRootList.end() ; pt++)
                            {
                                cout << "DBRoot IDs assigned to 'pm" + oam.itoa((*pt).DeviceID) + "' = ";

                                DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                                for ( ; pt1 != (*pt).dbrootConfigList.end() ; )
                                {
                                    cout << *pt1;
                                    pt1++;

                                    if (pt1 != (*pt).dbrootConfigList.end())
                                        cout << ", ";
                                }

                                cout << endl;
                            }
                        }

                        cout << "ModuleCPUCriticalThreshold % = " << moduletypeconfig.ModuleCPUCriticalThreshold << endl;
                        cout << "ModuleCPUMajorThreshold % = " << moduletypeconfig.ModuleCPUMajorThreshold << endl;
                        cout << "ModuleCPUMinorThreshold % = " << moduletypeconfig.ModuleCPUMinorThreshold << endl;
                        cout << "ModuleCPUMinorClearThreshold % = " << moduletypeconfig.ModuleCPUMinorClearThreshold << endl;
                        cout << "ModuleDiskCriticalThreshold % = " << moduletypeconfig.ModuleDiskCriticalThreshold << endl;
                        cout << "ModuleDiskMajorThreshold % = " << moduletypeconfig.ModuleDiskMajorThreshold << endl;
                        cout << "ModuleDiskMinorThreshold % = " << moduletypeconfig.ModuleDiskMinorThreshold << endl;
                        cout << "ModuleMemCriticalThreshold % = " << moduletypeconfig.ModuleMemCriticalThreshold << endl;
                        cout << "ModuleMemMajorThreshold % = " << moduletypeconfig.ModuleMemMajorThreshold << endl;
                        cout << "ModuleMemMinorThreshold % = " << moduletypeconfig.ModuleMemMinorThreshold << endl;
                        cout << "ModuleSwapCriticalThreshold % = " << moduletypeconfig.ModuleSwapCriticalThreshold << endl;
                        cout << "ModuleSwapMajorThreshold % = " << moduletypeconfig.ModuleSwapMajorThreshold << endl;
                        cout << "ModuleSwapMinorThreshold % = " << moduletypeconfig.ModuleSwapMinorThreshold << endl;

                        DiskMonitorFileSystems::iterator pt = moduletypeconfig.FileSystems.begin();
                        int id = 1;

                        for ( ; pt != moduletypeconfig.FileSystems.end() ; pt++)
                        {
                            string fs = *pt;
                            cout << "ModuleDiskMonitorFileSystem#" << id << " = " << fs << endl;
                            ++id;
                        }

                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                {
                    // get a parameter for a module type
                    try
                    {
                        oam.getSystemConfig(systemmoduletypeconfig);
                    }
                    catch (...)
                    {}

                    unsigned int i = 0;

                    for ( i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType == arguments[1])
                        {
                            string argument2 = arguments[2];
                            string::size_type pos = arguments[2].rfind("#", 200);

                            if (pos != string::npos)
                            {
                                string ID = arguments[2].substr(pos + 1, 5);
                                arguments[2] = arguments[2].substr(0, pos);
                                arguments[2] = arguments[2] + ID + "-";
                            }

                            Argument = arguments[2] + oam.itoa(i + 1);

                            try
                            {
                                oam.getSystemConfig(Argument, returnValue);
                                cout << endl << "   " << argument2 << " = " << returnValue << endl << endl;
                                break;
                            }
                            catch (exception& e)
                            {
                                cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                                break;
                            }
                        }
                    }

                    if ( i == systemmoduletypeconfig.moduletypeconfig.size() )
                    {
                        // module type not found
                        cout << endl << "**** getModuleTypeConfig Failed : Invalid Module Type" << endl;
                        break;
                    }
                }
            }
        }
        break;

        case 7: // setModuleTypeConfig - parameters: Module type, Parameter name and value
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            string Argument;

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** setModuleTypeConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[3] == "")
            {
                // need 3 arguments
                cout << endl << "**** setModuleTypeConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            if ( arguments[3] == "=" )
            {
                cout << endl << "**** setModuleTypeConfig Failed : Invalid Value of '=', please re-enter" << endl;
                break;
            }

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);
            }
            catch (...)
            {}

            unsigned int i = 0;

            for ( i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
            {
                if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType == arguments[1])
                {
                    string argument2 = arguments[2];
                    string::size_type pos = arguments[2].rfind("#", 200);

                    if (pos != string::npos)
                    {
                        string ID = arguments[2].substr(pos + 1, 5);
                        arguments[2] = arguments[2].substr(0, pos);
                        arguments[2] = arguments[2] + ID + "-";
                    }

                    Argument = arguments[2] + oam.itoa(i + 1);

                    try
                    {
                        oam.setSystemConfig(Argument, arguments[3]);
                        cout << endl << "   Successfully set " << argument2 << " = " << arguments[3] << endl << endl;
                        break;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** setModuleTypeConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }
            }

            if ( i == systemmoduletypeconfig.moduletypeconfig.size() )
            {
                // module type not found
                cout << endl << "**** setModuleTypeConfig Failed : Invalid Module Type" << endl;
                break;
            }
        }
        break;

        case 8: // getProcessConfig
        {
            SystemProcessConfig systemprocessconfig;
            ProcessConfig processconfig;
            string returnValue;

            if (arguments[1] == "all" || arguments[1] == "")
            {
                // get and all display Process config parameters

                try
                {
                    oam.getProcessConfig(systemprocessconfig);

                    cout << endl << "Process Configuration" << endl << endl;

                    for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
                    {
                        cout << "Process #" << i + 1 << " Configuration information" << endl;

                        cout << "ProcessName = " << systemprocessconfig.processconfig[i].ProcessName  << endl;
                        cout << "ModuleType = " << systemprocessconfig.processconfig[i].ModuleType << endl;
                        cout << "ProcessLocation = " << systemprocessconfig.processconfig[i].ProcessLocation << endl;

                        for ( int j = 0 ; j < oam::MAX_ARGUMENTS; j++)
                        {
                            if (systemprocessconfig.processconfig[i].ProcessArgs[j].empty())
                                break;

                            cout << "ProcessArg" << j + 1 << " = " << systemprocessconfig.processconfig[i].ProcessArgs[j] << endl;
                        }

                        cout << "BootLaunch = " << systemprocessconfig.processconfig[i].BootLaunch << endl;
                        cout << "LaunchID = " << systemprocessconfig.processconfig[i].LaunchID << endl;

                        for ( int j = 0 ; j < MAX_DEPENDANCY; j++)
                        {
                            if (systemprocessconfig.processconfig[i].DepProcessName[j].empty())
                                break;

                            cout << "DepModuleName" << j + 1 << " = " << systemprocessconfig.processconfig[i].DepModuleName[j] << endl;
                            cout << "DepProcessName" << j + 1 << " = " << systemprocessconfig.processconfig[i].DepProcessName[j] << endl;
                        }

                        // display Process Group variables, if they exist

                        cout << "RunType = " << systemprocessconfig.processconfig[i].RunType << endl;
                        cout << "LogFile = " << systemprocessconfig.processconfig[i].LogFile << endl;

                        cout << endl;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                }
            }
            else
            {
                // get a single process info - parameters: module-name, process-name
                if (arguments[2] == "")
                {
                    cout << endl << "**** getProcessConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                    break;
                }

                if (arguments[3] == "")
                {
                    //**** Add API to get single process info
                    try
                    {
                        oam.getProcessConfig(arguments[1], arguments[2], processconfig);

                        cout << endl << "Process Configuration for " << arguments[1] << " on module " << arguments[2] << endl << endl;

                        cout << "ProcessName = " << processconfig.ProcessName  << endl;
                        cout << "ModuleType = " << processconfig.ModuleType << endl;
                        cout << "ProcessLocation = " << processconfig.ProcessLocation  << endl;

                        for ( int j = 0 ; j < oam::MAX_ARGUMENTS; j++)
                        {
                            if (processconfig.ProcessArgs[j].empty())
                                break;

                            cout << "ProcessArg" << j + 1 << " = " << processconfig.ProcessArgs[j] << endl;
                        }

                        cout << "BootLaunch = " << processconfig.BootLaunch << endl;
                        cout << "LaunchID = " << processconfig.LaunchID << endl;

                        for ( int j = 0 ; j < MAX_DEPENDANCY; j++)
                        {
                            if (processconfig.DepProcessName[j].empty())
                                break;

                            cout << "DepProcessName" << j + 1 << " = " << processconfig.DepProcessName[j] << endl;
                            cout << "DepModuleName" << j + 1 << " = " << processconfig.DepModuleName[j] << endl;
                        }

                        cout << "RunType = " << processconfig.RunType << endl;
                        cout << "LogFile = " << processconfig.LogFile << endl;

                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                {
                    // get a parameter for a process - parameters: module-name, process-name,
                    // parameter-name
                    // get module ID from module name entered, then get parameter
                    try
                    {
                        oam.getProcessConfig(arguments[1], arguments[2], arguments[3], returnValue);
                        cout << endl << "   " << arguments[3] << " = " << returnValue << endl << endl;
                        break;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }
            }
        }
        break;

        case 9: // setProcessConfig - parameters: Module name, Process Name, Parameter name and value
        {
            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** setProcessConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[4] == "")
            {
                cout << endl << "**** setProcessConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            try
            {
                if ( arguments[4] == "=" )
                {
                    cout << endl << "**** setProcessConfig Failed : Invalid Value of '=', please re-enter" << endl;
                    break;
                }

                oam.setProcessConfig(arguments[1], arguments[2], arguments[3], arguments[4]);
                cout << endl << "   Successfully set " << arguments[3] << " = " << arguments[4] << endl << endl;
                break;
            }
            catch (exception& e)
            {
                cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 10: // getAlarmConfig- parameters: all or AlarmID
        {
            AlarmConfig alarmconfig;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Alarm config parameters

                cout << endl << "Alarm Configuration" << endl << endl;

                for ( int alarmID = 1 ; alarmID < MAX_ALARM_ID; alarmID++)
                {
                    try
                    {
                        oam.getAlarmConfig(alarmID, alarmconfig);

                        cout << "Alarm ID #" << alarmID << " Configuration information" << endl;

                        cout << "BriefDesc = " << alarmconfig.BriefDesc  << endl;
                        cout << "DetailedDesc = " << alarmconfig.DetailedDesc << endl;

                        //	cout << "EmailAddr = " << alarmconfig.EmailAddr  << endl;
                        //	cout << "PagerNum = " << alarmconfig.PagerNum << endl;

                        switch (alarmconfig.Severity)
                        {
                            case CRITICAL:
                                cout << "Severity = CRITICAL" << endl;
                                break;

                            case MAJOR:
                                cout << "Severity = MAJOR" << endl;
                                break;

                            case MINOR:
                                cout << "Severity = MINOR" << endl;
                                break;

                            case WARNING:
                                cout << "Severity = WARNING" << endl;
                                break;

                            default:
                                cout << "Severity = INFORMATIONAL" << endl;
                                break;
                        }

                        cout << "Threshold = " << alarmconfig.Threshold   << endl;
                        //	cout << "Occurrences = " << alarmconfig.Occurrences << endl;
                        //	cout << "LastIssueTime = " << alarmconfig.LastIssueTime  << endl << endl;
                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getAlarmConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }

                break;
            }
            else
            {
                // get a single Alarm info
                try
                {
                    oam.getAlarmConfig(atoi(arguments[1].c_str()), alarmconfig);

                    cout << endl << "Alarm ID #" << arguments[1] << " Configuration information" << endl;

                    cout << "BriefDesc = " << alarmconfig.BriefDesc  << endl;
                    cout << "DetailedDesc = " << alarmconfig.DetailedDesc << endl;

                    //	cout << "EmailAddr = " << alarmconfig.EmailAddr  << endl;
                    //	cout << "PagerNum = " << alarmconfig.PagerNum << endl;

                    switch (alarmconfig.Severity)
                    {
                        case CRITICAL:
                            cout << "Severity = CRITICAL" << endl;
                            break;

                        case MAJOR:
                            cout << "Severity = MAJOR" << endl;
                            break;

                        case MINOR:
                            cout << "Severity = MINOR" << endl;
                            break;

                        case WARNING:
                            cout << "Severity = WARNING" << endl;
                            break;

                        default:
                            cout << "Severity = INFORMATIONAL" << endl;
                            break;
                    }

                    cout << "Threshold = " << alarmconfig.Threshold   << endl;
                    //	cout << "Occurrences = " << alarmconfig.Occurrences << endl;
                    //	cout << "LastIssueTime = " << alarmconfig.LastIssueTime  << endl << endl;
                    cout << endl;
                    break;
                }
                catch (exception& e)
                {
                    cout << endl << "**** getAlarmConfig Failed =  " << e.what() << endl;
                    break;
                }
            }
        }
        break;

        case 11: // setAlarmConfig - parameters: AlarmID, Parameter name and value
        {
            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** setAlarmConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[3] == "")
            {
                // need 3 arguments
                cout << endl << "**** setAlarmConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            try
            {
                if ( arguments[3] == "=" )
                {
                    cout << endl << "**** setAlarmConfig Failed : Invalid Value of '=', please re-enter" << endl;
                    break;
                }

                if ( arguments[2] == "Threshold" && arguments[3] != "0" && atoi(arguments[3].c_str()) == 0 )
                {
                    cout << endl << "**** setAlarmConfig Failed : New value must be a number" << endl;
                    break;
                }

                oam.setAlarmConfig(atoi(arguments[1].c_str()), arguments[2], atoi(arguments[3].c_str()));
                cout << endl << "   Successfully set " << arguments[2] << " = " << arguments[3] << endl << endl;
                break;
            }
            catch (exception& e)
            {
                cout << endl << "**** setAlarmConfig Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 12: // getActiveAlarms - parameters: none
        {
            AlarmList alarmList;

            try
            {
                oam.getActiveAlarms(alarmList);
            }
            catch (...)
            {
                // need arguments
                cout << endl << "**** getActiveAlarms Failed : Error in oam.getActiveAlarms" << endl;
                break;
            }

            cout << endl << "Active Alarm List:" << endl << endl;

            AlarmList :: iterator i;

            for (i = alarmList.begin(); i != alarmList.end(); ++i)
            {
                cout << "AlarmID           = " << i->second.getAlarmID() << endl;
                cout << "Brief Description = " << i->second.getDesc() << endl;
                cout << "Alarm Severity    = ";

                switch (i->second.getSeverity())
                {
                    case CRITICAL:
                        cout << "CRITICAL" << endl;
                        break;

                    case MAJOR:
                        cout << "MAJOR" << endl;
                        break;

                    case MINOR:
                        cout << "MINOR" << endl;
                        break;

                    case WARNING:
                        cout << "WARNING" << endl;
                        break;

                    case INFORMATIONAL:
                        cout << "INFORMATIONAL" << endl;
                        break;
                }

                cout << "Time Issued       = " << i->second.getTimestamp() << endl;
                cout << "Reporting Module  = " << i->second.getSname() << endl;
                cout << "Reporting Process = " << i->second.getPname() << endl;
                cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;
            }
        }
        break;

        case 13: // getStorageConfig
        {
            try
            {
                systemStorageInfo_t t;
                t = oam.getStorageConfig();

                string cloud;

                try
                {
                    oam.getSystemConfig("Cloud", cloud);
                }
                catch (...) {}

                string::size_type pos = cloud.find("amazon", 0);

                if (pos != string::npos)
                    cloud = "amazon";

                cout << endl << "System Storage Configuration" << endl << endl;

                cout << "Performance Module (DBRoot) Storage Type = " << boost::get<0>(t) << endl;

                if ( cloud == "amazon" )
                    cout << "User Module Storage Type = " << boost::get<3>(t) << endl;

                cout << "System Assigned DBRoot Count = " << boost::get<1>(t) << endl;

                DeviceDBRootList moduledbrootlist = boost::get<2>(t);

                typedef std::vector<int> dbrootList;
                dbrootList dbrootlist;

                DeviceDBRootList::iterator pt = moduledbrootlist.begin();

                for ( ; pt != moduledbrootlist.end() ; pt++)
                {
                    cout << "DBRoot IDs assigned to 'pm" + oam.itoa((*pt).DeviceID) + "' = ";
                    DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                    for ( ; pt1 != (*pt).dbrootConfigList.end() ;)
                    {
                        cout << *pt1;
                        dbrootlist.push_back(*pt1);
                        pt1++;

                        if (pt1 != (*pt).dbrootConfigList.end())
                            cout << ", ";
                    }

                    cout << endl;
                }

                //get any unassigned DBRoots
                DBRootConfigList undbrootlist;

                try
                {
                    oam.getUnassignedDbroot(undbrootlist);
                }
                catch (...) {}

                if ( !undbrootlist.empty() )
                {
                    cout << endl << "DBRoot IDs unassigned = ";
                    DBRootConfigList::iterator pt1 = undbrootlist.begin();

                    for ( ; pt1 != undbrootlist.end() ;)
                    {
                        cout << *pt1;
                        pt1++;

                        if (pt1 != undbrootlist.end())
                            cout << ", ";
                    }

                    cout << endl;
                }

                cout << endl;

                // um volumes
                if (cloud == "amazon" && boost::get<3>(t) == "external")
                {
                    ModuleTypeConfig moduletypeconfig;
                    oam.getSystemConfig("um", moduletypeconfig);

                    for ( int id = 1; id < moduletypeconfig.ModuleCount + 1 ; id++)
                    {
                        string volumeNameID = "UMVolumeName" + oam.itoa(id);
                        string volumeName = oam::UnassignedName;
                        string deviceNameID = "UMVolumeDeviceName" + oam.itoa(id);
                        string deviceName = oam::UnassignedName;

                        try
                        {
                            oam.getSystemConfig( volumeNameID, volumeName);
                            oam.getSystemConfig( deviceNameID, deviceName);
                        }
                        catch (...)
                        {}

                        cout << "Amazon EC2 Volume Name/Device Name for 'um" << id << "': " << volumeName << ", " << deviceName << endl;
                    }
                }

                // pm volumes
                if (cloud == "amazon" && boost::get<0>(t) == "external")
                {
                    cout << endl;

                    DBRootConfigList dbrootConfigList;

                    try
                    {
                        oam.getSystemDbrootConfig(dbrootConfigList);

                        DBRootConfigList::iterator pt = dbrootConfigList.begin();

                        for ( ; pt != dbrootConfigList.end() ; pt++)
                        {
                            string volumeNameID = "PMVolumeName" + oam.itoa(*pt);
                            string volumeName = oam::UnassignedName;
                            string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt);
                            string deviceName = oam::UnassignedName;
                            string amazonDeviceNameID = "PMVolumeAmazonDeviceName" + oam.itoa(*pt);
                            string amazondeviceName = oam::UnassignedName;

                            try
                            {
                                oam.getSystemConfig( volumeNameID, volumeName);
                                oam.getSystemConfig( deviceNameID, deviceName);
                                oam.getSystemConfig( amazonDeviceNameID, amazondeviceName);
                            }
                            catch (...)
                            {
                                continue;
                            }

                            cout << "Amazon EC2 Volume Name/Device Name/Amazon Device Name for DBRoot" << oam.itoa(*pt) << ": " << volumeName << ", " << deviceName << ", " << amazondeviceName << endl;
                        }
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
                    }

                    // print un-assigned dbroots
                    DBRootConfigList::iterator pt1 = undbrootlist.begin();

                    for ( ; pt1 != undbrootlist.end() ; pt1++)
                    {
                        string volumeNameID = "PMVolumeName" + oam.itoa(*pt1);
                        string volumeName = oam::UnassignedName;
                        string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt1);
                        string deviceName = oam::UnassignedName;
                        string amazonDeviceNameID = "PMVolumeAmazonDeviceName" + oam.itoa(*pt1);
                        string amazondeviceName = oam::UnassignedName;

                        try
                        {
                            oam.getSystemConfig( volumeNameID, volumeName);
                            oam.getSystemConfig( deviceNameID, deviceName);
                            oam.getSystemConfig( amazonDeviceNameID, amazondeviceName);
                        }
                        catch (...)
                        {
                            continue;
                        }

                        cout << "Amazon EC2 Volume Name/Device Name/Amazon Device Name for DBRoot" << oam.itoa(*pt1) << ": " << volumeName << ", " << deviceName << ", " << amazondeviceName << endl;
                    }
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
                catch (...) {}

                if ( DataRedundancyConfig == "y" )
                {
                    cout << endl << "Data Redundant Configuration" << endl << endl;
                    cout << "Copies Per DBroot = " << DataRedundancyCopies << endl;
                    //cout << "Storage Type = " << DataRedundancyStorageType << endl;

                    oamModuleInfo_t st;
                    string moduleType;

                    try
                    {
                        st = oam.getModuleInfo();
                        moduleType = boost::get<1>(st);
                    }
                    catch (...) {}

                    if ( moduleType != "pm")
                        break;

                    try
                    {
                        DBRootConfigList dbrootConfigList;
                        oam.getSystemDbrootConfig(dbrootConfigList);

                        DBRootConfigList::iterator pt = dbrootConfigList.begin();

                        for ( ; pt != dbrootConfigList.end() ; pt++)
                        {
                            cout << "DBRoot #" << oam.itoa(*pt) << " has copies on PMs = ";

                            string pmList = "";

                            try
                            {
                                string errmsg;
                                oam.glusterctl(oam::GLUSTER_WHOHAS, oam.itoa(*pt), pmList, errmsg);
                            }
                            catch (...)
                            {}

                            boost::char_separator<char> sep(" ");
                            boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                                    it != tokens.end();
                                    ++it)
                            {
                                cout << *it << " ";
                            }

                            cout << endl;
                        }

                        cout << endl;
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

            cout << endl;

            break;
        }

        case 14: // addDbroot parameters: dbroot-number
        {
            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DataRedundancyConfig == "y")
            {
                cout << endl << "**** addDbroot Not Supported on Data Redundancy Configured System, use addModule command to expand your capacity" << endl;
                break;
            }

            if ( localModule != parentOAMModule )
            {
                // exit out since not on active module
                cout << endl << "**** addDbroot Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                break;
            }

            string cloud;
            bool amazon = false;

            try
            {
                oam.getSystemConfig("Cloud", cloud);
            }
            catch (...) {}

            string::size_type pos = cloud.find("amazon", 0);

            if (pos != string::npos)
                amazon = true;

            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** addDbroot Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            int dbrootNumber = atoi(arguments[1].c_str());

            string DBRootStorageType;

            try
            {
                oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
            }
            catch (...) {}

            string EBSsize = oam::UnassignedName;

            if (amazon && DBRootStorageType == "external" )
            {
                if ( arguments[2] != "")
                    EBSsize = arguments[2];
                else
                {
                    cout << endl;
                    oam.getSystemConfig("PMVolumeSize", EBSsize);

                    string prompt = "Enter EBS storage size in GB, current setting is " + EBSsize + " : ";
                    EBSsize = dataPrompt(prompt);
                }
            }

            //get dbroots ids for reside PM
            try
            {
                DBRootConfigList dbrootlist;
                oam.addDbroot(dbrootNumber, dbrootlist, EBSsize);

                cout << endl << " New DBRoot IDs added = ";

                DBRootConfigList::iterator pt = dbrootlist.begin();

                for ( ; pt != dbrootlist.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != dbrootlist.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** addDbroot Failed: " << e.what() << endl;
                break;
            }

            cout << endl;
        }
        break;

        case 15: // removeDbroot parameters: dbroot-list
        {

            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DataRedundancyConfig == "y")
            {
                cout << endl << "**** removeDbroot Not Supported on Data Redundancy Configured System, use removeModule command to remove modules and dbroots" << endl;
                break;
            }

            if ( localModule != parentOAMModule )
            {
                // exit out since not on active module
                cout << endl << "**** removeDbroot Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** removeDbroot Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            SystemStatus systemstatus;

            try
            {
                oam.getSystemStatus(systemstatus);

                if (systemstatus.SystemOpState != oam::ACTIVE )
                {
                    cout << endl << "**** removeDbroot Failed,  System has to be in a ACTIVE state" << endl;
                    break;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** removeDbroot Failed : " << e.what() << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** removeDbroot Failed,  Failed return from getSystemStatus API" << endl;
                break;
            }

            systemStorageInfo_t t;

            try
            {
                t = oam.getStorageConfig();
            }
            catch (...) {}

            string dbrootIDs = arguments[1];

            DBRootConfigList dbrootlist;

            bool assign = false;
            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                //check if dbroot is assigned to a pm
                DeviceDBRootList moduledbrootlist = boost::get<2>(t);

                DeviceDBRootList::iterator pt = moduledbrootlist.begin();

                for ( ; pt != moduledbrootlist.end() ; pt++)
                {
                    DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                    for ( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
                    {
                        if ( atoi((*it).c_str()) == *pt1 )
                        {
                            cout << endl << "**** removeDbroot Failed, dbroot " << *it << " is assigned to a module, unassign first before removing" << endl;
                            assign = true;
                            break;
                        }
                    }
                }

                if (assign)
                    break;

                dbrootlist.push_back(atoi((*it).c_str()));
            }

            if (assign)
                break;

            cout << endl;

            try
            {
                oam.removeDbroot(dbrootlist);

                cout << endl << "   Successful Removal of DBRoots " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** removeDbroot Failed: " << e.what() << endl;
                break;
            }
        }
        break;

        case 16: // stopSystem - parameters: graceful flag, Ack flag
        {
            BRM::DBRM dbrm;
            bool bDBRMReady = dbrm.isDBRMReady();
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            if ( gracefulTemp == INSTALL )
            {
                cout << endl << "Invalid Parameter, INSTALL option not supported. Please use shutdownSystem Command" << endl << endl;
                break;
            }

            cout << endl << "This command stops the processing of applications on all Modules within the MariaDB ColumnStore System" << endl;

            try
            {
                cout << endl << "   Checking for active transactions" << endl;

                if (gracefulTemp != GRACEFUL ||
                        !bDBRMReady ||
                        dbrm.isReadWrite())
                {
                    suspendAnswer = FORCE;
                }

                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line or some other state.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager;
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;

                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }

                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
                        //					if (suspendAnswer == FORCE)
                        //					{
                        //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances"))
                        //						{
                        //							break;
                        //						}
                        //					}
                        bNeedsConfirm = false;
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }

                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }

                if (bNeedsConfirm)
                {
                    if (confirmPrompt(""))
                        break;
                }

                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemShutdownPending(true, false, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemShutdownPending(true, true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case FORCE:
                        cout << endl << "   System being stopped now..." << flush;

                        if (bDBRMReady)
                        {
                            dbrm.setSystemShutdownPending(true, false, true);
                        }

                        break;

                    case CANCEL:
                        break;
                }

                oam.stopSystem(gracefulTemp, ackTemp);

                if ( waitForStop() )
                    cout << endl << "   Successful stop of System " << endl << endl;
                else
                    cout << endl << "**** stopSystem Failed : check log files" << endl;

                checkForDisabledModules();
            }
            catch (exception& e)
            {
                string Failed = e.what();

                if (Failed.find("Connection refused") != string::npos)
                {
                    cout << endl << "**** stopSystem Failure : ProcessManager not Active" << endl;
                    cout << "Retry or Run 'shutdownSystem FORCEFUL' command" << endl << endl;
                }
                else
                {
                    cout << endl << "**** stopSystem Failure : " << e.what() << endl;
                    cout << "Retry or Run 'shutdownSystem FORCEFUL' command" << endl << endl;
                }
            }
        }
        break;

        case 17: // shutdownSystem - parameters: graceful flag, Ack flag, suspendAnswer
        {
            BRM::DBRM dbrm;
            bool bDBRMReady = dbrm.isDBRMReady();
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            cout << endl << "This command stops the processing of applications on all Modules within the MariaDB ColumnStore System" << endl;

            try
            {
                cout << endl << "   Checking for active transactions" << endl;

                if (gracefulTemp != GRACEFUL ||
                        !bDBRMReady ||
                        dbrm.isReadWrite())
                {
                    suspendAnswer = FORCE;
                }

                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager;
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;

                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }

                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "   There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
                        bNeedsConfirm = false;
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }

                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }

                if (bNeedsConfirm)
                {
                    if (confirmPrompt(""))
                        break;
                }

                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemShutdownPending(true, false, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemShutdownPending(true, true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case FORCE:
                        cout << endl << "   Stopping System..." << flush;

                        if (bDBRMReady)
                        {
                            dbrm.setSystemShutdownPending(true, false, true);
                        }

                        break;

                    case CANCEL:
                        break;
                }

                // This won't return until the system is shutdown. It might take a while to finish what we're working on first.

                oam.stopSystem(gracefulTemp, ackTemp);

                if ( waitForStop() )
                    cout << endl << "   Successful stop of System " << endl;
                else
                    cout << endl << "**** stopSystem Failed : check log files" << endl;

                cout << endl << "   Shutting Down System..." << flush;

                oam.shutdownSystem(gracefulTemp, ackTemp);

                //hdfs / hadoop config
                string DBRootStorageType;

                try
                {
                    oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
                }
                catch (...) {}

                if ( DBRootStorageType == "hdfs")
                {
					string logFile = tmpDir + "/cc-stop.pdsh";

                    cmd = "pdsh -a 'columnstore stop' > " + logFile + " 2>&1";
                    system(cmd.c_str());

                    if (oam.checkLogStatus(logFile, "exit") )
                    {
                        cout << endl << "ERROR: Stopping MariaDB ColumnStore Service failure, check " << logFile << " exit..." << endl;
                    }
                }
                else
                {
                    cmd = "columnstore stop > " + tmpDir + "/status.log";
                    system(cmd.c_str());
                }
            }
            catch (exception& e)
            {
                string Failed = e.what();

                if ( gracefulTemp == FORCEFUL )
                {
                    cmd = "columnstore stop > " + tmpDir + "/status.log";
                    system(cmd.c_str());
                    cout << endl << "   Successful shutdown of System (stopped local columnstore service) " << endl << endl;
                }

                if (Failed.find("Connection refused") != string::npos)
                {
                    cout << endl << "**** shutdownSystem Error : ProcessManager not Active, stopping columnstore service" << endl;
                    cmd = "columnstore stop > " + tmpDir + "/status.log";
                    system(cmd.c_str());
                    cout << endl << "   Successful stop of local columnstore service " << endl << endl;
                }
                else
                {
                    cout << endl << "**** shutdownSystem Failure : " << e.what() << endl;
                    cout << "    Retry running command using FORCEFUL option" << endl << endl;
                }

                //hdfs / hadoop config
                string DBRootStorageType;

                try
                {
                    oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
                }
                catch (...) {}

                if ( DBRootStorageType == "hdfs")
                {
					string logFile = tmpDir + "cc-stop.pdsh";
                    cmd = "pdsh -a 'columnstore stop' > " + logFile + " 2>&1";
                    system(cmd.c_str());

                    if (oam.checkLogStatus(logFile, "exit") )
                    {
                        cout << endl << "ERROR: Stopping MariaDB ColumnStore Service failure, check " + logFile + ". exit..." << endl;
                        break;
                    }
                }
            }

            //this is here because a customer likes doing a shutdownsystem then startsystem in a script
            sleep(5);
        }
        break;

        case 18: // startSystem - parameters: Ack flag
        {
            // startSystem Command

            //don't start if a disable module has a dbroot assigned to it
            if (!checkForDisabledModules())
            {
                cout << endl << "Error: startSystem command can't be performed: disabled module has a dbroot assigned to it" << endl;
                break;
            }

            // if columnstore service is down, then start system by starting all of the columnstore services
            // this would be used after a shutdownSystem command
            // if columnstore service is up, send message to ProcMgr to start system (which starts all processes)

            if (!oam.checkSystemRunning())
            {
                cout << endl << "startSystem command, 'columnstore' service is down, sending command to" << endl;
                cout << "start the 'columnstore' service on all modules" << endl << endl;

                SystemModuleTypeConfig systemmoduletypeconfig;
                ModuleTypeConfig moduletypeconfig;
                ModuleConfig moduleconfig;
                systemmoduletypeconfig.moduletypeconfig.clear();
                int systemModuleNumber = 0;

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

                        systemModuleNumber = systemModuleNumber + systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** startSystem Failed =  " << e.what() << endl;
                    break;
                }

                if ( systemModuleNumber > 1 )
                {
                    if (arguments[1] != "")
                        password = arguments[1];
                    else
                        password = "ssh";

                    //
                    // perform start of columnstore of other servers in the system
                    //

                    DeviceNetworkList::iterator pt;
                    string modulename;

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                                pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end();
                                ++pt)
                        {
                            modulename = (*pt).DeviceName;

                            if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
                                    (*pt).DisableState == oam::AUTODISABLEDSTATE )
                            {
                                cout << "   Module '" << modulename << "' is disabled and will not be started" << endl;
                            }
                        }
                    }

                    cout << endl << "   System being started, please wait...";
                    cout.flush();
                    bool FAILED = false;

                    //hdfs / hadoop config
                    string DBRootStorageType;

                    try
                    {
                        oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
                    }
                    catch (...) {}

                    if ( DBRootStorageType == "hdfs")
                    {
						string logFile = tmpDir + "/cc-restart.pdsh";
                        cmd = "pdsh -a 'columnstore start' > " + logFile + " 2>&1";
                        system(cmd.c_str());

                        if (oam.checkLogStatus(logFile, "exit") )
                        {
                            cout << endl << "ERROR: Restart MariaDB ColumnStore Service failure, check " << logFile << ". exit..." << endl;
                            break;
                        }
                    }
                    else
                    {
                        for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                        {
                            if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                                // end of list
                                break;

                            int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                            if ( moduleCount == 0 )
                                // skip if no modules
                                continue;

                            for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                                    pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end();
                                    ++pt)
                            {
                                modulename = (*pt).DeviceName;

                                if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
                                        (*pt).DisableState == oam::AUTODISABLEDSTATE )
                                {
                                    continue;
                                }

                                if ( modulename == localModule )
                                {
                                    cmd = "columnstore start > " + tmpDir + "/startSystem.log 2>&1";
                                    int rtnCode = system(cmd.c_str());

                                    if (geteuid() == 0 && WEXITSTATUS(rtnCode) != 0)
                                    {
                                        cout << endl << "error with running 'columnstore start' on local module " << endl;
                                        cout << endl << "**** startSystem Failed" << endl;
                                        break;
                                    }

                                    continue;
                                }

                                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                                {
                                    //run remote command script
                                    cmd = "remote_command.sh " + (*pt1).IPAddr + " " + password + " 'columnstore start' 0";
                                    int rtnCode = system(cmd.c_str());

                                    if (WEXITSTATUS(rtnCode) < 0)
                                    {
                                        cout << endl << "error with running 'columnstore start' on module " + modulename << endl;
                                        cout << endl << "**** startSystem Failed" << endl;

                                        // stop local columnstore service
                                        cmd = "columnstore stop > " + tmpDir + "/stop.log 2>&1";
                                        system(cmd.c_str());

                                        FAILED = true;
                                        break;
                                    }
                                    else
                                    {
                                        if (rtnCode > 0)
                                        {
                                            cout << endl << "Invalid Password when running 'columnstore start' on module " + modulename << ", can retry by providing password as the second argument" << endl;
                                            cout << endl << "**** startSystem Failed" << endl;

                                            // stop local columnstore service
                                            cmd = "columnstore stop > " + tmpDir + "/stop.log 2>&1";
                                            system(cmd.c_str());

                                            FAILED = true;
                                            break;
                                        }
                                    }
                                }

                                if (FAILED)
                                    break;
                            }
                        }

                        if (FAILED)
                            break;
                    }

                    if (FAILED)
                        break;
                }
                else
                {
                    //just kick off local server
                    cout << endl << "   System being started, please wait...";
                    cout.flush();
                    cmd = "columnstore start > " + tmpDir + "/startSystem.log 2>&1";
                    int rtnCode = system(cmd.c_str());

                    if (geteuid() == 0 && WEXITSTATUS(rtnCode) != 0)
                    {
                        cout << endl << "error with running 'columnstore restart' on local module " << endl;
                        cout << endl << "**** startSystem Failed" << endl;
                        break;
                    }
                }

                if ( waitForActive() )
                    cout << endl << "   Successful start of System " << endl << endl;
                else
                    cout << endl << "**** startSystem Failed : check log files" << endl;
            }
            else
            {
                getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

                try
                {
                    cout << endl << "   System being started, please wait...";
                    cout.flush();
                    oam.startSystem(ackTemp);

                    if ( waitForActive() )
                        cout << endl << "   Successful start of System " << endl << endl;
                    else
                        cout << endl << "**** startSystem Failed : check log files" << endl;
                }
                catch (exception& e)
                {
                    cout << endl << "**** startSystem Failed :  " << e.what() << endl;
                    string Failed = e.what();

                    if (Failed.find("Database Test Error") != string::npos)
                        cout << "Database Test Error occurred, check Alarm and Logs for addition Information" << endl;
                }
            }
        }
        break;

        case 19: // restartSystem - parameters: graceful flag, Ack flag
        {
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm, &password);

            //don't start if a disable module has a dbroot assigned to it
            if (!checkForDisabledModules())
            {
                cout << endl << "Error: restartSystem command can't be performed: disabled module has a dbroot assigned to it" << endl;
                break;
            }

            // if columnstore service is down, then start system by starting all of the columnstore services
            // this would be used after a shutdownSystem command
            // if columnstore service is up, send message to ProcMgr to start system (which starts all processes)

            if (!oam.checkSystemRunning())
            {
                if (bNeedsConfirm)
                {
                    if (confirmPrompt("")) // returns true if user wants to quit.
                        break;
                }

                cout << "restartSystem command, 'columnstore' service is down, sending command to" << endl;
                cout << "start the 'columnstore' service on all modules" << endl << endl;

                SystemModuleTypeConfig systemmoduletypeconfig;
                ModuleTypeConfig moduletypeconfig;
                ModuleConfig moduleconfig;
                systemmoduletypeconfig.moduletypeconfig.clear();
                int systemModuleNumber = 0;

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

                        systemModuleNumber = systemModuleNumber + systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** restartSystem Failed =  " << e.what() << endl;
                    break;
                }

                if ( systemModuleNumber > 1 )
                {
                    if (password.empty())
                        password = "ssh";

                    //
                    // perform start of columnstore of other servers in the system
                    //

                    DeviceNetworkList::iterator pt;
                    string modulename;

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                                pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end();
                                ++pt)
                        {
                            modulename = (*pt).DeviceName;

                            if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
                                    (*pt).DisableState == oam::AUTODISABLEDSTATE )
                            {
                                cout << "   Module '" << modulename << "' is disabled and will not be started" << endl;
                            }
                        }
                    }

                    cout << endl << "   System being started, please wait...";
                    cout.flush();
                    bool FAILED = false;

                    //hdfs / hadoop config
                    string DBRootStorageType;

                    try
                    {
                        oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
                    }
                    catch (...) {}

                    if ( DBRootStorageType == "hdfs")
                    {
						string logFile = tmpDir + "/cc-restart.pdsh";
                        cmd = "pdsh -a 'columnstore restart' > " + logFile + " 2>&1";
                        system(cmd.c_str());

                        if (oam.checkLogStatus(logFile, "exit") )
                        {
                            cout << endl << "ERROR: Restart MariaDB ColumnStore Service failue, check " << logFile << ". exit..." << endl;
                            break;
                        }
                    }
                    else
                    {
                        for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                        {
                            if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                                // end of list
                                break;

                            int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                            if ( moduleCount == 0 )
                                // skip if no modules
                                continue;

                            for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                                    pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end();
                                    ++pt)
                            {
                                modulename = (*pt).DeviceName;

                                if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
                                        (*pt).DisableState == oam::AUTODISABLEDSTATE )
                                {
                                    continue;
                                }

                                if ( modulename == localModule )
                                    continue; // do last

                                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                                {
                                    //run remote command script
                                    cmd = "remote_command.sh " + (*pt1).IPAddr + " " + password + " 'columnstore restart' 0";

                                    int rtnCode = system(cmd.c_str());

                                    if (WEXITSTATUS(rtnCode) < 0)
                                    {
                                        cout << endl << "error with running 'columnstore start' on module " + modulename << endl;
                                        cout << endl << "**** restartSystem Failed" << endl;

                                        // stop local columnstore service
                                        cmd = "columnstore stop > " + tmpDir + "/stop.log 2>&1";
                                        system(cmd.c_str());

                                        FAILED = true;
                                        break;
                                    }
                                    else
                                    {
                                        if (rtnCode > 0)
                                        {
                                            cout << endl << "Invalid Password when running 'columnstore start' on module " + modulename << ", can retry by providing password as the second argument" << endl;
                                            cout << endl << "**** restartSystem Failed" << endl;
                                            FAILED = true;

                                            // stop local columnstore service
                                            cmd = "columnstore stop > " + tmpDir + "/stop.log 2>&1";
                                            system(cmd.c_str());

                                            break;
                                        }
                                    }
                                }

                                if (FAILED)
                                    break;
                            }

                            if (FAILED)
                                break;

                            //RESTART LOCAL HOST
                            cmd = "columnstore restart > " + tmpDir + "/start.log 2>&1";
                            int rtnCode = system(cmd.c_str());

                            if (geteuid() == 0 && WEXITSTATUS(rtnCode) != 0)
                            {
                                cout << endl << "error with running 'columnstore restart' on local module " << endl;
                                cout << endl << "**** restartSystem Failed" << endl;
                                break;
                            }
                        }

                        if (FAILED)
                            break;
                    }
                }
                else
                {
                    //just kick off local server
                    cout << "   System being restarted, please wait...";
                    cout.flush();
                    cmd = "columnstore restart > " + tmpDir + "/start.log 2>&1";
                    int rtnCode = system(cmd.c_str());

                    if (WEXITSTATUS(rtnCode) != 0)
                    {
                        cout << endl << "error with running 'columnstore start' on local module " << endl;
                        cout << endl << "**** restartSystem Failed" << endl;
                        break;
                    }
                }

                if ( waitForActive() )
                    cout << endl << "   Successful restart of System " << endl << endl;
                else
                    cout << endl << "**** restartSystem Failed : check log files" << endl;
            }
            else
            {
                BRM::DBRM dbrm;
                bool bDBRMReady = dbrm.isDBRMReady();

                try
                {
                    if (gracefulTemp != GRACEFUL ||
                            !bDBRMReady ||
                            dbrm.isReadWrite())
                    {
                        suspendAnswer = FORCE;
                    }

                    if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                    {
                        // If there are bulkloads, ddl or dml happening, Ask what to do.
                        bool bIsDbrmUp = true;
                        execplan::SessionManager sessionManager;
                        BRM::SIDTIDEntry blockingsid;
                        std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                        bool bActiveTransactions = false;

                        if (!tableLocks.empty())
                        {
                            oam.DisplayLockedTables(tableLocks, &dbrm);
                            bActiveTransactions = true;
                        }

                        if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                        {
                            cout << endl << "There are active transactions being processed" << endl;
                            bActiveTransactions = true;
                        }

                        if (bActiveTransactions)
                        {
                            suspendAnswer = AskSuspendQuestion(CmdID);
                            //						if (suspendAnswer == FORCE)
                            //						{
                            //							if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances"))
                            //							{
                            //								break;
                            //							}
                            //						}
                            bNeedsConfirm = false;
                        }
                        else
                        {
                            suspendAnswer = FORCE;
                        }
                    }

                    if (suspendAnswer == CANCEL)
                    {
                        // We're outa here.
                        break;
                    }

                    if (bNeedsConfirm)
                    {
                        if (confirmPrompt(""))
                            break;
                    }

                    switch (suspendAnswer)
                    {
                        case WAIT:
                            cout << endl << "   Waiting for all transactions to complete" << flush;
                            dbrm.setSystemShutdownPending(true, false, false);
                            gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                            break;

                        case ROLLBACK:
                            cout << endl << "   Rollback of all transactions" << flush;
                            dbrm.setSystemShutdownPending(true, true, false);
                            gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                            break;

                        case FORCE:
                            cout << endl << "   System being restarted now ..." << flush;

                            if (bDBRMReady)
                            {
                                dbrm.setSystemShutdownPending(true, false, true);
                            }

                            break;

                        case CANCEL:
                            break;
                    }

                    int returnStatus = oam.restartSystem(gracefulTemp, ackTemp);

                    switch (returnStatus)
                    {
                        case API_SUCCESS:
                            if ( waitForActive() )
                                cout << endl << "   Successful restart of System " << endl << endl;
                            else
                                cout << endl << "**** restartSystem Failed : check log files" << endl;

                            break;

                        case API_CANCELLED:
                            cout << endl << "   Restart of System canceled" << endl << endl;
                            break;

                        default:
                            cout << endl << "**** restartSystem Failed : Check system logs" << endl;
                            break;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** restartSystem Failed :  " << e.what() << endl;
                    string Failed = e.what();

                    if (Failed.find("Database Test Error") != string::npos)
                        cout << "Database Test Error occurred, check Alarm and Logs for additional Information" << endl;
                }
            }
        }
        break;

        case 20: // getSystemStatus - parameters: NONE
        {
            try
            {
                printSystemStatus();
            }
            catch (...)
            {
                break;
            }

        }
        break;

        case 21: // getProcessStatus - parameters: NONE
        {
            try
            {
                printProcessStatus();
            }
            catch (...)
            {
                break;
            }
        }
        break;

        case 22: // system - UNIX system command
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** system Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            for (int j = 2; j < ArgNum; j++)
            {
                arguments[1].append(" ");
                arguments[1].append(arguments[j]);
            }

            system (arguments[1].c_str());
        }
        break;

        case 23: // getAlarmHistory
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** getAlarmHistory Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            if ( arguments[1].size() != 8 )
            {
                cout << "date not in correct format, enter MM/DD/YY" << endl;
                break;
            }

            if ( !(arguments[1].substr(2, 1) == "/" && arguments[1].substr(5, 1) == "/") )
            {
                cout << "date not in correct format, enter MM/DD/YY" << endl;
                break;
            }

            AlarmList alarmList;

            try
            {
                oam.getAlarms(arguments[1], alarmList);
            }
            catch (exception& e)
            {
                cout << endl << "**** getAlarms Failed =  " << e.what() << endl;
                break;
            }

            cout << endl << "Historical Alarm List for " + arguments[1] + " :" << endl << endl;

            AlarmList :: iterator i;
            int counter = 0;

            for (i = alarmList.begin(); i != alarmList.end(); ++i)
            {
                // SET = 1, CLEAR = 0
                if (i->second.getState() == true)
                {
                    cout << "SET" << endl;
                }
                else
                {
                    cout << "CLEAR" << endl;
                }

                cout << "AlarmID           = " << i->second.getAlarmID() << endl;
                cout << "Brief Description = " << i->second.getDesc() << endl;
                cout << "Alarm Severity    = ";

                switch (i->second.getSeverity())
                {
                    case CRITICAL:
                        cout << "CRITICAL" << endl;
                        break;

                    case MAJOR:
                        cout << "MAJOR" << endl;
                        break;

                    case MINOR:
                        cout << "MINOR" << endl;
                        break;

                    case WARNING:
                        cout << "WARNING" << endl;
                        break;

                    case INFORMATIONAL:
                        cout << "INFORMATIONAL" << endl;
                        break;
                }

                cout << "Time Issued       = " << i->second.getTimestamp() << endl;
                cout << "Reporting Module  = " << i->second.getSname() << endl;
                cout << "Reporting Process = " << i->second.getPname() << endl;
                cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;

                counter++;

                if ( counter > 4 )
                {
                    // continue prompt
                    if (confirmPrompt("Displaying Alarm History"))
                        break;

                    counter = 0;
                }
            }
        }
        break;

        case 24: // monitorAlarms
        {
            cout << endl << "Monitor for System Alarms" << endl;
            cout << " Enter control-C to return to command line" << endl << endl;

            cmd = "tail -n 0 -f " + alarmmanager::ALARM_FILE;
            system(cmd.c_str());
        }
        break;

        case 25: // resetAlarm
        {
            if (arguments[1] == "")
            {
                // need 3 arguments
                cout << endl << "**** resetAlarm Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            try
            {
                // check if requested alarm is Active
                AlarmList alarmList;
                Oam oam;

                try
                {
                    oam.getActiveAlarms(alarmList);
                }
                catch (exception& e)
                {
                    cout << endl << "**** getActiveAlarm Failed =  " << e.what() << endl;
                    break;
                }

                bool found = false;
                AlarmList::iterator i;

                for (i = alarmList.begin(); i != alarmList.end(); ++i)
                {
                    // check if matching ID
                    if ( arguments[1] != "ALL" )
                    {
                        if (atoi(arguments[1].c_str()) != (i->second).getAlarmID() )
                            continue;

                        if ( arguments[2] != "ALL")
                        {
                            if (arguments[2].compare((i->second).getSname()) != 0)
                                continue;

                            if ( arguments[3] != "ALL")
                            {
                                if (arguments[3].compare((i->second).getComponentID()) != 0 )
                                    continue;
                            }
                        }
                    }

                    ALARMManager aManager;
                    aManager.sendAlarmReport((i->second).getComponentID().c_str(),
                                             (i->second).getAlarmID(),
                                             CLEAR,
                                             (i->second).getSname(),
                                             "mcsadmin");

                    cout << endl << "   Alarm Successfully Reset: ";
                    cout << "ID = " << oam.itoa((i->second).getAlarmID());
                    cout << " / Module = " << (i->second).getSname();
                    cout << " / Device = " << (i->second).getComponentID() << endl;
                    found = true;
                }

                // check is a SET alarm was found, if not return
                if (!found)
                {
                    cout << endl << "**** resetAlarm Failed : Requested Alarm is not Set" << endl;
                    break;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** resetAlarm Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 26: // enableLog
        {
            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** Failed : enableLog Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            // covert second argument (level) into lowercase
            transform (arguments[2].begin(), arguments[2].end(), arguments[2].begin(), to_lower());

            try
            {
                oam.updateLog(ENABLEDSTATE, arguments[1], arguments[2]);
                cout << endl << "   Successful Enabling of Logging " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** enableLog Failed :  " << e.what() << endl;
            }
        }
        break;

        case 27: // disableLog
        {
            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** disableLog Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            // covert second argument (level) into lowercase
            transform (arguments[2].begin(), arguments[2].end(), arguments[2].begin(), to_lower());

            try
            {
                oam.updateLog(MANDISABLEDSTATE, arguments[1], arguments[2]);
                cout << endl << "   Successful Disabling of Logging " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** disableLog Failed :  " << e.what() << endl;
            }
        }
        break;

        case 28: // switchParentOAMModule
        {
            BRM::DBRM dbrm;
            bool bDBRMReady = dbrm.isDBRMReady();
            string module;
            bool bUseHotStandby = true;
            SystemStatus systemstatus;
            Oam oam;

            //first check that the system is in a ACTIVE OR MAN_OFFLINE STATE
            try
            {
                oam.getSystemStatus(systemstatus);

                if (systemstatus.SystemOpState == ACTIVE ||
                        systemstatus.SystemOpState == MAN_OFFLINE)
                {
                    module = "";
                }
                else
                {
                    cout << endl << "**** switchParentOAMModule Failed : System Status needs to be ACTIVE or MAN_OFFLINE" << endl;
                    break;
                }
            }
            catch (...)
            {}

            // First get the values for the standard arguments
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            // Now check for arguments unique to this command. In this case, a valid
            // module name.
            for (int i = 1; i < ArgNum; i++)
            {
                if (arguments[i].size() > 0)
                {
                    if (oam.validateModule(arguments[i]) == API_SUCCESS)
                    {
                        module = arguments[i];
                        bUseHotStandby = false;
                        break;
                    }
                }
            }

            //check if there are more than 1 pm modules to start with
            ModuleTypeConfig moduletypeconfig;
            oam.getSystemConfig("pm", moduletypeconfig);

            if ( moduletypeconfig.ModuleCount < 2 )
            {
                cout << endl << "**** switchParentOAMModule Failed : Command only support on systems with Multiple Performance Modules" << endl;
//				break;
            }

            string DBRootStorageType;

            try
            {
                oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
            }
            catch (...) {}

            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DBRootStorageType == "internal" && DataRedundancyConfig == "n")
            {
                cout << endl << "**** switchParentOAMModule Failed : DBRoot Storage type =  internal/non-data-replication" << endl;
                break;
            }

            string ParentOAMModuleName;

            try
            {
                oam.getSystemConfig("ParentOAMModuleName", ParentOAMModuleName);
            }
            catch (...) {}

            if (bUseHotStandby)
            {
                oam.getSystemConfig("StandbyOAMModuleName", module);

                if ( module.empty() || module == oam::UnassignedName )
                {
                    cout << endl << "**** switchParentOAMModule Failed : There's no hot standby defined" << endl << "     enter a Performance Module" << endl;
                    break;
                }

                cout << endl << "Switching to the Hot-Standby Parent OAM Module '" << module << "'" << endl;
            }
            else
            {
                parentOAMModule = getParentOAMModule();

                if ( module == parentOAMModule )
                {
                    cout << endl << "**** switchParentOAMModule Failed : " << module << " is already the Active Parent OAM Module" << endl;
                    break;
                }

                cout << endl << "Switching to the Performance Module '" << module << "'" << endl;
            }

            //check for gluster system is do-able
            if (DataRedundancyConfig == "y")
            {
                // get to-module assigned DBRoots and see if current active PM
                // has a copy

                DBRootConfigList toPMbrootConfigList;

                try
                {
                    string moduleID = module.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);
                    oam.getPmDbrootConfig(atoi(moduleID.c_str()), toPMbrootConfigList);

                    bool match = false;
                    DBRootConfigList::iterator pt = toPMbrootConfigList.begin();

                    for ( ; pt != toPMbrootConfigList.end() ; pt++)
                    {
                        // check if ACTIVE PM has a copy of Dbroot
                        string pmList = "";

                        try
                        {
                            string errmsg;
                            int ret = oam.glusterctl(oam::GLUSTER_WHOHAS, oam.itoa(*pt), pmList, errmsg);

                            if ( ret != 0 )
                            {
                                cout << endl << "**** switchParentOAMModule Failed : " << module << " glusterctl error" << endl;
                                break;
                            }
                        }
                        catch (...)
                        {
                            cout << endl << "**** switchParentOAMModule Failed : " << module << " glusterctl error" << endl;
                            break;
                        }

                        boost::char_separator<char> sep(" ");
                        boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                        for ( boost::tokenizer< boost::char_separator<char> >::iterator it1 = tokens.begin();
                                it1 != tokens.end();
                                ++it1)
                        {
                            string pmModule = "pm" + *it1;

                            if ( pmModule == ParentOAMModuleName )
                            {
                                match = true;
                                break;
                            }
                        }
                    }

                    if (!match)
                    {
                        cout << endl << "**** switchParentOAMModule Failed : The Current Active PM doesn't have a copy of any DBROOTs that reside on the Siwtching PM " << endl;
                        break;
                    }

                    //check if switching to PM has DBROOT 1
                    string pmList = "";

                    try
                    {
                        string errmsg;
                        int ret = oam.glusterctl(oam::GLUSTER_WHOHAS, "1", pmList, errmsg);

                        if ( ret != 0 )
                        {
                            cout << endl << "**** switchParentOAMModule Failed : " << module << " glusterctl error" << endl;
                            break;
                        }
                    }
                    catch (...)
                    {
                        cout << endl << "**** switchParentOAMModule Failed : " << module << " glusterctl error" << endl;
                        break;
                    }

                    match = false;
                    boost::char_separator<char> sep(" ");
                    boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                    for ( boost::tokenizer< boost::char_separator<char> >::iterator it1 = tokens.begin();
                            it1 != tokens.end();
                            ++it1)
                    {
                        string pmModule = "pm" + *it1;

                        if ( pmModule == module )
                        {
                            match = true;
                            break;
                        }
                    }

                    if (!match)
                    {
                        cout << endl << "**** switchParentOAMModule Failed : The Switching to PM doesn't have a copy of the DBROOT #1" << endl;
                        break;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getPmDbrootConfig Failed for '" << module << "' : " << e.what() << endl;
                    break;
                }
            }


            if (bNeedsConfirm)
            {
                // confirm request
                if (confirmPrompt("This command switches the Active Parent OAM Module and should only be executed on an idle system."))
                    break;
            }

            string MySQLRep;

            try
            {
                oam.getSystemConfig("MySQLRep", MySQLRep);
            }
            catch (...) {}

            try
            {
                cout << endl << "   Check for active transactions" << endl;

                if (!bDBRMReady ||
                        dbrm.isReadWrite() != 0)
                {
                    suspendAnswer = FORCE;
                }

                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager;
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;

                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }

                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
                        //					if (suspendAnswer == FORCE)
                        //					{
                        //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances"))
                        //						{
                        //							break;
                        //						}
                        //					}
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }

                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }

                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemShutdownPending(true, false, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemShutdownPending(true, true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case FORCE:
                        cout << endl << "   Switch Active Parent OAM Module starting..." << endl;

                        if (bDBRMReady)
                        {
                            dbrm.setSystemShutdownPending(true, false, true);
                        }

                        break;

                    case CANCEL:
                        break;
                }

                if (oam.switchParentOAMModule(module, gracefulTemp))
                {
                    if (waitForActive())
                    {
                        // give time for new ProcMgr to go active
                        sleep (10);
                        cout << endl << "   Successful Switch Active Parent OAM Module" << endl << endl;
                    }
                    else
                        cout << endl << "**** Switch Active Parent OAM Module failed : check log files" << endl;
                }
                else
                {
                    // give time for new ProcMgr to go active
                    sleep (10);
                    cout << endl << "   Successful Switch Active Parent OAM Module" << endl << endl;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** switchParentOAMModule Failed :  " << e.what() << endl;
                break;
            }
        }
        break;

        case 29: // getStorageStatus
        {
            SystemStatus systemstatus;
            Oam oam;

            cout << "System External DBRoot Storage Statuses" << endl << endl;
            cout << "Component     Status                       Last Status Change" << endl;
            cout << "------------  --------------------------   ------------------------" << endl;

            try
            {
                oam.getSystemStatus(systemstatus, false);

                if ( systemstatus.systemdbrootstatus.dbrootstatus.size() == 0 )
                {
                    cout << " No External DBRoot Storage Configured" << endl;
                    break;
                }

                for ( unsigned int i = 0 ; i < systemstatus.systemdbrootstatus.dbrootstatus.size(); i++)
                {
                    if ( systemstatus.systemdbrootstatus.dbrootstatus[i].Name.empty() )
                        // end of list
                        break;

                    cout << "DBRoot #";
                    cout.setf(ios::left);
                    cout.width(6);
                    cout << systemstatus.systemdbrootstatus.dbrootstatus[i].Name;
                    cout.width(29);
                    int state = systemstatus.systemdbrootstatus.dbrootstatus[i].OpState;
                    printState(state, " ");
                    cout.width(24);
                    string stime = systemstatus.systemdbrootstatus.dbrootstatus[i].StateChangeDate ;
                    stime = stime.substr (0, 24);
                    cout << stime << endl;
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemStatus Failed =  " << e.what() << endl;
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
            catch (...) {}

            if ( DataRedundancyConfig == "y" )
            {
                string arg1 = "";
                string arg2 = "";
                string errmsg = "";
                int ret = oam.glusterctl(oam::GLUSTER_STATUS, arg1, arg2, errmsg);

                if ( ret == 0 )
                {
                    cout << arg2 << endl;
                }
                else
                {
                    cerr << "FAILURE: Status check error: " + errmsg << endl;
                }
            }
        }
        break;

        case 30: // getLogConfig
        {
            try
            {
                SystemLogConfigData systemconfigdata;
                LogConfigData logconfigdata;

                oam.getLogConfig(systemconfigdata);

                string configFileName;
                oam.getSystemConfig("SystemLogConfigFile", configFileName);

                cout << endl << "MariaDB ColumnStore System Log Configuration Data" << endl << endl;

                cout << "System Logging Configuration File being used: " <<  configFileName << endl << endl;

                cout << "Module    Configured Log Levels" << endl;
                cout << "------    ---------------------------------------" << endl;

                SystemLogConfigData::iterator pt = systemconfigdata.begin();

                for (; pt != systemconfigdata.end() ; pt++)
                {
                    logconfigdata = *pt;
                    string module = logconfigdata.moduleName;
                    int data = logconfigdata.configData;

                    if ( data < API_MAX )
                    {
                        // failure API status returned
                        cout.setf(ios::left);
                        cout.width(10);
                        cout << logconfigdata.moduleName;
                        cout << "getLogConfig Failed - Error : " << data << endl;
                    }
                    else
                    {
                        cout.setf(ios::left);
                        cout.width(10);
                        cout << logconfigdata.moduleName;

                        data = data - API_MAX;

                        if ( data == 0 )
                            // no level configured
                            cout << "None Configured" << endl;
                        else
                        {
                            if ( ((data & LEVEL_CRITICAL) ? 1 : 0) == 1 )
                                cout << "Critical ";

                            if ( ((data & LEVEL_ERROR) ? 1 : 0) == 1 )
                                cout << "Error ";

                            if ( ((data & LEVEL_WARNING) ? 1 : 0) == 1 )
                                cout << "Warning ";

                            if ( ((data & LEVEL_INFO) ? 1 : 0) == 1 )
                                cout << "Info ";

                            if ( ((data & LEVEL_DEBUG) ? 1 : 0) == 1 )
                                cout << "Debug ";

                            cout << endl;
                        }
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getLogConfig Failed :  " << e.what() << endl;
                break;
            }

        }
        break;

        case 31: // movePmDbrootConfig parameters: pm-reside dbroot-list pm-to
        {
            if ( localModule != parentOAMModule )
            {
                // exit out since not on active module
                cout << endl << "**** movePmDbrootConfig Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                break;
            }

            //check the system status / service status and only allow command when System is MAN_OFFLINE
            if (oam.checkSystemRunning())
            {
                SystemStatus systemstatus;

                try
                {
                    oam.getSystemStatus(systemstatus);

                    if (systemstatus.SystemOpState != oam::MAN_OFFLINE )
                    {
                        cout << endl << "**** movePmDbrootConfig Failed,  System has to be in a MAN_OFFLINE state, stop system first" << endl;
                        break;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** movePmDbrootConfig Failed : " << e.what() << endl;
                    break;
                }
                catch (...)
                {
                    cout << endl << "**** movePmDbrootConfig Failed,  Failed return from getSystemStatus API" << endl;
                    break;
                }
            }

            if (arguments[3] == "")
            {
                // need arguments
                cout << endl << "**** movePmDbrootConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            string residePM = arguments[1];
            string dbrootIDs = arguments[2];
            string toPM = arguments[3];

            string residePMID = residePM.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);;
            string toPMID = toPM.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);;

            // check module status
            try
            {
                bool degraded;
                int opState;
                oam.getModuleStatus(toPM, opState, degraded);

                if (opState == oam::AUTO_DISABLED ||
                        opState == oam::MAN_DISABLED)
                {
                    cout << "**** movePmDbrootConfig Failed: " << toPM << " is DISABLED." << endl;
                    cout << "Run alterSystem-EnableModule to enable module" << endl;
                    break;
                }

                if (opState == oam::FAILED)
                {
                    cout << "**** movePmDbrootConfig Failed: " << toPM << " is in a FAILED state." << endl;
                    break;
                }
            }
            catch (exception& ex)
            {}

            bool moveDBRoot1 = false;
            bool found = false;
            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                if (*it == "1" )
                {
                    moveDBRoot1 = true;
                    break;
                }

                //if gluster, check if toPM is has a copy
                string DataRedundancyConfig;

                try
                {
                    oam.getSystemConfig("DataRedundancyConfig", DataRedundancyConfig);
                }
                catch (...) {}

                if ( DataRedundancyConfig == "y" )
                {
                    string pmList = "";

                    try
                    {
                        string errmsg;
                        oam.glusterctl(oam::GLUSTER_WHOHAS, *it, pmList, errmsg);
                    }
                    catch (...)
                    {}

                    boost::char_separator<char> sep(" ");
                    boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                    for ( boost::tokenizer< boost::char_separator<char> >::iterator it1 = tokens.begin();
                            it1 != tokens.end();
                            ++it1)
                    {
                        if ( *it1 == toPMID )
                        {
                            found = true;
                            break;
                        }
                    }

                    if (!found)
                    {
                        cout << endl << "**** movePmDbrootConfig Failed : Data Redundancy Configured, DBRoot #" << *it << " doesn't have a copy on " << toPM << endl;
                        cout << "Run getStorageConfig to get copy information" << endl << endl;
                        break;
                    }
                }
                else
                    found = true;
            }

            if (moveDBRoot1)
            {
                cout << endl << "**** movePmDbrootConfig Failed : Can't move dbroot #1" << endl << endl;
                break;
            }

            if (!found)
            {
                break;
            }


            if (residePM.find("pm") == string::npos )
            {
                cout << endl << "**** movePmDbrootConfig Failed : Parmameter 1 is not a Performance Module name, enter 'help' for additional information" << endl;
                break;
            }

            if (toPM.find("pm") == string::npos )
            {
                cout << endl << "**** movePmDbrootConfig Failed : Parmameter 3 is not a Performance Module name, enter 'help' for additional information" << endl;
                break;
            }

            if (residePM == toPM )
            {
                cout << endl << "**** movePmDbrootConfig Failed : Reside and To Performance Modules are the same" << endl;
                break;
            }

            //get dbroots ids for reside PM
            DBRootConfigList residedbrootConfigList;

            try
            {
                oam.getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

                cout << endl << "DBRoot IDs currently assigned to '" + residePM + "' = ";

                DBRootConfigList::iterator pt = residedbrootConfigList.begin();

                for ( ; pt != residedbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != residedbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << residePM << "' : " << e.what() << endl;
                break;
            }

            //get dbroots ids for reside PM
            DBRootConfigList todbrootConfigList;

            try
            {
                oam.getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

                cout << "DBRoot IDs currently assigned to '" + toPM + "' = ";

                DBRootConfigList::iterator pt = todbrootConfigList.begin();

                for ( ; pt != todbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != todbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
                break;
            }

            cout << endl << "DBroot IDs being moved, please wait..." << endl << endl;

            try
            {
                oam.manualMovePmDbroot(residePM, dbrootIDs, toPM);
            }
            catch (...)
            {
                cout << endl << "**** manualMovePmDbroot Failed : API Failure" << endl;
                break;
            }

            //get dbroots ids for reside PM
            try
            {
                residedbrootConfigList.clear();
                oam.getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

                cout << "DBRoot IDs newly assigned to '" + residePM + "' = ";

                DBRootConfigList::iterator pt = residedbrootConfigList.begin();

                for ( ; pt != residedbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != residedbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
                break;
            }

            try
            {
                todbrootConfigList.clear();
                oam.getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

                cout << "DBRoot IDs newly assigned to '" + toPM + "' = ";

                DBRootConfigList::iterator pt = todbrootConfigList.begin();

                for ( ; pt != todbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != todbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
                break;
            }

        }
        break;

        case 32: // suspendDatabaseWrites
        {
            BRM::DBRM dbrm;
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            cout << endl << "This command suspends the DDL/DML writes to the MariaDB ColumnStore Database" << endl;

            try
            {

                if (!dbrm.isDBRMReady())
                {
                    cout << endl << "   The Controller Node is not responding.\n   The system can't be set into write suspend mode" << endl <<  flush;
                    break;
                }
                else if (dbrm.isReadWrite() != 0)
                {
                    suspendAnswer = FORCE;
                }

                // If there are bulkloads, ddl or dml happening, refuse the request
                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager;
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;

                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }

                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
                        //					if (suspendAnswer == FORCE)
                        //					{
                        //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances"))
                        //						{
                        //							break;
                        //						}
                        //					}
                        bNeedsConfirm = false;
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }

                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }

                if (bNeedsConfirm)
                {
                    if (confirmPrompt(""))
                        break;
                }

                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemSuspendPending(true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemSuspendPending(true, true);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;

                    case FORCE:
                    case CANCEL:
                    default:
                        gracefulTemp = FORCEFUL;
                        break;
                }

                // stop writes to MariaDB ColumnStore Database
                oam.SuspendWrites(gracefulTemp, ackTemp);
            }
            catch (exception& e)
            {
                cout << endl << "**** stopDatabaseWrites Failed: " << e.what() << endl;
            }
            catch (...)
            {
                cout << endl << "**** stopDatabaseWrites Failed" << endl;
                break;
            }

            break;
        }

        case 33: // resumeDatabaseWrites
        {
            if ( arguments[1] != "y" )
            {
                if (confirmPrompt("This command resumes the DDL/DML writes to the MariaDB ColumnStore Database"))
                    break;
            }

            // resume writes to MariaDB ColumnStore Database

            try
            {
                BRM::DBRM dbrm;

                dbrm.setSystemSuspended(false);





                oam.setSystemStatus(ACTIVE);
                cout << endl << "Resume MariaDB ColumnStore Database Writes Request successfully completed" << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** resumeDatabaseWrites Failed: " << e.what() << endl;
            }
            catch (...)
            {
                cout << endl << "**** resumeDatabaseWrites Failed" << endl;
                break;
            }

            break;
        }

        case 34: // unassignDbrootPmConfig parameters: dbroot-list reside-pm
        {
            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DataRedundancyConfig == "y")
            {
                cout << endl << "**** unassignDbrootPmConfig : command not supported on Data Redundancy configured system. " << endl;
                break;
            }

            if ( localModule != parentOAMModule )
            {
                // exit out since not on active module
                cout << endl << "**** unassignDbrootPmConfig Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                break;
            }


            if (arguments[2] == "")
            {
                // need atleast 2 arguments
                cout << endl << "**** unassignDbrootPmConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            string dbrootIDs = arguments[1];
            string residePM = arguments[2];

            if (arguments[2].find("pm") == string::npos )
            {
                cout << endl << "**** unassignDbrootPmConfig Failed : Parmameter 2 is not a Performance Module name, enter 'help' for additional information" << endl;
                break;
            }

            // check module status
            try
            {
                bool degraded;
                int opState;
                oam.getModuleStatus(residePM, opState, degraded);

                if (opState != oam::MAN_OFFLINE)
                {
                    cout << endl << "**** unassignDbrootPmConfig Failed, " + residePM + " has to be in a MAN_OFFLINE state" << endl;
                    break;
                }

            }
            catch (exception& ex)
            {}

            DBRootConfigList dbrootlist;

            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                dbrootlist.push_back(atoi((*it).c_str()));
            }

            cout << endl;

            //get dbroots ids for reside PM
            try
            {
                oam.unassignDbroot(residePM, dbrootlist);

                cout << endl << "   Successfully Unassigned DBRoots " << endl << endl;

            }
            catch (exception& e)
            {
                cout << endl << "**** Failed Unassign of DBRoots: " << e.what() << endl;
                break;
            }
        }
        break;

        case 35: // assignDbrootPmConfig parameters: pm dbroot-list
        {
            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DataRedundancyConfig == "y")
            {
                cout << endl << "**** assignDbrootPmConfig : command not supported on Data Redundancy configured system. " << endl;
                break;
            }

            if ( localModule != parentOAMModule )
            {
                // exit out since not on active module
                cout << endl << "**** assignDbrootPmConfig Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                break;
            }

            //check the system status / service status and only allow command when System is MAN_OFFLINE
            if (!oam.checkSystemRunning())
            {
                cout << endl << "**** assignDbrootPmConfig Failed,  System is down. Needs to be running" << endl;
                break;
            }

            if (arguments[2] == "")
            {
                // need atleast 2 arguments
                cout << endl << "**** assignDbrootPmConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            string dbrootIDs = arguments[1];
            string toPM = arguments[2];

            if (arguments[2].find("pm") == string::npos )
            {
                cout << endl << "**** assignDbrootPmConfig Failed : Parmameter 2 is not a Performance Module name, enter 'help' for additional information" << endl;
                break;
            }

            // check module status
            try
            {
                bool degraded;
                int opState;
                oam.getModuleStatus(toPM, opState, degraded);

                if (opState == oam::AUTO_DISABLED ||
                        opState == oam::MAN_DISABLED)
                {
                    cout << "**** assignDbrootPmConfig Failed: " << toPM << " is DISABLED." << endl;
                    cout << "Run alterSystem-EnableModule to enable module" << endl;
                    break;
                }

                if (!opState == oam::MAN_OFFLINE)
                {
                    cout << "**** assignDbrootPmConfig Failed: " << toPM << " needs to be MAN_OFFLINE." << endl;
                    break;
                }
            }
            catch (exception& ex)
            {}

            DBRootConfigList dbrootlist;

            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                dbrootlist.push_back(atoi((*it).c_str()));
            }

            cout << endl;

            //get dbroots ids for reside PM
            try
            {
                oam.assignDbroot(toPM, dbrootlist);

                cout << endl << "   Successfully Assigned DBRoots " << endl << endl;

                try
                {
                    string DBRootStorageType;
                    oam.getSystemConfig("DBRootStorageType", DBRootStorageType);

                    if (DBRootStorageType == "external" )
                    {
                        string DataRedundancyConfig = "n";
                        string cloud = oam::UnassignedName;

                        try
                        {
                            oam.getSystemConfig("Cloud", cloud);
                            oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
                        }
                        catch (...)
                        {}

                        if ( DataRedundancyConfig == "n" && cloud == oam::UnassignedName)
                            cout << "   REMINDER: Update the /etc/fstab on " << toPM << " to include these dbroot mounts" << endl << endl;

                        break;

                    }
                }
                catch (...) {}

            }
            catch (exception& e)
            {
                cout << endl << "**** Failed Assign of DBRoots: " << e.what() << endl;
                break;
            }
        }
        break;

        case 36: // getAlarmSummary
        {
            printAlarmSummary();
        }
        break;

        case 37: // getSystemInfo
        {
            try
            {
                printSystemStatus();
            }
            catch (...)
            {
                break;
            }

            try
            {
                printProcessStatus();
            }
            catch (...)
            {
                break;
            }

            printAlarmSummary();
        }
        break;

        case 38: // getModuleConfig
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            ModuleConfig moduleconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();
            string returnValue;
            string Argument;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Module Name config parameters

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl << "Module Name Configuration" << endl;

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

                        int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                        if ( moduleCount == 0 )
                            // skip if no modules
                            continue;

                        string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

                        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                        {
                            string modulename = (*pt).DeviceName;
                            string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);
                            cout << endl << "Module '" << modulename	<< "' Configuration information" << endl << endl;

                            cout << "ModuleType = " << moduletype << endl;
                            cout << "ModuleDesc = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc << " #" << moduleID << endl;

                            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                            {
                                cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
                                cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
                            }

                            DeviceDBRootList::iterator pt3 = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();

                            for ( ; pt3 != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt3++)
                            {
                                if ( (*pt3).DeviceID == atoi(moduleID.c_str()) )
                                {
                                    cout << "DBRootIDs assigned  = ";
                                    DBRootConfigList::iterator pt2 = (*pt3).dbrootConfigList.begin();

                                    for ( ; pt2 != (*pt3).dbrootConfigList.end() ;)
                                    {
                                        cout << oam.itoa(*pt2);
                                        pt2++;

                                        if (pt2 != (*pt3).dbrootConfigList.end() )
                                            cout << ", ";
                                    }

                                    cout << endl;
                                }
                            }
                        }
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
                }
            }
            else
            {
                // get a single module name info
                if (arguments[2] == "")
                {
                    try
                    {
                        oam.getSystemConfig(arguments[1], moduleconfig);

                        cout << endl << "Module Name Configuration for " << arguments[1] << endl << endl;

                        cout << "ModuleType = " << moduleconfig.ModuleType << endl;
                        cout << "ModuleDesc = " << moduleconfig.ModuleDesc << endl;
                        HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();

                        for ( ; pt1 != moduleconfig.hostConfigList.end() ; pt1++)
                        {
                            cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
                            cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
                        }

                        if ( moduleconfig.ModuleType == "pm" )
                        {

                            cout << "DBRootIDs assigned  = ";

                            DBRootConfigList::iterator pt2 = moduleconfig.dbrootConfigList.begin();

                            for ( ; pt2 != moduleconfig.dbrootConfigList.end() ; )
                            {
                                cout << oam.itoa(*pt2);
                                pt2++;

                                if (pt2 != moduleconfig.dbrootConfigList.end())
                                    cout << ", ";
                            }

                            cout << endl << endl;
                        }
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                {
                    // get a parameter for a module
                    // get module ID from module name entered, then get parameter
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl;
                    bool found = false;

                    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        string moduleType = arguments[1].substr(0, MAX_MODULE_TYPE_SIZE);
                        string moduleID = arguments[1].substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                        int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                        if ( moduleCount == 0 )
                            // skip if no modules
                            continue;

                        if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType == moduleType )
                        {
                            DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                            for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                            {
                                if ( (*pt).DeviceName != arguments[1] )
                                    continue;

                                found = true;

                                if ( arguments[2] == "ModuleIPAdd" || arguments[2] == "ModuleHostName")
                                {
                                    HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                                    for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                                    {
                                        if ( arguments[2] == "ModuleIPAdd" )
                                            cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
                                        else
                                            cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
                                    }
                                }
                                else
                                {
                                    Argument = arguments[2] + oam.itoa(i + 1);

                                    try
                                    {
                                        oam.getSystemConfig(Argument, returnValue);
                                        cout << endl << "   " << arguments[2] << " = " << returnValue << endl << endl;
                                        break;
                                    }
                                    catch (exception& e)
                                    {
                                        cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if ( !found )
                    {
                        // module name not found
                        cout << endl << "**** getModuleConfig Failed : Invalid Module Name" << endl;
                        break;
                    }

                    cout << endl;
                }
            }
        }
        break;

        case 39: // getSystemDirectories
        {
			cout << endl << "System Installation and Temporary File Directories" << endl << endl;

			cout << "System Temporary File Directory = " << tmpDir << endl << endl;
        }
        break;

        case 40:
        {
        }
        break;

        case 41:
        {
        }
        break;

        case 42:
        {
        }
        break;

        case 43: // assignElasticIPAddress
        {
            //get cloud configuration data
            string cloud = oam::UnassignedName;

            try
            {
                oam.getSystemConfig("Cloud", cloud);
            }
            catch (...) {}

            if ( cloud == oam::UnassignedName )
            {
                cout << endl << "**** assignElasticIPAddress Not Supported : For Amazon Systems only" << endl;
                break;
            }

            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** assignElasticIPAddress Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** assignElasticIPAddress Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            string IPaddress = arguments[1];
            string moduleName = arguments[2];

            if ( oam.validateModule(moduleName) != API_SUCCESS)
            {
                cout << endl << "**** assignElasticIPAddress Failed : Invalid Module name" << endl;
                break;
            }

            if ( moduleName == localModule )
            {
                if ( arguments[3] != "y")
                {
                    string warning = "Warning: Assigning Elastic IP Address to local module will lock up this terminal session.";

                    // confirm request
                    if (confirmPrompt(warning))
                        break;
                }
            }

            //check and add Elastic IP Address
            int AmazonElasticIPCount = 0;

            try
            {
                oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
            }
            catch (...)
            {
                AmazonElasticIPCount = 0;
            }

            bool found = false;
            int id = 1;

            for (  ; id < AmazonElasticIPCount + 1 ; id++ )
            {
                string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
                string ELmoduleName;
                string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
                string ELIPaddress;

                try
                {
                    oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
                    oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                }
                catch (...) {}

                if ( ELmoduleName == moduleName &&
                        ELIPaddress == IPaddress)
                {
                    //assign again incase it got unconnected
                    //get instance id
                    string instanceName = oam::UnassignedName;

                    try
                    {
                        ModuleConfig moduleconfig;
                        oam.getSystemConfig(moduleName, moduleconfig);
                        HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
                        instanceName = (*pt1).HostName;
                    }
                    catch (...)
                    {}

                    try
                    {
                        oam.assignElasticIP(instanceName, IPaddress);
                        cout << endl << "   Successfully completed Assigning Elastic IP Address " << endl << endl;
                    }
                    catch (...) {}

                    found = true;
                    break;
                }

                if ( ELmoduleName == moduleName )
                {
                    cout << endl << "**** assignElasticIPAddress Failed : module already assigned IP Address " << ELIPaddress << endl;
                    found = true;
                    break;
                }

                if ( ELIPaddress == IPaddress )
                {
                    cout << endl << "**** assignElasticIPAddress Failed : IP Address already assigned to module " << ELmoduleName << endl;
                    found = true;
                    break;
                }
            }

            if (found)
                break;

            AmazonElasticIPCount++;

            //get instance id
            string instanceName = oam::UnassignedName;

            try
            {
                ModuleConfig moduleconfig;
                oam.getSystemConfig(moduleName, moduleconfig);
                HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
                instanceName = (*pt1).HostName;
            }
            catch (...)
            {}

            try
            {
                oam.assignElasticIP(instanceName, IPaddress);
            }
            catch (...)
            {
                cout << endl << "**** assignElasticIPAddress Failed : assignElasticIP API Error" << endl;
                break;
            }

            //add to configuration
            string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
            string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);

            Config* sysConfig = Config::makeConfig();

            try
            {
                sysConfig->setConfig("Installation", "AmazonElasticIPCount", oam.itoa(AmazonElasticIPCount));
                sysConfig->setConfig("Installation", AmazonElasticModule, moduleName);
                sysConfig->setConfig("Installation", AmazonElasticIPAddr, IPaddress);
                sysConfig->write();
            }
            catch (...)
            {
                cout << "ERROR: Problem setting AmazonElasticModule in the MariaDB ColumnStore System Configuration file" << endl;
                break;
            }

            cout << endl << "   Successfully completed Assigning Elastic IP Address " << endl << endl;
        }
        break;

        case 44: // unassignElasticIPAddress
        {
            //get cloud configuration data
            string cloud = oam::UnassignedName;

            try
            {
                oam.getSystemConfig("Cloud", cloud);
            }
            catch (...) {}

            if ( cloud == oam::UnassignedName )
            {
                cout << endl << "**** unassignElasticIPAddress Not Supported : For Amazon Systems only" << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need 2 arguments
                cout << endl << "**** unassignElasticIPAddress Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** unassignElasticIPAddress Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            string IPaddress = arguments[1];

            //check and add Elastic IP Address
            int AmazonElasticIPCount = 0;

            try
            {
                oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
            }
            catch (...)
            {
                AmazonElasticIPCount = 0;
            }

            bool found = false;
            int id = 1;

            for (  ; id < AmazonElasticIPCount + 1 ; id++ )
            {
                string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
                string ELmoduleName;
                string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
                string ELIPaddress;

                try
                {
                    oam.getSystemConfig(AmazonElasticIPAddr, ELmoduleName);
                    oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                }
                catch (...) {}

                if ( ELIPaddress == IPaddress )
                {
                    found = true;

                    try
                    {
                        oam.deassignElasticIP(IPaddress);
                    }
                    catch (...)
                    {
                        cout << endl << "**** deassignElasticIPAddress Failed : deassignElasticIP API Error";
                        break;
                    }

                    int oldAmazonElasticIPCount = AmazonElasticIPCount;

                    Config* sysConfig = Config::makeConfig();

                    //move up any others
                    if ( oldAmazonElasticIPCount > id )
                    {
                        for ( int newid = id + 1 ; newid < oldAmazonElasticIPCount + 1 ; newid++ )
                        {
                            AmazonElasticModule = "AmazonElasticModule" + oam.itoa(newid);
                            AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(newid);

                            try
                            {
                                oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
                                oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                            }
                            catch (...) {}

                            AmazonElasticModule = "AmazonElasticModule" + oam.itoa(newid - 1);
                            AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(newid - 1);

                            try
                            {
                                oam.setSystemConfig(AmazonElasticModule, ELmoduleName);
                                oam.setSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                            }
                            catch (...) {}
                        }
                    }

                    AmazonElasticModule = "AmazonElasticModule" + oam.itoa(oldAmazonElasticIPCount);
                    AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(oldAmazonElasticIPCount);

                    //delete last entry and update count
                    AmazonElasticIPCount--;

                    try
                    {
                        sysConfig->setConfig("Installation", "AmazonElasticIPCount", oam.itoa(AmazonElasticIPCount));
                        sysConfig->delConfig("Installation", AmazonElasticModule);
                        sysConfig->delConfig("Installation", AmazonElasticIPAddr);
                        sysConfig->write();
                    }
                    catch (...)
                    {
                        cout << "ERROR: Problem setting AmazonElasticModule in the MariaDB ColumnStore System Configuration file" << endl;
                        break;
                    }
                }
            }

            if (!found)
            {
                cout << endl << "   Elastic IP Address " << IPaddress << " not assigned to a module" << endl << endl;
                break;
            }

            cout << endl << "   Successfully completed Unassigning Elastic IP Address " << endl << endl;

        }
        break;

        case 45: // getSystemNetworkConfig
        {
            // get and display Module Network Config
            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

            //check and add Elastic IP Address
            int AmazonElasticIPCount = 0;

            try
            {
                oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
            }
            catch (...)
            {
                AmazonElasticIPCount = 0;
            }

            // get max length of a host name for header formatting

            int maxSize = 9;

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);

                for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                        // end of list
                        break;

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                    string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

                    if ( moduleCount > 0 )
                    {
                        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                        {
                            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                            {
                                if ( maxSize < (int) (*pt1).HostName.size() )
                                    maxSize = (*pt1).HostName.size();
                            }
                        }
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemNetworkConfig Failed =  " << e.what() << endl;
            }

            cout << endl << "System Network Configuration" << endl << endl;

            cout.setf(ios::left);
            cout.width(15);
            cout << "Module Name";
            cout.width(30);
            cout << "Module Description";
            cout.width(10);
            cout << "NIC ID";
            cout.width(maxSize + 5);
            cout << "Host Name";
            cout.width(20);
            cout << "IP Address";
            cout.width(14);

            if ( AmazonElasticIPCount > 0 )
            {
                cout.width(20);
                cout << "Elastic IP Address";
            }

            cout << endl;
            cout.width(15);
            cout << "-----------";
            cout.width(30);
            cout << "-------------------------";
            cout.width(10);
            cout << "------";

            for ( int i = 0 ; i < maxSize ; i++ )
            {
                cout << "-";
            }

            cout << "     ";
            cout.width(20);
            cout << "---------------";

            if ( AmazonElasticIPCount > 0 )
            {
                cout.width(20);
                cout << "------------------";
            }

            cout << endl;

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);

                for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                        // end of list
                        break;

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                    string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

                    if ( moduleCount > 0 )
                    {
                        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                        {
                            string modulename = (*pt).DeviceName;
                            string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);
                            string modulenamedesc = moduletypedesc + " #" + moduleID;

                            cout.setf(ios::left);
                            cout.width(15);
                            cout << modulename;
                            cout.width(33);
                            cout << modulenamedesc;

                            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                            {
                                /* MCOL-1607.  IPAddr may be a host name here b/c it is read straight
                                from the config file. */
                                string tmphost = oam.getIPAddress(pt1->IPAddr);
                                string ipAddr;
                                if (tmphost.empty())
                                    ipAddr = pt1->IPAddr;
                                else
                                    ipAddr = tmphost;
                                string hostname = (*pt1).HostName;
                                string nicID = oam.itoa((*pt1).NicID);

                                if ( nicID != "1" )
                                {
                                    cout.width(48);
                                    cout << " ";
                                }

                                cout.width(7);
                                cout << nicID;
                                cout.width(maxSize + 5);
                                cout << hostname;
                                cout.width(20);
                                cout << ipAddr;
                                cout.width(14);

                                if ( nicID == "1" && AmazonElasticIPCount > 0 )
                                {
                                    int id = 1;

                                    for (  ; id < AmazonElasticIPCount + 1 ; id++ )
                                    {
                                        string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
                                        string ELmoduleName;
                                        string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
                                        string ELIPaddress;

                                        try
                                        {
                                            oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
                                            oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                                        }
                                        catch (...) {}

                                        if ( modulename == ELmoduleName )
                                        {
                                            cout.width(20);
                                            cout << ELIPaddress;
                                            break;
                                        }
                                    }
                                }

                                cout << endl;
                            }
                        }
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemNetworkConfig Failed =  " << e.what() << endl;
            }

            //get cloud configuration data
            string cloud = oam::UnassignedName;

            try
            {
                oam.getSystemConfig("Cloud", cloud);
            }
            catch (...) {}

            if ( cloud == "amazon-ec2" ||  cloud == "amazon-vpc" )
            {
                cout << endl << "Amazon Instance Configuration" << endl << endl;

                string PMInstanceType = oam::UnassignedName;
                string UMInstanceType = oam::UnassignedName;

                try
                {
                    oam.getSystemConfig("PMInstanceType", PMInstanceType);
                    oam.getSystemConfig("UMInstanceType", UMInstanceType);

                    cout << "PMInstanceType = " << PMInstanceType << endl;
                    cout << "UMInstanceType = " << UMInstanceType << endl;
                }
                catch (...) {}

            }

            cout << endl;

            break;
        }

        case 46: // enableReplication
        {
            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** enableReplication Failed : not supported on a Single-Server type installs  " << endl;
                break;
            }

            string MySQLRep;

            try
            {
                oam.getSystemConfig("MySQLRep", MySQLRep);
            }
            catch (...) {}

            if ( MySQLRep == "y" )
            {
                string warning = "MariaDB ColumnStore Replication Feature is already enabled";

                // confirm request
                if (confirmPrompt(warning))
                    break;
            }

            string password;

            if ( arguments[1] == "")
            {
                cout << endl;
                string prompt = "Enter the 'User' Password or 'ssh' if configured with ssh-keys";
                password = dataPrompt(prompt);
            }
            else
                password = arguments[1];

            if ( password == "")
                password = oam::UnassignedName;

            //set flag
            try
            {
                oam.setSystemConfig("MySQLRep", "y");
                sleep(2);
            }
            catch (...) {}

            try
            {
                oam.enableMySQLRep(password);
                cout << endl << "   Successful Enabling of MariaDB ColumnStore Replication " << endl << endl;

                //display Primary UM Module / Master Node
                string PrimaryUMModuleName;

                try
                {
                    oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
                }
                catch (...) {}

                cout << "   MariaDB ColumnStore Replication Master Node is " << PrimaryUMModuleName << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** enableRep Failed :  " << e.what() << endl;
            }

            break;
        }

        case 47: // getSoftwareInfo
        {
            cout << endl;

            if ( rootUser)
            {
				string logFile = tmpDir + "/columnstore.log";
				string cmd = "rpm -qi mariadb-columnstore-platform > " + logFile + " 2>&1";
                int rtnCode = system(cmd.c_str());

                if (WEXITSTATUS(rtnCode) == 0)
                {
					string cmd = "cat " + logFile;
                    system(cmd.c_str());
				}
                else
                {
					string cmd = "dpkg -s mariadb-columnstore-platform > " + logFile + " 2>&1";
                    rtnCode =  system(cmd.c_str());

                    if (WEXITSTATUS(rtnCode) == 0)
                    {
						string cmd = "cat " + logFile;
						system(cmd.c_str());
					}
                   else
                    {
                        cout << "SoftwareVersion = " << columnstore_version << endl;
                        cout << "SoftwareRelease = " << columnstore_release << endl;
                    }
                }
            }
            else
            {
                cout << "SoftwareVersion = " << columnstore_version << endl;
                cout << "SoftwareRelease = " << columnstore_release << endl;
            }

            cout << endl;
            break;
        }

        case 48: // addModule - parameters: Module type/Module Name, Number of Modules, Server Hostnames,
            // Server root password optional
        {
            Config* sysConfig = Config::makeConfig();

            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** addModule Failed : not support on a Single-Server type installs  " << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** addModule Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need at least  arguments
                cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            switch ( serverInstallType )
            {
                case (oam::INSTALL_COMBINE_DM_UM_PM):
                {
                    if (arguments[1].find("um") != string::npos )
                    {
                        cout << endl << "**** addModule Failed : User Module Types not supported on this Combined Server Installation" << endl;
                        return (0);
                    }

                    break;
                }
            }

            string DataRedundancyConfig = "n";
            int DataRedundancyCopies;
            string cloud = oam::UnassignedName;
            int DataRedundancyNetworkType;
            int DataRedundancyStorageType;
            string AmazonVPCNextPrivateIP;

            try
            {
                oam.getSystemConfig("Cloud", cloud);
                oam.getSystemConfig("AmazonVPCNextPrivateIP", AmazonVPCNextPrivateIP);
                oam.getSystemConfig("DataRedundancyConfig", DataRedundancyConfig);
                oam.getSystemConfig("DataRedundancyCopies", DataRedundancyCopies);
                oam.getSystemConfig("DataRedundancyNetworkType", DataRedundancyNetworkType);
                oam.getSystemConfig("DataRedundancyStorageType", DataRedundancyStorageType);
            }
            catch (...) {}

            ModuleTypeConfig moduletypeconfig;
            DeviceNetworkConfig devicenetworkconfig;
            DeviceNetworkList devicenetworklist;
            DeviceNetworkList enabledevicenetworklist;
            HostConfig hostconfig;

            bool storeHostnames = false;
            string moduleType;
            string moduleName;
            int moduleCount;
            string password = "ssh";
            typedef std::vector<string> inputNames;
            inputNames inputnames;
            typedef std::vector<string> umStorageNames;
            umStorageNames umstoragenames;
            int hostArg;
            int dbrootPerPM = 0;

            //check if module type or module name was entered
            if ( arguments[1].size() == 2 )
            {
                //Module Type was entered
                if (arguments[3] == "" && cloud == oam::UnassignedName)
                {
                    // need at least  arguments
                    cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                    break;
                }

                //Module Type was entered
                moduleType = arguments[1];
                moduleCount = atoi(arguments[2].c_str());
                hostArg = 4;

                // MCOL-1607.  Check whether we should store host names or IP addresses.
                if (arguments[3] != "" && (arguments[3][0] == 'y' || arguments[3][0] == 'Y'))
                    storeHostnames = true;

                if (arguments[5] != "")
                    password = arguments[5];
                else
                {
                    cout << endl;
                    string prompt = "Enter the 'User' Password or 'ssh' if configured with ssh-keys";
                    password = dataPrompt(prompt);
                }

                if (arguments[6] != "")
                    dbrootPerPM = atoi(arguments[6].c_str());
            }
            else
            {
                //Module Name was entered
                if (arguments[2] == "" && cloud == oam::UnassignedName)
                {
                    // need at least  arguments
                    cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                    break;
                }

                moduleName = arguments[1];
                moduleType = arguments[1].substr(0, MAX_MODULE_TYPE_SIZE);
                moduleCount = 1;
                hostArg = 3;

                // MCOL-1607.  Check whether we should store host names or IP addresses.
                if (arguments[2] != "" && (arguments[2][0] == 'y' || arguments[2][0] == 'Y'))
                    storeHostnames = true;

                if (arguments[4] != "")
                    password = arguments[4];
                else
                {
                    cout << endl;
                    string prompt = "Enter the 'User' Password or 'ssh' if configured with ssh-keys";
                    password = dataPrompt(prompt);
                }

                if (arguments[5] != "")
                    dbrootPerPM = atoi(arguments[5].c_str());
            }

//do we needed this check????
            if ( moduleCount < 1 || moduleCount > 10  )
            {
                cout << endl << "**** addModule Failed : Failed to Add Module, invalid number-of-modules entered (1-10)" << endl;
                break;
            }

            if ( DataRedundancyConfig == "y" && moduleType == "pm" )
            {
                if ( localModule != parentOAMModule )
                {
                    // exit out since not on active module
                    cout << endl << "**** addModule Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
                    break;
                }

                if ( fmod((float) moduleCount, (float) DataRedundancyCopies) != 0 )
                {
                    cout << endl << "**** addModule Failed : Failed to Add Module, invalid number-of-modules: must be multiple of Data Redundancy Copies, which is " << DataRedundancyCopies << endl;
                    break;
                }
            }

            //check and parse input Hostname/VPC-IP Addresses
            if (arguments[hostArg] != "")
            {
                boost::char_separator<char> sep(", ");
                boost::tokenizer< boost::char_separator<char> > tokens(arguments[hostArg], sep);

                for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                        it != tokens.end();
                        ++it)
                {
                    inputnames.push_back(*it);
                }
            }

            if ( inputnames.size() < (unsigned) moduleCount )
            {
                if ( cloud == oam::UnassignedName )
                {
                    cout << endl << "**** addModule Failed : Failed to Add Module, number of hostnames is less than Module Count" << endl;
                    break;
                }
                else
                {
                    if ( cloud == "amazon-ec2" )
                    {
                        cout << endl << "Launching new Instance(s)" << endl;

                        for ( int id = inputnames.size() ; id < moduleCount ; id++ )
                        {
                            inputnames.push_back(oam::UnassignedName);
                        }
                    }
                    else
                    {
                        // amazon-vpc
                        if ( inputnames.size() == 0 )
                        {
                            if ( AmazonVPCNextPrivateIP == oam::UnassignedName)
                            {
                                cout << endl << "**** addModule Failed : Failed to Add Module, enter VPC Private IP Address" << endl;
                                break;
                            }
                            else
                            {
                                if ( AmazonVPCNextPrivateIP == "autoassign")
                                {
                                    for ( int id = inputnames.size() ; id < moduleCount ; id++ )
                                    {
                                        inputnames.push_back("autoassign");
                                    }
                                }
                                else
                                {
                                    for ( int id = inputnames.size() ; id < moduleCount ; id++ )
                                    {
                                        inputnames.push_back(AmazonVPCNextPrivateIP);

                                        try
                                        {
                                            AmazonVPCNextPrivateIP = oam.incrementIPAddress(AmazonVPCNextPrivateIP);
                                        }
                                        catch (...)
                                        {
                                            cout << endl << "ERROR: incrementIPAddress API error, check logs" << endl;
                                            exit(1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //get configured moduleNames
            try
            {
                oam.getSystemConfig(moduleType, moduletypeconfig);
            }
            catch (...)
            {
                cout << endl << "**** addModule Failed : Failed to Add Module, getSystemConfig API Failed" << endl;
                break;
            }

            //get module names already in-use and Number of NIC IDs for module
            typedef std::vector<string>	moduleNameList;
            moduleNameList modulenamelist;
            int nicNumber = 1;

            DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

            for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
            {
                modulenamelist.push_back((*pt).DeviceName);
                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                {
                    if ( (*pt1).HostName != oam::UnassignedName )
                    {
                        if ( nicNumber < (*pt1).NicID )
                            nicNumber = (*pt1).NicID;
                    }
                }
            }

            if ( ((unsigned) nicNumber * moduleCount) != inputnames.size() && cloud == oam::UnassignedName )
            {
                cout << endl << "**** addModule Failed : Failed to Add Module, invalid number of hostNames entered. Enter " + oam.itoa(nicNumber * moduleCount) +  " hostname(s), which is the number of NICs times the number of modules" << endl;
                break;
            }

            int moduleID = 1;
            inputNames::const_iterator listPT1 = inputnames.begin();

            for ( int i = 0 ; i < moduleCount ; i++ )
            {
                string dataDupIPaddr = "ModuleIPAddr" + oam.itoa(moduleID) + "-1-3";
                string dataDupHostName = "ModuleHostName" + oam.itoa(moduleID) + "-1-3";
                //validate or determine module name
                moduleNameList::const_iterator listPT = modulenamelist.begin();

                for ( ; listPT != modulenamelist.end() ; listPT++)
                {
                    if ( !moduleName.empty() )
                    {
                        //add by moduleName, validate that Entered module name doesn't exist
                        if ( moduleName == (*listPT) )
                        {
                            cout << endl << "**** addModule Failed : Module Name already exist" << endl;
                            return 1;
                        }
                    }
                    else
                    {
                        //add by moduleType, get available module name
                        string newModuleName = moduleType + oam.itoa(moduleID);

                        if ( newModuleName == (*listPT) )
                            moduleID++;
                        else
                        {
                            moduleName = newModuleName;
                            moduleID++;
                            break;
                        }
                    }
                }

                if ( moduleName.empty() )
                {
                    moduleName = moduleType + oam.itoa(moduleID);
                    moduleID++;
                }

                // store module name
                devicenetworkconfig.DeviceName = moduleName;
                enabledevicenetworklist.push_back(devicenetworkconfig);

                for ( int j = 0 ; j < nicNumber ; j ++ )
                {
                    //get/check Server Hostnames IP address
                    string hostName;
                    string IPAddress;

                    // MCOL-1607.  Store hostnames in the config file if they entered one */
                    if (storeHostnames)
                    {
                        // special case
                        if (cloud == "amazon-vpc" && *listPT1 == "autoassign")
                        {
                            hostName = oam::UnassignedName;
                            IPAddress = *listPT1;
                        }
                        else if (oam.isValidIP(*listPT1))   // they entered an IP addr
                        {
                            hostName = oam::UnassignedName;
                            IPAddress = *listPT1;
                        }
                        else   // they entered a hostname
                            IPAddress = hostName = *listPT1;
                    }
                    else if ( cloud == "amazon-ec2")
                    {
                        hostName = *listPT1;

                        if ( hostName != oam::UnassignedName )
                        {
                            IPAddress = oam.getEC2InstanceIpAddress(hostName);

                            if (IPAddress == "stopped" || IPAddress == "terminated")
                            {
                                cout << "ERROR: Instance " + hostName + " not running, please start and retry" << endl << endl;
                                return 1;
                            }
                        }
                        else
                            IPAddress = oam::UnassignedName;
                    }
                    else
                    {
                        if ( cloud == "amazon-vpc")
                        {
                            if ( *listPT1 != "autoassign" )
                            {
                                if ( oam.isValidIP(*listPT1) )
                                {
                                    //ip address entered
                                    hostName = oam::UnassignedName;
                                    IPAddress = *listPT1;
                                }
                                else
                                {
                                    //instance id entered
                                    hostName = *listPT1;
                                    IPAddress = oam.getEC2InstanceIpAddress(hostName);

                                    if (IPAddress == "stopped" || IPAddress == "terminated")
                                    {
                                        cout << "ERROR: Instance " + hostName + " not running, please start and retry" << endl << endl;
                                        return 1;
                                    }
                                }
                            }
                            else
                            {
                                hostName = oam::UnassignedName;
                                IPAddress = "autoassign";
                            }
                        }
                        else
                        {
                            // non-amazon
                            hostName = *listPT1;
                            IPAddress = oam.getIPAddress(hostName);
                            if ( IPAddress.empty() )
                            {
                                // prompt for IP Address
                                string prompt = "IP Address of " + hostName + " not found, enter IP Address or enter 'abort'";
                                IPAddress = dataPrompt(prompt);

                                if ( IPAddress == "abort" || !oam.isValidIP(IPAddress) )
                                    return 1;
                            }
                        }
                    }

                    if ( DataRedundancyConfig == "y")
                    {
                        string errmsg1;
                        string errmsg2;
                        int ret = oam.glusterctl(oam::GLUSTER_PEERPROBE, IPAddress, password, errmsg2);

                        if ( ret != 0 )
                        {
                            return 1;
                        }
                    }

                    hostconfig.IPAddr = IPAddress;
                    hostconfig.HostName = hostName;
                    hostconfig.NicID = j + 1;
                    devicenetworkconfig.hostConfigList.push_back(hostconfig);
                    listPT1++;
                }

                devicenetworklist.push_back(devicenetworkconfig);
                devicenetworkconfig.hostConfigList.clear();
                moduleName.clear();

                if ( DataRedundancyConfig == "y" && DataRedundancyNetworkType == 2 && moduleType == "pm")
                {
                    string DataRedundancyIPAddress = sysConfig->getConfig("DataRedundancyConfig", dataDupIPaddr);
                    string DataRedundancyHostname = sysConfig->getConfig("DataRedundancyConfig", dataDupHostName);

                    if (DataRedundancyIPAddress.empty() || DataRedundancyHostname.empty())
                    {
                        string prompt = "DataRedundancy is configured for dedicated network, enter a hostname";
                        DataRedundancyHostname = dataPrompt(prompt);
                        if (storeHostnames)
                            DataRedundancyIPAddress = DataRedundancyHostname;
                        else
                        {
                            DataRedundancyIPAddress = oam.getIPAddress(DataRedundancyHostname);

                            if ( DataRedundancyIPAddress.empty() )
                            {
                                // prompt for IP Address
                                string prompt = "IP Address of " + DataRedundancyHostname + " not found, enter IP Address";
                                DataRedundancyIPAddress = dataPrompt(prompt);

                                if (!oam.isValidIP(DataRedundancyIPAddress))
                                    return 1;
                            }
                        }
                        sysConfig->setConfig("DataRedundancyConfig", dataDupHostName, DataRedundancyHostname);
                        sysConfig->setConfig("DataRedundancyConfig", dataDupIPaddr, DataRedundancyIPAddress);
                    }
                }
            }

            DBRootConfigList dbrootlist;
            int dbrootNumber = -1;
            typedef std::vector<string>	storageDeviceList;
            storageDeviceList storagedevicelist;
            string deviceType;

            if ( DataRedundancyConfig == "y" && moduleType == "pm")
            {
                cout << endl << "Data Redundancy storage will be expanded when module(s) are added." << endl;

                if ( dbrootPerPM == 0)
                {
                    cout << endl;
                    // prompt for number of DBRoot
                    string prompt = "Number of DBRoots Per Performance Module you want to add";
                    dbrootPerPM = atoi(dataPrompt(prompt).c_str());
                }
                else
                    cout << endl << "Number of DBRoots Per Performance Module to be added is " << oam.itoa(dbrootPerPM) << endl;

                dbrootNumber = dbrootPerPM * moduleCount;

                if ( DataRedundancyStorageType == 2 )
                {
                    cout << endl << "Data Redundancy Storage Type is configured for 'storage'" << endl;

                    cout << "You will need " << oam.itoa(dbrootNumber * DataRedundancyCopies);
                    cout << " total storage locations and " << oam.itoa(dbrootPerPM * DataRedundancyCopies) << " storage locations per PM. You will now " << endl;
                    cout << "be asked to enter the device names for the storage locations. You will enter " << endl;
                    cout << "them for each PM, on one line, separated by spaces (" << oam.itoa(dbrootPerPM * DataRedundancyCopies) << " names on each line)." << endl;

                    DeviceNetworkList::iterator pt = devicenetworklist.begin();
                    string firstPM = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                    for ( ; pt != devicenetworklist.end() ; pt++)
                    {
                        cout << endl;
                        string prompt = "Storage Device Names for " + (*pt).DeviceName;
                        string devices = dataPrompt(prompt);
                        storagedevicelist.push_back(devices);
                    }

                    cout << endl;
                    string prompt = "Filesystem type for these storage locations (ext2,ext3,xfs,etc)";
                    deviceType = dataPrompt(prompt);
                }

            }

            string mysqlpassword = oam::UnassignedName;

            try
            {
                cout << endl << "Adding Modules ";
                DeviceNetworkList::iterator pt = devicenetworklist.begin();
                string firstPM = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                for ( ; pt != devicenetworklist.end() ; pt++)
                {
                    cout << (*pt).DeviceName << ", ";
                }

                cout << "please wait..." << endl;

                oam.addModule(devicenetworklist, password, mysqlpassword, storeHostnames);

                cout << "Add Module(s) successfully completed" << endl;

                if ( DataRedundancyConfig == "y" && moduleType == "pm" )
                {

                    {
                        //send messages to update fstab to new modules, if needed
                        DeviceNetworkList::iterator pt2 = devicenetworklist.begin();
                        storageDeviceList::iterator pt3 = storagedevicelist.begin();

                        for ( ; pt2 != devicenetworklist.end() ; pt2++, pt3++)
                        {
                            HostConfigList::iterator hostConfigIter = (*pt2).hostConfigList.begin();
                            string moduleName = (*pt2).DeviceName;
                            int brickID = 1;

                            if ( DataRedundancyStorageType == 2 )
                            {
                                string devices = *pt3;
                                boost::char_separator<char> sep(" ");
                                boost::tokenizer< boost::char_separator<char> > tokens(devices, sep);

                                for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                                        it != tokens.end();
                                        ++it)
                                {
                                    string deviceName = *it;
                                    string entry = deviceName + " /var/lib/columnstore/gluster/brick" + oam.itoa(brickID)  + " " + deviceType + " defaults 1 2";
                                    //send update pm
                                    oam.distributeFstabUpdates(entry, moduleName);
                                }
                            }

                            string command = "remote_command.sh " + (*hostConfigIter).IPAddr + " " + password + " 'mkdir -p /var/lib/columnstore/gluster/brick" + oam.itoa(brickID) + "'";
                            system(command.c_str());
                            brickID++;
                        }
                    }

                    //enable modules
                    try
                    {
                        cout << endl << "Enabling Modules " << endl;
                        oam.enableModule(enabledevicenetworklist);
                        cout << "Successful Enable of Modules " << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** enableModule Failed :  " << e.what() << endl;
                        break;
                    }

                    cout << endl << "Adding DBRoots" << endl;

                    //add dbroots
                    string firstDBroot;

                    try
                    {
                        oam.addDbroot(dbrootNumber, dbrootlist);

                        cout << "New DBRoot IDs added = ";
                        DBRootConfigList::iterator pt1 = dbrootlist.begin();
                        firstDBroot = oam.itoa(*pt1);

                        for ( ; pt1 != dbrootlist.end() ;)
                        {
                            cout << oam.itoa(*pt1);
                            pt1++;

                            if (pt1 != dbrootlist.end())
                                cout << ", ";
                        }

                        cout << endl;

                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** addDbroot Failed: " << e.what() << endl;
                        break;
                    }

                    cout << endl << "Assigning DBRoots" << endl << endl;

                    DeviceNetworkList::iterator pt = devicenetworklist.begin();
                    DBRootConfigList::iterator pt1 = dbrootlist.begin();

                    for ( ; pt != devicenetworklist.end() ; pt++)
                    {
                        string moduleName = (*pt).DeviceName;

                        DBRootConfigList dbrootlist;

                        for ( int dbrootNum = 0; dbrootNum < dbrootPerPM ; dbrootNum++)
                        {
                            dbrootlist.push_back(*pt1);
                            pt1++;
                        }

                        //assign dbroots to pm
                        try
                        {
                            oam.assignDbroot(moduleName, dbrootlist);

                            cout << endl << "Successfully Assigned DBRoots " << endl;

                        }
                        catch (exception& e)
                        {
                            cout << endl << "**** Failed Assign of DBRoots: " << e.what() << endl;
                            break;
                        }
                    }

                    cout << endl << "Run Data Redundancy Setup for DBRoots" << endl;

                    try
                    {
                        int ret = oam.glusterctl(oam::GLUSTER_ADD, firstPM, firstDBroot, password);

                        if ( ret != 0 )
                        {
                            cout << endl << "**** Failed Data Redundancy Add of DBRoots, " << endl;
                            break;
                        }

                        cout << endl << "Successfully Completed Data Redundancy Add DBRoots " << endl;

                    }
                    catch (...)
                    {
                        cout << endl << "**** glusterctl GLUSTER_ADD Failed" << endl;
                        break;
                    }

                    cout << endl << "addModule Command Successfully completed: Run startSystem command to Activate newly added Performance Modules" << endl << endl;
                }
                else
                {
                    cout << "addModule Command Successfully completed: Modules are Disabled, run alterSystem-enableModule command to enable them" << endl << endl;
                }

                try
                {
                    oam.setSystemConfig("AmazonVPCNextPrivateIP", AmazonVPCNextPrivateIP);
                }
                catch (...) {}

            }
            catch (exception& e)
            {
                cout << endl << "**** addModule Failed: " << e.what() << endl;
            }
            catch (...)
            {
                cout << endl << "**** addModule Failed : Failed to Add Module" << endl;
            }

            break;
        }

        case 49: // removeModule - parameters: Module name/type, number-of-modules
        {
            string DataRedundancyConfig = "n";
            int DataRedundancyCopies;

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if (DataRedundancyConfig == "y")
            {
                try
                {
                    oam.getSystemConfig( "DataRedundancyCopies", DataRedundancyCopies);
                }
                catch (...)
                {}
            }

            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** removeModule Failed : not support on a Single-Server type installs  " << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** removeModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( arguments[1] == parentOAMModule )
            {
                // exit out since you can't manually remove OAM Parent Module
                cout << endl << "**** removeModule Failed : can't manually remove the Active OAM Parent Module." << endl;
                break;
            }

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** removeModule Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            switch ( serverInstallType )
            {
                case (oam::INSTALL_COMBINE_DM_UM_PM):
                {
                    if (arguments[1].find("um") != string::npos )
                    {
                        cout << endl << "**** removeModule Failed : User Modules not supported on the Combined  Server Installation" << endl;
                        return 0;
                    }
                }
            }

            ModuleTypeConfig moduletypeconfig;
            DeviceNetworkConfig devicenetworkconfig;
            DeviceNetworkList devicenetworklist;
            bool quit = false;

            string moduleType;

            //check if module type or module name was entered
            if ( arguments[1].size() == 2 )
            {
                //Module Type was entered

                if ( arguments[3] != "y")
                {
                    cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
                    string warning = "This command does a remove a module from the MariaDB ColumnStore System";

                    // confirm request
                    if (confirmPrompt(warning))
                        break;
                }

                int moduleCount = atoi(arguments[2].c_str());

                if ( moduleCount < 1 || moduleCount > 10  )
                {
                    cout << endl << "**** removeModule Failed : Failed to Remove Module, invalid number-of-modules entered (1-10)" << endl;
                    break;
                }

                if ( DataRedundancyConfig == "y" )
                {
                    cout << endl << "**** removeModule Failed : Data Redundancy requires you to specify modules to remove in groups." << endl;
                    break;
                }

                cout << endl;

                moduleType = arguments[1];

                //store moduleNames
                try
                {
                    oam.getSystemConfig(moduleType, moduletypeconfig);
                }
                catch (...)
                {
                    cout << endl << "**** removeModule Failed : Failed to Remove Module, getSystemConfig API Failed" << endl;
                    break;
                }

                int currentModuleCount = moduletypeconfig.ModuleCount;

                if ( moduleCount > currentModuleCount )
                {
                    cout << endl << "**** removeModule Failed : Failed to Remove Module, mount count entered to larger than configured" << endl;
                    break;
                }

                if ( moduleCount == currentModuleCount )
                {
                    if ( moduleType == "pm" )
                    {
                        cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last Director Module" << endl;
                        break;
                    }
                }

                //get module names in-use
                typedef std::vector<string>	moduleNameList;
                moduleNameList modulenamelist;

                DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

                for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
                {
                    HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                    if ( (*pt1).HostName != oam::UnassignedName )
                        modulenamelist.push_back((*pt).DeviceName);
                }

                moduleNameList::reverse_iterator pt1 = modulenamelist.rbegin();

                for ( int i = 0 ; i < moduleCount ; i++)
                {
                    devicenetworkconfig.DeviceName = *pt1;
                    pt1++;
                    devicenetworklist.push_back(devicenetworkconfig);
                }
            }
            else
            {
                //Module Name was entered

                if ( arguments[2] != "y")
                {
                    cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
                    string warning = "This command removes module(s) from the MariaDB ColumnStore System";

                    // confirm request
                    if (confirmPrompt(warning))
                        break;
                }

                cout << endl;

                //parse module names
                boost::char_separator<char> sep(", ");
                boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);

                for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                        it != tokens.end();
                        ++it)
                {
                    devicenetworkconfig.DeviceName = *it;
                    devicenetworklist.push_back(devicenetworkconfig);

                    moduleType = (*it).substr(0, MAX_MODULE_TYPE_SIZE);

                    try
                    {
                        oam.getSystemConfig(moduleType, moduletypeconfig);
                    }
                    catch (...)
                    {
                        cout << endl << "**** removeModule Failed : Failed to Remove Module, getSystemConfig API Failed" << endl;
                        quit = true;
                        break;
                    }

                    int currentModuleCount = moduletypeconfig.ModuleCount;

                    if ( moduleType == "pm" && currentModuleCount == 1)
                    {
                        cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last Performance Module" << endl;
                        quit = true;
                        break;
                    }

                    if ( moduleType == "um" && currentModuleCount == 1)
                    {
                        cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last User Module" << endl;
                        quit = true;
                        break;
                    }
                }
            }

            if ( DataRedundancyConfig == "y" && devicenetworklist.size() != (size_t)DataRedundancyCopies)
            {
                cout << endl << "**** removeModule Failed : Data Redundancy requires you to remove modules in groups equal to number of copies" << endl;
                quit = true;
            }

            if (quit)
                break;

            DeviceNetworkList::iterator pt = devicenetworklist.begin();
            DeviceNetworkList::iterator endpt = devicenetworklist.end();

            // check for module status and if any dbroots still assigned
            for ( ; pt != endpt ; pt++)
            {
                // check module status
                try
                {
                    bool degraded;
                    int opState;
                    oam.getModuleStatus((*pt).DeviceName, opState, degraded);

                    if (opState == oam::MAN_OFFLINE ||
                            opState == oam::MAN_DISABLED ||
                            opState == oam::FAILED)
                    {

                    }
                    else
                    {
                        cout << "**** removeModule Failed : " << (*pt).DeviceName << " is not MAN_OFFLINE, DISABLED, or FAILED state.";
                        quit = true;
                        cout << endl;
                        break;
                    }
                }
                catch (exception& ex)
                {}

                // check dbrootlist should be empty on non data redundancy setups and remove dbroots if dataredundancy removal check passes
                if ( moduleType == "pm" )
                {
                    // check for dbroots assigned
                    DBRootConfigList dbrootConfigList;
                    string moduleID = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                    try
                    {
                        oam.getPmDbrootConfig(atoi(moduleID.c_str()), dbrootConfigList);
                    }
                    catch (...)
                    {}

                    if ( !dbrootConfigList.empty() && DataRedundancyConfig == "n")
                    {
                        cout << "**** removeModule Failed : " << (*pt).DeviceName << " has dbroots still assigned. Please run movePmDbrootConfig or unassignDbrootPmConfig.";
                        quit = true;
                        cout << endl;
                        break;
                    }
                    else if (DataRedundancyConfig == "y" && !dbrootConfigList.empty())
                    {
                        bool PMlistError = true;
                        cout << "Removing DBRoot(s)" << endl;
                        DBRootConfigList::iterator dbrootListPt = dbrootConfigList.begin();

                        for ( ; dbrootListPt != dbrootConfigList.end() ; dbrootListPt++)
                        {
                            // check if ACTIVE PM has a copy of Dbroot
                            string pmList = "";

                            try
                            {
                                string errmsg;
                                int ret = oam.glusterctl(oam::GLUSTER_WHOHAS, oam.itoa(*dbrootListPt), pmList, errmsg);

                                if ( ret != 0 )
                                {
                                    cout << endl << "**** removeModule Failed : " << (*pt).DeviceName << " glusterctl error" << endl;
                                    break;
                                }
                            }
                            catch (...)
                            {
                                cout << endl << "**** removeModule Failed : " << (*pt).DeviceName << " glusterctl error" << endl;
                                break;
                            }

                            boost::char_separator<char> sep(" ");
                            boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                            for ( boost::tokenizer< boost::char_separator<char> >::iterator it1 = tokens.begin();
                                    it1 != tokens.end();
                                    ++it1)
                            {
                                PMlistError = true;
                                DeviceNetworkList::iterator deviceNetListStartPt = devicenetworklist.begin();
                                string pmWithThisdbrootCopy = (*it1);

                                // walk the list of PMs that have copies of this dbroot
                                // and be sure they are in the list of nodes to be removed
                                for ( ; deviceNetListStartPt != endpt ; deviceNetListStartPt++)
                                {
                                    string thisModuleID = (*deviceNetListStartPt).DeviceName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                                    //cout << "pmWithThisDBRoot: " << pmWithThisdbrootCopy << " thisModuleID: " << thisModuleID << endl;
                                    if (pmWithThisdbrootCopy == thisModuleID)
                                    {
                                        PMlistError = false;
                                    }
                                }

                                if (PMlistError)
                                {
                                    cout << "**** removeModule Failed : Attempting to remove PMs: " << arguments[1] << " -- DBRoot" << oam.itoa(*dbrootListPt) << " has copies on PMs " << pmList << endl;
                                    quit = true;
                                }
                            }
                        }

                        if (!quit)
                        {
                            try
                            {
                                if (!dbrootConfigList.empty())
                                {
                                    oam.removeDbroot(dbrootConfigList);
                                }

                                cout << endl << "   Successful Removal of DBRoots " << endl << endl;
                            }
                            catch (exception& e)
                            {
                                cout << endl << "**** removeModule : Removal of DBRoots Failed: " << e.what() << endl;
                                quit = true;
                            }
                        }
                    }
                }
            }

            if (quit)
            {
                cout << endl;
                break;
            }

            try
            {
                cout << endl << "Removing Module(s) ";
                DeviceNetworkList::iterator pt = devicenetworklist.begin();

                for ( ; pt != devicenetworklist.end() ; pt++)
                {
                    cout << (*pt).DeviceName << ", ";
                }

                cout << "please wait..." << endl;

                oam.removeModule(devicenetworklist);
                cout << endl << "Remove Module successfully completed" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "Failed to Remove Module: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** removeModule Failed : Failed to Remove Module" << endl << endl;
                break;
            }

            break;
        }

        case 50: // getModuleHostNames
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            ModuleConfig moduleconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();
            string returnValue;
            string Argument;

            // get and all display Module HostNames (NIC 1)
            // No other data will be displayed, only the hostnames.
            // This feature is designed for use by other processes.
            // It was specifically installed for the sqoop import feature (version 4.5)
            // If arguments[1] == PM, display only PMs, UM, display only UMs, else all.
            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);

                for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                        // end of list
                        break;

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                    if ( moduleCount == 0 )
                        // skip if no modules
                        continue;

                    string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

                    if (arguments[1] == "pm" && moduletype != "pm")
                        continue;

                    if (arguments[1] == "um" && moduletype != "um")
                        continue;

                    DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                    for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                    {
                        HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                        for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                        {
                            // Only print for NIC 1
                            if ((*pt1).NicID == 1)
                            {
                                // We need the name with domain and everything.
                                if ((*pt1).HostName == "localhost")
                                {
                                    char hostName[128] = {0};
                                    gethostname(hostName, 128);
                                    cout << hostName << endl;
                                }
                                else
                                {
                                    struct hostent* hentName = gethostbyname((*pt1).HostName.c_str());

                                    if (hentName)
                                    {
                                        cout << hentName->h_name << endl;
                                    }
                                    else
                                    {
                                        cout << (*pt1).HostName.c_str() << endl;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getModuleHostNames Failed =  " << e.what() << endl;
            }

            break;
        }

        case 51: // disableReplication
        {
            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** disableReplication Failed : not supported on a Single-Server type installs  " << endl;
                break;
            }

            string MySQLRep;

            try
            {
                oam.getSystemConfig("MySQLRep", MySQLRep);
            }
            catch (...) {}

            if ( MySQLRep == "n" )
            {
                string warning = "MariaDB ColumnStore Replication Feature is already disable";

                // confirm request
                if (confirmPrompt(warning))
                    break;
            }

            //set flag
            try
            {
                oam.setSystemConfig("MySQLRep", "n");
                sleep(2);
            }
            catch (...) {}

            try
            {
                oam.disableMySQLRep();
                cout << endl << "   Successful Disable of MariaDB ColumnStore Replication " << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** disableRep Failed :  " << e.what() << endl;
            }

            cout << endl;

            break;
        }

        case 52: // getModuleCpuUsers
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleCpuUsers Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            int topNumber = 5;

            if (arguments[2] != "")
            {
                topNumber = atoi(arguments[2].c_str());

                if ( topNumber < 1 || topNumber > 10 )
                {
                    cout << endl << "**** getModuleCpuUsers Failed : Invalid top Number entered" << endl;
                    break;
                }
            }

            TopProcessCpuUsers topprocesscpuusers;

            try
            {
                oam.getTopProcessCpuUsers(arguments[1], topNumber, topprocesscpuusers);

                printModuleCpuUsers(topprocesscpuusers);

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
                break;
            }

            break;
        }

        case 53: // getSystemCpuUsers
        {
            int topNumber = 5;

            if (arguments[1] != "")
            {
                topNumber = atoi(arguments[1].c_str());

                if ( topNumber < 1 || topNumber > 10 )
                {
                    cout << endl << "**** getSystemCpuUsers Failed : Invalid top Number entered" << endl;
                    break;
                }
            }

            cout << endl << "System Process Top CPU Users per Module" << endl << endl;

            SystemTopProcessCpuUsers systemtopprocesscpuusers;
            TopProcessCpuUsers topprocesscpuusers;

            try
            {
                oam.getTopProcessCpuUsers(topNumber, systemtopprocesscpuusers);

                for ( unsigned int i = 0 ; i < systemtopprocesscpuusers.topprocesscpuusers.size(); i++)
                {
                    printModuleCpuUsers(systemtopprocesscpuusers.topprocesscpuusers[i]);
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getSystemCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
                break;
            }

            break;
        }

        case 54: // getModuleCpu
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleCpu Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            ModuleCpu modulecpu;

            try
            {
                oam.getModuleCpuUsage(arguments[1], modulecpu);

                printModuleCpu(modulecpu);

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
                break;
            }

            break;
        }

        case 55: // getSystemCpu
        {
            cout << endl << "System CPU Usage per Module" << endl << endl;

            SystemCpu systemcpu;

            try
            {
                oam.getSystemCpuUsage(systemcpu);

                for ( unsigned int i = 0 ; i < systemcpu.modulecpu.size(); i++)
                {
                    printModuleCpu(systemcpu.modulecpu[i]);
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getSystemCpu Failed : Failed to get CPU Usage" << endl << endl;
                break;
            }

            break;
        }

        case 56: // getModuleMemoryUsers
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleMemoryUsers Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            int topNumber = 5;

            if (arguments[2] != "")
            {
                topNumber = atoi(arguments[2].c_str());

                if ( topNumber < 1 || topNumber > 10 )
                {
                    cout << endl << "**** getModuleMemoryUsers Failed : Invalid top Number entered" << endl;
                    break;
                }
            }

            TopProcessMemoryUsers topprocessmemoryusers;

            try
            {
                oam.getTopProcessMemoryUsers(arguments[1], topNumber, topprocessmemoryusers);

                printModuleMemoryUsers(topprocessmemoryusers);

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
                break;
            }

            break;
        }

        case 57: // getSystemMemoryUsers
        {
            int topNumber = 5;

            if (arguments[1] != "")
            {
                topNumber = atoi(arguments[1].c_str());

                if ( topNumber < 1 || topNumber > 10 )
                {
                    cout << endl << "**** getSystemMemoryUsers Failed : Invalid top Number entered" << endl;
                    break;
                }
            }

            cout << endl << "System Process Top Memory Users per Module" << endl << endl;

            SystemTopProcessMemoryUsers systemtopprocessmemoryusers;
            TopProcessMemoryUsers topprocessmemoryusers;

            try
            {
                oam.getTopProcessMemoryUsers(topNumber, systemtopprocessmemoryusers);

                for ( unsigned int i = 0 ; i < systemtopprocessmemoryusers.topprocessmemoryusers.size(); i++)
                {
                    printModuleMemoryUsers(systemtopprocessmemoryusers.topprocessmemoryusers[i]);
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getSystemMemoryUsers Failed : Failed to get Top CPU Users" << endl << endl;
                break;
            }

            break;
        }

        case 58: // getModuleMemory
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleMemory Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            ModuleMemory modulememory;

            try
            {
                oam.getModuleMemoryUsage(arguments[1], modulememory);

                cout << endl << "Module Memory Usage (in K bytes)" << endl << endl;

                cout.setf(ios::left);
                cout.width(8);
                cout << "Module";
                cout.width(11);
                cout << "Mem Total";
                cout.width(9);
                cout << "Mem Used";
                cout.width(9);
                cout << "cache";
                cout.width(12);
                cout << "Mem Usage %";
                cout.width(11);
                cout << "Swap Total";
                cout.width(10);
                cout << "Swap Used";
                cout.width(13);
                cout << "Swap Usage %";
                cout << endl;

                cout.setf(ios::left);
                cout.width(8);
                cout << "------";
                cout.width(11);
                cout << "---------";
                cout.width(9);
                cout << "-------";
                cout.width(9);
                cout << "-------";
                cout.width(12);
                cout << "----------";
                cout.width(11);
                cout << "----------";
                cout.width(10);
                cout << "---------";
                cout.width(13);
                cout << "-----------";
                cout << endl;

                printModuleMemory(modulememory);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
                break;
            }

            break;
        }

        case 59: // getSystemMemory
        {
            cout << endl << "System Memory Usage per Module (in K bytes)" << endl << endl;

            cout.setf(ios::left);
            cout.width(8);
            cout << "Module";
            cout.width(11);
            cout << "Mem Total";
            cout.width(10);
            cout << "Mem Used";
            cout.width(9);
            cout << "Cache";
            cout.width(13);
            cout << "Mem Usage %";
            cout.width(12);
            cout << "Swap Total";
            cout.width(11);
            cout << "Swap Used";
            cout.width(14);
            cout << "Swap Usage %";
            cout << endl;

            cout.setf(ios::left);
            cout.width(8);
            cout << "------";
            cout.width(11);
            cout << "---------";
            cout.width(10);
            cout << "--------";
            cout.width(9);
            cout << "-------";
            cout.width(13);
            cout << "-----------";
            cout.width(12);
            cout << "----------";
            cout.width(11);
            cout << "---------";
            cout.width(14);
            cout << "------------";
            cout << endl;

            SystemMemory systemmemory;

            try
            {
                oam.getSystemMemoryUsage(systemmemory);

                for ( unsigned int i = 0 ; i < systemmemory.modulememory.size(); i++)
                {
                    printModuleMemory(systemmemory.modulememory[i]);
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getSystemCpu Failed : Failed to get Memory Usage" << endl << endl;
                break;
            }

            break;
        }

        case 60: // getModuleDisk
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleDisk Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            ModuleDisk moduledisk;

            try
            {
                oam.getModuleDiskUsage(arguments[1], moduledisk);

                printModuleDisk(moduledisk);

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
                break;
            }

            break;
        }

        case 61: // getSystemDisk
        {
            cout << endl << "System Disk Usage per Module" << endl << endl;

            SystemDisk systemdisk;

            try
            {
                oam.getSystemDiskUsage(systemdisk);

                for ( unsigned int i = 0 ; i < systemdisk.moduledisk.size(); i++)
                {
                    printModuleDisk(systemdisk.moduledisk[i]);
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getSystemCpu Failed : Failed to get Memory Usage" << endl << endl;
                break;
            }

            break;
        }

        case 62: // getModuleResources
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleResources Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            int topNumber = 5;

            TopProcessCpuUsers topprocesscpuusers;

            try
            {
                oam.getTopProcessCpuUsers(arguments[1], topNumber, topprocesscpuusers);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
                break;
            }

            ModuleCpu modulecpu;

            try
            {
                oam.getModuleCpuUsage(arguments[1], modulecpu);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
                break;
            }

            TopProcessMemoryUsers topprocessmemoryusers;

            try
            {
                oam.getTopProcessMemoryUsers(arguments[1], topNumber, topprocessmemoryusers);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
                break;
            }

            ModuleMemory modulememory;

            try
            {
                oam.getModuleMemoryUsage(arguments[1], modulememory);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
                break;
            }

            ModuleDisk moduledisk;

            try
            {
                oam.getModuleDiskUsage(arguments[1], moduledisk);
            }
            catch (exception& e)
            {
                cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
                break;
            }

            printModuleResources(topprocesscpuusers, modulecpu, topprocessmemoryusers, modulememory, moduledisk);

            break;
        }

        case 63: // getSystemResources
        {
            cout << endl << "System Resource Usage per Module" << endl << endl;

            int topNumber = 5;

            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);

                for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                        // end of list
                        continue;

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

                    if ( moduleCount == 0 )
                        continue;

                    DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                    for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                    {
                        string modulename = (*pt).DeviceName;

                        if ( modulename == "unknown" )
                            continue;

                        TopProcessCpuUsers topprocesscpuusers;

                        try
                        {
                            oam.getTopProcessCpuUsers(modulename, topNumber, topprocesscpuusers);
                        }
                        catch (exception& e)
                        {
                            cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
                            break;
                        }
                        catch (...)
                        {
                            cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
                            break;
                        }

                        ModuleCpu modulecpu;

                        try
                        {
                            oam.getModuleCpuUsage(modulename, modulecpu);
                        }
                        catch (exception& e)
                        {
                            cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
                            break;
                        }
                        catch (...)
                        {
                            cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
                            break;
                        }

                        TopProcessMemoryUsers topprocessmemoryusers;

                        try
                        {
                            oam.getTopProcessMemoryUsers(modulename, topNumber, topprocessmemoryusers);
                        }
                        catch (exception& e)
                        {
                            cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
                            break;
                        }
                        catch (...)
                        {
                            cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
                            break;
                        }

                        ModuleMemory modulememory;

                        try
                        {
                            oam.getModuleMemoryUsage(modulename, modulememory);
                        }
                        catch (exception& e)
                        {
                            cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
                            break;
                        }
                        catch (...)
                        {
                            cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
                            break;
                        }

                        ModuleDisk moduledisk;

                        try
                        {
                            oam.getModuleDiskUsage(modulename, moduledisk);
                        }
                        catch (exception& e)
                        {
                            cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
                            break;
                        }
                        catch (...)
                        {
                            cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
                            break;
                        }

                        printModuleResources(topprocesscpuusers, modulecpu, topprocessmemoryusers, modulememory, moduledisk);
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemResources Failed :  " << e.what() << endl;
            }

            break;
        }

        case 64: // getActiveSQLStatements
        {
            cout << endl << "Get List of Active SQL Statements" << endl;
            cout <<         "=================================" << endl << endl;

            ActiveSqlStatements activesqlstatements;

            try
            {
                oam.getActiveSQLStatements(activesqlstatements);

                if ( activesqlstatements.size() == 0 )
                {
                    cout << "No Active SQL Statements at this time" << endl << endl;
                    break;
                }

                cout << "Start Time        Time (hh:mm:ss)   Session ID             SQL Statement" << endl;
                cout << "----------------  ----------------  --------------------   ------------------------------------------------------------" << endl;

                for ( unsigned int i = 0 ; i < activesqlstatements.size(); i++)
                {
                    struct tm tmStartTime;
                    char timeBuf[36];
                    time_t startTime = activesqlstatements[i].starttime;
                    localtime_r(&startTime, &tmStartTime);
                    (void)strftime(timeBuf, 36, "%b %d %H:%M:%S", &tmStartTime);

                    cout.setf(ios::left);
                    cout.width(21);
                    cout << timeBuf;

                    //get current time in Epoch
                    time_t cal;
                    time (&cal);

                    int runTime = cal - activesqlstatements[i].starttime;
                    int runHours = runTime / 3600;
                    int runMinutes = (runTime - (runHours * 3600)) / 60;
                    int runSeconds = runTime - (runHours * 3600)  - (runMinutes * 60);

                    cout.width(15);
                    string hours = oam.itoa(runHours);
                    string minutes = oam.itoa(runMinutes);
                    string seconds = oam.itoa(runSeconds);

                    string run;

                    if ( hours.size() == 1 )
                        run = "0" + hours + ":";
                    else
                        run = hours + ":";

                    if ( minutes.size() == 1 )
                        run = run + "0" + minutes + ":";
                    else
                        run = run + minutes + ":";

                    if ( seconds.size() == 1 )
                        run = run + "0" + seconds;
                    else
                        run = run + seconds;

                    cout << run;

                    cout.width(23);
                    cout << activesqlstatements[i].sessionid;

                    string SQLStatement = activesqlstatements[i].sqlstatement;
                    int pos = 0;

                    for ( ;; )
                    {
                        string printSQL = SQLStatement.substr(pos, 60);
                        pos = pos + 60;
                        cout << printSQL << endl;

                        if ( printSQL.size() < 60 )
                            break;

                        cout.width(59);
                        cout << " ";
                    }

                    cout << endl;
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get List of Active SQL Statements: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getActiveSQLStatements Failed : Failed to get List of Active SQL Statements" << endl << endl;
                break;
            }

            break;
        }

        case 65: // alterSystem-disableModule
        {

            string DataRedundancyConfig = "n";

            try
            {
                oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
            }
            catch (...)
            {}

            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** alterSystem-disableModule Failed : not support on a Single-Server type installs  " << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                //exit out since not on Parent OAM Module
                cout << endl << "**** alterSystem-disableModule Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** alterSystem-disableModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( arguments[1] == parentOAMModule )
            {
                // exit out since you can't manually remove OAM Parent Module
                cout << endl << "**** alterSystem-disableModule Failed : can't manually disable the Active OAM Parent Module." << endl;
                break;
            }

            string moduleType = arguments[1].substr(0, MAX_MODULE_TYPE_SIZE);

            gracefulTemp = INSTALL;

            //display Primary UM Module
            string PrimaryUMModuleName;

            try
            {
                oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
            }
            catch (...) {}

            bool primUM = false;

            if ( PrimaryUMModuleName == arguments[1] )
            {
                cout << endl << "This command stops the processing of applications on the Primary User Module, which is where DDL/DML are performed";

                if (confirmPrompt("If there is another module that can be changed to a new Primary User Module, this will be done"))
                    break;

                primUM = true;
            }
            else
            {
                // confirm request
                if ( arguments[2] != "y" )
                {
                    if (confirmPrompt("This command stops the processing of applications on a Module within the MariaDB ColumnStore System"))
                        break;
                }
            }

            //parse module names
            DeviceNetworkConfig devicenetworkconfig;
            DeviceNetworkList devicenetworklist;

            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                devicenetworkconfig.DeviceName = *it;
                devicenetworklist.push_back(devicenetworkconfig);
            }

            DeviceNetworkList::iterator pt = devicenetworklist.begin();
            DeviceNetworkList::iterator endpt = devicenetworklist.end();

            bool quit = false;

            // check for module status and if any dbroots still assigned
            if ( moduleType == "pm" )
            {
                for ( ; pt != endpt ; pt++)
                {
                    // check for dbroots assigned
                    DBRootConfigList dbrootConfigList;
                    string moduleID = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                    try
                    {
                        oam.getPmDbrootConfig(atoi(moduleID.c_str()), dbrootConfigList);
                    }
                    catch (...)
                    {}

                    if ( !dbrootConfigList.empty() && DataRedundancyConfig == "n")
                    {
                        cout << endl << "**** alterSystem-disableModule Failed : " << (*pt).DeviceName << " has dbroots still assigned and will not be disabled. Please run movePmDbrootConfig or unassignDbrootPmConfig.";
                        quit = true;
                        cout << endl;
                        break;
                    }
                    else if (!dbrootConfigList.empty() && DataRedundancyConfig == "y")
                    {
                        //check if dbroot requested to be removed is empty and dboot #1 is requested to be removed
                        DBRootConfigList::iterator pt = dbrootConfigList.begin();

                        for ( ; pt != dbrootConfigList.end() ; pt++)
                        {
                            int dbrootID = *pt;

                            //check if dbroot is empty
                            bool isEmpty = false;
                            string errMsg;

                            try
                            {
                                BRM::DBRM dbrm;

                                if ( dbrm.isDBRootEmpty(dbrootID, isEmpty, errMsg) != 0)
                                {
                                    cout << endl << "**** alterSystem-disableModule Failed : Data Redundancy detected DBRoots must be empty to be disabled. Remove data from DBRoot #" << oam.itoa(dbrootID) << " to continue." << endl;
                                    cout << "ERROR: isDBRootEmpty API error, dbroot #" << oam.itoa(dbrootID) << " :" << errMsg << endl;
                                    quit = true;
                                }
                            }
                            catch (...)
                            {}
                        }
                    }
                }

                if (quit)
                {
                    cout << endl;
                    break;
                }
            }

            if ( devicenetworklist.empty() )
            {
                cout << endl << "quiting, no modules to remove." << endl << endl;
                break;
            }

            // stop module
            try
            {
                cout << endl << "   Stopping Modules" << endl;
                oam.stopModule(devicenetworklist, gracefulTemp, ackTemp);
                cout << "   Successful stop of Modules " << endl;
            }
            catch (exception& e)
            {

                string Failed = e.what();

                if (Failed.find("Disabled") != string::npos)
                    cout << endl << "   Successful stop of Modules " << endl;
                else
                {
                    cout << endl << "**** stopModule Failed :  " << e.what() << endl;
                    break;
                }
            }

            // disable module
            try
            {
                cout << endl << "   Disabling Modules" << endl;
                oam.disableModule(devicenetworklist);
                cout << "   Successful disable of Modules " << endl;

                //display Primary UM Module
                string PrimaryUMModuleName;

                try
                {
                    oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
                }
                catch (...) {}

                if ( primUM &&
                        PrimaryUMModuleName != arguments[1] )
                    cout << endl << "   New Primary User Module = " << PrimaryUMModuleName << endl;

            }
            catch (exception& e)
            {
                cout << endl << "**** disableModule Failed :  " << e.what() << endl;
                break;
            }

            cout << endl;
            break;
        }

        case 66: // alterSystem-enableModule
        {
            if ( SingleServerInstall == "y" )
            {
                // exit out since not on single-server install
                cout << endl << "**** alterSystem-enableModule Failed : not support on a Single-Server type installs  " << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                //exit out since not on Parent OAM Module
                cout << endl << "**** alterSystem-enableModule Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** alterSystem-enableModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            string moduleType = arguments[1].substr(0, MAX_MODULE_TYPE_SIZE);

            ACK_FLAG ackTemp = ACK_YES;

            // confirm request
            if ( arguments[2] != "y" )
            {
                if (confirmPrompt("This command starts the processing of applications on a Module within the MariaDB ColumnStore System"))
                    break;
            }

            //parse module names
            DeviceNetworkConfig devicenetworkconfig;
            DeviceNetworkList devicenetworklist;
            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                devicenetworkconfig.DeviceName = *it;
                devicenetworklist.push_back(devicenetworkconfig);
            }

            //get the system status, enable modules and startmodules if system is ACTIVE
            SystemStatus systemstatus;

            try
            {
                oam.getSystemStatus(systemstatus);

                // enable module
                try
                {
                    cout << endl << "   Enabling Modules " << endl;
                    oam.enableModule(devicenetworklist);
                    cout << "   Successful enable of Modules " << endl;
                }
                catch (exception& e)
                {
                    cout << endl << "**** enableModule Failed :  " << e.what() << endl;
                    break;
                }

                if ( moduleType == "pm" )
                {
                    cout << endl << "   Performance Module(s) Enabled, run movePmDbrootConfig or assignDbrootPmConfig to assign dbroots, if needed" << endl << endl;
                    break;
                }
                else
                {
                    if (systemstatus.SystemOpState == oam::ACTIVE )
                    {
						try
						{
							cout << endl << "   Restarting System " << endl;
							gracefulTemp = oam::FORCEFUL;
							int returnStatus = oam.restartSystem(gracefulTemp, ackTemp);
							switch (returnStatus)
							{
								case API_SUCCESS:
									if ( waitForActive() )
										cout << endl << "   Successful restart of System " << endl << endl;
									else
										cout << endl << "**** restartSystem Failed : check log files" << endl;
									break;
								case API_CANCELLED:
									cout << endl << "   Restart of System canceled" << endl << endl;
									break;
								default:
									cout << endl << "**** restartSystem Failed : Check system logs" << endl;
									break;
							}
						}
						catch (exception& e)
						{
							cout << endl << "**** restartSystem Failed :  " << e.what() << endl;
							break;
						}
                    }
                    else
                        cout << endl << "   System not Active, run 'startSystem' to start system if needed" << endl;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** alterSystem-enableModule Failed : " << e.what() << endl;
                break;
            }
            catch (...)
            {
                cout << endl << "**** alterSystem-enableModule Failed,  Failed return from getSystemStatus API" << endl;
                break;
            }

            cout << endl;

            break;
        }

        case 67: // stopModule parameters moduleID
        {

            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** stopModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            string moduleType = arguments[1].substr(0, MAX_MODULE_TYPE_SIZE);

            //gracefulTemp = INSTALL;

            // confirm request
            if ( arguments[2] != "y" )
            {
                if (confirmPrompt("This command stops the processing of applications on a Module within the MariaDB ColumnStore System"))
                    break;
            }

            //parse module names
            DeviceNetworkConfig devicenetworkconfig;
            DeviceNetworkList devicenetworklist;

            boost::char_separator<char> sep(", ");
            boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);

            for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                    it != tokens.end();
                    ++it)
            {
                devicenetworkconfig.DeviceName = *it;
                devicenetworklist.push_back(devicenetworkconfig);
            }

            if ( devicenetworklist.empty() )
            {
                cout << endl << "No modules to stop." << endl << endl;
                break;
            }

            // stop module
            try
            {
                cout << endl << "   Stopping Module(s)" << endl;
                oam.stopModule(devicenetworklist, gracefulTemp, ackTemp);
                cout << "   Successful stop of Module(s) " << endl;
            }
            catch (exception& e)
            {

                string Failed = e.what();

                if (Failed.find("Disabled") != string::npos)
                    cout << endl << "   Successful stop of Module(s) " << endl;
                else
                {
                    cout << endl << "**** stopModule Failed :  " << e.what() << endl;
                    break;
                }
            }

            break;
        }


        default:
        {
            cout << arguments[0] << ": Unknown Command, type help for list of commands" << endl << endl;
            return 1;
        }
    }

    return 0;
}

/******************************************************************************************
 * @brief	ProcessSupportCommand
 *
 * purpose:	Process Support commands
 *
 ******************************************************************************************/
int ProcessSupportCommand(int CommandID, std::string arguments[])
{
    Oam oam;
    GRACEFUL_FLAG gracefulTemp = GRACEFUL;
    ACK_FLAG ackTemp = ACK_YES;
    CC_SUSPEND_ANSWER suspendAnswer = WAIT;
    bool bNeedsConfirm = true;
    string cmd;

    switch ( CommandID )
    {
        case 0: // helpsupport
        {
            // display commands in the Support Command list
            cout << endl << "List of Support commands" << endl << endl;

            for (int i = 1;; i++)
            {
                if (supportCmds[i] == "")
                    // end of list
                    break;

                cout << "   " << supportCmds[i] << endl;
            }

            cout << endl;
        }
        break;

        case 1: // stopprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** stopprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

            // don't allow stopping of Process-Monitor
            if ( arguments[1] == "ProcessMonitor" )
            {
                cout << "ProcessMonitor is controlled by 'init' and can not be stopped" << endl;
                break;
            }
            else
            {
                // give warning for Process-Monitor
                if ( arguments[1] == "ProcessManager" )
                {
                    if (confirmPrompt("ProcessManager is the Interface for the Console and should only be removed as part of a MariaDB ColumnStore Package installation"))
                        break;
                }
                else
                {
                    if ( arguments[3] != "y" )
                    {
                        getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

                        // confirm request
                        if (confirmPrompt("This command stops the processing of an application on a Module within the MariaDB ColumnStore System"))
                            break;
                    }
                }
            }

            try
            {
                oam.stopProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful stop of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 2: // startprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** startprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            try
            {
                oam.startProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful start of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 3: // restartprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** restartprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            if (arguments[3] != "y")
            {
                // confirm request
                if (confirmPrompt("This command restarts the processing of an application on a Module within the MariaDB ColumnStore System"))
                    break;
            }


            try
            {
                oam.restartProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful restart of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 4: // killpid
        {
            if (arguments[1] == "" || arguments[2] != "")
            {
                // need arguments
                cout << endl << "**** killpid Failed : Invalid or Missing Parameter, Enter local Process-ID" << endl;
                break;
            }

            pid_t PID = atoi(arguments[1].c_str());

            if ( PID <= 0 )
            {
                cout << endl << "**** killpid Failed : Invalid Process-ID Entered" << endl;
                break;
            }

            int status = kill( PID, SIGTERM);

            if ( status != API_SUCCESS)
                cout << endl << "   Failure in kill of Process-ID " << arguments[1] << ", Failed: " << errno << endl << endl;
            else
                cout << endl << "   Successful kill of Process-ID " << arguments[1] << endl << endl;
        }
        break;

        case 5: // rebootsystem - parameters: password
        {
            if ( !rootUser)
            {
                cout << endl << "**** rebootsystem Failed : command not available when running as non-root user" << endl;
                break;
            }

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** rebootsystem Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            if (arguments[1] == "" || arguments[1] == "y")
            {
                // need arguments
                cout << endl << "**** rebootsystem Failed : Invalid or Missing Parameter, Provide root-password" << endl;
                break;
            }

            string password = arguments[1];

            if ( arguments[2] != "y")
            {
                cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
                string warning = "This command stops the Processing of applications and reboots all modules within the MariaDB ColumnStore System";

                // confirm request
                if (confirmPrompt(warning))
                    break;
            }

            cout << endl << "   Stop System being performed, please wait..." << endl;

            try
            {
                cout << endl << "   System being stopped, please wait... " << endl;
                oam.stopSystem(GRACEFUL, ACK_YES);

                if ( waitForStop() )
                    cout << endl << "   Successful stop of System " << endl << endl;
                else
                    cout << endl << "**** stopSystem Failed : check log files" << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopSystem Failed :  " << e.what() << endl;
                string warning = "stopSystem command failed,";

                // confirm request
                if (confirmPrompt(warning))
                    break;
            }

            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);

                bool FAILED = false;

                for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                        // end of list
                        break;

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

                    if ( moduleCount > 0 )
                    {
                        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                        {
                            string modulename = (*pt).DeviceName;

                            if (modulename == parentOAMModule )
                            {
                                //do me last
                                continue;
                            }

                            //skip modules in MAN_DISABLED state
                            try
                            {
                                int opState;
                                bool degraded;
                                oam.getModuleStatus(modulename, opState, degraded);

                                if (opState == oam::MAN_DISABLED )
                                    //skip
                                    continue;
                            }
                            catch (exception& ex)
                            {}

                            //run remote command script
                            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
                            cmd = "remote_command.sh " + (*pt1).IPAddr + " " + password + " reboot " ;
                            int rtnCode = system(cmd.c_str());

                            if (WEXITSTATUS(rtnCode) != 0)
                            {
                                cout << "Failed with running remote_command.sh" << endl;
                                FAILED = true;
                            }
                            else
                                cout << endl << "   Successful reboot request of Module " << modulename << endl;
                        }
                    }
                }

                if ( FAILED )
                    break;

                //reboot local module
                int rtnCode = system("reboot");

                if (WEXITSTATUS(rtnCode) != 0)
                    cout << "Failed rebooting local module" << endl;
                else
                {
                    cout << endl << "   Successful reboot request of local Module" << endl;
                    // close the log file
                    writeLog("End of a command session!!!");
                    logFile.close();
                    cout << endl << "Exiting the MariaDB ColumnStore Command Console" << endl;
                    exit (0);
                }
            }
            catch (...)
            {
                cout << endl << "**** rebootsystem Failed : Failed on getSystemConfig API" << endl;
                break;
            }
        }
        break;

        case 6: // rebootnode - parameters: module-name password
        {
            if ( !rootUser)
            {
                cout << endl << "**** rebootnode Failed : command not available when running as non-root user" << endl;
                break;
            }

            if (arguments[1] == "" || arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** rebootnode Failed : Invalid or Missing Parameter, Enter module-name and root-password" << endl;
                break;
            }

            string inputModuleName = arguments[1];
            string password = arguments[2];

            if ( arguments[3] != "y")
            {
                cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
                string warning = "This command reboots a node within the MariaDB ColumnStore System";

                // confirm request
                if (confirmPrompt(warning))
                    break;
            }

            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

            try
            {
                oam.getSystemConfig(systemmoduletypeconfig);
                unsigned int i = 0;

                for (  ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                    {
                        // end of list
                        break;
                    }

                    int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
                    string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

                    if ( moduleCount > 0 )
                    {
                        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

                        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                        {
                            string modulename = (*pt).DeviceName;

                            if (inputModuleName == modulename )
                            {
                                if (inputModuleName == localModule )
                                {
                                    //reboot local module
                                    int rtnCode = system("reboot");

                                    if (WEXITSTATUS(rtnCode) != 0)
                                        cout << "Failed rebooting local node" << endl;
                                    else
                                    {
                                        cout << endl << "   Successful reboot request of Node " << modulename << endl;
                                        // close the log file
                                        writeLog("End of a command session!!!");
                                        logFile.close();
                                        cout << endl << "Exiting the MariaDB ColumnStore Command Console" << endl;
                                        exit (0);
                                    }
                                }
                                else
                                {
                                    HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
                                    string ipAddr = (*pt1).IPAddr;
                                    //run remote command script
                                    cmd = "remote_command.sh " + ipAddr + " " + password + " reboot " ;
                                    int rtnCode = system(cmd.c_str());

                                    if (WEXITSTATUS(rtnCode) != 0)
                                        cout << "Failed with running remote_command.sh" << endl;
                                    else
                                        cout << endl << "   Successful reboot request of Node " << modulename << endl;

                                    return (0);
                                }
                            }
                        }
                    }
                }
            }
            catch (...)
            {
                cout << endl << "**** rebootnode Failed : Failed on getSystemConfig API" << endl;
                break;
            }
        }
        break;

        case 7: // stopdbrmprocess
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command stops the dbrm processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.stopProcessType("DBRM");
                cout << endl << "   Successful stop of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 8: // startdbrmprocess
        {
            try
            {
                oam.startProcessType("DBRM");
                cout << endl << "   Successful Start of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 9: // restartdbrmprocess
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command restarts the dbrm processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.restartProcessType("DBRM");
                cout << endl << "   Successful Restart of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 10: // setsystemstartupstate
        {
            Config* sysConfig = Config::makeConfig();

            parentOAMModule = getParentOAMModule();

            if ( localModule != parentOAMModule )
            {
                // exit out since not on Parent OAM Module
                cout << endl << "**** setsystemstartupstate Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
            }

            string systemStartupOffline;

            try
            {
                systemStartupOffline = sysConfig->getConfig("Installation", "SystemStartupOffline");
                cout << "SystemStartupOffline currently set to '" + systemStartupOffline + "'" << endl;
            }
            catch (...)
            {
                cout << "ERROR: Problem getting systemStartupOffline from the MariaDB ColumnStore System Configuration file" << endl;
                return 1;
            }

            while (true)
            {
                char* pcommand = 0;
                string prompt;
                string temp = "cancel";
                prompt = "Set system startup state to offline: (y,n,cancel) [cancel]: ";
                pcommand = readline(prompt.c_str());

                if (pcommand)
                {
                    if (strlen(pcommand) > 0) temp = pcommand;

                    free(pcommand);
                    pcommand = 0;
                }

                if ( temp == "cancel" )
                    return 0;

                if ( temp == "n" || temp == "y")
                {
                    systemStartupOffline = temp;
                    break;
                }

                cout << "Invalid Option, please re-enter" << endl;
            }

            try
            {
                sysConfig->setConfig("Installation", "SystemStartupOffline", systemStartupOffline);
                sysConfig->write();
            }
            catch (...)
            {
                cout << "ERROR: Problem setting systemStartupOffline in the MariaDB ColumnStore System Configuration file" << endl;
                exit(-1);
            }

            cout << endl << "   Successful setting of systemStartupOffline to '" << systemStartupOffline << "'" << endl << endl;
        }
        break;

        case 11: // stopPrimProcs
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command stops the PrimProc processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.stopProcessType("PrimProc");
                cout << endl << "   Successful stop of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 12: // startPrimProcs
        {
            try
            {
                oam.startProcessType("PrimProc");
                cout << endl << "   Successful Start of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 13: // restartPrimProcs
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command restarts the PrimProc processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.restartProcessType("PrimProc");
                cout << endl << "   Successful Restart of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 14: // stopExeMgrs
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command stops the ExeMgr processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.stopProcessType("ExeMgr");
                cout << endl << "   Successful stop of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 15: // startExeMgrs
        {
            try
            {
                oam.startProcessType("ExeMgr");
                cout << endl << "   Successful Start of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 16: // restartExeMgrs
        {
            if ( arguments[1] != "y" )
            {
                // confirm request
                if (confirmPrompt("This command restarts the ExeMgr processes within the MariaDB ColumnStore System"))
                    break;
            }

            try
            {
                oam.restartProcessType("ExeMgr");
                cout << endl << "   Successful Restart of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 17: // getProcessStatusStandby - parameters: NONE
        {
            printProcessStatus("ProcStatusControlStandby");
        }
        break;

        case 18: // distributeconfigfile - parameters: option, moduleName
        {
            string name = "system";

            if ( arguments[1] != "" )
                name = arguments[1];

            try
            {
                oam.distributeConfigFile(name);
                cout << endl << "   Successful Distribution of MariaDB ColumnStore Config File" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** Distribution of MariaDB ColumnStore Config File Failed :  " << e.what() << endl;
            }
        }
        break;

        case 19: // getPmDbrootConfig - paramaters: pm id
        {
            string pmID;

            if (arguments[1] == "")
            {
                cout << endl;
                string prompt = "Enter the Performance Module ID";
                pmID = dataPrompt(prompt);
            }
            else
                pmID = arguments[1];

            try
            {
                DBRootConfigList dbrootConfigList;
                oam.getPmDbrootConfig(atoi(pmID.c_str()), dbrootConfigList);

                cout << "DBRoot IDs assigned to 'pm" + pmID + "' = ";

                DBRootConfigList::iterator pt = dbrootConfigList.begin();

                for ( ; pt != dbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != dbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 20: // getDbrootPmConfig - parameters dbroot id
        {
            string dbrootID;

            if (arguments[1] == "")
            {
                cout << endl;
                string prompt = "Enter the DBRoot ID";
                dbrootID = dataPrompt(prompt);
            }
            else
                dbrootID = arguments[1];

            try
            {
                int pmID;
                oam.getDbrootPmConfig(atoi(dbrootID.c_str()), pmID);

                cout << endl << " DBRoot ID " << dbrootID << " is assigned to 'pm" << pmID << "'" << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getDbrootPmConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 21: // getSystemDbrootConfig
        {
            cout << endl << "System DBroot Configuration" << endl << endl;

            try
            {
                DBRootConfigList dbrootConfigList;
                oam.getSystemDbrootConfig(dbrootConfigList);

                cout << "System DBRoot IDs = ";
                DBRootConfigList::iterator pt = dbrootConfigList.begin();

                for ( ; pt != dbrootConfigList.end() ;)
                {
                    cout << oam.itoa(*pt);
                    pt++;

                    if (pt != dbrootConfigList.end())
                        cout << ", ";
                }

                cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 22: // checkDBFunctional
        {
            try
            {
                oam.checkDBFunctional(false);
                cout << endl << "   checkDBFunctional Successful" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** checkDBFunctional Failed :  " << e.what() << endl;
                cout << endl << "     can check UM " + tmpDir + "/dbfunctional.log for possible additional information" << endl << endl;
            }
            catch (...)
            {
                cout << endl << "   checkDBFunctional Failed: check UM " + tmpDir + "/dbfunctional.log" << endl << endl;
            }
        }
        break;

        case 23: // getsystemreadflags
        {

            cout << " Print the DB System Flags. 1 = set and ready, 0 = clear and not ready" << endl;

            BRM::DBRM dbrm;

            cout << endl;

            try
            {
                cout << "getSystemQueryReady = " << dbrm.getSystemQueryReady() << endl;
            }
            catch (...)
            {}

            try
            {
                cout << "getSystemReady = " << dbrm.getSystemReady() << endl;
            }
            catch (...)
            {}

            try
            {
                cout << "getSystemSuspended = " << dbrm.getSystemSuspended() << endl;
            }
            catch (...)
            {}
        }
        break;

        case 24: // setsystemqueryready
        {
            BRM::DBRM dbrm;

            string set = "0";

            if (arguments[1] == "")
            {
                cout << endl;
                string prompt = "Enter 1 for set and 0 for clear";
                set = dataPrompt(prompt);
            }
            else
                set = arguments[1];

            bool flag = true;

            if ( set == "0" )
                flag = false;

            cout << endl;

            try
            {
                cout << "getSystemQueryReady = " << dbrm.getSystemQueryReady() << endl;
            }
            catch (...)
            {}

            cout << endl;

            try
            {
                dbrm.setSystemQueryReady(flag);
                cout << "setSystemQueryReady = " << flag << endl;
            }
            catch (...)
            {}

            cout << endl;

            try
            {
                cout << "getSystemQueryReady = " << dbrm.getSystemQueryReady() << endl;
            }
            catch (...)
            {}
        }

        default: // shouldn't get here, but...
            return 1;

    } // end of switch

    return 0;
}

/******************************************************************************************
 * @brief	getFlags
 *
 * purpose:	get and convert Graceful and Ack flags
 *
 ******************************************************************************************/
void getFlags(const string* arguments, GRACEFUL_FLAG& gracefulTemp, ACK_FLAG& ackTemp, oam::CC_SUSPEND_ANSWER& suspendAnswer, bool& bNeedsConfirm, string* password)
{
    gracefulTemp = GRACEFUL;                      // default
    ackTemp = ACK_YES;                             // default
    suspendAnswer = CANCEL;
    bNeedsConfirm = true;

    for ( int i = 1; i < ArgNum; i++)
    {
        if (strcasecmp(arguments[i].c_str(), "Y") == 0)
            bNeedsConfirm = false;
        else if (strcasecmp(arguments[i].c_str(), "N") == 0)
            bNeedsConfirm = true;
        else if (strcasecmp(arguments[i].c_str(), "GRACEFUL") == 0)
            gracefulTemp = oam::GRACEFUL;
        else if (strcasecmp(arguments[i].c_str(), "FORCEFUL") == 0)
            gracefulTemp = FORCEFUL;
        else if (strcasecmp(arguments[i].c_str(), "INSTALL") == 0)
            gracefulTemp = INSTALL;
        else if (strcasecmp(arguments[i].c_str(), "ACK_YES") == 0 || strcasecmp(arguments[i].c_str(), "YES_ACK") == 0)
            ackTemp = ACK_YES;
        else if (strcasecmp(arguments[i].c_str(), "ACK_NO") == 0 || strcasecmp(arguments[i].c_str(), "NO_ACK") == 0)
            ackTemp = ACK_NO;
        else if (strcasecmp(arguments[i].c_str(), "WAIT") == 0)
            suspendAnswer = WAIT;
        else if (strcasecmp(arguments[i].c_str(), "ROLLBACK") == 0)
            suspendAnswer = ROLLBACK;
        else if (strcasecmp(arguments[i].c_str(), "FORCE") == 0)
            suspendAnswer = FORCE;
        else if (password && arguments[i].length() > 0)
            *password = arguments[i];
    }
}


/******************************************************************************************
 * @brief	confirmPrompt
 *
 * purpose:	Confirmation prompt
 *
 ******************************************************************************************/
int confirmPrompt(std::string warningCommand)
{
    char* pcommand = 0;
    char* p;
    string argument = "n";

    while (true)
    {
        // read input
        if (warningCommand.size() > 0)
        {
            cout << endl << warningCommand << endl;
        }

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

        // covert argument into lowercase
        transform (argument.begin(), argument.end(), argument.begin(), to_lower());

        if ( argument == "y")
            return 0;
        else if ( argument == "n")
            return 1;
    }
}

/******************************************************************************************
 * @brief	dataPrompt
 *
 * purpose:	Prompt for additional data
 *
 ******************************************************************************************/
std::string dataPrompt(std::string promptCommand)
{
    char data[CmdSize];
    char* pdata = data;
    char* pd;
    string argument;

    while (true)
    {
        // read input
        cout << promptCommand << endl;
        pdata = readline("           Please enter: ");

        if (!pdata)                            // user hit <Ctrl>-D
            pdata = strdup("exit");

        else if (!*pdata)
            // just an enter-key was entered, ignore and reprompt
            continue;

        pd = pdata;
        argument = pd;

        return argument;
    }
}


/******************************************************************************************
 * @brief	writeLog for command
 *
 * purpose:	write command to the log file
 *
 ******************************************************************************************/
void writeLog(string command)
{
    Oam oam;

    //filter off password on reboot commands

    logFile << oam.getCurrentTime() << ": " << command << endl;
    logFile.flush();
    return;
}

/******************************************************************************************
 * @brief	printAlarmSummary
 *
 * purpose:	get active alarms and produce a summary
 *
 ******************************************************************************************/
void printAlarmSummary()
{
    AlarmList alarmList;
    Oam oam;

    try
    {
        oam.getActiveAlarms(alarmList);
    }
    catch (...)
    {
        return;
    }

    int critical = 0, major = 0, minor = 0, warning = 0, info = 0;
    AlarmList :: iterator i;

    for (i = alarmList.begin(); i != alarmList.end(); ++i)
    {
        switch (i->second.getSeverity())
        {
            case CRITICAL:
                ++critical;
                break;

            case MAJOR:
                ++major;
                break;

            case MINOR:
                ++minor;
                break;

            case WARNING:
                ++warning;
                break;

            case INFORMATIONAL:
                ++info;
                break;
        }
    }

    cout << endl << "Active Alarm Counts: ";
    cout << "Critical = " << critical;
    cout << ", Major = " << major;
    cout << ", Minor = " << minor;
    cout << ", Warning = " << warning;
    cout << ", Info = " << info;
    cout << endl;
}

/******************************************************************************************
 * @brief	printCriticalAlarms
 *
 * purpose:	get active Critical alarms
 *
 ******************************************************************************************/
void printCriticalAlarms()
{
    AlarmList alarmList;
    Oam oam;

    try
    {
        oam.getActiveAlarms(alarmList);
    }
    catch (...)
    {
        return;
    }

    cout << endl << "Critical Active Alarms:" << endl << endl;

    AlarmList :: iterator i;

    for (i = alarmList.begin(); i != alarmList.end(); ++i)
    {
        switch (i->second.getSeverity())
        {
            case CRITICAL:
                cout << "AlarmID           = " << i->second.getAlarmID() << endl;
                cout << "Brief Description = " << i->second.getDesc() << endl;
                cout << "Alarm Severity    = ";
                cout << "CRITICAL" << endl;
                cout << "Time Issued       = " << i->second.getTimestamp() << endl;
                cout << "Reporting Module  = " << i->second.getSname() << endl;
                cout << "Reporting Process = " << i->second.getPname() << endl;
                cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;
                break;

            case MAJOR:
            case MINOR:
            case WARNING:
            case INFORMATIONAL:
                break;
        }
    }
}

/******************************************************************************************
 * @brief	printSystemStatus
 *
 * purpose:	get and Display System and Module Statuses
 *
 ******************************************************************************************/
void printSystemStatus()
{
    SystemStatus systemstatus;
    Oam oam;
    BRM::DBRM dbrm(true);

    cout << endl << "System " << systemName << endl << endl;
    cout << "System and Module statuses" << endl << endl;
    cout << "Component     Status                       Last Status Change" << endl;
    cout << "------------  --------------------------   ------------------------" << endl;

    try
    {
        oam.getSystemStatus(systemstatus, false);
        cout << "System        ";
        cout.setf(ios::left);
        cout.width(29);
        int state = systemstatus.SystemOpState;
        string extraInfo = " ";
        bool bRollback = false;
        bool bForce = false;

        if (dbrm.isDBRMReady())
        {
            if (dbrm.getSystemSuspended() > 0)
            {
                extraInfo = " WRITE SUSPENDED";
            }
            else if (dbrm.getSystemSuspendPending(bRollback) > 0)
            {
                extraInfo = " WRITE SUSPEND PENDING";
            }
            else if (dbrm.getSystemShutdownPending(bRollback, bForce) > 0)
            {
                extraInfo = " SHUTDOWN PENDING";
            }
        }

        printState(state, extraInfo);
        cout.width(24);
        string stime = systemstatus.StateChangeDate;
        stime = stime.substr (0, 24);
        cout << stime << endl << endl;

        for ( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
        {
            if ( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
                // end of list
                break;

            cout << "Module ";
            cout.setf(ios::left);
            cout.width(7);
            cout << systemstatus.systemmodulestatus.modulestatus[i].Module;
            cout.width(29);
            state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;

            // get NIC functional state (degraded or not)
            bool degraded = false;

            try
            {
                int state;
                oam.getModuleStatus(systemstatus.systemmodulestatus.modulestatus[i].Module, state, degraded);
            }
            catch (...)
            {}

            string nicFun = " ";

            if (degraded)
                nicFun = "/" + DEGRADEDSTATE;

            printState(state, nicFun);

            cout.width(24);
            string stime = systemstatus.systemmodulestatus.modulestatus[i].StateChangeDate ;
            stime = stime.substr (0, 24);
            cout << stime << endl;
        }

        cout << endl;

        if ( systemstatus.systemmodulestatus.modulestatus.size() > 1)
        {
            // get and display Parent OAM Module
            cout << "Active Parent OAM Performance Module is '" << getParentOAMModule() << "'" << endl;

            //display Primary UM Module
            string PrimaryUMModuleName;

            try
            {
                oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
            }
            catch (...) {}

            if ( serverInstallType != oam::INSTALL_COMBINE_DM_UM_PM )
            {
                ModuleTypeConfig moduletypeconfig;

                try
                {
                    oam.getSystemConfig("um", moduletypeconfig);
                }
                catch (...)
                {}

                if ( moduletypeconfig.ModuleCount > 1 )
                {
                    if ( PrimaryUMModuleName != oam::UnassignedName )
                        cout << "Primary Front-End MariaDB ColumnStore Module is '" << PrimaryUMModuleName << "'" << endl;
                }
            }
            else
            {
                if ( PrimaryUMModuleName != oam::UnassignedName )
                    cout << "Primary Front-End MariaDB ColumnStore Module is '" << PrimaryUMModuleName << "'" << endl;
            }
        }

        //display local Query / PMwithUM feature, if enabled
        string PMwithUM;

        try
        {
            oam.getSystemConfig("PMwithUM", PMwithUM);
        }
        catch (...) {}

        if ( PMwithUM == "y" )
            cout << "Local Query Feature is enabled" << endl;

        //display MySQL replication feature, if enabled
        string MySQLRep;

        try
        {
            oam.getSystemConfig("MySQLRep", MySQLRep);
        }
        catch (...) {}

        if ( MySQLRep == "y" )
            cout << "MariaDB ColumnStore Replication Feature is enabled" << endl;

    }
    catch (exception& e)
    {
        cout << endl << "**** printSystemStatus Failed =  " << e.what() << endl;
        throw runtime_error("");
    }
}

/******************************************************************************************
 * @brief	printProcessStatus
 *
 * purpose:	get and Display Process Statuses
 *
 ******************************************************************************************/
void printProcessStatus(std::string port)
{
    SystemProcessStatus systemprocessstatus;
    ProcessStatus processstatus;
    ModuleTypeConfig moduletypeconfig;
    Oam oam;
    BRM::DBRM dbrm(true);

    int state;
    string extraInfo = " ";
    bool bRollback = false;
    bool bForce = false;
    bool bSuspend = false;

    if (dbrm.isDBRMReady())
    {
        if (dbrm.getSystemSuspended() > 0)
        {
            bSuspend = true;
            extraInfo = "WRITE_SUSPEND";
        }
        else if (dbrm.getSystemSuspendPending(bRollback) > 0)
        {
            bSuspend = true;

            if (bRollback)
            {
                extraInfo = "ROLLBACK";
            }
            else
            {
                extraInfo = "SUSPEND_PENDING";
            }
        }
        else if (dbrm.getSystemShutdownPending(bRollback, bForce) > 0)
        {
            bSuspend = true;

            if (bRollback)
            {
                extraInfo = "ROLLBACK";
            }
            else
            {
                extraInfo = "SHUTDOWN_PENDING";
            }
        }
    }

    cout << endl << "MariaDB ColumnStore Process statuses" << endl << endl;
    cout << "Process             Module    Status            Last Status Change        Process ID" << endl;
    cout << "------------------  ------    ---------------   ------------------------  ----------" << endl;

    try
    {
        oam.getProcessStatus(systemprocessstatus, port);

        string prevModule = systemprocessstatus.processstatus[0].Module;

        for ( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
        {
            if ( prevModule != systemprocessstatus.processstatus[i].Module)
                cout << endl;	//added a space line between different modules

            cout.setf(ios::left);
            cout.width(20);
            cout << systemprocessstatus.processstatus[i].ProcessName;
            cout.width(10);
            cout << systemprocessstatus.processstatus[i].Module;
            cout.width(18);
            state = systemprocessstatus.processstatus[i].ProcessOpState;

            // For these processes, if state is ACTIVE and we're in write
            // suspend, then we want to display the extra data instead of state.
            // Otherwise, we ignore extra data and display state.
            if (state == ACTIVE && bSuspend &&
                    (   systemprocessstatus.processstatus[i].ProcessName == "DMLProc"
                        || systemprocessstatus.processstatus[i].ProcessName == "DDLProc"
                        || systemprocessstatus.processstatus[i].ProcessName == "WriteEngineServer"))
            {
                printState(LEAVE_BLANK, extraInfo);
            }
            else
            {
                state = systemprocessstatus.processstatus[i].ProcessOpState;
                printState(state, " ");
            }

            cout.width(24);
            string stime = systemprocessstatus.processstatus[i].StateChangeDate ;
            stime = stime.substr (0, 24);
            cout << stime;

            if ( state == COLD_STANDBY )
            {
                cout << endl;
                continue;
            }
            else
            {
                cout.setf(ios::right);
                cout.width(12);

                if ( systemprocessstatus.processstatus[i].ProcessID != 0 )
                    cout << systemprocessstatus.processstatus[i].ProcessID << endl;
                else
                    cout << endl;
            }

            cout.unsetf(ios::right);

            prevModule = systemprocessstatus.processstatus[i].Module;

        }
    }
    catch (exception& e)
    {
        cout << endl << "**** printProcessStatus Failed =  " << e.what() << endl;
        throw runtime_error("");
    }
}

/******************************************************************************************
 * @brief	printModuleCpuUsers
 *
 * purpose:	get and Display Module TOP CPU users
 *
 ******************************************************************************************/
void printModuleCpuUsers(TopProcessCpuUsers topprocesscpuusers)
{
    cout << "Module '" + topprocesscpuusers.ModuleName + "' Top CPU Users" << endl << endl;
    cout << "Process             CPU Usage %" << endl;
    cout << "-----------------   -----------" << endl;

    for ( unsigned int i = 0 ; i < topprocesscpuusers.processcpuuser.size(); i++)
    {
        cout.setf(ios::left);
        cout.width(25);
        cout << topprocesscpuusers.processcpuuser[i].ProcessName;
        cout.width(10);
        cout << topprocesscpuusers.processcpuuser[i].CpuUsage << endl;
    }

    cout << endl;
}

/******************************************************************************************
 * @brief	printModuleCpu
 *
 * purpose:	get and Display Module CPU Usage
 *
 ******************************************************************************************/
void printModuleCpu(ModuleCpu modulecpu)
{
    Oam oam;

    cout << endl << "Module '" + modulecpu.ModuleName + "' CPU Usage % = " + oam.itoa(modulecpu.CpuUsage) << endl;
}

/******************************************************************************************
 * @brief	printModuleMemoryUsers
 *
 * purpose:	get and Display Module TOP Memory users
 *
 ******************************************************************************************/
void printModuleMemoryUsers(TopProcessMemoryUsers topprocessmemoryusers)
{
    cout << "Module '" + topprocessmemoryusers.ModuleName + "' Top Memory Users (in bytes)" << endl << endl;
    cout << "Process             Memory Used  Memory Usage %" << endl;
    cout << "-----------------   -----------  --------------" << endl;

    for ( unsigned int i = 0 ; i < topprocessmemoryusers.processmemoryuser.size(); i++)
    {
        cout.setf(ios::left);
        cout.width(20);
        cout << topprocessmemoryusers.processmemoryuser[i].ProcessName;
        cout.width(19);
        cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsed;
        cout.width(3);
        cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsage << endl;
    }

    cout << endl;
}

/******************************************************************************************
 * @brief	printModuleMemory
 *
 * purpose:	get and Display Module Memory Usage
 *
 ******************************************************************************************/
void printModuleMemory(ModuleMemory modulememory)
{
    Oam oam;
    cout.setf(ios::left);
    cout.width(8);
    cout << modulememory.ModuleName;
    cout.width(11);
    cout << oam.itoa(modulememory.MemoryTotal);
    cout.width(10);
    cout << oam.itoa(modulememory.MemoryUsed);
    cout.width(13);
    cout << oam.itoa(modulememory.cache);
    cout.width(9);
    cout << oam.itoa(modulememory.MemoryUsage);
    cout.width(12);
    cout << oam.itoa(modulememory.SwapTotal);
    cout.width(16);
    cout << oam.itoa(modulememory.SwapUsed);
    cout.width(7);
    cout << oam.itoa(modulememory.SwapUsage);
    cout << endl;
}

/******************************************************************************************
 * @brief	printModuleDisk
 *
 * purpose:	get and Display Module disk usage
 *
 ******************************************************************************************/
void printModuleDisk(ModuleDisk moduledisk)
{
    Oam oam;

    cout << "Module '" + moduledisk.ModuleName + "' Disk Usage (in 1K blocks)" << endl << endl;
    cout << "Mount Point                    Total Blocks  Used Blocks   Usage %" << endl;
    cout << "-----------------------------  ------------  ------------  -------" << endl;

    string etcdir = std::string(MCSSYSCONFDIR) + "/columnstore";

    for ( unsigned int i = 0 ; i < moduledisk.diskusage.size(); i++)
    {
        //skip mounts to other server disk
        if ( moduledisk.diskusage[i].DeviceName.find("/mnt", 0) == string::npos &&
                moduledisk.diskusage[i].DeviceName.find(etcdir, 0) == string::npos )
        {
            cout.setf(ios::left);
            cout.width(31);

            if (moduledisk.diskusage[i].DeviceName.length() > 29)
            {
                cout << "..." + moduledisk.diskusage[i].DeviceName.substr(moduledisk.diskusage[i].DeviceName.length() - 26);
            }
            else
            {
                cout << moduledisk.diskusage[i].DeviceName;
            }

            cout.width(14);
            cout << moduledisk.diskusage[i].TotalBlocks;
            cout.width(17);
            cout << moduledisk.diskusage[i].UsedBlocks;
            cout.width(2);
            cout << moduledisk.diskusage[i].DiskUsage << endl;
        }
    }

    cout << endl;
}

/******************************************************************************************
 * @brief	printModuleResources
 *
 * purpose:	get and Display Module resource usage
 *
 ******************************************************************************************/
void printModuleResources(TopProcessCpuUsers topprocesscpuusers, ModuleCpu modulecpu, TopProcessMemoryUsers topprocessmemoryusers, ModuleMemory modulememory, ModuleDisk moduledisk)
{
    Oam oam;
    string etcdir = std::string(MCSSYSCONFDIR) + "/columnstore";

    cout << endl << "Module '" + topprocesscpuusers.ModuleName + "' Resource Usage" << endl << endl;

    cout << "CPU: " + oam.itoa(modulecpu.CpuUsage) << "% Usage" << endl;

    cout << "Mem:  " << oam.itoa(modulememory.MemoryTotal) << "k total, " << oam.itoa(modulememory.MemoryUsed);
    cout << "k used, " << oam.itoa(modulememory.cache) << "k cache, " << oam.itoa(modulememory.MemoryUsage) << "% Usage" << endl;
    cout << "Swap: " << oam.itoa(modulememory.SwapTotal) << " k total, " << oam.itoa(modulememory.SwapUsed);
    cout << "k used, " << oam.itoa(modulememory.SwapUsage) << "% Usage" << endl;

    cout << "Top CPU Process Users: ";

    for ( unsigned int i = 0 ; i < topprocesscpuusers.processcpuuser.size(); i++)
    {
        cout << topprocesscpuusers.processcpuuser[i].ProcessName << " ";
        cout << topprocesscpuusers.processcpuuser[i].CpuUsage;

        if ( i + 1 != topprocesscpuusers.processcpuuser.size() )
            cout << "%, ";
        else
            cout << "%";
    }

    cout << endl;

    cout << "Top Memory Process Users: ";

    for ( unsigned int i = 0 ; i < topprocessmemoryusers.processmemoryuser.size(); i++)
    {
        cout << topprocessmemoryusers.processmemoryuser[i].ProcessName << " ";
        cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsage;

        if ( i + 1 != topprocessmemoryusers.processmemoryuser.size() )
            cout << "%, ";
        else
            cout << "%";
    }

    cout << endl;

    cout << "Disk Usage: ";

    for ( unsigned int i = 0 ; i < moduledisk.diskusage.size(); i++)
    {
        //skip mounts to other server disk
        if ( moduledisk.diskusage[i].DeviceName.find("/mnt", 0) == string::npos &&
                moduledisk.diskusage[i].DeviceName.find(etcdir, 0) == string::npos )
        {
            cout << moduledisk.diskusage[i].DeviceName << " ";
            cout << moduledisk.diskusage[i].DiskUsage;

            if ( i + 1 != moduledisk.diskusage.size() )
                cout << "%, ";
            else
                cout << "%";
        }
    }

    cout << endl << endl;
}

/******************************************************************************************
 * @brief	printModuleResources
 *
 * purpose:	get and Display Module resource usage
 *
 ******************************************************************************************/
void printState(int state, std::string addInfo)
{
    switch (state)
    {
        case MAN_OFFLINE:
            cout << MANOFFLINE + addInfo;
            break;

        case AUTO_OFFLINE:
            cout << AUTOOFFLINE + addInfo;
            break;

        case MAN_INIT:
            cout << MANINIT + addInfo;
            break;

        case AUTO_INIT:
            cout << AUTOINIT + addInfo;
            break;

        case ACTIVE:
            cout << ACTIVESTATE + addInfo;
            break;

        case LEAVE_BLANK:
            cout << addInfo;
            break;

        case STANDBY:
            cout << STANDBYSTATE + addInfo;
            break;

        case FAILED:
            cout << FAILEDSTATE + addInfo;
            break;

        case UP:
            cout << UPSTATE + addInfo;
            break;

        case DOWN:
            cout << DOWNSTATE + addInfo;
            break;

        case COLD_STANDBY:
            cout << COLDSTANDBYSTATE + addInfo;
            break;

        case INITIAL:
            cout << INITIALSTATE + addInfo;
            break;

        case MAN_DISABLED:
            cout << MANDISABLEDSTATE + addInfo;
            break;

        case AUTO_DISABLED:
            cout << AUTODISABLEDSTATE + addInfo;
            break;

        case STANDBY_INIT:
            cout << STANDBYINIT + addInfo;
            break;

        case BUSY_INIT:
            cout << BUSYINIT + addInfo;
            break;

        case DEGRADED:
            cout << DEGRADEDSTATE + addInfo;
            break;

        default:
            cout << INITIALSTATE + addInfo;
            break;
    }
}

/******************************************************************************************
 * @brief	checkPromptThread
 *
 * purpose:	check for exit out of repeat command
 *
 ******************************************************************************************/
static void checkPromptThread()
{
    char* pcommand = 0;

    while (true)
    {
        // check input
        pcommand = readline("");

        if (!pcommand)                          // user hit <Ctrl>-D
        {
            repeatStop = true;
            break;
        }

        free(pcommand);
        pcommand = 0;
    }

    pthread_exit(0);
    return;
}

/******************************************************************************************
 * @brief	getParentOAMModule
 *
 * purpose:	get Parent OAm Module name
 *
 ******************************************************************************************/
std::string getParentOAMModule()
{
    Oam oam;

    // Get Parent OAM module Name
    try
    {
        string parentOAMModule;
        oam.getSystemConfig("ParentOAMModuleName", parentOAMModule);
        return parentOAMModule;
    }
    catch (...)
    {
        cout << endl << "**** Failed : Failed to read Parent OAM Module Name" << endl;
        exit(-1);
    }
}

/******************************************************************************************
 * @brief	checkForDisabledModules
 *
 * purpose:	Chcek and report any modules in a disabled state
 *
 ******************************************************************************************/
bool checkForDisabledModules()
{

    SystemModuleTypeConfig systemmoduletypeconfig;
    Oam oam;

    try
    {
        oam.getSystemConfig(systemmoduletypeconfig);
    }
    catch (...)
    {
        return false;
    }

    bool found = false;
    bool dbroot = false;

    for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
        int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

        if ( moduleCount == 0)
            continue;

        string moduleType = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

        DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
        {
            string moduleName = (*pt).DeviceName;

            // report DISABLED modules
            try
            {
                int opState;
                bool degraded;
                oam.getModuleStatus(moduleName, opState, degraded);

                if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
                {
                    if (!found)
                    {
                        cout << "   NOTE: These module(s) are DISABLED: ";
                        found = true;
                    }

                    cout << moduleName << " ";

                    if ( moduleType == "um" )
                        continue;

                    //check if module has any dbroots assigned to it
                    string PMID = moduleName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);;
                    DBRootConfigList dbrootConfigList;

                    try
                    {
                        oam.getPmDbrootConfig(atoi(PMID.c_str()), dbrootConfigList);

                        if ( dbrootConfigList.size() != 0 )
                            dbroot = true;
                    }
                    catch (exception& e)
                    {}
                }
            }
            catch (...)
            {}
        }
    }

    if (found)
        cout << endl << endl;

    if (dbroot)
        return false;

    return true;
}

/** @brief Ask the user for cancel/wait/rollback/force
 *
 *  When a Shutdown, stop, restart or suspend operation is
 *  requested but there are active transactions of some sort,
 *  we ask the user what to do.
 */
CC_SUSPEND_ANSWER AskSuspendQuestion(int CmdID)
{
    char* szAnswer = 0;
    char* p;
    string argument = "cancel";

    const char* szCommand = "Unknown";

    switch (CmdID)
    {
        case 16:
            szCommand = "stop";
            break;

        case 17:
            szCommand = "shutdown";
            break;

        case 19:
            szCommand = "restart";
            break;

        case 28:
            szCommand = "switch parent oam";
            break;

        case 32:
            szCommand = "suspend";
            break;

        default:
            return CANCEL;
            break;
    }

    cout << "Your options are:" << endl
         << "    Cancel    -- Cancel the " << szCommand << " request" << endl
         << "    Wait      -- Wait for write operations to end and then " << szCommand << endl;

//		 << "    Rollback  -- Rollback all transactions and then " << szCommand << endl;
    if (CmdID != 28 && CmdID != 32)
    {
        cout << "    Force     -- Force a " << szCommand << endl;
    }

    while (true)
    {
        argument = "cancel";
        // read input
        szAnswer = readline("What would you like to do: [Cancel]: ");

        if (szAnswer && *szAnswer)
        {
            p = strtok(szAnswer, " ");
            argument = p;
            free(szAnswer);
            szAnswer = 0;
        }

        // In case they just hit return.
        if (szAnswer)
        {
            free(szAnswer);
            szAnswer = 0;
        }

        // convert argument into lowercase
        transform(argument.begin(), argument.end(), argument.begin(), to_lower());

        if ( argument == "cancel")
        {
            return CANCEL;
        }
        else if ( argument == "wait")
        {
            return WAIT;
        }
//		else if( argument == "rollback")
//		{
//			return ROLLBACK;
//		}
        else if ( argument == "force" && (CmdID == 16 || CmdID == 17 || CmdID == 19))
        {
            return FORCE;
        }
        else
        {
            cout << argument << " is an invalid response" << endl;
        }
    }
}

// Make a connection to the PM that uses DBRoot1. Used in redistribute
// return true if successful, false if fail.
bool connectToDBRoot1PM(Oam& oam, boost::scoped_ptr<MessageQueueClient>&  msgQueueClient)
{
    int pmId = 0;
    ModuleTypeConfig moduletypeconfig;

    try
    {
        oam.getDbrootPmConfig(1, pmId);
        oam.getSystemConfig("pm", moduletypeconfig);
    }
    catch (const std::exception& ex)
    {
        cerr << "Caught exception when getting DBRoot1" << ex.what() << endl;
        return false;
    }
    catch (...)
    {
        cerr << "Caught exception when  getting DBRoot1 -- unknown" << endl;
        return false;
    }

    // Find the PM that has dbroot1, then make connection to its WES.
    ostringstream oss;
    oss << "pm" << pmId << "_WriteEngineServer";

    try
    {
        msgQueueClient.reset(new MessageQueueClient(oss.str()));
    }
    catch (const std::exception& ex)
    {
        cerr << "Caught exception when connecting to " << oss.str() << " : " << ex.what() << endl;
        return false;
    }
    catch (...)
    {
        cerr << "Caught exception when connecting to " << oss.str() << " : unknown" << endl;
    }

    return true;
}

bool SendToWES(Oam& oam, ByteStream bs)
{
    boost::scoped_ptr<MessageQueueClient>  msgQueueClient;

    if (!connectToDBRoot1PM(oam, msgQueueClient))
        return false;

    uint32_t status = RED_STATE_UNDEF;
    msgQueueClient->write(bs);

    SBS sbs;
    sbs = msgQueueClient->read();

    if (sbs->length() == 0)
    {
        cerr << "WriteEngineServer returned an empty stream. Might be a network error" << endl;
    }
    else if (sbs->length() < 5)
    {
        cerr << "WriteEngineServer returned too few bytes. Refistribute status is unknown" << endl;
    }
    else
    {
        ByteStream::byte wesMsgId;
        *sbs >> wesMsgId;
        *sbs >> status;

        string msg;
        *sbs >> msg;
        cout << "WriteEngineServer returned status " << status << ": " << msg << endl;
    }

    return true;
}
// vim:ts=4 sw=4:
