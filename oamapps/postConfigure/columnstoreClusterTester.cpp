/* Copyright (C) 2016 MariaDB Corporaton

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
* $Id: columnstoreClusterTester.cpp 64 2006-10-12 22:21:51Z dhill $
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
#include <string.h>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "alarmmanager.h"

using namespace std;
using namespace oam;
using namespace config;
using namespace alarmmanager;

#include "helpers.h"
using namespace installer;

using namespace std;

typedef struct ModuleIP_struct
{
	std::string     IPaddress;
	std::string     status;
} ModuleIP;

typedef std::vector<ModuleIP> ModuleIPList;

char* pcommand = 0;
string prompt;

void checkSuccess( bool pass);

bool check = true;

int main(int argc, char *argv[])
{
    ModuleIPList moduleiplist;

    string IPaddresses = "";
    string OS = "";
    string user = "";
    string password = ""; 

    for( int i = 1; i < argc; i++ )
    {
	if( string("-h") == argv[i] || string("--help") == argv[i]) {
		cout << endl;
		cout << "This is the MariaDB Columnstore Cluster System Test tool." << endl;
		cout << "It will run a set of test to check the checkup of the system." << endl;
		cout << "This can be run prior to the install to make sure the servers/nodes" << endl;
		cout << "are configured properly" << endl;
		cout << "User will be prompted to provide the server/node IP Addresses, OS" << endl; 
		cout << "Root/Non-root user install name, and password" << endl << endl;
		cout << "Items that are checked:" << endl;
		cout << "	Node ping test" << endl;
		cout << "	Node SSH test" << endl;
		cout << "	OS version" << endl;
		cout << "	Firewall settings" << endl;
		cout << "	Locale settings" << endl;
		cout << "	Dependent packages installed" << endl;
		cout << "	ColumnStore Port test" << endl << endl;
		cout << "Usage: columnstoreClusterTester -h -i -i -u -p -c" << endl;
		cout << "   -h  	Help" << endl;
		cout << "   -i  	IP Addresses, starting with local (x.x.x.x,y.y.y.y)" << endl;
		cout << "   -o  	OS Version (centos6, centos7, debian8, suse12, ubuntu16)" << endl;
		cout << "   -u  	Username (root or 'non-root username')" << endl;		
		cout << "   -p  	Password (User Password or 'ssh' if using SSH-KEYS)" << endl;
		cout << "   -c  	Continue on failures" << endl << endl;
		cout << "Dependent package : 'nmap', 'readline', and 'boost' packages need to be installed locally" << endl;
		exit (0);
	}
	else if( string("-i") == argv[i] ) {
		i++;
		if (i >= argc ) {
			cout << "   ERROR: IP Addresses not provided" << endl;
			exit (1);
		}
		IPaddresses = argv[i];
	}
	else if( string("-o") == argv[i] ) {
		i++;
		if (i >= argc ) {
			cout << "   ERROR: OS type not provided" << endl;
			exit (1);
		}
		OS = argv[i];
	}
	else if( string("-u") == argv[i] ) {
		i++;
		if (i >= argc ) {
			cout << "   ERROR: Username not provided" << endl;
			exit (1);
		}
		user = argv[i];
	}
	else if( string("-p") == argv[i] ) {
		i++;
		if (i >= argc ) {
			cout << "   ERROR: Password not provided" << endl;
			exit (1);
		}
		password = argv[i];
	}
	else if( string("-c") == argv[i] ) {
		check = false;
	}
    }
    
	cout << endl << endl;
	cout << "*** This is the MariaDB Columnstore Cluster System test tool ***" << endl << endl;

	if ( IPaddresses.empty() )
	{
	    cout << endl;
	    //get Ip Addresses
	    prompt = "Enter List of IP Addresses of each server/node starting with the local first (x.x.x.x,y.y.y.y) > ";
	    pcommand = readline(prompt.c_str());
	    if (pcommand) {
		    if (strlen(pcommand) > 0) IPaddresses = pcommand;
		    free(pcommand);
		    pcommand = 0;
	    }
	}
	
	if ( IPaddresses.empty() )
	{
		cout << "Error: no IP addresses where entered, exiting..." << endl;
		exit (1);
	}
	else
	{
		boost::char_separator<char> sep(",:");
		boost::tokenizer< boost::char_separator<char> > tokens(IPaddresses, sep);
		for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
				it != tokens.end();
				++it)
		{
			ModuleIP moduleip;
			moduleip.IPaddress = *it;
			if ( it == tokens.begin() )
			  moduleip.status = "local";
			else
			  moduleip.status = "pass";
			moduleiplist.push_back(moduleip);
		}
	}

	if ( OS.empty() )
	{
	    cout << endl;
	    //get OS
	    prompt = "Enter OS version (centos6, centos7, debian8, suse12, ubuntu16) > ";
	    pcommand = readline(prompt.c_str());
	    if (pcommand) {
		    if (strlen(pcommand) > 0) OS = pcommand;
		    free(pcommand);
		    pcommand = 0;
	    }
	}
	
	if ( OS.empty() )
	{
		cout << "Error: no OS Version was entered, exiting..." << endl;
		exit (1);
	}

	if ( user.empty() )
	{
	    cout << endl;
	    //get User
	    prompt = "Enter Install user (root or 'non-root username') > ";
	    pcommand = readline(prompt.c_str());
	    if (pcommand) {
		    if (strlen(pcommand) > 0) user = pcommand;
		    free(pcommand);
		    pcommand = 0;
	    }
	}
	
	if ( user.empty() )
	{
		cout << "Error: no User type was entered, exiting..." << endl;
		exit (1);
	}


	if ( password.empty() )
	{
	    cout << endl;
	    //get root password
	    prompt = "Enter User Password or 'ssh' if using SSH-KEYS, which is used to access other nodes > ";
	    pcommand = readline(prompt.c_str());
	    if (pcommand) {
		    if (strlen(pcommand) > 0) password = pcommand;
		    free(pcommand);
		    pcommand = 0;
	    }
	}
	
	if ( password.empty() )
	{
		cout << "Error: no password or 'ssh' was entered, exiting..." << endl;
		exit (1);
	}

	//
	// ping test
	//
	cout << endl << "Run Ping access Test" << endl << endl;
	
	string cmdLine = "ping ";
	string cmdOption = " -c 1 -w 5 >> /dev/null";
	string cmd;

	bool pass = true;
	ModuleIPList::iterator  list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;

	    if ( status == "local" )
	      continue;

	    cout << " Testing " + ipAddress + " : ";
	    cout.flush();
	    
	    // perform login test
	    string cmd = "./remote_command.sh " + ipAddress + " " + password + " ls";
	    int rtnCode = system(cmd.c_str());
	    if  (WEXITSTATUS(rtnCode) == 0 )
		cout << " PASSED" << endl;
	    else
	    {
		ModuleIP moduleip;
		moduleip.IPaddress = ipAddress;
		moduleip.status = "failed";
		
		moduleiplist.erase(list);
		moduleiplist.insert(list, moduleip);

		cout << " FAILED, check connection" << endl;
		pass = false;
	    }
	}

	checkSuccess(pass);

	//
	// ssh login test
	//
	cout << endl << "Run Login access Test" << endl << endl;
	
	pass = true;
	list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;
	    if ( status == "local" || status == "failed" )
	      continue;
	    
	    cout << " Testing " + ipAddress + " : ";
	    cout.flush();
	    
	    // perform ping test
	    cmd = cmdLine + ipAddress + cmdOption;
	    int rtnCode = system(cmd.c_str());
	    if  (WEXITSTATUS(rtnCode) == 0 )
		cout << " PASSED" << endl;
	    else
	    {
		cout << " FAILED, check password or ssh-key setup" << endl;
		pass = false;
	    }
	}

	checkSuccess(pass);
	
	//
	// OS compare
	//
	cout << endl << "Getting OS versions" << endl << endl;
	
	string OScmd = "./os_check.sh > /tmp/os_check 2>&1";

	pass = true;
	list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;
	    if ( status == "failed" )
	      continue;
	    
	    if ( status == "local" )
	    {
		int rtnCode = system(OScmd.c_str());
		if  (WEXITSTATUS(rtnCode) == 0 )
		{
		    cout << " Local OS Version : ";
		    cout.flush();
		    system("cat /tmp/os_check");
		}
		else
		{
		    cout << " FAILED, os_check.sh failed, check /tmp/os_check" << endl;
		    pass = false;
		}
	    }
	    else
	    {
		cout << " OS version for " << ipAddress << " : ";
		cout.flush();

		// push os_check to remote node
		string cmd = "./remote_scp_put.sh " + ipAddress + " " + password + " os_check.sh 1 > /tmp/put_os_check.log";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) == 0) 
		{  
		    //run command
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " '" + OScmd + " 1' > /tmp/run_os_check.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/os_check 1 > /tmp/get_os_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    system("cat os_check");
			    
			    //compare with local
			    int rtnCode = system("diff /tmp/os_check os_check > /dev/null 2>&1");
			    if (WEXITSTATUS(rtnCode) != 0)
			    {
				cout << " FAILED, doesn't match local servers OS version" << endl;
				pass = false;
			    }
			    
			    unlink("os_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/get_os_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_os_check.log" << endl;
			pass = false;
		    }
		}
		else
		{
		    cout << " FAILED, /tmp/put_os_check.log" << endl;
		    pass = false;
		}
	    }
	}

	checkSuccess(pass);
	
	
	//
	// Locale compare
	//
	cout << endl << "Getting Locale" << endl << endl;
	
	string Localecmd = "locale | grep LANG= > /tmp/locale_check 2>&1";
	string LocaleREMOTEcmd = "'locale | grep LANG= > /tmp/locale_check 2>&1'";

	pass = true;
	list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;
	    if ( status == "failed" )
	      continue;
	    
	    if ( status == "local" )
	    {
		int rtnCode = system(Localecmd.c_str());
		if  (WEXITSTATUS(rtnCode) == 0 )
		{
		    cout << " Local Locale Setting : ";
		    cout.flush();
		    system("cat /tmp/locale_check");
		}
		else
		{
		    cout << " FAILED, locale command failed, check /tmp/locale_check" << endl;
		    pass = false;
		}
	    }
	    else
	    {
		cout << " Getting Locale for " + ipAddress + " : ";
		cout.flush();
	    
		//run command
		string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + LocaleREMOTEcmd + " 1 > /tmp/run_locale.log";
		int rtnCode = system(cmd.c_str());
		if  (WEXITSTATUS(rtnCode) != 1 )
		{
		    //get results
		    string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/locale_check 1 > /tmp/get_locale_check.log";
		    int rtnCode = system(cmd.c_str());
		    if (WEXITSTATUS(rtnCode) == 0)
		    {
			// get local again
			system(Localecmd.c_str());

			system("cat locale_check");

			//compare with local
			int rtnCode = system("diff /tmp/locale_check locale_check > /dev/null 2>&1");
			if (WEXITSTATUS(rtnCode) != 0)
			{
			    cout << " FAILED, doesn't match local servers Locale" << endl;
			    pass = false;
			}
			
			unlink("locale_check");
		    }
		    else
		    {
			cout << " FAILED, check /tmp/get_locale_check.log" << endl;
			pass = false;
		    }
		}
		else
		{
		    cout << " FAILED, check /tmp/run_locale.log" << endl;
		    pass = false;
		}
	    }
	}

	checkSuccess(pass);
	

	//
	// Check SELINUX
	//
	cout << endl << "Checking Firewall setting - SELINUX should be disabled" << endl << endl;
	
	string SELINUXcmd = "cat /etc/selinux/config | grep SELINUX | grep enforcing > /tmp/selinux_check 2>&1";
	string SELINUXREMOTEcmd = "'cat /etc/selinux/config' > /tmp/selinux_check 2>&1";
	string SELINUXREMOTECHECKcmd = "cat selinux_check | grep SELINUX | grep enforcing > /tmp/selinux_check 2>&1";

	pass = true;
	list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;
	    if ( status == "failed" )
	      continue;
	    
	    if ( status == "local" )
	    {
		cout << " Local SELINUX setting check : ";
		cout.flush();
	    
		//check if config file exist
		ifstream oldFile ("/etc/selinex/config");
		if (!oldFile)
		    cout << " PASSED, is disabled" << endl;
		else
		{
		    int rtnCode = system(SELINUXcmd.c_str());
		    if  (WEXITSTATUS(rtnCode) == 0 )
		    {
			cout << " FAILED, /etc/selinex/config is enabled, please disable" << endl;
			pass = false;
		    }
		    else
		    {
			cout << " PASSED, it is disabled" << endl;
		    }
		}
	    }
	    else
	    {
		cout << " Checking SELINUX for " + ipAddress + " : ";
		cout.flush();
	    
		//run command
		string cmd = "./remote_command.sh " + ipAddress + " " + password + " 'ls /etc/selinex/config' 1 > /tmp/check_selinux.log";
		int rtnCode = system(cmd.c_str());
		if  (WEXITSTATUS(rtnCode) != 0 )
		{
		  cout << " PASSED, it is disabled" << endl;
		}
		else
		{
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + SELINUXREMOTEcmd + " 1 > /tmp/run_selinux.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/selinux_check 1 > /tmp/get_selinux_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    int rtnCode = system(SELINUXREMOTECHECKcmd.c_str());
			    if  (WEXITSTATUS(rtnCode) == 0 )
			    {
				cout << " FAILED, /etc/selinex/config is enabled, please disable" << endl;
				pass = false;
			    }
			    else
			    {
				cout << " PASSED, it is disabled" << endl;
			    }

			    unlink("selinux_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/get_selinux_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_selinux.log" << endl;
			pass = false;
		    }
		}
	    }
	}

	checkSuccess(pass);
	
	//
	// Check IPTABLES for Centos6
	//
	if ( OS == "centos6")
	{
	    cout << endl << "Checking Firewall setting - IPTABLES should be disabled" << endl << endl;
	    
	    string IPTABLEScmd = "chkconfig | grep iptables | grep on > /tmp/iptables_check 2>&1";
	    string IPTABLESREMOTEcmd = "chkconfig > /tmp/iptables_check 2>&1";
	    string IPTABLESREMOTECHECKcmd = "cat iptables_check | grep iptables | grep on > /tmp/iptables_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		if ( status == "local" )
		{
		    cout << " Local IPTABLES setting check : ";
		    cout.flush();

		    int rtnCode = system(IPTABLEScmd.c_str());
		    if  (WEXITSTATUS(rtnCode) == 0 )
		    {
			cout << " FAILED, iptables is enabled, please disable" << endl;
			pass = false;
		    }
		    else
		    {
			cout << " PASSED, it is disabled" << endl;
		    }
		}
		else
		{
		    cout << " Checking IPTABLES for " + ipAddress + " : ";
		    cout.flush();
		
		    //run command
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + IPTABLESREMOTEcmd + " 1 > /tmp/run_iptables.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/iptables_check 1 > /tmp/get_iptables_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    int rtnCode = system(IPTABLESREMOTECHECKcmd.c_str());
			    if  (WEXITSTATUS(rtnCode) == 0 )
			    {
				cout << " FAILED, iptables is enabled, please disable" << endl;
				pass = false;
			    }
			    else
			    {
				cout << " PASSED, it is disabled" << endl;
			    }

			    unlink("iptables_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/get_iptables_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_iptables.log" << endl;
			pass = false;
		    }
		}
	    }

	    checkSuccess(pass);
	}
	
	//
	// Check UFW for ubuntu 16
	//
	if ( OS == "ubuntu16")
	{
	    cout << endl << "Checking Firewall setting - UFW should be disabled" << endl << endl;
	    
	    string UFWcmd = "chkconfig | grep ufw | grep on > /tmp/ufw_check 2>&1";
	    string UFWREMOTEcmd = "chkconfig > /tmp/ufw_check 2>&1";
	    string UFWREMOTECHECKcmd = "cat ufw_check | grep ufw | grep on > /tmp/ufw_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		if ( status == "local" )
		{
		    cout << " Local UFW setting check : ";
		    cout.flush();

		    int rtnCode = system(UFWcmd.c_str());
		    if  (WEXITSTATUS(rtnCode) == 0 )
		    {
			cout << " FAILED, ufw is enabled, please disable" << endl;
			pass = false;
		    }
		    else
		    {
			cout << " PASSED, it is disabled" << endl;
		    }
		}
		else
		{
		    cout << " Checking UFW for " + ipAddress + " : ";
		    cout.flush();
		
		    //run command
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + UFWREMOTEcmd + " 1 > /tmp/run_ufw.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/iptables_check 1 > /tmp/get_ufw_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    int rtnCode = system(UFWREMOTECHECKcmd.c_str());
			    if  (WEXITSTATUS(rtnCode) == 0 )
			    {
				cout << " FAILED, ufw is enabled, please disable" << endl;
				pass = false;
			    }
			    else
			    {
				cout << " PASSED, it is disabled" << endl;
			    }

			    unlink("ufw_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/get_ufw_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_ufw.log" << endl;
			pass = false;
		    }
		}
	    }

	    checkSuccess(pass);
	}
	
	//
	// Check filewalls for SUSE
	//
	if ( OS == "suse12")
	{
	    cout << endl << "Checking Firewall setting - rcSuSEfirewall2 should be disabled" << endl << endl;
	    
	    string rcSuSEfirewall2cmd = "/sbin/rcSuSEfirewall2 status | grep active > /tmp/rcSuSEfirewall2_check 2>&1";
	    string rcSuSEfirewall2REMOTEcmd = "'/sbin/rcSuSEfirewall2 status' > /tmp/rcSuSEfirewall2_check 2>&1";
	    string rcSuSEfirewall2REMOTECHECKcmd = "cat rcSuSEfirewall2_check | grep active > /tmp/rcSuSEfirewall2_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		if ( status == "local" )
		{
		    cout << " Local rcSuSEfirewall2 setting check : ";
		    cout.flush();

		    int rtnCode = system(rcSuSEfirewall2cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) == 0 )
		    {
			cout << " FAILED, rcSuSEfirewall2 is enabled, please disable" << endl;
			pass = false;
		    }
		    else
		    {
			cout << " PASSED, it is disabled" << endl;
		    }
		}
		else
		{
		    cout << " Checking rcSuSEfirewall2 for " + ipAddress + " : ";
		    cout.flush();
		
		    //run command
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + rcSuSEfirewall2REMOTEcmd + " 1 > /tmp/run_rcSuSEfirewall2.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/firewalld_check 1 > /tmp/get_rcSuSEfirewall2_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    int rtnCode = system(rcSuSEfirewall2REMOTECHECKcmd.c_str());
			    if  (WEXITSTATUS(rtnCode) == 0 )
			    {
				cout << " FAILED, rcSuSEfirewall2 is enabled, please disable" << endl;
				pass = false;
			    }
			    else
			    {
				cout << " PASSED, it is disabled" << endl;
			    }

			    unlink("rcSuSEfirewall2_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/rcSuSEfirewall2_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_rcSuSEfirewall2.log" << endl;
			pass = false;
		    }
		}
	    }

	    checkSuccess(pass);
	}
	
	//
	// Check filewalls for non-Centos6
	//
	if ( OS != "centos6")
	{
	    cout << endl << "Checking Firewall setting - firewalld should be disabled" << endl << endl;
	    
	    string FIREWALLDcmd = "systemctl status firewalld | grep running > /tmp/firewalld_check 2>&1";
	    string FIREWALLDREMOTEcmd = "'systemctl status firewalld > /tmp/firewalld_check 2>&1'";
	    string FIREWALLDREMOTECHECKcmd = "cat firewalld_check | grep running > /tmp/firewalld_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		if ( status == "local" )
		{
		    cout << " Local firewalld setting check : ";
		    cout.flush();

		    int rtnCode = system(FIREWALLDcmd.c_str());
		    if  (WEXITSTATUS(rtnCode) == 0 )
		    {
			cout << " FAILED, firewalld is enabled, please disable" << endl;
			pass = false;
		    }
		    else
		    {
			cout << " PASSED, it is disabled" << endl;
		    }
		}
		else
		{
		    cout << " Checking firewalld for " + ipAddress + " : ";
		    cout.flush();
		
		    //run command
		    string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + FIREWALLDREMOTEcmd + " 1 > /tmp/run_firewalld.log";
		    int rtnCode = system(cmd.c_str());
		    if  (WEXITSTATUS(rtnCode) != 1 )
		    {
			//get results
			string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/firewalld_check 1 > /tmp/get_firewalld_check.log";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
			{
			    int rtnCode = system(FIREWALLDREMOTECHECKcmd.c_str());
			    if  (WEXITSTATUS(rtnCode) == 0 )
			    {
				cout << " FAILED, firewalld is enabled, please disable" << endl;
				pass = false;
			    }
			    else
			    {
				cout << " PASSED, it is disabled" << endl;
			    }

			    unlink("firewalld_check");
			}
			else
			{
			    cout << " FAILED, check /tmp/get_firewalld_check.log" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			cout << " FAILED, check /tmp/run_firewalld.log" << endl;
			pass = false;
		    }
		}
	    }

	    checkSuccess(pass);
	}
	
	//
	// Dependent package install check
	//

	bool nmapPass = true;

	//centos
	
	const std::string centosPgk[] = {	
	    "boost",
	    "expect", 
	    "perl",
	    "perl-DBI", 
	    "openssl",
	    "zlib",
	    "file",
	    "sudo",
	    "perl-DBD-MySQL", 
	    "libaio",
	    "rsync",
	    "snappy",
	    "net-tools",
	    ""
	};

	if ( OS == "centos6" || OS == "centos7")
	{
	    cout << endl << "Checking Dependent Package installations" << endl << endl;
	    
	    string pgkcmd1 = "yum list installed ";
	    string pgkcmd2 = " | grep Installed > /tmp/pkg_check 2>&1";
	    string pgkREMOTEcmd1 = "'yum list installed ";
	    string pgkREMOTEcmd2 = " > /tmp/pkg_check 2>&1'";
	    string pgkREMOTECHECKcmd = "cat pkg_check | grep Installed > /tmp/pkg_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		if ( status == "local" )
		    cout << " Local Dependent Package Installations checking" << endl;
		else
		    cout << " Checking Dependent Package Installations for " + ipAddress << endl;

		for( int i = 0;;i++)
		{
		    if ( centosPgk[i] == "" )
		    {
			// end of section list, check for nmap which is needed for port testing
			string pkgCMD = pgkcmd1 + "nmap" + pgkcmd2;
			
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package 'nmap' isn't installed, please install for port testing" << endl;
			    nmapPass = false;
			}
		      
			break;
		    }

		    string pkg = centosPgk[i];

		    string pkgCMD = pgkcmd1 + pkg + pgkcmd2;
		    string pkgREMOTECMD = pgkREMOTEcmd1 + pkg + pgkREMOTEcmd2;
		    
		    if ( status == "local" )
		    {  
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			//run command
			string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + pkgREMOTECMD + " 1 > /tmp/run_pkg.log";
			int rtnCode = system(cmd.c_str());
			if  (WEXITSTATUS(rtnCode) != 1 )
			{
			    //get results
			    string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/pkg_check 1 > /tmp/get_pkg_check.log";
			    int rtnCode = system(cmd.c_str());
			    if (WEXITSTATUS(rtnCode) == 0)
			    {
				int rtnCode = system(pgkREMOTECHECKcmd.c_str());
				if  (WEXITSTATUS(rtnCode) == 0 )
				if  (WEXITSTATUS(rtnCode) != 0 )
				{
				    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
				    pass = false;
				}

				unlink("pkg_check");
			    }
			    else
			    {
				cout << " FAILED, check /tmp/get_pkg_check.log" << endl;
				pass = false;
			    }
			}
			else
			{
			    cout << " FAILED, check /tmp/run_pkg.log" << endl;
			    pass = false;
			}
		    }
		}
		
		if (pass) 
		    cout << " Passed, all MariaDB Columnstore dependent packages are installed" << endl << endl;
	    }

	    checkSuccess(pass);
	}

	// suse
	if ( OS == "suse12" )
	{
	    cout << endl << "Checking Dependent Package installations" << endl << endl;
	    
	    string pgkcmd1 = "zypper list installed ";
	    string pgkcmd2 = " | grep Installed > /tmp/pkg_check 2>&1";
	    string pgkREMOTEcmd1 = "zypper list installed ";
	    string pgkREMOTEcmd2 = " > /tmp/pgk_check 2>&1";
	    string pgkREMOTECHECKcmd = "cat pgk_check | grep Installed > /tmp/pgk_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		cout << " Checking Dependent Package Installations for " + ipAddress << endl;
		
		for( int i = 0;;i++)
		{
		    if ( centosPgk[i] == "" )
		    {
			// end of section list, check for nmap which is needed for port testing
			string pkgCMD = pgkcmd1 + "nmap" + pgkcmd2;
			
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package 'nmap' isn't installed, please install for port testing" << endl;
			    nmapPass = false;
			}
		      
			break;
		    }

		    string pkg = centosPgk[i];

		    string pkgCMD = pgkcmd1 + pkg + pgkcmd2;
		    string pkgREMOTECMD = pgkREMOTEcmd1 + pkg + pgkREMOTEcmd2;
		    
		    if ( status == "local" )
		    {
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			//run command
			string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + pkgREMOTECMD + " 1 > /tmp/run_pgk.log";
			int rtnCode = system(cmd.c_str());
			if  (WEXITSTATUS(rtnCode) != 1 )
			{
			    //get results
			    string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/pgk_check 1 > /tmp/get_pgk_check.log";
			    int rtnCode = system(cmd.c_str());
			    if (WEXITSTATUS(rtnCode) == 0)
			    {
				int rtnCode = system(pgkREMOTECHECKcmd.c_str());
				if  (WEXITSTATUS(rtnCode) == 0 )
				if  (WEXITSTATUS(rtnCode) != 0 )
				{
				    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
				    pass = false;
				}

				unlink("pgk_check");
			    }
			    else
			    {
				cout << " FAILED, check /tmp/pgk_check.log" << endl;
				pass = false;
			    }
			}
			else
			{
			    cout << " FAILED, check /tmp/run_pgk.log" << endl;
			    pass = false;
			}
		    }
		}
		
		if (pass) 
		    cout << " Passed, all MariaDB Columnstore dependent packages are installed" << endl << endl;
	    }

	    checkSuccess(pass);
	}


	//ubuntu and debian
	
	const std::string ubuntuPgk[] = {
	    "libboost-all-dev",
	    "expect", 
	    "libdbi-perl",
	    "perl", 
	    "openssl",
	    "libreadline-dev",
	    "file",
	    "sudo",
	    "libaio",
	    "rsync",
	    "libsnappy1",
	    "net-tools",
	    ""
	};
	
	if ( OS == "ubuntu16" || OS == "debian8" )
	{
	    cout << endl << "Checking Dependent Package installations" << endl << endl;
	    
	    string pgkcmd1 = "dpkg -s ";
	    string pgkcmd2 = " | grep installed > /tmp/pkg_check 2>&1";
	    string pgkREMOTEcmd1 = "dpkg -s ";
	    string pgkREMOTEcmd2 = " > /tmp/pgk_check 2>&1";
	    string pgkREMOTECHECKcmd = "cat pgk_check | grep installed > /tmp/pgk_check 2>&1";

	    pass = true;
	    list = moduleiplist.begin();
	    for (; list != moduleiplist.end() ; list++)
	    {
		string ipAddress = (*list).IPaddress;
		string status = (*list).status;
		if ( status == "failed" )
		  continue;
		
		cout << " Checking Dependent Package Installations for " + ipAddress << endl;
		
		for( int i = 0;;i++)
		{
		    if ( ubuntuPgk[i] == "" )
		    {
			// end of section list, check for nmap which is needed for port testing
			string pkgCMD = pgkcmd1 + "nmap" + pgkcmd2;
			
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package 'nmap' isn't installed, please install for port testing" << endl;
			    nmapPass = false;
			}
		      
			break;
		    }

		    string pkg = ubuntuPgk[i];

		    string pkgCMD = pgkcmd1 + pkg + pgkcmd2;
		    string pkgREMOTECMD = pgkREMOTEcmd1 + pkg + pgkREMOTEcmd2;
		    
		    if ( status == "local" )
		    {
			int rtnCode = system(pkgCMD.c_str());
			if  (WEXITSTATUS(rtnCode) != 0 )
			{
			    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
			    pass = false;
			}
		    }
		    else
		    {
			//run command
			string cmd = "./remote_command.sh " + ipAddress + " " + password + " " + pkgREMOTECMD + " 1 > /tmp/run_pgk.log";
			int rtnCode = system(cmd.c_str());
			if  (WEXITSTATUS(rtnCode) != 1 )
			{
			    //get results
			    string cmd = "./remote_scp_get.sh " + ipAddress + " " + password + " /tmp/pgk_check 1  > /tmp/get_pgk_check.log";
			    int rtnCode = system(cmd.c_str());
			    if (WEXITSTATUS(rtnCode) == 0)
			    {
				int rtnCode = system(pgkREMOTECHECKcmd.c_str());
				if  (WEXITSTATUS(rtnCode) == 0 )
				if  (WEXITSTATUS(rtnCode) != 0 )
				{
				    cout << " FAILED, Package " + pkg + " isn't installed, please install" << endl;
				    pass = false;
				}

				unlink("pgk_check");
			    }
			    else
			    {
				cout << " FAILED, check /tmp/pgk_check.log" << endl;
				pass = false;
			    }
			}
			else
			{
			    cout << " FAILED, check /tmp/run_pgk.log" << endl;
			    pass = false;
			}
		    }
		}
		
		if (pass) 
		    cout << " Passed, all MariaDB Columnstore dependent packages are installed" << endl << endl;
	    }

	    checkSuccess(pass);
	}

	// port testing
	if (!nmapPass)
	{
	    cout << endl << "Package 'nmap' isn't installed, skipping the port testing" << endl;
	    exit (0);
	}

	cout << endl << "Checking MariaDb ColumnStore Port availibility" << endl << endl;
	
	string nmapcmd1 = "nmap ";
	string nmapcmd2 = " -p 8602 | grep 'closed unknown' > /tmp/nmap_check 2>&1";

	pass = true;
	list = moduleiplist.begin();
	for (; list != moduleiplist.end() ; list++)
	{
	    string ipAddress = (*list).IPaddress;
	    string status = (*list).status;
	    if ( status == "failed" || status == "local")
		continue;
	    
	    cout << " Checking Port (8602) using 'nmap' for " + ipAddress + " : ";
	    cout.flush();
	    
	    string nmapcmd = nmapcmd1 + ipAddress + nmapcmd2;
	    
	    //run command
	    int rtnCode = system(nmapcmd.c_str());
	    if  (WEXITSTATUS(rtnCode) != 0 )
		cout << " FAILED, iptables is enabled, please disable" << endl;
	    else
		cout << " PASSED" << endl;
	}
    
	cout << endl << endl << "Finished Validation of the Cluster, correct any failures" << endl << endl;
	exit(0);
    
}


void checkSuccess( bool pass)
{
    if (!pass && check)
    {
	cout << endl;
	string answer = "y";
	prompt = "Failure occurred, do you want to continue? (y,n) > ";
	pcommand = readline(prompt.c_str());
	if (pcommand) {
		if (strlen(pcommand) > 0) answer = pcommand;
		free(pcommand);
		pcommand = 0;
	}

	if ( answer == "n" )
	{
		cout << "Exiting..." << endl;
		exit (1);
	}
    }
}
