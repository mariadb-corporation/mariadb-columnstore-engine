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


using namespace std;

typedef std::vector<std::string> PrivateIPList;

string password = "ssh"; 
string OS = "centos7";
string user = "root";

char* pcommand1 = 0;
string prompt1;

int main(int argc, char *argv[])
{


   for( int i = 1; i < argc; i++ )
   {
	if( string("-h") == argv[i] || string("--help") == argv[i]) {
		cout << endl;
		cout << "This is the MariaDB Columnstore Cluster System Test tool." << endl;
		cout << "It will run a set of test to check the checkup of the system." << endl;
		cout << "This can be run prior to the install to make sure the servers/nodes" << endl;
		cout << "are configured properly" << endl;
		cout << "User will be prompted to provide the server/node IP Addresses, OS" << endl; 
		cout << "Root/Non-root user install name, and password" << endl;
		cout << "Items that are checked:" << endl;
		cout << "	Dependent packages installed" << endl;
		cout << "	OS version" << endl;
		cout << "	Firewall settings" << endl;
		cout << "	Locale settings" << endl;
		cout << "	Node ping test" << endl;
		cout << "	Node SSH test" << endl;
		cout << "	ColumnStore Port test" << endl << endl;
		cout << "Usage: columnstoreClusterTester -h " << endl;
		cout << "   -h  	Help" << endl;
		exit (0);
	}

	cout << endl;
	cout << "This is the MariaDB Columnstore Cluster System test tool." << endl;

	cout << endl;
	string IPaddresses;
	//get Ip Addresses
	prompt1 = "Enter List of IP Addresses of each server/node starting with the local first (x.x.x.x,y.y.y.y) > ";
	pcommand1 = readline(prompt1.c_str());
	if (pcommand1) {
		if (strlen(pcommand1) > 0) IPaddresses = pcommand1;
		free(pcommand1);
		pcommand1 = 0;
	}

	if ( IPaddresses.empty )
	{
		cout << "Error: no IP addresses where entered, exit..." << endl;
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
		      PrivateIPList.push_back(*it);
		}
	}

	cout << endl;
	//get OS
	prompt1 = "Enter OS version (centos6, centos7, debian8, suse12, ubuntu16) > ";
	pcommand1 = readline(prompt1.c_str());
	if (pcommand1) {
		if (strlen(pcommand1) > 0) OS = pcommand1;
		free(pcommand1);
		pcommand1 = 0;
	}

	if ( OS.empty )
	{
		cout << "Error: no OS Version was entered, exit..." << endl;
		exit (1);
	}

	cout << endl;
	//get User
	prompt1 = "Enter Install user (root or 'non-root user name') > ";
	pcommand1 = readline(prompt1.c_str());
	if (pcommand1) {
		if (strlen(pcommand1) > 0) user = pcommand1;
		free(pcommand1);
		pcommand1 = 0;
	}

	if ( user.empty )
	{
		cout << "Error: no User type was entered, exit..." << endl;
		exit (1);
	}

	cout << endl;
	//get root password
	prompt1 = "Enter User Password or 'ssh' if using SSH-KEYS, which is used to access other nodes > ";
	pcommand1 = readline(prompt1.c_str());
	if (pcommand1) {
		if (strlen(pcommand1) > 0) password = pcommand1;
		free(pcommand1);
		pcommand1 = 0;
	}

	//get Elastic IP to module assignments
	if ( password.empty )
	{
		cout << "Error: no password or 'ssh' was entered, exit..." << endl;
		exit (1);
	}

	
	
	***********************************************************************************************
	
	
			string cmd = installDir + "/bin/remote_command.sh " + ipAddress + " " + AMIrootPassword + "  '/root/updatePassword.sh " + rootPassword + "' > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "ERROR: failed update of root password on " + module << endl;
			cleanupSystem();
		}

	
				string cmd = installDir + "/bin/remote_scp_put.sh " + ipAddress + " " + AMIrootPassword + " "  + x509Cert + " > /tmp/scp.log_" + instanceName;
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0) {
				pass = true;
				break;
			}

			//assign um Instances
			std::vector<std::string>::iterator list = existinguminstance.begin();
			for (; list != existinguminstance.end() ; list++)
			{
				string module = "um" + oam.itoa(um);
		
				cout << "Assigning Instance for " + module;
		
				string instanceName = *list;
			}
		}
	

void setRootPassword()
{
	Oam oam;

	cout << endl << "--- Updating Root Password on all Instance(s) ---" << endl << endl;

	InstanceList::iterator list1 = uminstancelist.begin();
	for (; list1 != uminstancelist.end() ; list1++)
	{
		string instance = (*list1).instanceName;
		string module = (*list1).moduleName;

		//get IP Address of um instance
		string ipAddress = oam.getEC2InstanceIpAddress(instance);

		if (ipAddress == "stopped" || ipAddress == "terminated" ) {
			cout << "ERROR: Instance " << instance << " failed to get private IP Address" << endl;
			cleanupSystem();
		}

		string cmd = installDir + "/bin/remote_command.sh " + ipAddress + " " + AMIrootPassword + "  '/root/updatePassword.sh " + rootPassword + "' > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "ERROR: failed update of root password on " + module << endl;
			cleanupSystem();
		}
	}

	InstanceList::iterator list2 = pminstancelist.begin();
	for (; list2 != pminstancelist.end() ; list2++)
	{
		string instance = (*list2).instanceName;
		string module = (*list2).moduleName;

		string cmd = installDir + "/bin/remote_command.sh " + ipAddress + " " + AMIrootPassword + "  '/root/updatePassword.sh " + rootPassword + "' > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "ERROR: failed update root of password on " + module << endl;
			cleanupSystem();
		}
	}
}
