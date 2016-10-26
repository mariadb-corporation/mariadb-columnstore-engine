/* Copyright (C) 2016 MariaDB Corporation.

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

#include <cstring>
#include <cppunit/extensions/HelperMacros.h>

#include "alarmmanager.h"
#include "liboamcpp.h"

using namespace alarmmanager;
using namespace oam;
using namespace messageqcpp;
using namespace std;

class ALARMManagerTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ALARMManagerTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
//CPPUNIT_TEST( test3 );	// requires ProcMgr to be running

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		// set alarm
		ALARMManager sm;
		sm.sendAlarmReport("EC-DISK-1", 4, SET);
		AlarmList activeAlarms;
		sm.getActiveAlarm(activeAlarms);

		//clear alarm
		sm.sendAlarmReport("EC-DISK-1", 4, CLEAR);
		sm.getActiveAlarm(activeAlarms);
	}
	
	void test2() {
		Alarm alarm;
		alarm.setAlarmID (2);
		alarm.setComponentID ("atlanta");
		alarm.setSeverity (MAJOR);
		alarm.setState (1);
		cout << alarm << endl;
		string a;
		uint32_t b;
		b = alarm.getCtnThreshold();
		alarm.setCtnThreshold(b);
		b = alarm.getOccurrence();
		alarm.setOccurrence(b);
		a = alarm.getTimestamp();
		b = alarm.getLastIssueTime();
		alarm.setLastIssueTime(b);
		a = alarm.getSname();
		alarm.setSname(a);
		a = alarm.getPname();
		alarm.setPname(a);
		b = alarm.getTid();
		alarm.setTid(b);
		b = alarm.getPid();
		alarm.setPid(b);
	}
	
	void test3()
	{
		ALARMManager sm;
		string value;
		sm.setSNMPConfig ("atlanta", SUB_AGENT, "DISK_CRITICAL", "2000000");
		sm.getSNMPConfig ("atlanta", SUB_AGENT, "DISK_CRITICAL", value);
		cout << "DISK: " << value << endl;
		sm.setSNMPConfig ("atlanta", SUB_AGENT, "MEM_MAJOR", "333333");
		sm.getSNMPConfig ("atlanta", SUB_AGENT, "MEM_MAJOR", value);
		cout << "MEM " << value << endl;
		sm.setSNMPConfig ("atlanta", SUB_AGENT, "SWAP_MINOR", "543216");
		sm.getSNMPConfig ("atlanta", SUB_AGENT, "SWAP_MINOR", value);
		cout << "SWAP " << value << endl;
		sm.setNMSAddr ("10.100.3.141");
		sm.getNMSAddr (value);
		cout << "NMS address: " << value << endl;
	}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( ALARMManagerTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}

