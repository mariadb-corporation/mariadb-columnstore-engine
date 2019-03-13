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

// $Id: tdriver.cpp 3495 2013-01-21 14:09:51Z rdempsey $
#include <string>
#include <stdexcept>
#include <iostream>
#include <fstream>
using namespace std;
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

#include <cppunit/extensions/HelperMacros.h>

#include "threadpool.h"

int thecount = 0;
boost::mutex mutex;



class ThreadPoolTestSuite : public CppUnit::TestFixture
{

    CPPUNIT_TEST_SUITE( ThreadPoolTestSuite );

    CPPUNIT_TEST( test_1 );

    CPPUNIT_TEST_SUITE_END();

private:

    // Functor class
    struct foo
    {
        void operator ()()
        {
            for (int i = 0; i < 500; i++)
            {
                // simulate some work
                fData++;
            }

            boost::mutex::scoped_lock lock(mutex);

            std::cout << "count = " << ++thecount << ' ' << fData << std::endl;
        }

        foo(int i):
            fData(i)
        {}

        foo(const foo& copy)
            : fData(copy.fData)
        {}

        int fData;

    };

public:
    void setUp()
    {}

    void tearDown()
    {}

    void test_1()
    {


        threadpool::ThreadPool pool( 5, 10 );

        for (int y = 0; y < 10; y++)
        {
            foo bar(y);

            for (int i = 0; i < 10; ++i)
            {
                pool.invoke(bar);
            }

            // Wait until all of the queued up and in-progress work has finished
            pool.wait();
            pool.dump();
        }


    }

};


CPPUNIT_TEST_SUITE_REGISTRATION( ThreadPoolTestSuite );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>


int main( int argc, char** argv)
{
    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry& registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );
    bool wasSuccessful = runner.run( "", false );
    return (wasSuccessful ? 0 : 1);
}

