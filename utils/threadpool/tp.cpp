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
#include "threadpool.h"

int64_t thecount = 0;
boost::mutex mutex;

const string timeNow()
{
  time_t outputTime = time(0);
  struct tm ltm;
  char buf[32];  // ctime(3) says at least 26
  size_t len = 0;
#ifdef _MSC_VER
  asctime_s(buf, 32, localtime_r(&outputTime, &ltm));
#else
  asctime_r(localtime_r(&outputTime, &ltm), buf);
#endif
  len = strlen(buf);

  if (len > 0)
    --len;

  if (buf[len] == '\n')
    buf[len] = 0;

  return buf;
}

// Functor class
struct foo
{
  int64_t fData;
  int64_t fThd;
  string start;
  bool running;

  void operator()()
  {
    start = timeNow();

    std::cout << "foo thd = " << fThd << " start " << start << std::endl;

    for (int64_t i = 0; i < 1024 * 1024 * (fThd + 0) * 128; i++)
      // simulate some work
      fData++;

    boost::mutex::scoped_lock lock(mutex);
    std::cout << "foo thd = " << fThd << " start " << start << " fin " << timeNow() << std::endl;
  }

  foo(int64_t i) : fThd(i), fData(i), running(true)
  {
    start = timeNow();
  }

  foo(const foo& copy) : fData(copy.fData), fThd(copy.fThd), start(copy.start), running(copy.running)
  {
    std::cout << "new foo " << fThd << endl;
  }

  ~foo()
  {
    running = false;
  }
};

int main(int argc, char** argv)
{
  threadpool::ThreadPool pool(20, 10);
  std::vector<uint64_t> hndl;
  hndl.reserve(10);
  int t1 = hndl.capacity();
  uint64_t testHndl;
  uint64_t thdhndl = 999;
  int64_t thd = 1;
  boost::function0<void> foofunc;
  boost::function0<void> foofunc2;

  for (int64_t y = 0; y < 1; y++)
  {
    foo bar(y);
    //		foofunc = bar;
    //		foofunc2 = foofunc;
    std::cout << "Done with assign" << std::endl;

    for (int64_t i = 0; i < 1; ++i)
    {
      bar.fThd = thd++;
      thdhndl = pool.invoke(bar);

      if (y < 10)
      {
        hndl.push_back(thdhndl);
      }

      if (y == 0)
      {
        testHndl = thdhndl;
      }
    }

    boost::mutex::scoped_lock lock(mutex);
  }

  // Wait until all of the queued up and in-progress work has finished
  std::cout << "Threads for join " << hndl.size() << std::endl;
  pool.dump();
  std::cout << "*** JOIN 1 ***" << std::endl;
  pool.join(testHndl);
  pool.dump();
  std::cout << "*** JOIN 10 ***" << std::endl;
  pool.join(hndl);
  pool.dump();
  std::cout << "*** WAIT ***" << std::endl;
  pool.wait();
  pool.dump();
  sleep(2);
  return 0;
}
