/* Copyright (C) 2019 MariaDB Corporaton.

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

// $Id: tdriver-filter.cpp 9210 2013-01-21 14:10:42Z rdempsey $

#include <list>
#include <sstream>
#include <pthread.h>
#include <iomanip>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "jobstep.h"
#include "funcexp.h"
#include "jlf_common.h"
#include "tupleannexstep.h"
#include "calpontsystemcatalog.h"
#include "resourcemanager.h"
#include <boost/any.hpp>
#include <boost/function.hpp>
#include "bytestream.h"
#include <time.h>
#include <sys/time.h>
#include <limits.h>

#define DEBUG
#define MEMORY_LIMIT 14983602176

using namespace std;
using namespace joblist;
using namespace messageqcpp;

// Timer class used by this tdriver to output elapsed times, etc.
class Timer
{
 public:
  void start(const string& message)
  {
    if (!fHeaderDisplayed)
    {
      header();
      fHeaderDisplayed = true;
    }

    gettimeofday(&fTvStart, 0);
    cout << timestr() << "          Start " << message << endl;
  }

  void stop(const string& message)
  {
    time_t now;
    time(&now);
    string secondsElapsed;
    getTimeElapsed(secondsElapsed);
    cout << timestr() << " " << secondsElapsed << " Stop  " << message << endl;
  }

  Timer() : fHeaderDisplayed(false)
  {
  }

 private:
  struct timeval fTvStart;
  bool fHeaderDisplayed;

  double getTimeElapsed(string& seconds)
  {
    struct timeval tvStop;
    gettimeofday(&tvStop, 0);
    double secondsElapsed =
        (tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) - (fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));
    ostringstream oss;
    oss << secondsElapsed;
    seconds = oss.str();
    seconds.resize(8, '0');
    return secondsElapsed;
  }

  string timestr()
  {
    struct tm tm;
    struct timeval tv;

    gettimeofday(&tv, 0);
    localtime_r(&tv.tv_sec, &tm);

    ostringstream oss;
    oss << setfill('0') << setw(2) << tm.tm_hour << ':' << setw(2) << tm.tm_min << ':' << setw(2) << tm.tm_sec
        << '.' << setw(6) << tv.tv_usec;
    return oss.str();
  }

  void header()
  {
    cout << endl;
    cout << "Time            Seconds  Activity" << endl;
  }
};

class FilterDriver : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE(FilterDriver);

  CPPUNIT_TEST(ORDERBY_TIME_TEST);

  CPPUNIT_TEST_SUITE_END();

 private:
  void orderByTest_nRGs(uint64_t numRows, uint64_t limit, uint64_t maxThreads, bool parallelExecution,
                        bool generateRandValues, bool hasDistinct)
  {
    Timer timer;
    // This test creates TAS for single bigint column and run sorting on it.

    cout << endl;
    cout << "------------------------------------------------------------" << endl;
    timer.start("insert");

    stringstream ss2;
    ss2.flush();
    ss2 << "loading " << numRows << " rows into initial RowGroup.";
    std::string message = ss2.str();
    timer.start(message);

    ResourceManager* rm = ResourceManager::instance(true);

    joblist::JobInfo jobInfo = joblist::JobInfo(rm);
    // 1st column is the sorting key
    uint8_t tupleKey = 1;
    uint8_t offset = 0;
    uint16_t rowsPerRG = 8192;  // hardcoded max rows in RowGroup value
    uint32_t numberOfRGs = numRows / rowsPerRG;

    // set sorting rules
    // true - ascending order, false otherwise
    jobInfo.orderByColVec.push_back(make_pair(tupleKey, false));
    jobInfo.limitStart = offset;
    jobInfo.limitCount = limit;
    jobInfo.hasDistinct = hasDistinct;
    // JobInfo doesn't set these SP in ctor
    jobInfo.umMemLimit.reset(new int64_t);
    *(jobInfo.umMemLimit) = MEMORY_LIMIT;
    SErrorInfo errorInfo(new ErrorInfo());
    jobInfo.errorInfo = errorInfo;
    uint32_t oid = 3001;
    // populate JobInfo.nonConstDelCols with dummy shared_pointers
    // to notify TupleAnnexStep::initialise() about number of non-constant columns
    execplan::SRCP srcp1, srcp2;
    jobInfo.nonConstDelCols.push_back(srcp1);
    jobInfo.nonConstDelCols.push_back(srcp2);

    // create two columns RG. 1st is the sorting key, second is the data column
    std::vector<uint32_t> offsets, roids, tkeys, cscale, cprecision;
    std::vector<uint32_t> charSetNumVec;
    std::vector<execplan::CalpontSystemCatalog::ColDataType> types;
    offsets.push_back(2);
    offsets.push_back(10);
    offsets.push_back(18);
    roids.push_back(oid);
    roids.push_back(oid);
    tkeys.push_back(1);
    tkeys.push_back(1);
    types.push_back(execplan::CalpontSystemCatalog::UBIGINT);
    types.push_back(execplan::CalpontSystemCatalog::UBIGINT);
    cscale.push_back(0);
    cscale.push_back(0);
    cprecision.push_back(20);
    cprecision.push_back(20);
    charSetNumVec.push_back(8);
    charSetNumVec.push_back(8);
    rowgroup::RowGroup inRG(2,              // column count
                            offsets,        // oldOffset
                            roids,          // column oids
                            tkeys,          // keys
                            types,          // types
                            charSetNumVec,  // charset numbers
                            cscale,         // scale
                            cprecision,     // precision
                            20,             // sTableThreshold
                            false           // useStringTable
    );
    rowgroup::RowGroup jobInfoRG(inRG);
    joblist::TupleAnnexStep tns = joblist::TupleAnnexStep(jobInfo);
    tns.addOrderBy(new joblist::LimitedOrderBy());
    tns.delivery(true);
    // activate parallel sort
    if (parallelExecution)
    {
      tns.setParallelOp();
    }
    tns.setMaxThreads(maxThreads);
    tns.initialize(jobInfoRG, jobInfo);
    tns.setLimit(0, limit);

    // Create JobStepAssociation mediator class ins to connect DL and JS
    joblist::AnyDataListSPtr spdlIn(new AnyDataList());
    // joblist::RowGroupDL* dlIn = new RowGroupDL(maxThreads, jobInfo.fifoSize);
    joblist::RowGroupDL* dlIn = new RowGroupDL(maxThreads, 21001);
    dlIn->OID(oid);
    spdlIn->rowGroupDL(dlIn);
    joblist::JobStepAssociation jsaIn;
    jsaIn.outAdd(spdlIn);
    tns.inputAssociation(jsaIn);

    uint64_t base = 42;
    uint64_t maxInt = 0;
    if (generateRandValues)
      ::srand(base);
    uint64_t nextValue;
    for (uint32_t i = 1; i <= numberOfRGs; i++)
    {
      // create RGData with the RG structure and manually populate RG
      // Use reinint(numberOfRGs) to preset an array
      rowgroup::RGData rgD = rowgroup::RGData(inRG);
      inRG.setData(&rgD);
      rowgroup::Row r;
      inRG.initRow(&r);
      uint32_t rowSize = r.getSize();
      inRG.getRow(0, &r);
      // populate RGData
      // for(uint64_t i = rowsPerRG+1; i > 0; i--)
      for (uint64_t i = 0; i < rowsPerRG; i++)  // Worst case scenario for PQ
      {
        // TBD Use set..._offset methods to avoid offset calculation instructions
        if (generateRandValues)
        {
          nextValue = ::rand();
        }
        else
        {
          nextValue = base + i;
        }

        r.setUintField<8>(nextValue, 0);
        r.setUintField<8>(nextValue, 1);
        if (maxInt < nextValue)
        {
          maxInt = nextValue;
        }
        r.nextRow(rowSize);
      }
      base += 1;
      inRG.setRowCount(rowsPerRG);

      // Insert RGData into input DL
      dlIn->insert(rgD);
    }
    // end of input signal
    std::cout << "orderByTest_nRGs input DataList totalSize " << dlIn->totalSize() << std::endl;
    dlIn->endOfInput();
    timer.stop(message);

    joblist::AnyDataListSPtr spdlOut(new AnyDataList());
    // Set the ring buffer big enough to take RGData-s with results b/c
    // there is nobody to read out of the buffer.
    joblist::RowGroupDL* dlOut = new RowGroupDL(1, numberOfRGs);
    dlOut->OID(oid);
    spdlOut->rowGroupDL(dlOut);
    joblist::JobStepAssociation jsaOut;
    jsaOut.outAdd(spdlOut);
    // uint64_t outputDLIter = dlOut->getIterator();
    tns.outputAssociation(jsaOut);

    // Run Annex Step
    message = "Sorting";
    timer.start(message);
    tns.run();
    tns.join();
    timer.stop(message);

    // serialize RGData into bs and later back
    // to follow ExeMgr whilst getting TAS result RowGroup
    messageqcpp::ByteStream bs;
    uint32_t result = 0;
    rowgroup::RowGroup outRG(inRG);
    rowgroup::RGData outRGData(outRG);
    result = tns.nextBand(bs);
    /*bool more = false;
    do
    {
        dlOut->next(outputDLIter, &outRGData);
    } while (more);*/
    std::cout << "orderByTest_nRGs result " << result << std::endl;
    // CPPUNIT_ASSERT( result == limit );
    outRGData.deserialize(bs);
    outRG.setData(&outRGData);

    // std::cout << "orderByTest_nRGs output RG " << outRG.toString() << std::endl;
    std::cout << "maxInt " << maxInt << std::endl;
    {
      rowgroup::Row r;
      outRG.initRow(&r);
      outRG.getRow(0, &r);
      CPPUNIT_ASSERT(limit == outRG.getRowCount() || outRG.getRowCount() == 8192);
      CPPUNIT_ASSERT_EQUAL(maxInt, r.getUintField(1));
    }

    cout << "------------------------------------------------------------" << endl;
  }

  void ORDERBY_TIME_TEST()
  {
    uint64_t numRows = 8192;
    uint64_t maxThreads = 8;
    // limit == 100000 is still twice as good to sort in parallel
    // limit == 1000000 however is better to sort using single threaded sorting
    uint64_t limit = 100000;
    bool parallel = true;
    bool woParallel = false;
    bool generateRandValues = true;
    bool hasDistinct = true;
    bool noDistinct = false;
    orderByTest_nRGs(numRows * 14400, limit, maxThreads, woParallel, generateRandValues, noDistinct);
    orderByTest_nRGs(numRows * 14400, limit, maxThreads, parallel, generateRandValues, noDistinct);
    orderByTest_nRGs(numRows * 14400, limit, maxThreads, woParallel, generateRandValues, hasDistinct);
    orderByTest_nRGs(numRows * 14400, limit, maxThreads, parallel, generateRandValues, hasDistinct);
  }
  void QUICK_TEST()
  {
    float f = 1.1;
    double d = 1.2;
    uint64_t i = 1;
    uint64_t* i_ptr = &i;
    double* d_ptr = &d;
    uint64_t* i2_ptr = (uint64_t*)d_ptr;
    float* f_ptr = &f;
    i_ptr = (uint64_t*)f_ptr;

    cout << "*i_ptr=" << *i_ptr << endl;
    cout << "*i2_ptr=" << *i2_ptr << endl;
    f_ptr = (float*)i_ptr;

    cout << "*f_ptr=" << *f_ptr << endl;

    cout << endl;

    if (d > i)
      cout << "1.2 is greater than 1." << endl;

    if (f > i)
      cout << "1.1 is greater than 1." << endl;

    if (d > f)
      cout << "1.2 is greater than 1.1" << endl;

    if (*i_ptr < *i2_ptr)
      cout << "1.1 < 1.2 when represented as uint64_t." << endl;

    cout << "sizeof(f) = " << sizeof(f) << endl;
    cout << "sizeof(i) = " << sizeof(i) << endl;
    cout << "sizeof(d) = " << sizeof(d) << endl;

    double dbl = 9.7;
    double dbl2 = 1.3;
    i_ptr = (uint64_t*)&dbl;
    i2_ptr = (uint64_t*)&dbl2;
    cout << endl;
    cout << "9.7 as int is " << *i_ptr << endl;
    cout << "9.7 as int is " << *i2_ptr << endl;
    cout << "1.2 < 9.7 is " << (*i_ptr < *i2_ptr) << endl;
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(FilterDriver);

int main(int argc, char** argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry& registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest(registry.makeTest());
  bool wasSuccessful = runner.run("", false);
  return (wasSuccessful ? 0 : 1);
}
