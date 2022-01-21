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
#include <time.h>
#include <sys/time.h>
#include <limits.h>

#include "jobstep.h"
#include "funcexp.h"
#include "jlf_common.h"
#include "tupleannexstep.h"
#include "calpontsystemcatalog.h"
#include <boost/any.hpp>
#include <boost/function.hpp>
#include "bytestream.h"
#include "utils/windowfunction/idborderby.h"
#include "mcs_decimal.h"

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

  CPPUNIT_TEST(INT_TEST);
  CPPUNIT_TEST(FLOAT_TEST);
  CPPUNIT_TEST(WIDEDT_TEST);

  CPPUNIT_TEST_SUITE_END();

 private:
  // The tests creates an RG with 1 column of the cscDt type
  // then initialize RGData. After that it adds two numeric values (v1 < v2)and two NULL.
  // Then creates comparator structures and run a number of tests. v1 < v2
  void testComparatorWithDT(execplan::CalpontSystemCatalog::ColDataType cscDt, uint32_t width,
                            bool generateRandValues, uint8_t precision)
  {
    std::cout << std::endl << "------------------------------------------------------------" << std::endl;
    uint32_t oid = 3001;
    std::vector<uint32_t> offsets, roids, tkeys, cscale, cprecision;
    std::vector<uint32_t> charSetNumVec;
    std::vector<execplan::CalpontSystemCatalog::ColDataType> types;
    offsets.push_back(2);
    offsets.push_back(2 + width);
    roids.push_back(oid);
    tkeys.push_back(1);
    types.push_back(cscDt);
    cscale.push_back(0);
    cprecision.push_back(precision);
    charSetNumVec.push_back(8);
    rowgroup::RowGroup inRG(1,              // column count
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

    rowgroup::RGData rgD = rowgroup::RGData(inRG);
    inRG.setData(&rgD);
    rowgroup::Row r, r1, r2, r3;
    inRG.initRow(&r);
    uint32_t rowSize = r.getSize();
    inRG.getRow(0, &r);

    // Sorting spec describes sorting direction and NULL comparision
    // preferences
    ordering::IdbSortSpec spec = ordering::IdbSortSpec(0,      // column index
                                                       true,   // ascending
                                                       true);  // NULLs first
    std::vector<ordering::IdbSortSpec> specVect;
    specVect.push_back(spec);

    switch (cscDt)
    {
      case execplan::CalpontSystemCatalog::UTINYINT:
      {
        std::cout << "UTINYINT " << std::endl;
        r.setUintField<1>(42, 0);
        r.nextRow(rowSize);
        r.setUintField<1>(43, 0);
        r.nextRow(rowSize);
        r.setUintField<1>(joblist::UTINYINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::USMALLINT:
      {
        std::cout << "USMALLINT " << std::endl;
        r.setUintField<2>(42, 0);
        r.nextRow(rowSize);
        r.setUintField<2>(43, 0);
        r.nextRow(rowSize);
        r.setUintField<2>(joblist::USMALLINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::UMEDINT:
      case execplan::CalpontSystemCatalog::UINT:
      {
        std::cout << "UINT " << std::endl;
        r.setUintField<4>(42, 0);
        r.nextRow(rowSize);
        r.setUintField<4>(43, 0);
        r.nextRow(rowSize);
        r.setUintField<4>(joblist::UINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::DATETIME:
      case execplan::CalpontSystemCatalog::UBIGINT:
      {
        std::cout << "UBIGINT " << std::endl;
        r.setUintField<8>(42, 0);
        r.nextRow(rowSize);
        r.setUintField<8>(43, 0);
        r.nextRow(rowSize);
        r.setUintField<8>(joblist::UBIGINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::TINYINT:
      {
        std::cout << "TINYINT " << std::endl;
        r.setIntField<1>(42, 0);
        r.nextRow(rowSize);
        r.setIntField<1>(43, 0);
        r.nextRow(rowSize);
        r.setIntField<1>(joblist::TINYINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::SMALLINT:
      {
        std::cout << "SMALLINT " << std::endl;
        r.setIntField<2>(42, 0);
        r.nextRow(rowSize);
        r.setIntField<2>(43, 0);
        r.nextRow(rowSize);
        r.setIntField<2>(joblist::SMALLINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::DATE:
      {
        if (cscDt == execplan::CalpontSystemCatalog::DATE)
          std::cout << "DATE" << std::endl;
        else
          std::cout << "INT " << std::endl;
        r.setIntField<4>(42, 0);
        r.nextRow(rowSize);
        r.setIntField<4>(43, 0);
        r.nextRow(rowSize);
        if (cscDt == execplan::CalpontSystemCatalog::DATE)
          r.setIntField<4>(joblist::DATENULL, 0);
        else
          r.setIntField<4>(joblist::INTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::BIGINT:
      {
        std::cout << "BIGINT " << std::endl;
        r.setIntField<8>(42, 0);
        r.nextRow(rowSize);
        r.setIntField<8>(43, 0);
        r.nextRow(rowSize);
        r.setIntField<8>(joblist::BIGINTNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        std::cout << "DECIMAL " << std::endl;
        switch (width)
        {
          case 1:
          {
            r.setUintField<1>(42, 0);
            r.nextRow(rowSize);
            r.setUintField<1>(43, 0);
            r.nextRow(rowSize);
            r.setUintField<1>(joblist::TINYINTNULL, 0);
            break;
          }
          case 2:
          {
            r.setUintField<2>(42, 0);
            r.nextRow(rowSize);
            r.setUintField<2>(43, 0);
            r.nextRow(rowSize);
            r.setUintField<2>(joblist::SMALLINTNULL, 0);
            break;
          }
          case 4:
          {
            r.setUintField<4>(42, 0);
            r.nextRow(rowSize);
            r.setUintField<4>(43, 0);
            r.nextRow(rowSize);
            r.setUintField<4>(joblist::INTNULL, 0);
            break;
          }
          case 8:
          {
            r.setUintField<8>(42, 0);
            r.nextRow(rowSize);
            r.setUintField<8>(43, 0);
            r.nextRow(rowSize);
            r.setUintField<8>(joblist::BIGINTNULL, 0);
            break;
          }
          case 16:
          {
            uint128_t dec = 42;
            r.setBinaryField(&dec, 0);
            r.nextRow(rowSize);
            dec++;
            r.setBinaryField(&dec, 0);
            r.nextRow(rowSize);
            r.setBinaryField(const_cast<int128_t*>(&datatypes::Decimal128Null), 0);
            break;
          }
          default: std::cout << " This is the end. My only friend the end... " << std::endl;
        }
        break;
      }
      case execplan::CalpontSystemCatalog::FLOAT:
      case execplan::CalpontSystemCatalog::UFLOAT:
      {
        std::cout << "FLOAT " << std::endl;
        r.setFloatField(42.1, 0);
        r.nextRow(rowSize);
        r.setFloatField(43.1, 0);
        r.nextRow(rowSize);
        r.setUintField(joblist::FLOATNULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::DOUBLE:
      case execplan::CalpontSystemCatalog::UDOUBLE:
      {
        std::cout << "DOUBLE " << std::endl;
        r.setDoubleField(42.1, 0);
        r.nextRow(rowSize);
        r.setDoubleField(43.1, 0);
        r.nextRow(rowSize);
        r.setUintField(joblist::DOUBLENULL, 0);
        break;
      }
      case execplan::CalpontSystemCatalog::LONGDOUBLE:
      {
        r.setLongDoubleField(42.1, 0);
        r.nextRow(rowSize);
        r.setLongDoubleField(43.1, 0);
        r.nextRow(rowSize);
        r.setLongDoubleField(joblist::LONGDOUBLENULL, 0);
        break;
      }
      default:
      {
        break;
      }
    }

    inRG.setRowCount(3);
    inRG.initRow(&r1);
    inRG.initRow(&r2);
    inRG.initRow(&r3);
    inRG.getRow(0, &r1);
    inRG.getRow(1, &r2);
    inRG.getRow(2, &r3);

    std::cout << "r1 " << r1.toString() << " r2 " << r2.toString() << " r3 " << r3.toString() << std::endl;

    ordering::IdbCompare idbCompare;
    idbCompare.initialize(inRG);
    ordering::OrderByData odbData = ordering::OrderByData(specVect, inRG);
    bool result = odbData(r1.getPointer(), r2.getPointer());
    std::cout << r1.toString() << " < " << r2.toString() << " is " << ((result) ? "true" : "false")
              << std::endl;
    CPPUNIT_ASSERT(result == true);
    result = odbData(r2.getPointer(), r1.getPointer());
    std::cout << r2.toString() << " < " << r1.toString() << " is " << ((result) ? "true" : "false")
              << std::endl;
    CPPUNIT_ASSERT(result == false);
    result = odbData(r2.getPointer(), r2.getPointer());
    std::cout << r2.toString() << " < " << r2.toString() << " is " << ((result) ? "true" : "false")
              << std::endl;
    CPPUNIT_ASSERT(result == false);
    // Compare value with NULL. if spec.fNf then NULLs goes first
    result = odbData(r3.getPointer(), r1.getPointer());
    std::cout << r3.toString() << " < " << r1.toString() << " is " << ((result) ? "true" : "false")
              << std::endl;
    CPPUNIT_ASSERT(result == true);
    // Compare NULL with NULL
    result = odbData(r3.getPointer(), r1.getPointer());
    std::cout << r3.toString() << " < " << r3.toString() << " is " << ((result) ? "true" : "false")
              << std::endl;
    CPPUNIT_ASSERT(result == true);
  }

  void INT_TEST()
  {
#ifdef __x86_64__
    // bool generateValues = true;
    bool fixedValues = false;
    testComparatorWithDT(execplan::CalpontSystemCatalog::UTINYINT, 1, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::USMALLINT, 2, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::UMEDINT, 4, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::UBIGINT, 8, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::DATETIME, 8, fixedValues, 20);

    testComparatorWithDT(execplan::CalpontSystemCatalog::TINYINT, 1, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::SMALLINT, 2, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::MEDINT, 4, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::DATE, 4, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::BIGINT, 8, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::DECIMAL, 8, fixedValues, 20);

    testComparatorWithDT(execplan::CalpontSystemCatalog::FLOAT, 4, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::DOUBLE, 8, fixedValues, 20);
    testComparatorWithDT(execplan::CalpontSystemCatalog::LONGDOUBLE, 8, fixedValues, 20);
#elif __arm__
    // TODO: add arm tests
#endif
  }

  void FLOAT_TEST()
  {
  }

  void WIDEDT_TEST()
  {
    bool fixedValues = false;
    testComparatorWithDT(execplan::CalpontSystemCatalog::DECIMAL, 16, fixedValues, 38);
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
