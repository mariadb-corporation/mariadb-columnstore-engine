/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

/*****************************************************************************
 * $Id: tupleunion.cpp 9665 2013-07-02 21:47:39Z pleblanc $
 *
 ****************************************************************************/

#include <string>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "querytele.h"
using namespace querytele;

#include "dataconvert.h"
#include "hasher.h"
#include "jlf_common.h"
#include "resourcemanager.h"
#include "tupleunion.h"

using namespace std;
using namespace std::tr1;
using namespace boost;
using namespace execplan;
using namespace rowgroup;
using namespace dataconvert;

#ifndef __linux__
#ifndef M_LN10
#define M_LN10 2.30258509299404568402 /* log_e 10 */
#endif
namespace
{
// returns the value of 10 raised to the power x.
inline double exp10(double x)
{
  return exp(x * M_LN10);
}
}  // namespace
#endif

namespace
{
  // union helper functions.

  inline uint64_t pickScaleForDouble(Row* out, uint32_t i, double val)
  {
    /* have to pick a scale to use for the double. using 5... */
    uint32_t scale = 5;
    uint64_t ival = (uint64_t)(double)(val * datatypes::scaleDivisor<double>(scale));
    const int diff = out->getScale(i) - scale;
    ival = datatypes::applySignedScale<uint64_t>(ival, diff);
    return ival;
  }

  inline uint64_t pickScaleForLongDouble(Row* out, uint32_t i, long double val)
  {
    /* have to pick a scale to use for the double. using 5... */
    uint32_t scale = 5;
    uint64_t ival = (uint64_t)(double)(val * datatypes::scaleDivisor<double>(scale));
    int diff = out->getScale(i) - scale;
    ival = datatypes::applySignedScale<uint64_t>(ival, diff);
    return ival;
  }

  void normalizeIntToIntNoScale(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getIntField(i), i); 
  }

  void normalizeIntToIntWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int128_t val = datatypes::applySignedScale<int128_t>(in.getIntField(i), diff);
    out->setInt128Field(val, i);
  }

  void normalizeIntToIntWithScaleInt64(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int64_t val = datatypes::applySignedScale<int64_t>(in.getIntField(i), diff);
    out->setIntField(val, i);
  }
  
  void normalizeIntToUintNoScale(const Row& in, Row* out, uint32_t i) 
  {
    out->setUintField(in.getIntField(i), i); 
  }

  void normalizeIntToUintWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int128_t val = datatypes::applySignedScale<int128_t>(in.getIntField(i), diff);
    out->setInt128Field(val, i);
  }

  void normalizeIntToUintWithScaleInt64(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int64_t val = datatypes::applySignedScale<int64_t>(in.getIntField(i), diff);
    out->setIntField(val, i);
  }

  void normalizeIntToStringWithScale(const Row& in, Row* out, uint32_t i) 
  {
    ostringstream os;
    double d = in.getIntField(i);
    d /= exp10(in.getScale(i));
    os.precision(15);
    os << d;
    out->setStringField(os.str(), i);
  }

  void normalizeIntToStringNoScale(const Row& in, Row* out, uint32_t i) 
  {
    ostringstream os;
    os << in.getIntField(i);
    out->setStringField(os.str(), i);
  }

  void normalizeIntToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledSInt64FieldAsXFloat<double>(i);
    out->setFloatField((float)d, i);
  }

  void normalizeIntToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledSInt64FieldAsXFloat<double>(i);
    out->setDoubleField(d, i);
  }

  void normalizeIntToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledSInt64FieldAsXFloat<long double>(i);
    out->setLongDoubleField(d, i);
  }

  void normalizeIntToXDecimalInt128(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int128_t val = datatypes::applySignedScale<int128_t>(in.getIntField(i), diff);
    out->setInt128Field(val, i);
  }

  void normalizeIntToXDecimalInt64(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int64_t val = datatypes::applySignedScale<int64_t>(in.getIntField(i), diff);
    out->setIntField(val, i);
  }

  void normalizeUintToIntNoScale(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getUintField(i), i); 
  }

  void normalizeUintToIntWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int128_t val = datatypes::applySignedScale<int128_t>(in.getUintField(i), diff);
    out->setInt128Field(val, i);
  }

  void normalizeUntToIntWithScaleInt64(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    uint64_t val = datatypes::applySignedScale<uint64_t>(in.getUintField(i), diff);
    out->setIntField(val, i);
  }

  void normalizeUintToUint(const Row& in, Row* out, uint32_t i) 
  {
    out->setUintField(in.getUintField(i), i); 
  }

  void normalizeUintToStringWithScale(const Row& in, Row* out, uint32_t i) 
  {
    ostringstream os;
    double d = in.getUintField(i);
    d /= exp10(in.getScale(i));
    os.precision(15);
    os << d;
    out->setStringField(os.str(), i);
  }

  void normalizeUintToStringNoScale(const Row& in, Row* out, uint32_t i) 
  {
    ostringstream os;
    os << in.getUintField(i);
    out->setStringField(os.str(), i);
  }

  void normalizUintToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledUInt64FieldAsXFloat<double>(i);
    out->setFloatField((float)d, i);
  }

  void normalizeUintToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledUInt64FieldAsXFloat<double>(i);
    out->setDoubleField(d, i);
  }

  void normalizeUintToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    auto d = in.getScaledUInt64FieldAsXFloat<long double>(i);
    out->setLongDoubleField(d, i);
  }

  void normalizeUintToXDecimalInt128(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    int128_t val = datatypes::applySignedScale<int128_t>(in.getUintField(i), diff);
    out->setInt128Field(val, i);
  }

  void normalizeUintToXDecimalInt64(const Row& in, Row* out, uint32_t i) 
  {
    const int diff = out->getScale(i) - in.getScale(i);
    idbassert(diff >= 0);
    uint64_t val = datatypes::applySignedScale<uint64_t>(in.getUintField(i), diff);
    out->setIntField(val, i);
  }

  void normalizeStringToString(const Row& in, Row* out, uint32_t i) 
  {
    out->setStringField(in.getStringField(i), i);
  }

  void normalizeDateToDate(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getIntField(i), i);
  }

  void normalizeDateToDatetime(const Row& in, Row* out, uint32_t i) 
  {
    uint64_t date = in.getUintField(i);
    date &= ~0x3f;  // zero the 'spare' field
    date <<= 32;
    out->setUintField(date, i);
  }

  void normalizeDateToTimestamp(const Row& in, Row* out, uint32_t i, long fTimeZone) 
  {
    dataconvert::Date date(in.getUintField(i));
    dataconvert::MySQLTime m_time;
    m_time.year = date.year;
    m_time.month = date.month;
    m_time.day = date.day;
    m_time.hour = 0;
    m_time.minute = 0;
    m_time.second = 0;
    m_time.second_part = 0;

    dataconvert::TimeStamp timeStamp;
    bool isValid = true;
    int64_t seconds = dataconvert::mySQLTimeToGmtSec(m_time, fTimeZone, isValid);

    if (!isValid)
    {
      timeStamp.reset();
    }
    else
    {
      timeStamp.second = seconds;
      timeStamp.msecond = m_time.second_part;
    }

    uint64_t outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&timeStamp));
    out->setUintField(outValue, i);
  }

  void normalizeDateToString(const Row& in, Row* out, uint32_t i) 
  {
    string d = DataConvert::dateToString(in.getUintField(i));
    out->setStringField(d, i);
  }

  void normalizeDatetimeToDatetime(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getIntField(i), i);
  }

  void normalizeDatetimeToDate(const Row& in, Row* out, uint32_t i) 
  {
    uint64_t val = in.getUintField(i);
    val >>= 32;
    out->setUintField(val, i);
  }

  void normalizeDatetimeToTimestamp(const Row& in, Row* out, uint32_t i, long fTimeZone) 
  {
    uint64_t val = in.getUintField(i);
    dataconvert::DateTime dtime(val);
    dataconvert::MySQLTime m_time;
    dataconvert::TimeStamp timeStamp;

    m_time.year = dtime.year;
    m_time.month = dtime.month;
    m_time.day = dtime.day;
    m_time.hour = dtime.hour;
    m_time.minute = dtime.minute;
    m_time.second = dtime.second;
    m_time.second_part = dtime.msecond;

    bool isValid = true;
    int64_t seconds = mySQLTimeToGmtSec(m_time, fTimeZone, isValid);

    if (!isValid)
    {
      timeStamp.reset();
    }
    else
    {
      timeStamp.second = seconds;
      timeStamp.msecond = m_time.second_part;
    }

    uint64_t outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&timeStamp));
    out->setUintField(outValue, i);
  }

  void normalizeDatetimeToString(const Row& in, Row* out, uint32_t i) 
  {
    string d = DataConvert::datetimeToString(in.getUintField(i));
    out->setStringField(d, i);
  }

  void normalizeTimestampToTimestamp(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getIntField(i), i);
  }

  void normalizeTimestampToDate(const Row& in, Row* out, uint32_t i, long fTimeZone) 
  {
    uint64_t val = in.getUintField(i);
    dataconvert::TimeStamp timestamp(val);
    int64_t seconds = timestamp.second;
    uint64_t outValue;

    dataconvert::MySQLTime time;
    dataconvert::gmtSecToMySQLTime(seconds, time, fTimeZone);

    dataconvert::Date date;
    date.year = time.year;
    date.month = time.month;
    date.day = time.day;
    date.spare = 0;
    outValue = (uint32_t) * (reinterpret_cast<uint32_t*>(&date));

    out->setUintField(outValue, i);
  }

  void normalizeTimestampToDatetime(const Row& in, Row* out, uint32_t i, long fTimeZone) 
  {
    uint64_t val = in.getUintField(i);
    dataconvert::TimeStamp timestamp(val);
    int64_t seconds = timestamp.second;
    uint64_t outValue;

    dataconvert::MySQLTime time;
    dataconvert::gmtSecToMySQLTime(seconds, time, fTimeZone);

    dataconvert::DateTime datetime;
    datetime.year = time.year;
    datetime.month = time.month;
    datetime.day = time.day;
    datetime.hour = time.hour;
    datetime.minute = time.minute;
    datetime.second = time.second;
    datetime.msecond = timestamp.msecond;
    outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&datetime));

    out->setUintField(outValue, i);
  }

  void normalizeTimestampToString(const Row& in, Row* out, uint32_t i, long fTimeZone) 
  {
    string d = DataConvert::timestampToString(in.getUintField(i), fTimeZone);
    out->setStringField(d, i);
  }

  void normalizeTimeToTime(const Row& in, Row* out, uint32_t i) 
  {
    out->setIntField(in.getIntField(i), i);
  }

  void normalizeTimeToString(const Row& in, Row* out, uint32_t i) 
  {
    string d = DataConvert::timeToString(in.getIntField(i));
    out->setStringField(d, i);
  }

  void normalizeXFloatToIntWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setInt128Field(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXDoubleToIntWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setInt128Field(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXFloatToIntWithScaleInt64(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setIntField(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXDoubleToIntWithScaleInt64(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setIntField(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXFloatToIntNoScale(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setIntField((int64_t)val, i);
  }

  void normalizeXDoubleToIntNoScale(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setIntField((int64_t)val, i);
  }

  void normalizeXFloatToUint(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setUintField((uint64_t)val, i);
  }

  void normalizeXDoubleToUint(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setUintField((uint64_t)val, i);
  }

  void normalizeXFloatToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setFloatField(val, i);
  }

  void normalizeXDoubleToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setFloatField(val, i);
  }

  void normalizeXFloatToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setDoubleField(val, i);
  }

  void normalizeXDoubleToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setDoubleField(val, i);
  }

  void normalizeXFloatToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setLongDoubleField(val, i);
  }

  void normalizeXDoubleToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setLongDoubleField(val, i);
  }

  void normalizeXFloatToString(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    ostringstream os;
    os.precision(15);  // to match mysql's output
    os << val;
    out->setStringField(os.str(), i);
  }

  void normalizeXDoubleToString(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    ostringstream os;
    os.precision(15);  // to match mysql's output
    os << val;
    out->setStringField(os.str(), i);
  }

  void normalizeXFloatToWideXDecimal(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setInt128Field(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXDoubleToWideXDecimal(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setInt128Field(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXFloatToXDecimal(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getFloatField(i);
    out->setIntField(pickScaleForDouble(out, i, val), i);
  }

  void normalizeXDoubleToXDecimal(const Row& in, Row* out, uint32_t i) 
  {
    double val = in.getDoubleField(i);
    out->setIntField(pickScaleForDouble(out, i, val), i);
  }

  void normalizeLongDoubleToIntNoScale(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setIntField((int64_t)val, i);
  }

  void normalizeLongDoubleToIntWithScaleInt128(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setInt128Field(pickScaleForLongDouble(out, i, val), i);
  }

  void normalizeLongDoubleToIntWithScaleInt(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setIntField(pickScaleForLongDouble(out, i, val), i);
  }

  void normalizeLongDoubleToUint(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setUintField((uint64_t)val, i);
  }

  void normalizeLongDoubleToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setFloatField(val, i);
  }

  void normalizeLongDoubleToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setDoubleField(val, i);
  }

  void normalizeLongDoubleToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setLongDoubleField(val, i);
  }

  void normalizeLongDoubleToString(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    ostringstream os;
    os.precision(15);  // to match mysql's output
    os << val;
    out->setStringField(os.str(), i);
  }

  void normalizeLongDoubleToXDecimalInt128(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setInt128Field(pickScaleForLongDouble(out, i, val), i);
  }

  void normalizeLongDoubleToXDecimalInt(const Row& in, Row* out, uint32_t i) 
  {
    long double val = in.getLongDoubleField(i);
    out->setIntField(pickScaleForLongDouble(out, i, val), i);
  }

  void normalizeWideXDecimalToWideXDecimalNoScale(const Row& in, Row* out, uint32_t i) 
  {
    int128_t val128 = 0;
    in.getInt128Field(i, val128);
    out->setInt128Field(val128, i);
  }

  void normalizeXDecimalToWideXDecimalNoScale(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);      
    out->setInt128Field(val, i);
  }

  void normalizeWideXDecimalToWideXDecimalWithScale(const Row& in, Row* out, uint32_t i) 
  {
    int128_t val128 = 0;
    in.getInt128Field(i, val128);
    int128_t temp = datatypes::applySignedScale<int128_t>(val128, out->getScale(i) - in.getScale(i));
    out->setInt128Field(temp, i);
  }

  void normalizeXDecimalToWideXDecimalWithScale(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    int128_t temp = datatypes::applySignedScale<int128_t>(val, out->getScale(i) - in.getScale(i));
    out->setInt128Field(temp, i);
  }

  void normalizeXDecimalToOtherNoScale(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    out->setIntField(val, i);
  }

  void normalizeXDecimalToOtherWithScale(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    int64_t temp = datatypes::applySignedScale<int64_t>(val, out->getScale(i) - in.getScale(i));
    out->setIntField(temp, i);
  }

  void normalizeXDecimalToXFloat(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    float fval = ((float)val) / IDB_pow[in.getScale(i)];
    out->setFloatField(fval, i);
  }

  void normalizeXDecimalToXDouble(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    double dval = ((double)val) / IDB_pow[in.getScale(i)];
    out->setDoubleField(dval, i);
  }

  void normalizeXDecimalToLongDouble(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    long double dval = ((long double)val) / IDB_pow[in.getScale(i)];
    out->setLongDoubleField(dval, i);
  }

  void normalizeWideXDecimalToString(const Row& in, Row* out, uint32_t i) 
  {
    int128_t val128 = 0;
    in.getInt128Field(i, val128);
    datatypes::Decimal dec(0, in.getScale(i), in.getPrecision(i), val128);
    out->setStringField(dec.toString(), i);
  }

  void normalizeXDecimalToString(const Row& in, Row* out, uint32_t i) 
  {
    int64_t val = in.getIntField(i);
    datatypes::Decimal dec(val, in.getScale(i), in.getPrecision(i));
    out->setStringField(dec.toString(), i);
  }

  void normalizeBlobVarbinary(const Row& in, Row* out, uint32_t i) 
  {
    // out->setVarBinaryField(in.getVarBinaryStringField(i), i);  // not efficient
    out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), i);
  }

  joblist::normalizeFunctionsT inferNormalizeFunctions(const Row& in, Row* out, long fTimeZone)
  {
    uint32_t i;
    joblist::normalizeFunctionsT result;

    for (i = 0; i < out->getColumnCount(); i++)
    {
      switch (in.getColTypes()[i])
      {
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::BIGINT:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            {
              if (out->getScale(i) || in.getScale(i)) 
              {
                if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                  result.emplace_back(normalizeIntToIntWithScaleInt128);
                else
                  result.emplace_back(normalizeIntToIntWithScaleInt64);
              } 
              else
                result.emplace_back(normalizeIntToIntNoScale); 
              break;
            }

            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            {
              if (in.getScale(i))
              {
                if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                  result.emplace_back(normalizeIntToUintWithScaleInt128);
                else
                  result.emplace_back(normalizeIntToUintWithScaleInt64);
              } 
              else
                result.emplace_back(normalizeIntToUintNoScale); 
              break;
            }

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: 
            {
              if (in.getScale(i))
                result.emplace_back(normalizeIntToStringWithScale);
              else
                result.emplace_back(normalizeIntToStringNoScale);
              break;
            }

            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
              throw logic_error(
                  "TupleUnion::normalize(): tried to normalize an int to a timestamp, time, date or datetime");

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT: result.emplace_back(normalizeIntToXFloat); break;

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE: result.emplace_back(normalizeIntToXDouble); break;

            case CalpontSystemCatalog::LONGDOUBLE: result.emplace_back(normalizeIntToLongDouble); break;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              /*
                Signed INT to XDecimal
                TODO:
                - This code does not handle overflow that may happen on
                  scale multiplication. Instead of returning a garbage value
                  we should probably apply saturation here. In long terms we
                  should implement DECIMAL(65,x) to avoid overflow completely
                  (so the UNION between DECIMAL and integer can choose a proper
                    DECIMAL(M,N) result data type to guarantee that any incoming
                    integer value can fit into it).
              */
              if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                result.emplace_back(normalizeIntToXDecimalInt128);
              else
                result.emplace_back(normalizeIntToXDecimalInt64);
              break;
            }

            default:
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
          }

          break;

        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UBIGINT:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            {
              if (out->getScale(i))
              {
                if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                  result.emplace_back(normalizeUintToIntWithScaleInt128);
                else
                  result.emplace_back(normalizeUntToIntWithScaleInt64);
              } 
              else
                result.emplace_back(normalizeUintToIntNoScale); 
              break;
            }

            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT: result.emplace_back(normalizeUintToUint); break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: 
            {
              if (in.getScale(i))
                result.emplace_back(normalizeUintToStringWithScale);
              else
                result.emplace_back(normalizeUintToStringNoScale);
              break;
            }
            
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
              throw logic_error(
                  "TupleUnion::normalize(): tried to normalize an int to a timestamp, time, date or datetime");

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT: result.emplace_back(normalizUintToXFloat); break;

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE: result.emplace_back(normalizeUintToXDouble); break;

            case CalpontSystemCatalog::LONGDOUBLE: result.emplace_back(normalizeUintToLongDouble); break;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              /*
                Unsigned INT to XDecimal
                TODO:
                - The overflow problem mentioned in the code under case "Signed INT to XDecimal:" is
                  also applicable here.
              */

              if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                result.emplace_back(normalizeUintToXDecimalInt128);
              else
                result.emplace_back(normalizeUintToXDecimalInt64);
              break;
            }

            default:
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
          }

          break;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::VARCHAR:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(normalizeStringToString); break;

            default:
            {
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: string to " << out->getColTypes()[i];
              throw logic_error(os.str());
            }
          }

          break;

        case CalpontSystemCatalog::DATE:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::DATE: result.emplace_back(normalizeDateToDate); break;

            case CalpontSystemCatalog::DATETIME: result.emplace_back(normalizeDateToDatetime); break;

            case CalpontSystemCatalog::TIMESTAMP: result.emplace_back(std::bind(normalizeDateToTimestamp, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, fTimeZone)); break;
            
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(normalizeDateToString); break;

            default:
            {
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: date to " << out->getColTypes()[i];
              throw logic_error(os.str());
            }
          }

          break;

        case CalpontSystemCatalog::DATETIME:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::DATETIME: result.emplace_back(normalizeDatetimeToDatetime); break;

            case CalpontSystemCatalog::DATE: result.emplace_back(normalizeDatetimeToDate); break;

            case CalpontSystemCatalog::TIMESTAMP: result.emplace_back(std::bind(normalizeDatetimeToTimestamp, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, fTimeZone)); break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(normalizeDatetimeToString); break;

            default:
            {
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: datetime to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
            }
          }

          break;

        case CalpontSystemCatalog::TIMESTAMP:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TIMESTAMP: result.emplace_back(normalizeTimestampToTimestamp); break;

            case CalpontSystemCatalog::DATE: result.emplace_back(std::bind(normalizeTimestampToDate, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, fTimeZone)); break;
          
            case CalpontSystemCatalog::DATETIME: result.emplace_back(std::bind(normalizeTimestampToDatetime, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, fTimeZone)); break;
            
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(std::bind(normalizeTimestampToString, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, fTimeZone)); break;

            default:
            {
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: timestamp to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
            }
          }

          break;

        case CalpontSystemCatalog::TIME:
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TIME: result.emplace_back(normalizeTimeToTime); break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(normalizeTimeToString); break;

            default:
            {
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: time to " << out->getColTypes()[i];
              throw logic_error(os.str());
            }
          }

          break;

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          {
            switch (out->getColTypes()[i])
            {
              case CalpontSystemCatalog::TINYINT:
              case CalpontSystemCatalog::SMALLINT:
              case CalpontSystemCatalog::MEDINT:
              case CalpontSystemCatalog::INT:
              case CalpontSystemCatalog::BIGINT:
              {
                if (out->getScale(i))
                {
                  if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                  {
                    if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                      result.emplace_back(normalizeXFloatToIntWithScaleInt128);
                    else
                      result.emplace_back(normalizeXDoubleToIntWithScaleInt128);
                  }
                  else
                  {
                    if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                      result.emplace_back(normalizeXFloatToIntWithScaleInt64);
                    else
                      result.emplace_back(normalizeXDoubleToIntWithScaleInt64);
                  }
                } 
                else
                {
                  if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                    result.emplace_back(normalizeXFloatToIntNoScale);
                  else
                    result.emplace_back(normalizeXDoubleToIntNoScale);
                }
                break;
              }

            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT: 
            {
              if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                result.emplace_back(normalizeXFloatToUint);
              else
                result.emplace_back(normalizeXDoubleToUint);
              break;
            }

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT: 
            {
              if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                result.emplace_back(normalizeXFloatToXFloat);
              else
                result.emplace_back(normalizeXDoubleToXFloat);
              break;
            }
            
            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE: 
            {
              if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                result.emplace_back(normalizeXFloatToXDouble);
              else
                result.emplace_back(normalizeXDoubleToXDouble);
              break;
            }
            
            case CalpontSystemCatalog::LONGDOUBLE: 
            {
              if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                result.emplace_back(normalizeXFloatToLongDouble);
              else
                result.emplace_back(normalizeXDoubleToLongDouble);
              break;
            }
            
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: 
            {
              if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                result.emplace_back(normalizeXFloatToString);
              else
                result.emplace_back(normalizeXDoubleToString);
              break;
            }            
                        
            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              // xFLOAT or xDOUBLE to xDECIMAL conversion. Is it really possible?
              // TODO:
              // Perhaps we should add an assert here that this combination is not possible
              // In the current reduction all problems mentioned in the code under
              //  case "Signed INT to XDecimal" are also applicable here.
              // TODO: isn't overflow possible below?
              if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
              {
                if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                  result.emplace_back(normalizeXFloatToWideXDecimal);
                else
                  result.emplace_back(normalizeXDoubleToWideXDecimal);
                break;
              }
              else              
              {
                if (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT || in.getColTypes()[i] == CalpontSystemCatalog::UFLOAT)
                  result.emplace_back(normalizeXFloatToXDecimal);
                else
                  result.emplace_back(normalizeXDoubleToXDecimal);
                break;
              }              
              break;
            }

            default:
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: floating point to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
          }

          break;
        }

        case CalpontSystemCatalog::LONGDOUBLE:
        {
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            {
              if (out->getScale(i))
              {
                if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                  result.emplace_back(normalizeLongDoubleToIntWithScaleInt128);
                else
                  result.emplace_back(normalizeLongDoubleToIntWithScaleInt);
              } 
              else
                result.emplace_back(normalizeLongDoubleToIntNoScale); 
              break;
            }

            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT: result.emplace_back(normalizeLongDoubleToUint); break;

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT: result.emplace_back(normalizeLongDoubleToXFloat); break;

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE: result.emplace_back(normalizeLongDoubleToXDouble); break;

            case CalpontSystemCatalog::LONGDOUBLE: result.emplace_back(normalizeLongDoubleToLongDouble); break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR: result.emplace_back(normalizeLongDoubleToString); break;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              // LONGDOUBLE to xDECIMAL conversions: is it really possible?
              // TODO:
              // Perhaps we should add an assert here that this combination is not possible
              // In the current reduction all problems mentioned in the code under
              //  case "Signed INT to XDecimal" are also applicable here.
              if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                result.emplace_back(normalizeLongDoubleToXDecimalInt128);
              else
                result.emplace_back(normalizeLongDoubleToXDecimalInt);
              
              break;
            }

            default:
              ostringstream os;
              os << "TupleUnion::normalize(): tried an illegal conversion: floating point to "
                << out->getColTypes()[i];
              throw logic_error(os.str());
          }

          break;
        }

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
          switch (out->getColTypes()[i])
          {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              if (datatypes::isWideDecimalType(out->getColTypes()[i], out->getColumnWidth(i)))
              {
                if (out->getScale(i) == in.getScale(i))
                {
                  if (in.getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                    result.emplace_back(normalizeWideXDecimalToWideXDecimalNoScale);
                  else
                    result.emplace_back(normalizeXDecimalToWideXDecimalNoScale);
                }
                else if (out->getScale(i) > in.getScale(i))
                {
                  if (in.getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
                    result.emplace_back(normalizeWideXDecimalToWideXDecimalWithScale);
                  else
                    result.emplace_back(normalizeXDecimalToWideXDecimalWithScale);
                }
                else  // should not happen, the output's scale is the largest
                  throw logic_error("TupleUnion::normalize(): incorrect scale setting");
              }
              // If output type is narrow decimal, input type
              // has to be narrow decimal as well.
              else
              {
                if (out->getScale(i) == in.getScale(i))
                  result.emplace_back(normalizeXDecimalToOtherNoScale);
                else if (out->getScale(i) > in.getScale(i))
                  result.emplace_back(normalizeXDecimalToOtherWithScale);
                else  // should not happen, the output's scale is the largest
                  throw logic_error("TupleUnion::normalize(): incorrect scale setting");
              }

              break;
            }

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT: result.emplace_back(normalizeXDecimalToXFloat); break; 

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE: result.emplace_back(normalizeXDecimalToXDouble); break;

            case CalpontSystemCatalog::LONGDOUBLE: result.emplace_back(normalizeXDecimalToLongDouble); break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::VARCHAR:
            default:
            {
              if (LIKELY(in.getColumnWidth(i) == datatypes::MAXDECIMALWIDTH))
                result.emplace_back(normalizeWideXDecimalToString);
              else
                result.emplace_back(normalizeXDecimalToString);
              break;
            }
          }

          break;
        }

        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::VARBINARY: result.emplace_back(normalizeBlobVarbinary); break;

        default:
        {
          ostringstream os;
          os << "TupleUnion::normalize(): unknown input type (" << in.getColTypes()[i] << ")";
          cout << os.str() << endl;
          throw logic_error(os.str());
        }
      }
    }

    idbassert(out->getColumnCount() == result.size());
    return result;
  }

}  // namespace

namespace joblist
{
inline uint64_t TupleUnion::Hasher::operator()(const RowPosition& p) const
{
  Row& row = ts->row;

  if (p.group & RowPosition::normalizedFlag)
    ts->normalizedData[p.group & ~RowPosition::normalizedFlag].getRow(p.row, &row);
  else
    ts->rowMemory[p.group].getRow(p.row, &row);

  return row.hash();
}

inline bool TupleUnion::Eq::operator()(const RowPosition& d1, const RowPosition& d2) const
{
  Row &r1 = ts->row, &r2 = ts->row2;

  if (d1.group & RowPosition::normalizedFlag)
    ts->normalizedData[d1.group & ~RowPosition::normalizedFlag].getRow(d1.row, &r1);
  else
    ts->rowMemory[d1.group].getRow(d1.row, &r1);

  if (d2.group & RowPosition::normalizedFlag)
    ts->normalizedData[d2.group & ~RowPosition::normalizedFlag].getRow(d2.row, &r2);
  else
    ts->rowMemory[d2.group].getRow(d2.row, &r2);

  return r1.equals(r2);
}

TupleUnion::TupleUnion(CalpontSystemCatalog::OID tableOID, const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fTableOID(tableOID)
 , output(NULL)
 , outputIt(-1)
 , memUsage(0)
 , rm(jobInfo.rm)
 , runnersDone(0)
 , distinctCount(0)
 , distinctDone(0)
 , fRowsReturned(0)
 , runRan(false)
 , joinRan(false)
 , sessionMemLimit(jobInfo.umMemLimit)
 , fTimeZone(jobInfo.timeZone)
{
  uniquer.reset(new Uniquer_t(10, Hasher(this), Eq(this), allocator));
  fExtendedInfo = "TUN: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TUN;
}

TupleUnion::~TupleUnion()
{
  rm->returnMemory(memUsage, sessionMemLimit);

  if (!runRan && output)
    output->endOfInput();
}

CalpontSystemCatalog::OID TupleUnion::tableOid() const
{
  return fTableOID;
}

void TupleUnion::setInputRowGroups(const vector<rowgroup::RowGroup>& in)
{
  inputRGs = in;
}

void TupleUnion::setOutputRowGroup(const rowgroup::RowGroup& out)
{
  outputRG = out;
  rowLength = outputRG.getRowSizeWithStrings();
}

void TupleUnion::setDistinctFlags(const vector<bool>& v)
{
  distinctFlags = v;
}

void TupleUnion::readInput(uint32_t which)
{
  /* The handling of the output got a little kludgey with the string table enhancement.
   * When there is no distinct check, the outputs are all generated independently of
   * each other locally in this fcn.  When there is a distinct check, threads
   * share the output, which is built in the 'rowMemory' vector rather than in
   * thread-local memory.  Building the result in a common space allows us to
   * store 8-byte offsets in rowMemory rather than 16-bytes for absolute pointers.
   */

  RowGroupDL* dl = NULL;
  bool more = true;
  RGData inRGData, outRGData, *tmpRGData;
  uint32_t it = numeric_limits<uint32_t>::max();
  RowGroup l_inputRG, l_outputRG, l_tmpRG;
  Row inRow, outRow, tmpRow;
  bool distinct;
  uint64_t memUsageBefore, memUsageAfter, memDiff;
  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;

  l_outputRG = outputRG;
  dl = inputs[which];
  l_inputRG = inputRGs[which];
  l_inputRG.initRow(&inRow);
  l_outputRG.initRow(&outRow);
  distinct = distinctFlags[which];

  if (distinct)
  {
    l_tmpRG = outputRG;
    tmpRGData = &normalizedData[which];
    l_tmpRG.initRow(&tmpRow);
    l_tmpRG.setData(tmpRGData);
    l_tmpRG.resetRowGroup(0);
    l_tmpRG.getRow(0, &tmpRow);
  }
  else
  {
    outRGData = RGData(l_outputRG);
    l_outputRG.setData(&outRGData);
    l_outputRG.resetRowGroup(0);
    l_outputRG.getRow(0, &outRow);
  }

  try
  {
    it = dl->getIterator();
    more = dl->next(it, &inRGData);

    if (dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    if (fStartTime == -1)
    {
      sts.msg_type = StepTeleStats::ST_START;
      sts.total_units_of_work = 1;
      postStepStartTele(sts);
    }

    while (more && !cancelled())
    {
      /*
          normalize each row
            if distinct flag is set
                  copy the row into the output and test for uniqueness
                    if unique, increment the row count
            else
              copy the row into the output & inc row count
      */
      l_inputRG.setData(&inRGData);
      l_inputRG.getRow(0, &inRow);

      if (distinct)
      {
        memDiff = 0;
        l_tmpRG.resetRowGroup(0);
        l_tmpRG.getRow(0, &tmpRow);
        l_tmpRG.setRowCount(l_inputRG.getRowCount());

        const normalizeFunctionsT normalizeFunctions = inferNormalizeFunctions(inRow, &tmpRow, fTimeZone);
        for (uint32_t i = 0; i < l_inputRG.getRowCount(); i++, inRow.nextRow(), tmpRow.nextRow())
          normalize(inRow, &tmpRow, normalizeFunctions);

        l_tmpRG.getRow(0, &tmpRow);
        {
          boost::mutex::scoped_lock lk(uniquerMutex);
          getOutput(&l_outputRG, &outRow, &outRGData);
          memUsageBefore = allocator.getMemUsage();

          uint32_t tmpOutputRowCount = l_outputRG.getRowCount();
          const uint32_t tmpRGRowCount = l_tmpRG.getRowCount();
          for (uint32_t i = 0; i < tmpRGRowCount; i++, tmpRow.nextRow())
          {
            pair<Uniquer_t::iterator, bool> inserted;
            inserted = uniquer->insert(RowPosition(which | RowPosition::normalizedFlag, i));

            if (inserted.second)
            {
              copyRow(tmpRow, &outRow);
              const_cast<RowPosition&>(*(inserted.first)) =
                  RowPosition(rowMemory.size() - 1, tmpOutputRowCount);
              memDiff += outRow.getRealSize();
              addToOutput(&outRow, &l_outputRG, true, outRGData, tmpOutputRowCount);
              fRowsReturned++;
            }
          }

          l_outputRG.setRowCount(tmpOutputRowCount);

          memUsageAfter = allocator.getMemUsage();
          memDiff += (memUsageAfter - memUsageBefore);
        }

        if (rm->getMemory(memDiff, sessionMemLimit))
        {
          memUsage += memDiff;
        }
        else
        {
          fLogger->logMessage(logging::LOG_TYPE_INFO, logging::ERR_UNION_TOO_BIG);

          if (status() == 0)  // preserve existing error code
          {
            errorMessage(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_UNION_TOO_BIG));
            status(logging::ERR_UNION_TOO_BIG);
          }

          abort();
        }
      }
      else
      {
        const normalizeFunctionsT normalizeFunctions = inferNormalizeFunctions(inRow, &outRow, fTimeZone);
        const uint32_t inputRGRowCount = l_inputRG.getRowCount();
        uint32_t tmpOutputRowCount = l_outputRG.getRowCount();

        for (uint32_t i = 0; i < inputRGRowCount; i++, inRow.nextRow())
        {
          normalize(inRow, &outRow, normalizeFunctions);
          addToOutput(&outRow, &l_outputRG, false, outRGData, tmpOutputRowCount);
        }

        fRowsReturned += inputRGRowCount;
        l_outputRG.setRowCount(tmpOutputRowCount);
      }

      more = dl->next(it, &inRGData);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::unionStepErr, logging::ERR_UNION_TOO_BIG,
                    "TupleUnion::readInput()");
    status(logging::unionStepErr);
    abort();
  }

  /* make sure that the input was drained before exiting.  This can happen if the
  query was aborted */
  if (dl && it != numeric_limits<uint32_t>::max())
    while (more)
      more = dl->next(it, &inRGData);

  {
    boost::mutex::scoped_lock lock1(uniquerMutex);
    boost::mutex::scoped_lock lock2(sMutex);

    if (!distinct && l_outputRG.getRowCount() > 0)
      output->insert(outRGData);

    if (distinct)
    {
      getOutput(&l_outputRG, &outRow, &outRGData);

      if (++distinctDone == distinctCount && l_outputRG.getRowCount() > 0)
        output->insert(outRGData);
    }

    if (++runnersDone == fInputJobStepAssociation.outSize())
    {
      output->endOfInput();

      sts.msg_type = StepTeleStats::ST_SUMMARY;
      sts.total_units_of_work = sts.units_of_work_completed = 1;
      sts.rows = fRowsReturned;
      postStepSummaryTele(sts);

      if (traceOn())
      {
        dlTimes.setLastReadTime();
        dlTimes.setEndOfInputTime();

        time_t t = time(0);
        char timeString[50];
        ctime_r(&t, timeString);
        timeString[strlen(timeString) - 1] = '\0';
        ostringstream logStr;
        logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString
               << "; total rows returned-" << fRowsReturned << endl
               << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
               << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
               << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
               << "\tJob completion status " << status() << endl;
        logEnd(logStr.str().c_str());
        fExtendedInfo += logStr.str();
        formatMiniStats();
      }
    }
  }
}

uint32_t TupleUnion::nextBand(messageqcpp::ByteStream& bs)
{
  RGData mem;
  bool more;
  uint32_t ret = 0;

  bs.restart();
  more = output->next(outputIt, &mem);

  if (more)
    outputRG.setData(&mem);
  else
  {
    mem = RGData(outputRG, 0);
    outputRG.setData(&mem);
    outputRG.resetRowGroup(0);
    outputRG.setStatus(status());
  }

  outputRG.serializeRGData(bs);
  ret = outputRG.getRowCount();

  return ret;
}

void TupleUnion::getOutput(RowGroup* rg, Row* row, RGData* data)
{
  if (UNLIKELY(rowMemory.empty()))
  {
    *data = RGData(*rg);
    rg->setData(data);
    rg->resetRowGroup(0);
    rowMemory.push_back(*data);
  }
  else
  {
    *data = rowMemory.back();
    rg->setData(data);
  }

  rg->getRow(rg->getRowCount(), row);
}

void TupleUnion::addToOutput(Row* r, RowGroup* rg, bool keepit, RGData& data, uint32_t& tmpOutputRowCount)
{
  r->nextRow();
  tmpOutputRowCount++;

  if (UNLIKELY(tmpOutputRowCount == 8192))
  {
    rg->setRowCount(8192);
    {
      boost::mutex::scoped_lock lock(sMutex);
      output->insert(data);
    }
    data = RGData(*rg);
    rg->setData(&data);
    rg->resetRowGroup(0);
    rg->getRow(0, r);
    tmpOutputRowCount = 0;

    if (keepit)
      rowMemory.push_back(data);
  }
}

void TupleUnion::normalize(const Row& in, Row* out, const normalizeFunctionsT& normalizeFunctions)
{
  uint32_t i;

  out->setRid(0);

  for (i = 0; i < out->getColumnCount(); i++)
  {
    if (in.isNullValue(i))
    {
      TupleUnion::writeNull(out, i);
      continue;
    }

    /// Call the pre-compiled function.
    normalizeFunctions[i](in, out, i);
  }
}

void TupleUnion::run()
{
  uint32_t i;

  boost::mutex::scoped_lock lk(jlLock);

  if (runRan)
    return;

  runRan = true;
  lk.unlock();

  for (i = 0; i < fInputJobStepAssociation.outSize(); i++)
    inputs.push_back(fInputJobStepAssociation.outAt(i)->rowGroupDL());

  output = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

  if (fDelivery)
  {
    outputIt = output->getIterator();
  }

  outputRG.initRow(&row);
  outputRG.initRow(&row2);

  distinctCount = 0;
  normalizedData.reset(new RGData[inputs.size()]);

  for (i = 0; i < inputs.size(); i++)
  {
    if (distinctFlags[i])
    {
      distinctCount++;
      normalizedData[i].reinit(outputRG);
    }
  }

  runners.reserve(inputs.size());

  for (i = 0; i < inputs.size(); i++)
  {
    runners.push_back(jobstepThreadPool.invoke(Runner(this, i)));
  }
}

void TupleUnion::join()
{
  boost::mutex::scoped_lock lk(jlLock);

  if (joinRan)
    return;

  joinRan = true;
  lk.unlock();

  jobstepThreadPool.join(runners);
  runners.clear();
  uniquer->clear();
  rowMemory.clear();
  rm->returnMemory(memUsage, sessionMemLimit);
  memUsage = 0;
}

const string TupleUnion::toString() const
{
  ostringstream oss;
  oss << "TupleUnion       ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId;
  oss << " st:" << fStepId;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << ((i == 0) ? " " : ", ") << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << ((i == 0) ? " " : ", ") << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleUnion::writeNull(Row* out, uint32_t col)
{
  switch (out->getColTypes()[col])
  {
    case CalpontSystemCatalog::TINYINT: out->setUintField<1>(joblist::TINYINTNULL, col); break;

    case CalpontSystemCatalog::SMALLINT: out->setUintField<1>(joblist::SMALLINTNULL, col); break;

    case CalpontSystemCatalog::UTINYINT: out->setUintField<1>(joblist::UTINYINTNULL, col); break;

    case CalpontSystemCatalog::USMALLINT: out->setUintField<1>(joblist::USMALLINTNULL, col); break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      uint32_t len = out->getColumnWidth(col);

      switch (len)
      {
        case 1: out->setUintField<1>(joblist::TINYINTNULL, col); break;

        case 2: out->setUintField<1>(joblist::SMALLINTNULL, col); break;

        case 4: out->setUintField<4>(joblist::INTNULL, col); break;

        case 8: out->setUintField<8>(joblist::BIGINTNULL, col); break;

        default:
        {
        }
      }

      break;
    }

    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: out->setUintField<4>(joblist::INTNULL, col); break;

    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: out->setUintField<4>(joblist::UINTNULL, col); break;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: out->setUintField<4>(joblist::FLOATNULL, col); break;

    case CalpontSystemCatalog::DATE: out->setUintField<4>(joblist::DATENULL, col); break;

    case CalpontSystemCatalog::BIGINT: out->setUintField<8>(joblist::BIGINTNULL, col); break;

    case CalpontSystemCatalog::UBIGINT: out->setUintField<8>(joblist::UBIGINTNULL, col); break;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: out->setUintField<8>(joblist::DOUBLENULL, col); break;

    case CalpontSystemCatalog::DATETIME: out->setUintField<8>(joblist::DATETIMENULL, col); break;

    case CalpontSystemCatalog::TIMESTAMP: out->setUintField<8>(joblist::TIMESTAMPNULL, col); break;

    case CalpontSystemCatalog::TIME: out->setUintField<8>(joblist::TIMENULL, col); break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
    {
      uint32_t len = out->getColumnWidth(col);

      switch (len)
      {
        case 1: out->setUintField<1>(joblist::CHAR1NULL, col); break;

        case 2: out->setUintField<2>(joblist::CHAR2NULL, col); break;

        case 3:
        case 4: out->setUintField<4>(joblist::CHAR4NULL, col); break;

        case 5:
        case 6:
        case 7:
        case 8: out->setUintField<8>(joblist::CHAR8NULL, col); break;

        default: out->setStringField(nullptr, 0, col); break;
      }

      break;
    }

    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::VARBINARY:
      // could use below if zero length and NULL are treated the same
      // out->setVarBinaryField("", col); break;
      out->setVarBinaryField(nullptr, 0, col);
      break;

    default:
    {
    }
  }
}

void TupleUnion::formatMiniStats()
{
  ostringstream oss;
  oss << "TUS "
      << "UM "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- " << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
      << fRowsReturned << " ";
  fMiniInfo += oss.str();
}

}  // namespace joblist
