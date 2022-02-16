/* Copyright (C) 2021 MariaDB Corporation

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

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "intervalcolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "bytestream.h"
using namespace messageqcpp;
using namespace funcexp;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace funcexp
{
// need my_tz_find in dataconvert

CalpontSystemCatalog::ColType Func_convert_tz::operationType(FunctionParm& fp,
                                                             CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_convert_tz::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                           execplan::CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::intToDatetime(getIntVal(row, parm, isNull, ct));
}

int64_t Func_convert_tz::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  messageqcpp::ByteStream bs;
  bool bFromTz = false;
  bool bToTz = false;
  dataconvert::TIME_ZONE_INFO tzinfo;
  std::vector<int64_t> ats;
  std::vector<unsigned char> types;
  std::vector<TRAN_TYPE_INFO> ttis;
#ifdef ABBR_ARE_USED
  std::vector<char> chars;
#endif
  std::vector<LS_INFO> lsis;
  std::vector<int64_t> revts;
  std::vector<REVT_INFO> revtis;
  std::vector<TRAN_TYPE_INFO> fallback_tti;

  int64_t seconds = 0;
  uint32_t err_code = 0;
  int64_t datetime_start = parm[0]->data()->getDatetimeIntVal(row, isNull);
  int64_t rtn = 0;

  if (isNull)
    return 0;
  // Adding a zero date to a time is always NULL
  if (datetime_start == 0)
  {
    isNull = true;
    return -1;
  }

  const string& from_tz = parm[1]->data()->getStrVal(row, isNull);
  if (isNull)
  {
    return 0;
  }
  const string& to_tz = parm[2]->data()->getStrVal(row, isNull);
  if (isNull)
  {
    return 0;
  }

  cout << "from " << from_tz << endl;
  cout << "to " << to_tz << endl;

  const string& serialized_from_tzinfo = parm[3]->data()->getStrVal(row, isNull);
  const string& serialized_to_tzinfo = parm[4]->data()->getStrVal(row, isNull);

  if (!serialized_from_tzinfo.empty())
  {
    bs.append((uint8_t*)serialized_from_tzinfo.c_str(), serialized_from_tzinfo.length());
    dataconvert::unserializeTimezoneInfo(bs, &tzinfo);
    deserializeInlineVector<int64_t>(bs, ats);
    tzinfo.ats = ats.data();
    deserializeInlineVector<unsigned char>(bs, types);
    tzinfo.types = types.data();
    deserializeInlineVector<TRAN_TYPE_INFO>(bs, ttis);
    tzinfo.ttis = ttis.data();
#ifdef ABBR_ARE_USED
    deserializeInlineVector<char>(bs_fromTZ, chars);
    tzinfo.chars = chars.data();
#endif
    deserializeInlineVector<LS_INFO>(bs, lsis);
    tzinfo.lsis = lsis.data();
    deserializeInlineVector<int64_t>(bs, revts);
    tzinfo.revts = revts.data();
    deserializeInlineVector<REVT_INFO>(bs, revtis);
    tzinfo.revtis = revtis.data();
    deserializeInlineVector<TRAN_TYPE_INFO>(bs, fallback_tti);
    tzinfo.fallback_tti = fallback_tti.data();
    bFromTz = true;
  }

  dataconvert::DateTime datetime(datetime_start);
  if (!dataconvert::DataConvert::isColumnDateTimeValid(datetime_start))
    return datetime_start;

  // int64_t result_datetime;
  bool valid = true;
  MySQLTime my_time_tmp, my_start_time;
  my_time_tmp.reset();
  my_start_time.year = datetime.year;
  my_start_time.month = datetime.month;
  my_start_time.day = datetime.day;
  my_start_time.hour = datetime.hour;
  my_start_time.minute = datetime.minute;
  my_start_time.second = datetime.second;
  my_start_time.second_part = datetime.msecond;

  if (bFromTz)
  {
    seconds = dataconvert::TIME_to_gmt_sec(my_start_time, &tzinfo, &err_code);
    if (err_code)
    {
      return datetime.convertToMySQLint();
    }
  }
  else
  {
    long from_tz_offset;
    dataconvert::timeZoneToOffset(from_tz.c_str(), from_tz.size(), &from_tz_offset);
    seconds = dataconvert::mySQLTimeToGmtSec(my_start_time, from_tz_offset, valid);
    if (!valid)
    {
      if (seconds != 0)
        isNull = true;
      return datetime.convertToMySQLint();
    }
  }

  if (!serialized_to_tzinfo.empty())
  {
    bs.reset();
    bs.append((uint8_t*)serialized_to_tzinfo.c_str(), serialized_to_tzinfo.length());
    dataconvert::unserializeTimezoneInfo(bs, &tzinfo);
    deserializeInlineVector<int64_t>(bs, ats);
    tzinfo.ats = ats.data();
    deserializeInlineVector<unsigned char>(bs, types);
    tzinfo.types = types.data();
    deserializeInlineVector<TRAN_TYPE_INFO>(bs, ttis);
    tzinfo.ttis = ttis.data();
#ifdef ABBR_ARE_USED
    deserializeInlineVector<char>(bs_fromTZ, chars);
    tzinfo.chars = chars.data();
#endif
    deserializeInlineVector<LS_INFO>(bs, lsis);
    tzinfo.lsis = lsis.data();
    deserializeInlineVector<int64_t>(bs, revts);
    tzinfo.revts = revts.data();
    deserializeInlineVector<REVT_INFO>(bs, revtis);
    tzinfo.revtis = revtis.data();
    deserializeInlineVector<TRAN_TYPE_INFO>(bs, fallback_tti);
    tzinfo.fallback_tti = fallback_tti.data();
    bToTz = true;
  }
  if (bToTz)
  {
    dataconvert::gmt_sec_to_TIME(&my_time_tmp, seconds, &tzinfo);
    if (my_time_tmp.second == 60 || my_time_tmp.second == 61)
      my_time_tmp.second = 59;
  }
  else
  {
    long to_tz_offset;
    dataconvert::timeZoneToOffset(to_tz.c_str(), to_tz.size(), &to_tz_offset);
    dataconvert::gmtSecToMySQLTime(seconds, my_time_tmp, to_tz_offset);
  }

  dataconvert::DateTime result_datetime(my_time_tmp.year, my_time_tmp.month, my_time_tmp.day,
                                        my_time_tmp.hour, my_time_tmp.minute, my_time_tmp.second,
                                        my_time_tmp.second_part);

  if ((rtn = result_datetime.convertToMySQLint()) == 0)
    isNull = true;

  return rtn;
}

string Func_convert_tz::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                  CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::datetimeToString(getIntVal(row, parm, isNull, ct));
}

}  // namespace funcexp
// vim:ts=4 sw=4:
