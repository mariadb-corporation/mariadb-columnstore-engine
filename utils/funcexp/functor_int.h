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

//  $Id: functor_int.h 3642 2013-03-18 19:09:53Z bpaul $

/** @file */

#pragma once

#include "functor.h"

namespace funcexp
{
/** @brief Func_Int class
 *    For function that returns a integer result.
 *        Must implement getIntVal()
 *        Implement any other methods that behave differently from the default.
 */
class Func_Int : public Func
{
 public:
  Func_Int() = default;
  explicit Func_Int(const std::string& funcName) : Func(funcName)
  {
  }
  ~Func_Int() override = default;

  /*
      int64_t getIntVal(rowgroup::Row& row,
                                              FunctionParm& fp,
                                              bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
  */

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return ((double)getIntVal(row, fp, isNull, op_ct));
  }

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return ((long double)getIntVal(row, fp, isNull, op_ct));
  }

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return intToString(getIntVal(row, fp, isNull, op_ct));
  }
};

class Func_BitOp : public Func_Int
{
 public:
  explicit Func_BitOp(const std::string& funcName) : Func_Int(funcName)
  {
  }
  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& /*fp*/, execplan::CalpontSystemCatalog::ColType& resultType) override
  {
    return resultType;
  }
  bool validateArgCount(execplan::FunctionColumn& col, uint expected) const;
  void setFunctorByParm(execplan::FunctionColumn& col, const execplan::SPTP& parm,
                        Func_Int& return_uint64_from_uint64, Func_Int& return_uint64_from_sint64,
                        Func_Int& return_uint64_from_generic) const;
  // Fix for << and >>
  bool fixForBitShift(execplan::FunctionColumn& col, Func_Int& return_uint64_from_uint64,
                      Func_Int& return_uint64_from_sint64, Func_Int& return_uint64_from_generic) const;
  // Fix for & | ^
  bool fixForBitOp2(execplan::FunctionColumn& col, Func_Int& return_uint64_from_uint64_uint64,
                    Func_Int& return_uint64_from_sint64_sint64,
                    Func_Int& return_uint64_from_generic_generic) const;

  int64_t getIntVal(rowgroup::Row& /*row*/, FunctionParm& /*fp*/, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    isNull = true;
    return 0;
  }
};

/** @brief Func_instr class
 */
class Func_instr : public Func_Int
{
 public:
  Func_instr() : Func_Int("instr")
  {
  }
  ~Func_instr() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_length class
 */
class Func_length : public Func_Int
{
 public:
  Func_length() : Func_Int("length")
  {
  }
  ~Func_length() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_sign class
 */
class Func_sign : public Func_Int
{
 public:
  Func_sign() : Func_Int("sign")
  {
  }
  ~Func_sign() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_day class
 */
class Func_day : public Func_Int
{
 public:
  Func_day() : Func_Int("day")
  {
  }
  ~Func_day() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_minute class
 */
class Func_minute : public Func_Int
{
 public:
  Func_minute() : Func_Int("minute")
  {
  }
  ~Func_minute() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_month class
 */
class Func_month : public Func_Int
{
 public:
  Func_month() : Func_Int("month")
  {
  }
  ~Func_month() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_week class
 */
class Func_week : public Func_Int
{
 public:
  Func_week() : Func_Int("week")
  {
  }
  ~Func_week() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_year class
 */
class Func_year : public Func_Int
{
 public:
  Func_year() : Func_Int("year")
  {
  }
  ~Func_year() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_to_days class
 */
class Func_to_days : public Func_Int
{
 public:
  Func_to_days() : Func_Int("to_days")
  {
  }
  ~Func_to_days() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_char_length class
 */
class Func_char_length : public Func_Int
{
 public:
  Func_char_length() : Func_Int("length")
  {
  }
  ~Func_char_length() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_extract class
 */
class Func_extract : public Func_Int
{
 public:
  Func_extract() : Func_Int("extract")
  {
  }
  ~Func_extract() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cast_signed class
 */
class Func_cast_signed : public Func_Int
{
 public:
  Func_cast_signed() : Func_Int("cast_signed")
  {
  }
  ~Func_cast_signed() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cast_unsigned class
 */
class Func_cast_unsigned : public Func_Int
{
 public:
  Func_cast_unsigned() : Func_Int("cast_unsigned")
  {
  }
  ~Func_cast_unsigned() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return (int64_t)(getUintVal(row, fp, isNull, op_ct));
  }

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_bitand class
 */
class Func_bitand : public Func_BitOp
{
 public:
  Func_bitand() : Func_BitOp("bitand")
  {
  }
  ~Func_bitand() override = default;
  bool fix(execplan::FunctionColumn& col) const override;
};

/** @brief Func_bitor class
 */
class Func_bitor : public Func_BitOp
{
 public:
  Func_bitor() : Func_BitOp("bitor")
  {
  }
  ~Func_bitor() override = default;

  bool fix(execplan::FunctionColumn& col) const override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_bitxor class
 */
class Func_bitxor : public Func_BitOp
{
 public:
  Func_bitxor() : Func_BitOp("bitxor")
  {
  }
  ~Func_bitxor() override = default;
  bool fix(execplan::FunctionColumn& col) const override;
};

/** @brief Func_bit_count class
 */
class Func_bit_count : public Func_BitOp
{
 public:
  Func_bit_count() : Func_BitOp("bit_count")
  {
  }
  ~Func_bit_count() override = default;
  bool fix(execplan::FunctionColumn& col) const override;
};

/** @brief Func_hour class
 */
class Func_hour : public Func_Int
{
 public:
  Func_hour() : Func_Int("hour")
  {
  }
  ~Func_hour() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_second class
 */
class Func_second : public Func_Int
{
 public:
  Func_second() : Func_Int("second")
  {
  }
  ~Func_second() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_dayofweek class
 */
class Func_dayofweek : public Func_Int
{
 public:
  Func_dayofweek() : Func_Int("dayofweek")
  {
  }
  ~Func_dayofweek() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_dayofyear class
 */
class Func_dayofyear : public Func_Int
{
 public:
  Func_dayofyear() : Func_Int("dayofyear")
  {
  }
  ~Func_dayofyear() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_leftshift class
 */
class Func_leftshift : public Func_BitOp
{
 public:
  Func_leftshift() : Func_BitOp("leftshift")
  {
  }
  ~Func_leftshift() override = default;
  bool fix(execplan::FunctionColumn& col) const override;
};

/** @brief Func_rightshift class
 */
class Func_rightshift : public Func_BitOp
{
 public:
  Func_rightshift() : Func_BitOp("rightshift")
  {
  }
  ~Func_rightshift() override = default;
  bool fix(execplan::FunctionColumn& col) const override;
};

/** @brief Func_quarter class
 */
class Func_quarter : public Func_Int
{
 public:
  Func_quarter() : Func_Int("quarter")
  {
  }
  ~Func_quarter() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_ascii class
 */
class Func_ascii : public Func_Int
{
 public:
  Func_ascii() : Func_Int("ascii")
  {
  }
  ~Func_ascii() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_dayname class
 */
class Func_dayname : public Func_Int
{
 public:
  Func_dayname() : Func_Int("dayname")
  {
  }
  ~Func_dayname() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_weekday class
 */
class Func_weekday : public Func_Int
{
 public:
  Func_weekday() : Func_Int("weekday")
  {
  }
  ~Func_weekday() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_yearweek class
 */
class Func_yearweek : public Func_Int
{
 public:
  Func_yearweek() : Func_Int("yearweek")
  {
  }
  ~Func_yearweek() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_last_day class
 */
class Func_last_day : public Func_Int
{
 public:
  Func_last_day() : Func_Int("last_day")
  {
  }
  ~Func_last_day() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_time_to_sec class
 */
class Func_time_to_sec : public Func_Int
{
 public:
  Func_time_to_sec() : Func_Int("time_to_sec")
  {
  }
  ~Func_time_to_sec() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_microsecond class
 */
class Func_microsecond : public Func_Int
{
 public:
  Func_microsecond() : Func_Int("microsecond")
  {
  }
  ~Func_microsecond() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_crc32 class
 */
class Func_crc32 : public Func_Int
{
 public:
  Func_crc32() : Func_Int("crc32")
  {
  }
  ~Func_crc32() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_period_add class
 */
class Func_period_add : public Func_Int
{
 public:
  Func_period_add() : Func_Int("period_add")
  {
  }
  ~Func_period_add() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_period_diff class
 */
class Func_period_diff : public Func_Int
{
 public:
  Func_period_diff() : Func_Int("period_diff")
  {
  }
  ~Func_period_diff() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_strcmp class
 */
class Func_strcmp : public Func_Int
{
 public:
  Func_strcmp() : Func_Int("strcmp")
  {
  }
  ~Func_strcmp() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_unix_timestamp class
 */
class Func_unix_timestamp : public Func_Int
{
 public:
  Func_unix_timestamp() : Func_Int("unix_timestamp")
  {
  }
  ~Func_unix_timestamp() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_strcmp class
 */
class Func_find_in_set : public Func_Int
{
 public:
  Func_find_in_set() : Func_Int("find_in_set")
  {
  }
  ~Func_find_in_set() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

}  // namespace funcexp
