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
  Func_Int()
  {
  }
  Func_Int(const std::string& funcName) : Func(funcName)
  {
  }
  virtual ~Func_Int()
  {
  }

  /*
      int64_t getIntVal(rowgroup::Row& row,
                                              FunctionParm& fp,
                                              bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
  */

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return ((double)getIntVal(row, fp, isNull, op_ct));
  }

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return ((long double)getIntVal(row, fp, isNull, op_ct));
  }

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return intToString(getIntVal(row, fp, isNull, op_ct));
  }
};

class Func_BitOp : public Func_Int
{
 public:
  Func_BitOp(const std::string& funcName) : Func_Int(funcName)
  {
  }
  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override
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

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override
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
  virtual ~Func_instr()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_length class
 */
class Func_length : public Func_Int
{
 public:
  Func_length() : Func_Int("length")
  {
  }
  virtual ~Func_length()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_sign class
 */
class Func_sign : public Func_Int
{
 public:
  Func_sign() : Func_Int("sign")
  {
  }
  virtual ~Func_sign()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_day class
 */
class Func_day : public Func_Int
{
 public:
  Func_day() : Func_Int("day")
  {
  }
  virtual ~Func_day()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_minute class
 */
class Func_minute : public Func_Int
{
 public:
  Func_minute() : Func_Int("minute")
  {
  }
  virtual ~Func_minute()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_month class
 */
class Func_month : public Func_Int
{
 public:
  Func_month() : Func_Int("month")
  {
  }
  virtual ~Func_month()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_week class
 */
class Func_week : public Func_Int
{
 public:
  Func_week() : Func_Int("week")
  {
  }
  virtual ~Func_week()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_year class
 */
class Func_year : public Func_Int
{
 public:
  Func_year() : Func_Int("year")
  {
  }
  virtual ~Func_year()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_to_days class
 */
class Func_to_days : public Func_Int
{
 public:
  Func_to_days() : Func_Int("to_days")
  {
  }
  virtual ~Func_to_days()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_char_length class
 */
class Func_char_length : public Func_Int
{
 public:
  Func_char_length() : Func_Int("length")
  {
  }
  virtual ~Func_char_length()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_extract class
 */
class Func_extract : public Func_Int
{
 public:
  Func_extract() : Func_Int("extract")
  {
  }
  virtual ~Func_extract()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_cast_signed class
 */
class Func_cast_signed : public Func_Int
{
 public:
  Func_cast_signed() : Func_Int("cast_signed")
  {
  }
  virtual ~Func_cast_signed()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_cast_unsigned class
 */
class Func_cast_unsigned : public Func_Int
{
 public:
  Func_cast_unsigned() : Func_Int("cast_unsigned")
  {
  }
  virtual ~Func_cast_unsigned()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return (int64_t)(getUintVal(row, fp, isNull, op_ct));
  }

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_bitand class
 */
class Func_bitand : public Func_BitOp
{
 public:
  Func_bitand() : Func_BitOp("bitand")
  {
  }
  virtual ~Func_bitand()
  {
  }
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
  virtual ~Func_bitor()
  {
  }

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
  virtual ~Func_bitxor()
  {
  }
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
  virtual ~Func_bit_count()
  {
  }
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
  virtual ~Func_hour()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_second class
 */
class Func_second : public Func_Int
{
 public:
  Func_second() : Func_Int("second")
  {
  }
  virtual ~Func_second()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  bool isCompilable(const execplan::CalpontSystemCatalog::ColType& colType);

  llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                       llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                       execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_dayofweek class
 */
class Func_dayofweek : public Func_Int
{
 public:
  Func_dayofweek() : Func_Int("dayofweek")
  {
  }
  virtual ~Func_dayofweek()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_dayofyear class
 */
class Func_dayofyear : public Func_Int
{
 public:
  Func_dayofyear() : Func_Int("dayofyear")
  {
  }
  virtual ~Func_dayofyear()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_leftshift class
 */
class Func_leftshift : public Func_BitOp
{
 public:
  Func_leftshift() : Func_BitOp("leftshift")
  {
  }
  virtual ~Func_leftshift()
  {
  }
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
  virtual ~Func_rightshift()
  {
  }
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
  virtual ~Func_quarter()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_ascii class
 */
class Func_ascii : public Func_Int
{
 public:
  Func_ascii() : Func_Int("ascii")
  {
  }
  virtual ~Func_ascii()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_dayname class
 */
class Func_dayname : public Func_Int
{
 public:
  Func_dayname() : Func_Int("dayname")
  {
  }
  virtual ~Func_dayname()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_weekday class
 */
class Func_weekday : public Func_Int
{
 public:
  Func_weekday() : Func_Int("weekday")
  {
  }
  virtual ~Func_weekday()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_yearweek class
 */
class Func_yearweek : public Func_Int
{
 public:
  Func_yearweek() : Func_Int("yearweek")
  {
  }
  virtual ~Func_yearweek()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_last_day class
 */
class Func_last_day : public Func_Int
{
 public:
  Func_last_day() : Func_Int("last_day")
  {
  }
  virtual ~Func_last_day()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_time_to_sec class
 */
class Func_time_to_sec : public Func_Int
{
 public:
  Func_time_to_sec() : Func_Int("time_to_sec")
  {
  }
  virtual ~Func_time_to_sec()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_microsecond class
 */
class Func_microsecond : public Func_Int
{
 public:
  Func_microsecond() : Func_Int("microsecond")
  {
  }
  virtual ~Func_microsecond()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_crc32 class
 */
class Func_crc32 : public Func_Int
{
 public:
  Func_crc32() : Func_Int("crc32")
  {
  }
  virtual ~Func_crc32()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_period_add class
 */
class Func_period_add : public Func_Int
{
 public:
  Func_period_add() : Func_Int("period_add")
  {
  }
  virtual ~Func_period_add()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_period_diff class
 */
class Func_period_diff : public Func_Int
{
 public:
  Func_period_diff() : Func_Int("period_diff")
  {
  }
  virtual ~Func_period_diff()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_strcmp class
 */
class Func_strcmp : public Func_Int
{
 public:
  Func_strcmp() : Func_Int("strcmp")
  {
  }
  virtual ~Func_strcmp()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_unix_timestamp class
 */
class Func_unix_timestamp : public Func_Int
{
 public:
  Func_unix_timestamp() : Func_Int("unix_timestamp")
  {
  }
  virtual ~Func_unix_timestamp()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_strcmp class
 */
class Func_find_in_set : public Func_Int
{
 public:
  Func_find_in_set() : Func_Int("find_in_set")
  {
  }
  virtual ~Func_find_in_set()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct);

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct);
  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct);
  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct);
};

}  // namespace funcexp
