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

//  $Id: functor_real.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#pragma once

#include "functor.h"

namespace funcexp
{
/** @brief Func_Real class
 *    For function that returns a int/decimal/double result.
 *        Must implement getDoubleVal()
 *        Implement any other methods that behave differently from the default.
 *        Note: getIntVal is a good candidate to be implemented, too.
 */
class Func_Real : public Func
{
 public:
  Func_Real() = default;
  explicit Func_Real(const std::string& funcName) : Func(funcName)
  {
  }
  ~Func_Real() override = default;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return ((int64_t)getDoubleVal(row, fp, isNull, op_ct));
  }

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return ((uint64_t)getDoubleVal(row, fp, isNull, op_ct));
  }

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return ((long double)getDoubleVal(row, fp, isNull, op_ct));
  }

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return doubleToString(getDoubleVal(row, fp, isNull, op_ct));
  }
};

/** @brief Func_abs class
 */
class Func_abs : public Func_Real
{
 public:
  Func_abs() : Func_Real("abs")
  {
  }
  ~Func_abs() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_exp class
 */
class Func_exp : public Func_Real
{
 public:
  Func_exp() : Func_Real("exp")
  {
  }
  ~Func_exp() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_pow class
 */
class Func_pow : public Func_Real
{
 public:
  Func_pow() : Func_Real("pow")
  {
  }
  ~Func_pow() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_round class
 */
class Func_round : public Func_Real
{
 public:
  Func_round() : Func_Real("round")
  {
  }
  ~Func_round() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_truncate class
 */
class Func_truncate : public Func_Real
{
 public:
  Func_truncate() : Func_Real("truncate")
  {
  }
  ~Func_truncate() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_ceil class
 */
class Func_ceil : public Func_Real
{
 public:
  Func_ceil() : Func_Real("ceil")
  {
  }
  ~Func_ceil() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_floor class
 */
class Func_floor : public Func_Real
{
 public:
  Func_floor() : Func_Real("floor")
  {
  }
  ~Func_floor() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cast_decimal class
 */
class Func_cast_decimal : public Func_Real
{
 public:
  Func_cast_decimal() : Func_Real("cast_decimal")
  {
  }
  ~Func_cast_decimal() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cast_double class
 */
class Func_cast_double : public Func_Real
{
 public:
  Func_cast_double() : Func_Real("cast_double")
  {
  }
  ~Func_cast_double() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_mod class
 */
class Func_mod : public Func_Real
{
 public:
  Func_mod() : Func_Real("mod")
  {
  }
  ~Func_mod() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& operationColType) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& operationColType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

 private:
  template <typename ModType>
  ModType doDecimal(const FunctionParm& parm, const int64_t div, rowgroup::Row& row, bool isNull)
  {
    execplan::IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

    if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
    {
      if (!d.isScaled())
      {
        return d.toTSInt128() % div;
      }
      else
      {
        auto intAndFract = d.getIntegralAndDividedFractional();

        datatypes::TSInt128 integralRemainder = intAndFract.first % div;
        return static_cast<ModType>(integralRemainder.toTFloat128() + intAndFract.second);
      }
    }
    int64_t value = d.value / static_cast<int64_t>(pow(10.0, d.scale));
    return value % div;
  }
};

/** @brief Func_acos class
 */
class Func_acos : public Func_Real
{
 public:
  Func_acos() : Func_Real("acos")
  {
  }
  ~Func_acos() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_asin class
 */
class Func_asin : public Func_Real
{
 public:
  Func_asin() : Func_Real("asin")
  {
  }
  ~Func_asin() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_atan class
 */
class Func_atan : public Func_Real
{
 public:
  Func_atan() : Func_Real("atan")
  {
  }
  ~Func_atan() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cos class
 */
class Func_cos : public Func_Real
{
 public:
  Func_cos() : Func_Real("cos")
  {
  }
  ~Func_cos() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_cot class
 */
class Func_cot : public Func_Real
{
 public:
  Func_cot() : Func_Real("cot")
  {
  }
  ~Func_cot() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_log class
 */
class Func_log : public Func_Real
{
 public:
  Func_log() : Func_Real("log")
  {
  }
  ~Func_log() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_log2 class
 */
class Func_log2 : public Func_Real
{
 public:
  Func_log2() : Func_Real("log2")
  {
  }
  ~Func_log2() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_log10 class
 */
class Func_log10 : public Func_Real
{
 public:
  Func_log10() : Func_Real("log10")
  {
  }
  ~Func_log10() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_sin class
 */
class Func_sin : public Func_Real
{
 public:
  Func_sin() : Func_Real("sin")
  {
  }
  ~Func_sin() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_sqrt class
 */
class Func_sqrt : public Func_Real
{
 public:
  Func_sqrt() : Func_Real("sqrt")
  {
  }
  ~Func_sqrt() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_tan class
 */
class Func_tan : public Func_Real
{
 public:
  Func_tan() : Func_Real("tan")
  {
  }
  ~Func_tan() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_radians class
 */
class Func_radians : public Func_Real
{
 public:
  Func_radians() : Func_Real("radians")
  {
  }
  ~Func_radians() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_div class
 */
class Func_div : public Func_Real
{
 public:
  Func_div() : Func_Real("DIV")
  {
  }
  ~Func_div() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  uint64_t getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_inet_aton class to convert ascii IP address to big-endian
 * (network ordered) int
 */
class Func_inet_aton : public Func_Real
{
 public:
  Func_inet_aton() : Func_Real("inet_aton")
  {
  }
  ~Func_inet_aton() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int32_t getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

 private:
  int64_t convertAton(const std::string& ipString, bool& isNull);
};

/** @brief Func_degrees class
 */
class Func_degrees : public Func_Real
{
 public:
  Func_degrees() : Func_Real("degrees")
  {
  }
  ~Func_degrees() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

}  // namespace funcexp