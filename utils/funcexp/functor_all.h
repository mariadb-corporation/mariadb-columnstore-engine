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

//  $Id: functor_all.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#pragma once

#include "functor.h"

namespace funcexp
{
/** @brief Func_All class
 *    For function that can retun all supported types.
 *        Must implement getIntVal(), getDoubleVal(), and getStrVal()
 *        Implement any other methods that behave differently from the default.
 *    Note: implementation based on result type will be more efficient.
 */
class Func_All : public Func
{
 public:
  Func_All() = default;
  explicit Func_All(const std::string& funcName) : Func(funcName)
  {
  }
  ~Func_All() override = default;

  /*
      int64_t getIntVal(rowgroup::Row& row,
                                              FunctionParm& fp,
                                              bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

      double getDoubleVal(rowgroup::Row& row,
                                              FunctionParm& fp,
                                              bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

      std::string getStrVal(rowgroup::Row& row,
                                              FunctionParm& fp,
                                              bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
  */
};

/** @brief Func_simple_case class
 */
class Func_simple_case : public Func_All
{
 public:
  Func_simple_case() : Func_All("case_simple")
  {
  }
  ~Func_simple_case() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

class Func_decode_oracle : public Func_All
{
 public:
  Func_decode_oracle() : Func_All("decode_oracle")
  {
  }
  ~Func_decode_oracle() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

/** @brief Func_searched_case class
 */
class Func_searched_case : public Func_All
{
 public:
  Func_searched_case() : Func_All("case_searched")
  {
  }
  ~Func_searched_case() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

/** @brief Func_if class
 */
class Func_if : public Func_All
{
 public:
  Func_if() : Func_All("Func_if")
  {
  }
  ~Func_if() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

/** @brief Func_ifnull class
 */
class Func_ifnull : public Func_All
{
 public:
  Func_ifnull() : Func_All("ifnull")
  {
  }
  ~Func_ifnull() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_greatest class
 */
class Func_greatest : public Func_All
{
 public:
  Func_greatest() : Func_All("greatest")
  {
  }
  ~Func_greatest() override = default;

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

  int32_t getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_least class
 */
class Func_least : public Func_All
{
 public:
  Func_least() : Func_All("least")
  {
  }
  ~Func_least() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

/** @brief Func_coalesce class
 */
class Func_coalesce : public Func_All
{
 public:
  Func_coalesce() : Func_All("coalesce")
  {
  }
  ~Func_coalesce() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override;

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
};

/** @brief Func_nullif class
 */
class Func_nullif : public Func_All
{
 public:
  Func_nullif() : Func_All("nullif")
  {
  }
  ~Func_nullif() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int32_t getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override;

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct) override;

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

}  // namespace funcexp
