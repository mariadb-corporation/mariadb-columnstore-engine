/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2020 MariaDB Corporation

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

//  $Id: functor_bool.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#pragma once

#include "functor.h"

namespace funcexp
{
/** @brief Func_Bool class
 *    For function that returns a boolean result.
 *        Must implement getBoolVal()
 *        Implement any other methods that behave differently from the default.
 */
class Func_Bool : public Func
{
 public:
  Func_Bool() = default;
  explicit Func_Bool(const std::string& funcName) : Func(funcName)
  {
  }
  ~Func_Bool() override = default;

  /*
      virtual bool getBoolVal(rowgroup::Row& row,
                                                              FunctionParm& fp,
                                                              bool& isNull,
                                                              execplan::CalpontSystemCatalog::ColType& op_ct)
     = 0;
  */

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1 : 0);
  }

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1.0 : 0.0);
  }

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1.0 : 0.0);
  }

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? "1" : "0");
  }

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct) override
  {
    return execplan::IDB_Decimal(getIntVal(row, fp, isNull, op_ct), 0, 0);
  }

  int32_t getDateIntVal(rowgroup::Row& /*row*/, FunctionParm& /*fp*/, bool& /*isNull*/,
                        execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    return 0;
  }

  int64_t getDatetimeIntVal(rowgroup::Row& /*row*/, FunctionParm& /*fp*/, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    isNull = true;
    return 0;
  }

  int64_t getTimestampIntVal(rowgroup::Row& /*row*/, FunctionParm& /*fp*/, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    isNull = true;
    return 0;
  }

  int64_t getTimeIntVal(rowgroup::Row& /*row*/, FunctionParm& /*fp*/, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    isNull = true;
    return 0;
  }
};

/** @brief Func_between class
 */
class Func_between : public Func_Bool
{
 public:
  Func_between() : Func_Bool("between")
  {
  }
  ~Func_between() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_notbetween class
 */
class Func_notbetween : public Func_Bool
{
 public:
  Func_notbetween() : Func_Bool("notbetween")
  {
  }
  ~Func_notbetween() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_in class
 */
class Func_in : public Func_Bool
{
 public:
  Func_in() : Func_Bool("in")
  {
  }
  ~Func_in() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_notin class
 */
class Func_notin : public Func_Bool
{
 public:
  Func_notin() : Func_Bool("notin")
  {
  }
  ~Func_notin() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_regexp class
 */
class Func_regexp : public Func_Bool
{
 public:
  Func_regexp() : Func_Bool("regexp")
  {
  }
  ~Func_regexp() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;
};

/** @brief Func_isnull class
 */
class Func_isnull : public Func_Bool
{
 public:
  /*
   * Constructor. Pass the function name to the base constructor.
   */
  Func_isnull() : fIsNotNull(false)
  {
  }
  explicit Func_isnull(bool isnotnull) : fIsNotNull(isnotnull)
  {
  }
  /*
   * Destructor. isnull does not need to do anything here to clean up.
   */
  ~Func_isnull() override = default;

  /**
   * Decide on the function's operation type
   */
  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct) override;

 private:
  bool fIsNotNull;
};

/** @brief Func_Truth class
 */
class Func_Truth : public Func_Bool
{
 public:
  Func_Truth(const std::string& funcName, bool a_value, bool a_affirmative)
   : Func_Bool(funcName), value(a_value), affirmative(a_affirmative)
  {
  }

  ~Func_Truth() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/) override
  {
    assert(fp.size() == 1);
    return fp[0]->data()->resultType();
  }

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& /*op_ct*/) override
  {
    bool val = fp[0]->data()->getBoolVal(row, isNull);

    /*
      NULL val IS {TRUE, FALSE} --> FALSE
      NULL val IS NOT {TRUE, FALSE} --> TRUE
      {TRUE, FALSE} val IS {TRUE, FALSE} value --> val == value
      {TRUE, FALSE} val IS NOT {TRUE, FALSE} value --> val != value
      These cases can be reduced to the following bitwise operation.
    */
    bool ret = (isNull & (!affirmative)) | ((!isNull) & (affirmative ^ (value ^ val)));

    isNull = false;

    return ret;
  }

 private:
  const bool value, affirmative;
};

/** @brief Func_IsTrue class
 */
class Func_IsTrue : public Func_Truth
{
 public:
  Func_IsTrue() : Func_Truth("istrue", true, true)
  {
  }
  ~Func_IsTrue() override = default;
};

/** @brief Func_IsNotTrue class
 */
class Func_IsNotTrue : public Func_Truth
{
 public:
  Func_IsNotTrue() : Func_Truth("isnottrue", true, false)
  {
  }
  ~Func_IsNotTrue() override = default;
};

/** @brief Func_IsFalse class
 */
class Func_IsFalse : public Func_Truth
{
 public:
  Func_IsFalse() : Func_Truth("isfalse", false, true)
  {
  }
  ~Func_IsFalse() override = default;
};

/** @brief Func_IsNotFalse class
 */
class Func_IsNotFalse : public Func_Truth
{
 public:
  Func_IsNotFalse() : Func_Truth("isnotfalse", false, false)
  {
  }
  ~Func_IsNotFalse() override = default;
};

}  // namespace funcexp
