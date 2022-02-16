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

/***********************************************************************
 *   $Id: constantcolumn.h 9679 2013-07-11 22:32:03Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef CONSTANTCOLUMN_H
#define CONSTANTCOLUMN_H
#include <string>

#include "returnedcolumn.h"

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
class ConstantColumn;

/**
 * @brief A class to represent a constant return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles a constant column such as number and literal string.
 */
class ConstantColumn : public ReturnedColumn
{
 public:
  enum TYPE
  {
    NUM,
    LITERAL,
    NULLDATA
  };

  /**
   * ctor
   */
  ConstantColumn();
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, TYPE type = LITERAL);
  /**
   * ctor
   */
  ConstantColumn(const int64_t val, TYPE type = NUM);  // deprecate
  /**
   * ctor
   */
  ConstantColumn(const uint64_t val, TYPE type = NUM, int8_t scale = 0, uint8_t precision = 0);  // deprecate
  // There are more ctors below...

  /**
   * dtor
   */
  virtual ~ConstantColumn();

  /*
   * Accessor Methods
   */
  /**
   * accessor
   */
  inline unsigned int type() const
  {
    return fType;
  }
  /**
   * accessor
   */
  inline void type(unsigned int type)
  {
    fType = type;
  }
  /**
   * accessor
   */
  inline const std::string& constval() const
  {
    return fConstval;
  }
  /**
   * accessor
   */
  inline void constval(const std::string& constval)
  {
    fConstval = constval;
  }
  /**
   * accessor
   */
  inline long timeZone() const
  {
    return fTimeZone;
  }
  /**
   * mutator
   */
  inline void timeZone(const long timeZone)
  {
    fTimeZone = timeZone;
  }
  /**
   * accessor
   */
  virtual const std::string data() const;
  /**
   * accessor
   */
  virtual void data(const std::string data)
  {
    fData = data;
  }
  /**
   * accessor
   */
  virtual const std::string toString() const;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline virtual ConstantColumn* clone() const
  {
    return new ConstantColumn(*this);
  }

  /*
   * The serialization interface
   */
  /**
   * serialize
   */
  virtual void serialize(messageqcpp::ByteStream&) const;
  /**
   * unserialize
   */
  virtual void unserialize(messageqcpp::ByteStream&);

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  bool operator==(const ConstantColumn& t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  virtual bool operator!=(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  bool operator!=(const ConstantColumn& t) const;

  virtual bool hasWindowFunc()
  {
    return false;
  }

  /** Constant column on the filte can always be moved into derived table */
  virtual void setDerivedTable()
  {
    fDerivedTable = "*";
  }

 private:
  std::string fConstval;
  int fType;
  std::string fData;
  long fTimeZone;

  /***********************************************************
   *                  F&E framework                          *
   ***********************************************************/
 public:
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, const double val);
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, const long double val);
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, const int64_t val, TYPE type = NUM);
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, const uint64_t val, TYPE type = NUM);
  /**
   * ctor
   */
  ConstantColumn(const std::string& sql, const IDB_Decimal& val);
  /**
   * copy ctor
   */
  ConstantColumn(const ConstantColumn& rhs);
  /**
   * F&E
   */
  using ReturnedColumn::evaluate;
  virtual void evaluate(rowgroup::Row& row)
  {
  }
  /**
   * F&E
   */
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return TreeNode::getBoolVal();
  }
  /**
   * F&E
   */
  virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.strVal;
  }
  /**
   * F&E
   */
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.intVal;
  }
  /**
   * F&E
   */
  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.uintVal;
  }
  /**
   * F&E
   */
  virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.floatVal;
  }
  /**
   * F&E
   */
  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.doubleVal;
  }
  /**
   * F&E
   */
  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);
    return fResult.decimalVal;
  }
  /**
   * F&E
   */
  virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);

    if (!fResult.valueConverted)
    {
      fResult.intVal = dataconvert::DataConvert::stringToDate(fResult.strVal);
      fResult.valueConverted = true;
    }

    return fResult.intVal;
  }
  /**
   * F&E
   */
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);

    if (!fResult.valueConverted)
    {
      fResult.intVal = dataconvert::DataConvert::stringToDatetime(fResult.strVal);
      fResult.valueConverted = true;
    }

    return fResult.intVal;
  }
  /**
   * F&E
   */
  virtual int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);

    if (!fResult.valueConverted)
    {
      fResult.intVal = dataconvert::DataConvert::stringToTimestamp(fResult.strVal, fTimeZone);
      fResult.valueConverted = true;
    }

    return fResult.intVal;
  }
  /**
   * F&E
   */
  virtual int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    isNull = isNull || (fType == NULLDATA);

    if (!fResult.valueConverted)
    {
      fResult.intVal = dataconvert::DataConvert::stringToTime(fResult.strVal);
      fResult.valueConverted = true;
    }

    return fResult.intVal;
  }
  /**
   * F&E
   */
  inline float getFloatVal() const
  {
    return fResult.floatVal;
  }
  /**
   * F&E
   */
  inline double getDoubleVal() const
  {
    return fResult.doubleVal;
  }
};

class ConstantColumnNull : public ConstantColumn
{
 public:
  ConstantColumnNull() : ConstantColumn("", ConstantColumn::NULLDATA)
  {
  }
};

class ConstantColumnString : public ConstantColumn
{
 public:
  ConstantColumnString(const std::string& str) : ConstantColumn(str, ConstantColumn::LITERAL)
  {
  }
};

class ConstantColumnNum : public ConstantColumn
{
 public:
  ConstantColumnNum(const CalpontSystemCatalog::ColType& type, const std::string& str)
   : ConstantColumn(str, ConstantColumn::NUM)
  {
    resultType(type);
  }
};

class ConstantColumnUInt : public ConstantColumn
{
 public:
  ConstantColumnUInt(uint64_t val, int8_t scale, uint8_t precision)
   : ConstantColumn(val, ConstantColumn::NUM, scale, precision)
  {
  }
};

class ConstantColumnSInt : public ConstantColumn
{
 public:
  ConstantColumnSInt(const CalpontSystemCatalog::ColType& type, const std::string& str, int64_t val)
   : ConstantColumn(str, val, ConstantColumn::NUM)
  {
    resultType(type);
  }
};

class ConstantColumnReal : public ConstantColumn
{
 public:
  ConstantColumnReal(const CalpontSystemCatalog::ColType& type, const std::string& str, double val)
   : ConstantColumn(str, val)
  {
    resultType(type);
  }
};

class ConstantColumnTemporal : public ConstantColumn
{
 public:
  ConstantColumnTemporal(const CalpontSystemCatalog::ColType& type, const std::string& str)
   : ConstantColumn(str)
  {
    resultType(type);
  }
};

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const ConstantColumn& rhs);

}  // namespace execplan
#endif  // CONSTANTCOLUMN_H
