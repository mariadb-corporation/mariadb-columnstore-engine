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

//   $Id: treenode.h 9635 2013-06-19 21:42:30Z bwilkinson $

/** @file */

#ifndef CALPONT_TREENODE_H
#define CALPONT_TREENODE_H

#include <string>
#include <iostream>
#include <cmath>
#include <boost/shared_ptr.hpp>

#include <stdlib.h>
#include <unistd.h>

#include "calpontsystemcatalog.h"
#include "exceptclasses.h"
#include "dataconvert.h"
#include "columnwidth.h"
#include "mcs_decimal.h"
#include "mcs_int64.h"

namespace messageqcpp
{
class ByteStream;
}

namespace rowgroup
{
class Row;
}

/**
 * Namespace
 */
namespace execplan
{
typedef execplan::CalpontSystemCatalog::ColType Type;
typedef datatypes::Decimal IDB_Decimal;

/**
 * @brief IDB_Regex struct
 *
 */
#ifdef POSIX_REGEX
typedef regex_t IDB_Regex;
#else
typedef boost::regex IDB_Regex;
#endif

typedef IDB_Regex CNX_Regex;

typedef boost::shared_ptr<IDB_Regex> SP_IDB_Regex;
typedef SP_IDB_Regex SP_CNX_Regex;

/** Trim trailing 0 from val. All insignificant zeroes to the right of the
 *  decimal point are removed. Also, the decimal point is not included on
 *  whole numbers. It works like %g flag with printf, but always print
 *  double value in fixed-point notation.
 *
 *  @parm val valid double value in fixed-point notation from printf %f.
 *            No format validation is perfomed in this function.
 *  @parm length length of the buffer val
 */
inline std::string removeTrailing0(char* val, uint32_t length)
{
  char* ptr = val;
  int64_t i = 0;
  bool decimal_point = false;

  for (; i < length; i++, ptr++)
  {
    if (*ptr == '+' || *ptr == '-' || (*ptr >= '0' && *ptr <= '9'))
    {
      continue;
    }

    if (*ptr == '.')
    {
      decimal_point = true;
      continue;
    }

    *ptr = 0;
    break;
  }

  if (decimal_point)
  {
    for (i = i - 1; i >= 0; i--)
    {
      if (val[i] == '0')
      {
        val[i] = 0;
      }
      else if (val[i] == '.')
      {
        val[i] = 0;
        break;
      }
      else
      {
        break;
      }
    }
  }

  return std::string(val);
}

/**
 * @brief Result type added for F&E framework
 *
 */
struct Result
{
  Result()
   : intVal(0)
   , uintVal(0)
   , origIntVal(0)
   , dummy(0)
   , doubleVal(0)
   , longDoubleVal(0)
   , floatVal(0)
   , boolVal(false)
   , strVal("")
   , decimalVal(IDB_Decimal())
   , valueConverted(false)
  {
  }
  int64_t intVal;
  uint64_t uintVal;
  uint64_t origIntVal;
  // clear up the memory following origIntVal to make sure null terminated string
  // when converting origIntVal
  uint64_t dummy;
  double doubleVal;
  long double longDoubleVal;
  float floatVal;
  bool boolVal;
  std::string strVal;
  IDB_Decimal decimalVal;
  bool valueConverted;
};

/**
 * @brief An abstract class to represent a node data on the expression tree
 *
 */
class TreeNode
{
 public:
  TreeNode();
  TreeNode(const TreeNode& rhs);
  virtual ~TreeNode();
  virtual const std::string data() const = 0;
  virtual void data(const std::string data) = 0;
  virtual const std::string toString() const = 0;
  virtual TreeNode* clone() const = 0;

  /**
   * Interface for serialization
   */

  /** @brief Convert *this to a stream of bytes
   *
   * Convert *this to a stream of bytes.
   * @param b The ByteStream to add *this to.
   */
  virtual void serialize(messageqcpp::ByteStream& b) const = 0;

  /** @brief Construct a TreeNode from a stream of bytes
   *
   * Construct a TreeNode from a stream of bytes.
   * @param b The ByteStream to parse
   * @return The newly allocated TreeNode
   */
  virtual void unserialize(messageqcpp::ByteStream& b) = 0;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const TreeNode* t) const = 0;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  virtual bool operator!=(const TreeNode* t) const = 0;

  // derivedTable mutator and accessor
  virtual const std::string& derivedTable() const
  {
    return fDerivedTable;
  }

  virtual void derivedTable(const std::string& derivedTable)
  {
    fDerivedTable = derivedTable;
  }

  // must to be implented by treenode that could potentially belong to
  // one single derived table
  virtual void setDerivedTable()
  {
    fDerivedTable = "";
  }

  virtual TreeNode* derivedRefCol() const
  {
    return fDerivedRefCol;
  }

  virtual void derivedRefCol(TreeNode* derivedRefCol)
  {
    fDerivedRefCol = derivedRefCol;
  }

  virtual uint64_t refCount() const
  {
    return fRefCount;
  }

  virtual void refCount(const uint64_t refCount)
  {
    fRefCount = refCount;
  }

  // the inc and dec functions areparm[n]->data() used by connector single thread.
  virtual void decRefCount()
  {
    fRefCount--;
  }

  virtual void incRefCount()
  {
    fRefCount++;
  }

  /***********************************************************************
   *                     F&E framework                                   *
   ***********************************************************************/
  virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.strVal;
  }
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.intVal;
  }
  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.uintVal;
  }
  datatypes::TUInt64Null toTUInt64Null(rowgroup::Row& row)
  {
    bool isNull = false;
    uint64_t val = getUintVal(row, isNull);
    return datatypes::TUInt64Null(val, isNull);
  }
  datatypes::TSInt64Null toTSInt64Null(rowgroup::Row& row)
  {
    bool isNull = false;
    int64_t val = getIntVal(row, isNull);
    return datatypes::TSInt64Null(val, isNull);
  }
  virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.floatVal;
  }
  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.doubleVal;
  }
  virtual long double getLongDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.longDoubleVal;
  }
  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.decimalVal;
  }
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.boolVal;
  }
  virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.intVal;
  }
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.intVal;
  }
  virtual int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.intVal;
  }
  virtual int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    return fResult.intVal;
  }
  virtual void evaluate(rowgroup::Row& row, bool& isNull)
  {
  }

  inline bool getBoolVal();
  inline const std::string& getStrVal(const long timeZone);
  inline int64_t getIntVal();
  inline uint64_t getUintVal();
  inline float getFloatVal();
  inline double getDoubleVal();
  inline long double getLongDoubleVal();
  inline IDB_Decimal getDecimalVal();
  inline int32_t getDateIntVal();
  inline int64_t getDatetimeIntVal(long timeZone = 0);
  inline int64_t getTimestampIntVal();
  inline int64_t getTimeIntVal();

  virtual const execplan::CalpontSystemCatalog::ColType& resultType() const
  {
    return fResultType;
  }
  virtual execplan::CalpontSystemCatalog::ColType& resultType()
  {
    return fResultType;
  }
  virtual void resultType(const execplan::CalpontSystemCatalog::ColType& resultType);
  virtual void operationType(const Type& type)
  {
    fOperationType = type;
  }
  virtual const execplan::CalpontSystemCatalog::ColType& operationType() const
  {
    return fOperationType;
  }

  // result mutator and accessor. for speical functor to set for optimization.
  virtual void result(const Result& result)
  {
    fResult = result;
  }
  virtual const Result& result() const
  {
    return fResult;
  }

  uint32_t charsetNumber() const
  {
    return fResultType.charsetNumber;
  }
  void charsetNumber(uint32_t cnum)
  {
    fResultType.charsetNumber = cnum;
    fOperationType.charsetNumber = cnum;
  }

 protected:
  Result fResult;
  execplan::CalpontSystemCatalog::ColType fResultType;  // mapped from mysql data type
  execplan::CalpontSystemCatalog::ColType
      fOperationType;  // operator type, could be different from the result type

  // double's range is +/-1.7E308 with at least 15 digits of precision
  char tmp[312];  // for conversion use

  // @bug5635 If any item involved in this filter belongs to a derived table,
  // the derived table alias is added to the reference vector.
  std::string fDerivedTable;
  uint64_t fRefCount;
  TreeNode* fDerivedRefCol;

 private:
  // default okay
  // TreeNode& operator=(const TreeNode& rhs);
};

inline bool TreeNode::getBoolVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        return (atoi((char*)(&fResult.origIntVal)) != 0);

      return (atoi(fResult.strVal.c_str()) != 0);

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        return (atoi((char*)(&fResult.origIntVal)) != 0);

      return (atoi(fResult.strVal.c_str()) != 0);

    // FIXME: Huh???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        return (atoi((char*)(&fResult.origIntVal)) != 0);

      return (atoi(fResult.strVal.c_str()) != 0);

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return (fResult.intVal != 0);

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::UINT: return (fResult.uintVal != 0);

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return (fResult.floatVal != 0);

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return (fResult.doubleVal != 0);

    case CalpontSystemCatalog::LONGDOUBLE: return (fResult.longDoubleVal != 0);

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (fResultType.colWidth == datatypes::MAXDECIMALWIDTH)
        return (fResult.decimalVal.s128Value != 0);
      else
        return (fResult.decimalVal.value != 0);

    default: throw logging::InvalidConversionExcept("TreeNode::getBoolVal: Invalid conversion.");
  }

  return fResult.boolVal;
}

inline const std::string& TreeNode::getStrVal(const long timeZone)
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        fResult.strVal = (char*)(&fResult.origIntVal);

      break;

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        fResult.strVal = (char*)(&fResult.origIntVal);

      break;

    // FIXME: ???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        fResult.strVal = (char*)(&fResult.origIntVal);

      break;

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
    {
#ifndef __LP64__
      snprintf(tmp, 20, "%lld", fResult.intVal);
#else
      snprintf(tmp, 20, "%ld", fResult.intVal);
#endif
      fResult.strVal = std::string(tmp);
      break;
    }

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::UINT:
    {
#ifndef __LP64__
      snprintf(tmp, 20, "%llu", fResult.uintVal);
#else
      snprintf(tmp, 20, "%lu", fResult.uintVal);
#endif
      fResult.strVal = std::string(tmp);
      break;
    }

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      if ((fabs(fResult.floatVal) > (1.0 / IDB_pow[4])) && (fabs(fResult.floatVal) < (float)IDB_pow[6]))
      {
        snprintf(tmp, 312, "%f", fResult.floatVal);
        fResult.strVal = removeTrailing0(tmp, 312);
      }
      else
      {
        // MCOL-299 Print scientific with 5 mantissa and no + sign for exponent
        int exponent = (int)floor(log10(fabs(fResult.floatVal)));  // This will round down the exponent
        double base = fResult.floatVal * pow(10, -1.0 * exponent);

        if (std::isnan(exponent) || std::isnan(base))
        {
          snprintf(tmp, 312, "%f", fResult.floatVal);
          fResult.strVal = removeTrailing0(tmp, 312);
        }
        else
        {
          snprintf(tmp, 312, "%.5f", base);
          fResult.strVal = removeTrailing0(tmp, 312);
          snprintf(tmp, 312, "e%02d", exponent);
          fResult.strVal += tmp;
        }

        //				snprintf(tmp, 312, "%e.5", fResult.floatVal);
        //				fResult.strVal = tmp;
      }

      break;
    }

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      if ((fabs(fResult.doubleVal) > (1.0 / IDB_pow[13])) && (fabs(fResult.doubleVal) < (float)IDB_pow[15]))
      {
        snprintf(tmp, 312, "%f", fResult.doubleVal);
        fResult.strVal = removeTrailing0(tmp, 312);
      }
      else
      {
        // MCOL-299 Print scientific with 9 mantissa and no + sign for exponent
        int exponent = (int)floor(log10(fabs(fResult.doubleVal)));  // This will round down the exponent
        double base = fResult.doubleVal * pow(10, -1.0 * exponent);

        if (std::isnan(exponent) || std::isnan(base))
        {
          snprintf(tmp, 312, "%f", fResult.doubleVal);
          fResult.strVal = removeTrailing0(tmp, 312);
        }
        else
        {
          snprintf(tmp, 312, "%.9f", base);
          fResult.strVal = removeTrailing0(tmp, 312);
          snprintf(tmp, 312, "e%02d", exponent);
          fResult.strVal += tmp;
        }

        //				snprintf(tmp, 312, "%e", fResult.doubleVal);
        //				fResult.strVal = tmp;
      }

      break;
    }

    case CalpontSystemCatalog::LONGDOUBLE:
    {
      if ((fabsl(fResult.longDoubleVal) > (1.0 / IDB_pow[13])) &&
          (fabsl(fResult.longDoubleVal) < (float)IDB_pow[15]))
      {
        snprintf(tmp, 312, "%Lf", fResult.longDoubleVal);
        fResult.strVal = removeTrailing0(tmp, 312);
      }
      else
      {
        // MCOL-299 Print scientific with 9 mantissa and no + sign for exponent
        int exponent = (int)floorl(log10(fabsl(fResult.longDoubleVal)));  // This will round down the exponent
        long double base = fResult.longDoubleVal * pow(10, -1.0 * exponent);

        if (std::isnan(exponent) || std::isnan(base))
        {
          snprintf(tmp, 312, "%Lf", fResult.longDoubleVal);
          fResult.strVal = removeTrailing0(tmp, 312);
        }
        else
        {
          snprintf(tmp, 312, "%.14Lf", base);
          fResult.strVal = removeTrailing0(tmp, 312);
          snprintf(tmp, 312, "e%02d", exponent);
          fResult.strVal += tmp;
        }

        //				snprintf(tmp, 312, "%e", fResult.doubleVal);
        //				fResult.strVal = tmp;
      }

      break;
    }

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      if (fResultType.colWidth == datatypes::MAXDECIMALWIDTH)
        // Explicit path for TSInt128 decimals with low precision
        fResult.strVal = fResult.decimalVal.toString(true);
      else
        fResult.strVal = fResult.decimalVal.toString();
      break;
    }

    case CalpontSystemCatalog::DATE:
    {
      dataconvert::DataConvert::dateToString(fResult.intVal, tmp, 255);
      fResult.strVal = std::string(tmp);
      break;
    }

    case CalpontSystemCatalog::DATETIME:
    {
      dataconvert::DataConvert::datetimeToString(fResult.intVal, tmp, 255, fResultType.precision);
      fResult.strVal = std::string(tmp);
      break;
    }

    case CalpontSystemCatalog::TIMESTAMP:
    {
      dataconvert::DataConvert::timestampToString(fResult.intVal, tmp, 255, timeZone, fResultType.precision);
      fResult.strVal = std::string(tmp);
      break;
    }

    case CalpontSystemCatalog::TIME:
    {
      dataconvert::DataConvert::timeToString(fResult.intVal, tmp, 255, fResultType.precision);
      fResult.strVal = std::string(tmp);
      break;
    }

    default: throw logging::InvalidConversionExcept("TreeNode::getStrVal: Invalid conversion.");
  }

  return fResult.strVal;
}

inline int64_t TreeNode::getIntVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        return fResult.intVal;

      return atoll(fResult.strVal.c_str());

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        return fResult.intVal;

      return atoll(fResult.strVal.c_str());

    // FIXME: ???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        return fResult.intVal;

      return atoll(fResult.strVal.c_str());

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: return fResult.intVal;

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return fResult.uintVal;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return (int64_t)fResult.floatVal;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return (int64_t)fResult.doubleVal;

    case CalpontSystemCatalog::LONGDOUBLE: return (int64_t)fResult.longDoubleVal;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: return fResult.decimalVal.toSInt64Round();

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return fResult.intVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getIntVal: Invalid conversion.");
  }

  return fResult.intVal;
}
inline uint64_t TreeNode::getUintVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: return fResult.intVal;

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return fResult.uintVal;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return (uint64_t)fResult.floatVal;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return (uint64_t)fResult.doubleVal;

    case CalpontSystemCatalog::LONGDOUBLE: return (uint64_t)fResult.longDoubleVal;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: return fResult.decimalVal.toUInt64Round();

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return fResult.intVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getIntVal: Invalid conversion.");
  }

  return fResult.intVal;
}
inline float TreeNode::getFloatVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        return atof((char*)(&fResult.origIntVal));

      return atof(fResult.strVal.c_str());

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        return atof((char*)(&fResult.origIntVal));

      return atof(fResult.strVal.c_str());

    // FIXME: ???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        return atof((char*)(&fResult.origIntVal));

      return atof(fResult.strVal.c_str());

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: return (float)fResult.intVal;

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return (float)fResult.uintVal;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return fResult.floatVal;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return (float)fResult.doubleVal;

    case CalpontSystemCatalog::LONGDOUBLE: return (float)fResult.doubleVal;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      if (fResultType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        return static_cast<float>(fResult.decimalVal);
      }
      else
      {
        return (float)fResult.decimalVal.decimal64ToXFloat<double>();
      }
    }

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return (float)fResult.intVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getFloatVal: Invalid conversion.");
  }

  return fResult.floatVal;
}
inline double TreeNode::getDoubleVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        return strtod((char*)(&fResult.origIntVal), NULL);

      return strtod(fResult.strVal.c_str(), NULL);

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        return strtod((char*)(&fResult.origIntVal), NULL);

      return strtod(fResult.strVal.c_str(), NULL);

    // FIXME: ???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        return strtod((char*)(&fResult.origIntVal), NULL);

      return strtod(fResult.strVal.c_str(), NULL);

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: return (double)fResult.intVal;

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return (double)fResult.uintVal;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return (double)fResult.floatVal;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return fResult.doubleVal;

    case CalpontSystemCatalog::LONGDOUBLE: return (double)fResult.longDoubleVal;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      if (fResultType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        return static_cast<double>(fResult.decimalVal);
      }
      else
      {
        return fResult.decimalVal.decimal64ToXFloat<double>();
      }
    }

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return (double)fResult.intVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getDoubleVal: Invalid conversion.");
  }

  return fResult.doubleVal;
}
inline long double TreeNode::getLongDoubleVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
      if (fResultType.colWidth <= 8)
        return strtold((char*)(&fResult.origIntVal), NULL);

      return strtold(fResult.strVal.c_str(), NULL);

    case CalpontSystemCatalog::VARCHAR:
      if (fResultType.colWidth <= 7)
        return strtold((char*)(&fResult.origIntVal), NULL);

      return strtold(fResult.strVal.c_str(), NULL);

    // FIXME: ???
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
      if (fResultType.colWidth <= 7)
        return strtold((char*)(&fResult.origIntVal), NULL);

      return strtold(fResult.strVal.c_str(), NULL);

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: return (long double)fResult.intVal;

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return (long double)fResult.uintVal;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return (long double)fResult.floatVal;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return (long double)fResult.doubleVal;

    case CalpontSystemCatalog::LONGDOUBLE: return (long double)fResult.longDoubleVal;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      if (fResultType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        return static_cast<long double>(fResult.decimalVal);
      }
      else
      {
        return fResult.decimalVal.decimal64ToXFloat<long double>();
      }
    }

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIME: return (long double)fResult.intVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getDoubleVal: Invalid conversion.");
  }

  return fResult.doubleVal;
}
inline IDB_Decimal TreeNode::getDecimalVal()
{
  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::TEXT:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from string");

    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
      throw logging::InvalidConversionExcept(
          "TreeNode::getDecimalVal: non-support conversion from binary string");

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    {
      idbassert(fResultType.scale == 0);
      fResult.decimalVal = IDB_Decimal(fResult.intVal, 0, fResultType.precision, fResult.intVal);
      break;
    }

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UTINYINT:
    {
      idbassert(fResultType.scale == 0);
      fResult.decimalVal = IDB_Decimal((int64_t)fResult.uintVal, 0, fResultType.precision, fResult.uintVal);
      break;
    }

    case CalpontSystemCatalog::LONGDOUBLE:
    {
      long double dlScaled = fResult.longDoubleVal;

      if (fResultType.scale > 0)
      {
        dlScaled = fResult.longDoubleVal * pow((double)10.0, fResultType.scale);
      }

      if ((dlScaled > (long double)INT64_MAX) || (dlScaled < (long double)(INT64_MIN)))
      {
        datatypes::TFloat128 temp((float128_t)dlScaled);
        fResult.decimalVal =
            IDB_Decimal(0, fResultType.scale, fResultType.precision, static_cast<int128_t>(temp));
      }
      else
      {
        int64_t val = (int64_t)lroundl(dlScaled);
        fResult.decimalVal = IDB_Decimal(val, fResultType.scale, fResultType.precision, val);
      }

      break;
    }

    case CalpontSystemCatalog::DATE:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion from date.");

    case CalpontSystemCatalog::DATETIME:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion from datetime.");

    case CalpontSystemCatalog::TIMESTAMP:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion from timestamp.");

    case CalpontSystemCatalog::TIME:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion from time.");

    case CalpontSystemCatalog::FLOAT:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from float");

    case CalpontSystemCatalog::UFLOAT:
      throw logging::InvalidConversionExcept(
          "TreeNode::getDecimalVal: non-support conversion from float unsigned");

    case CalpontSystemCatalog::DOUBLE:
      throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: non-support conversion from double");

    case CalpontSystemCatalog::UDOUBLE:
      throw logging::InvalidConversionExcept(
          "TreeNode::getDecimalVal: non-support conversion from double unsigned");

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: return fResult.decimalVal;

    default: throw logging::InvalidConversionExcept("TreeNode::getDecimalVal: Invalid conversion.");
  }

  return fResult.decimalVal;
}

inline int64_t TreeNode::getDatetimeIntVal(long timeZone)
{
  if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATE)
    return (fResult.intVal & 0x00000000FFFFFFC0LL) << 32;
  else if (fResultType.colDataType == execplan::CalpontSystemCatalog::TIME)
  {
    dataconvert::Time tt;
    int day = 0;

    void* ttp = static_cast<void*>(&tt);

    memcpy(ttp, &fResult.intVal, 8);

    // Note, this should probably be current date +/- time
    if ((tt.hour > 23) && (!tt.is_neg))
    {
      day = tt.hour / 24;
      tt.hour = tt.hour % 24;
    }
    else if ((tt.hour < 0) || (tt.is_neg))
    {
      tt.hour = 0;
    }

    dataconvert::DateTime dt(0, 0, day, tt.hour, tt.minute, tt.second, tt.msecond);
    memcpy(&fResult.intVal, &dt, 8);
    return fResult.intVal;
  }
  else if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATETIME)
    // return (fResult.intVal & 0xFFFFFFFFFFF00000LL);
    return (fResult.intVal);
  else if (fResultType.colDataType == execplan::CalpontSystemCatalog::TIMESTAMP)
  {
    dataconvert::TimeStamp timestamp(fResult.intVal);
    int64_t seconds = timestamp.second;
    dataconvert::MySQLTime m_time;
    dataconvert::gmtSecToMySQLTime(seconds, m_time, timeZone);
    dataconvert::DateTime dt(m_time.year, m_time.month, m_time.day, m_time.hour, m_time.minute, m_time.second,
                             timestamp.msecond);
    memcpy(&fResult.intVal, &dt, 8);
    return fResult.intVal;
  }
  else
    return getIntVal();
}

inline int64_t TreeNode::getTimestampIntVal()
{
  if (fResultType.colDataType == execplan::CalpontSystemCatalog::TIMESTAMP)
    return fResult.intVal;
  else
    return getIntVal();
}

inline int64_t TreeNode::getTimeIntVal()
{
  if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATETIME)
  {
    dataconvert::DateTime dt;

    memcpy((int64_t*)(&dt), &fResult.intVal, 8);
    dataconvert::Time tt(0, dt.hour, dt.minute, dt.second, dt.msecond, false);
    memcpy(&fResult.intVal, &tt, 8);
    return fResult.intVal;
  }
  else if (fResultType.colDataType == execplan::CalpontSystemCatalog::TIME)
    return (fResult.intVal);
  else
    return getIntVal();
}

inline int32_t TreeNode::getDateIntVal()
{
  if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATETIME)
    return (((int32_t)(fResult.intVal >> 32) & 0xFFFFFFC0) | 0x3E);
  else if (fResultType.colDataType == execplan::CalpontSystemCatalog::DATE)
    return ((fResult.intVal & 0xFFFFFFC0) | 0x3E);
  else
    return getIntVal();
}

typedef boost::shared_ptr<TreeNode> STNP;

/**
 * Operations
 */
std::ostream& operator<<(std::ostream& output, const TreeNode& rhs);

}  // namespace execplan

#endif
