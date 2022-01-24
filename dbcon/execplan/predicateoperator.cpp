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
 *   $Id: predicateoperator.cpp 9306 2013-03-12 15:49:11Z rdempsey $
 *
 *
 ***********************************************************************/

#include <iostream>

#include "bytestream.h"
#include "predicateoperator.h"
#include "objectreader.h"

#include "liboamcpp.h"

using namespace oam;

using namespace std;

namespace execplan
{
/**
 * Constructors/Destructors
 */
PredicateOperator::PredicateOperator() : cs(NULL)
{
}

PredicateOperator::PredicateOperator(const string& operatorName) : cs(NULL)
{
  data(operatorName);
}

PredicateOperator::PredicateOperator(const PredicateOperator& rhs) : Operator(rhs)
{
  data(rhs.data());
  cs = rhs.getCharset();
}

PredicateOperator::~PredicateOperator()
{
}

/**
 * Operations
 */

/**
 * friend function
 */
ostream& operator<<(ostream& output, const PredicateOperator& rhs)
{
  output << rhs.toString() << endl;
  output << "OpType=" << rhs.operationType().colDataType << endl;
  return output;
}

/**
 * The serialization interface
 */
void PredicateOperator::serialize(messageqcpp::ByteStream& b) const
{
  b << (ObjectReader::id_t)ObjectReader::PREDICATEOPERATOR;
  // b << fData;
  Operator::serialize(b);
}

void PredicateOperator::unserialize(messageqcpp::ByteStream& b)
{
  ObjectReader::checkType(b, ObjectReader::PREDICATEOPERATOR);
  // b >> fData;
  Operator::unserialize(b);
  cs = &datatypes::Charset(fOperationType.charsetNumber).getCharset();
}

bool PredicateOperator::operator==(const PredicateOperator& t) const
{
  if (data() == t.data())
    return true;

  return false;
}

bool PredicateOperator::operator==(const TreeNode* t) const
{
  const PredicateOperator* o;

  o = dynamic_cast<const PredicateOperator*>(t);

  if (o == NULL)
    return false;

  return *this == *o;
}

bool PredicateOperator::operator!=(const PredicateOperator& t) const
{
  return (!(*this == t));
}

bool PredicateOperator::operator!=(const TreeNode* t) const
{
  return (!(*this == t));
}

void PredicateOperator::setOpType(Type& l, Type& r)
{
  fOperationType = l;  // Default to left side. Modify as needed.
  if (l.colDataType == execplan::CalpontSystemCatalog::DATETIME ||
      l.colDataType == execplan::CalpontSystemCatalog::TIME ||
      l.colDataType == execplan::CalpontSystemCatalog::TIMESTAMP ||
      l.colDataType == execplan::CalpontSystemCatalog::DATE)
  {
    switch (r.colDataType)
    {
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::VARCHAR: fOperationType.charsetNumber = r.charsetNumber; break;

      case execplan::CalpontSystemCatalog::DATETIME:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DATETIME;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::TIMESTAMP:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::TIMESTAMP;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::TIME:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::TIME;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::DATE: fOperationType = l; break;

      default:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        fOperationType.colWidth = 8;
        break;
    }
  }
  else if (r.colDataType == execplan::CalpontSystemCatalog::DATETIME ||
           r.colDataType == execplan::CalpontSystemCatalog::TIME ||
           r.colDataType == execplan::CalpontSystemCatalog::TIMESTAMP ||
           r.colDataType == execplan::CalpontSystemCatalog::DATE)
  {
    switch (l.colDataType)
    {
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::VARCHAR:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
        fOperationType.colWidth = 255;
        break;

      case execplan::CalpontSystemCatalog::DATETIME:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DATETIME;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::TIMESTAMP:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::TIMESTAMP;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::TIME:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::TIME;
        fOperationType.colWidth = 8;
        break;

      case execplan::CalpontSystemCatalog::DATE: fOperationType = r; break;

      default:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        fOperationType.colWidth = 8;
        break;
    }
  }
  else if (l.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
           l.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
  {
    switch (r.colDataType)
    {
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        // should following the result type that MySQL gives
        fOperationType = l;
        fOperationType.scale = std::max(l.scale, r.scale);
        fOperationType.precision = std::max(l.precision, r.precision);
        fOperationType.colWidth = std::max(l.colWidth, r.colWidth);
        break;
      }

      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::TINYINT:
      case execplan::CalpontSystemCatalog::BIGINT:
      case execplan::CalpontSystemCatalog::UINT:
      case execplan::CalpontSystemCatalog::UMEDINT:
      case execplan::CalpontSystemCatalog::UTINYINT:
      case execplan::CalpontSystemCatalog::UBIGINT:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
        fOperationType.scale = l.scale;
        fOperationType.precision = l.precision;
        fOperationType.colWidth = (l.colWidth == datatypes::MAXDECIMALWIDTH) ? l.colWidth : 8;
        break;

      default:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        fOperationType.colWidth = 8;
    }
  }
  else if (r.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
           r.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
  {
    switch (l.colDataType)
    {
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        // should following the result type that MySQL gives based on the following logic?
        // @NOTE is this trustable?
        fOperationType = fResultType;
        break;
      }

      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::TINYINT:
      case execplan::CalpontSystemCatalog::BIGINT:
      case execplan::CalpontSystemCatalog::UINT:
      case execplan::CalpontSystemCatalog::UMEDINT:
      case execplan::CalpontSystemCatalog::UTINYINT:
      case execplan::CalpontSystemCatalog::UBIGINT:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
        fOperationType.scale = r.scale;
        fOperationType.precision = r.precision;
        fOperationType.colWidth = (r.colWidth == datatypes::MAXDECIMALWIDTH) ? r.colWidth : 8;
        break;

      case execplan::CalpontSystemCatalog::LONGDOUBLE:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::LONGDOUBLE;
        fOperationType.colWidth = sizeof(long double);
        break;
      default:
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        fOperationType.colWidth = 8;
    }
  }
  // If both sides are unsigned, use UBIGINT as result type, otherwise
  // "promote" to BIGINT.
  else if (isUnsigned(l.colDataType) && isUnsigned(r.colDataType))
  {
    fOperationType.colDataType = execplan::CalpontSystemCatalog::UBIGINT;
    fOperationType.colWidth = 8;
  }
  else if ((isSignedInteger(l.colDataType) && isUnsigned(r.colDataType)) ||
           (isUnsigned(l.colDataType) && isSignedInteger(r.colDataType)) ||
           (isSignedInteger(l.colDataType) && isSignedInteger(r.colDataType)))
  {
    fOperationType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
    fOperationType.colWidth = 8;
  }
  else if ((l.colDataType == execplan::CalpontSystemCatalog::CHAR ||
            l.colDataType == execplan::CalpontSystemCatalog::VARCHAR ||
            l.colDataType == execplan::CalpontSystemCatalog::TEXT) &&
           (r.colDataType == execplan::CalpontSystemCatalog::CHAR ||
            r.colDataType == execplan::CalpontSystemCatalog::VARCHAR ||
            r.colDataType == execplan::CalpontSystemCatalog::TEXT))
  {
#if 0
        // Currently, STRINT isn't properly implemented everywhere
        // For short strings, we can get a faster execution for charset that fit in one byte.
        if ( ( (l.colDataType == execplan::CalpontSystemCatalog::CHAR && l.colWidth <= 8) ||
                (l.colDataType == execplan::CalpontSystemCatalog::VARCHAR && l.colWidth < 8) ) &&
                ( (r.colDataType == execplan::CalpontSystemCatalog::CHAR && r.colWidth <= 8) ||
                  (r.colDataType == execplan::CalpontSystemCatalog::VARCHAR && r.colWidth < 8) ) )
        {
            switch (fOperationType.charsetNumber) 
            {
                case 8:  // latin1_swedish_ci
                case 9:  // latin2_general_ci
                case 11: // ascii_general_ci
                case 47: // latin1_bin
                case 48: // latin1_general_ci
                case 49: // latin1_general_cs
                case 65: // ascii_bin
                case 77: // latin2_bin
                    // char[] as network order int for fast comparison.
                    fOperationType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
                    fOperationType.scale = 0;
                    fOperationType.colWidth = 8;
                    l.colDataType = execplan::CalpontSystemCatalog::STRINT;
                    r.colDataType = execplan::CalpontSystemCatalog::STRINT;
                default:
                    fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
                    fOperationType.colWidth = 255;
            }
        }
        else
#endif
    {
      fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
      fOperationType.colWidth = 255;
    }
  }
  else if (l.colDataType == execplan::CalpontSystemCatalog::LONGDOUBLE ||
           r.colDataType == execplan::CalpontSystemCatalog::LONGDOUBLE)
  {
    fOperationType.colDataType = execplan::CalpontSystemCatalog::LONGDOUBLE;
    fOperationType.colWidth = sizeof(long double);
  }
  else
  {
    fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
    fOperationType.colWidth = 8;
  }

  cs = &datatypes::Charset(fOperationType.charsetNumber).getCharset();
}

inline bool PredicateOperator::strTrimCompare(const std::string& op1, const std::string& op2)
{
  int r1 = cs->strnncollsp(op1.c_str(), op1.length(), op2.c_str(), op2.length());
  switch (fOp)
  {
    case OP_EQ: return r1 == 0;

    case OP_NE: return r1 != 0;

    case OP_GT: return r1 > 0;

    case OP_GE: return r1 >= 0;

    case OP_LT: return r1 < 0;

    case OP_LE: return r1 <= 0;

    default:
    {
      std::ostringstream oss;
      oss << "Unsupported predicate operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

bool PredicateOperator::getBoolVal(rowgroup::Row& row, bool& isNull, ReturnedColumn* lop, ReturnedColumn* rop)
{
  // like operator. both sides are string.
  if (fOp == OP_LIKE || fOp == OP_NOTLIKE)
  {
    const std::string& subject = lop->getStrVal(row, isNull);
    if (isNull)
      return false;
    const std::string& pattern = rop->getStrVal(row, isNull);
    if (isNull)
      return false;
    return datatypes::Charset(cs).like(fOp == OP_NOTLIKE, utils::ConstString(subject),
                                       utils::ConstString(pattern));
  }

  // fOpType should have already been set on the connector during parsing
  switch (fOperationType.colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      int64_t val1 = lop->getIntVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getIntVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getUintVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getUintVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      uint64_t val1 = lop->getUintVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getUintVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getDoubleVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getDoubleVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      double val1 = lop->getDoubleVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getDoubleVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getLongDoubleVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getLongDoubleVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      long double val1 = lop->getLongDoubleVal(row, isNull);
      if (isNull)
        return false;

      long double val2 = rop->getLongDoubleVal(row, isNull);
      if (isNull)
        return false;

      // In many case, rounding error will prevent an eq compare to work
      // In these cases, use the largest scale of the two items.
      if (fOp == execplan::OP_EQ)
      {
        // In case a val is a representation of a very large integer,
        // we won't want to just multiply by scale, as it may move
        // significant digits out of scope. So we break them apart
        // and compare each separately
        int64_t scale = std::max(lop->resultType().scale, rop->resultType().scale);
        if (scale)
        {
          long double intpart1;
          long double fract1 = modfl(val1, &intpart1);
          long double intpart2;
          long double fract2 = modfl(val2, &intpart2);
          if (numericCompare(intpart1, intpart2))
          {
            double factor = pow(10.0, (double)scale);
            fract1 = roundl(fract1 * factor);
            fract2 = roundl(fract2 * factor);
            return numericCompare(fract1, fract2);
          }
          else
          {
            return false;
          }
        }
      }
      return numericCompare(val1, val2);
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getDecimalVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getDecimalVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      IDB_Decimal val1 = lop->getDecimalVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getDecimalVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getDateIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getDateIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      int64_t val1 = lop->getDateIntVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, (int64_t)rop->getDateIntVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getDatetimeIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getDatetimeIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      int64_t val1 = lop->getDatetimeIntVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getDatetimeIntVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getTimestampIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getTimestampIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      int64_t val1 = lop->getTimestampIntVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getTimestampIntVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getTimeIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getTimeIntVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      int64_t val1 = lop->getTimeIntVal(row, isNull);

      if (isNull)
        return false;

      return numericCompare(val1, rop->getTimeIntVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      if (fOp == OP_ISNULL)
      {
        lop->getStrVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return ret;
      }

      if (fOp == OP_ISNOTNULL)
      {
        lop->getStrVal(row, isNull);
        bool ret = isNull;
        isNull = false;
        return !ret;
      }

      if (isNull)
        return false;

      const std::string& val1 = lop->getStrVal(row, isNull);
      if (isNull)
        return false;

      return strTrimCompare(val1, rop->getStrVal(row, isNull)) && !isNull;
    }

    case execplan::CalpontSystemCatalog::VARBINARY:
    case execplan::CalpontSystemCatalog::BLOB: return false; break;

    default:
    {
      std::ostringstream oss;
      oss << "invalid predicate operation type: " << fOperationType.colDataType;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }

  return false;
}

}  // namespace execplan
