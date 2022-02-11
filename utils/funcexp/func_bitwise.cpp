/* Copyright (C) 2014 InfiniDB, Inc.

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

/****************************************************************************
 * $Id: func_bitwise.cpp 3616 2013-03-04 14:56:29Z rdempsey $
 *
 *
 ****************************************************************************/

#include <string>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "mcs_int64.h"
#include "mcs_decimal.h"
#include "dataconvert.h"
#include "numericliteral.h"

using namespace dataconvert;

namespace
{
using namespace funcexp;

bool validateBitOperandTypeOrError(execplan::FunctionColumn& col, const Func& func, uint argno)
{
  auto& type = col.functionParms()[argno]->data()->resultType();
  if (type.canReturnXInt64())
    return false;
  func.raiseIllegalParameterDataTypeError(type);
  return true;
}

template <typename T>
datatypes::TUInt64Null ConvertToBitOperand(const T& val)
{
  if (val > static_cast<T>(UINT64_MAX))
    return datatypes::TUInt64Null(UINT64_MAX);
  if (val >= 0)
    return datatypes::TUInt64Null(static_cast<uint64_t>(val));
  if (val < static_cast<T>(INT64_MIN))
    return datatypes::TUInt64Null(static_cast<uint64_t>(INT64_MAX) + 1);
  return datatypes::TUInt64Null((uint64_t)(int64_t)val);
}

static datatypes::TUInt64Null DecimalToBitOperand(Row& row, const execplan::SPTP& parm,
                                                  const funcexp::Func& thisFunc)
{
  bool tmpIsNull = false;
  datatypes::Decimal d = parm->data()->getDecimalVal(row, tmpIsNull);
  if (tmpIsNull)
    return datatypes::TUInt64Null();

  if (parm->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
  {
    int128_t val = d.getPosNegRoundedIntegralPart(0).getValue();
    return ConvertToBitOperand<int128_t>(val);
  }

  return datatypes::TUInt64Null((uint64_t)d.decimal64ToSInt64Round());
}

//  Functions TypeHolderStd::canReturnXInt64() and GenericToBitOperand()
//  should be splitted eventually to virtual methods in TypeHandler.
//
//  However, TypeHandler::getBitOperand() would seem to be too specific.
//  It would be nice to have a more generic functionality in TypeHandler.
//
//  Let's consider having something similar to MariaDB Longlong_hybrid,
//  which holds a signed/unsigned 64bit value together with a sign flag.
//  Having TypeHandler::getXInt64Hybrid() would be more useful:
//  it can be reused for other purposes, not only for bitwise operations.

// @bug 4703 - the actual bug was only in the DATETIME case
// part of this statement below, but instead of leaving 5 identical
// copies of this code, extracted into a single utility function
// here.  This same method is potentially useful in other methods
// and could be extracted into a utility class with its own header
// if that is the case - this is left as future exercise
datatypes::TUInt64Null GenericToBitOperand(Row& row, const execplan::SPTP& parm,
                                           const funcexp::Func& thisFunc, bool temporalRounding,
                                           long timeZone)
{
  switch (parm->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      datatypes::TSInt64Null tmp = parm->data()->toTSInt64Null(row);
      return tmp.isNull() ? datatypes::TUInt64Null() : datatypes::TUInt64Null((uint64_t)(int64_t)tmp);
    }
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      bool tmpIsNull = false;
      double val = parm->data()->getDoubleVal(row, tmpIsNull);
      return tmpIsNull ? datatypes::TUInt64Null() : ConvertToBitOperand<double>(round(val));
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT: return parm->data()->toTUInt64Null(row);

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      bool tmpIsNull = false;
      const string& str = parm->data()->getStrVal(row, tmpIsNull);
      if (tmpIsNull)
        return datatypes::TUInt64Null();

      datatypes::DataCondition cnverr;
      literal::Converter<literal::SignedNumericLiteral> cnv(str, cnverr);
      cnv.normalize();
      return cnv.negative() ? datatypes::TUInt64Null((uint64_t)cnv.toPackedSDecimal<int64_t>(0, cnverr))
                            : datatypes::TUInt64Null(cnv.toPackedUDecimal<uint64_t>(0, cnverr));
    }
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL: return DecimalToBitOperand(row, parm, thisFunc);

    case execplan::CalpontSystemCatalog::DATE:
    {
      bool tmpIsNull = false;
      int32_t time = parm->data()->getDateIntVal(row, tmpIsNull);
      if (tmpIsNull)
        return datatypes::TUInt64Null();

      int64_t value = Date(time).convertToMySQLint();
      return datatypes::TUInt64Null((uint64_t)value);
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      bool tmpIsNull = false;
      int64_t time = parm->data()->getDatetimeIntVal(row, tmpIsNull);
      if (tmpIsNull)
        return datatypes::TUInt64Null();

      // @bug 4703 - missing year when convering to int
      DateTime dt(time);
      int64_t value = dt.convertToMySQLint();
      if (temporalRounding && dt.msecond >= 500000)
        value++;
      return datatypes::TUInt64Null((uint64_t)value);
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      bool tmpIsNull = false;
      int64_t time = parm->data()->getTimestampIntVal(row, tmpIsNull);
      if (tmpIsNull)
        return datatypes::TUInt64Null();

      TimeStamp dt(time);
      int64_t value = dt.convertToMySQLint(timeZone);
      if (temporalRounding && dt.msecond >= 500000)
        value++;
      return datatypes::TUInt64Null((uint64_t)value);
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      bool tmpIsNull = false;
      int64_t time = parm->data()->getTimeIntVal(row, tmpIsNull);

      Time dt(time);
      int64_t value = dt.convertToMySQLint();
      if (temporalRounding && dt.msecond >= 500000)
        value < 0 ? value-- : value++;
      return datatypes::TUInt64Null((uint64_t)value);
    }

    default:
      idbassert(0);  // Not possible: checked during the preparation stage.
      break;
  }

  return datatypes::TUInt64Null();
}

}  // namespace

namespace funcexp
{
class BitOperandGeneric : public datatypes::TUInt64Null
{
 public:
  BitOperandGeneric()
  {
  }
  BitOperandGeneric(Row& row, const execplan::SPTP& parm, const funcexp::Func& thisFunc, long timeZone)
   : TUInt64Null(GenericToBitOperand(row, parm, thisFunc, true, timeZone))
  {
  }
};

// The shift amount operand in MariaDB does not round temporal values
// when sql_mode=TIME_FRAC_ROUND is not set.
class BitOperandGenericShiftAmount : public datatypes::TUInt64Null
{
 public:
  BitOperandGenericShiftAmount()
  {
  }
  BitOperandGenericShiftAmount(Row& row, const execplan::SPTP& parm, const funcexp::Func& thisFunc,
                               long timeZone)
   : TUInt64Null(GenericToBitOperand(row, parm, thisFunc, false, timeZone))
  {
  }
};

// A functor to return NULL as a bitwise operation result.
// Used when an unexpected argument count
// is encounteded during the preparation step.
class Func_bitwise_null : public Func_BitOp
{
 public:
  Func_bitwise_null() : Func_BitOp("bitwise")
  {
  }
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    isNull = true;
    return 0;
  }
};

bool Func_BitOp::validateArgCount(execplan::FunctionColumn& col, uint expected) const
{
  static Func_bitwise_null return_null;
  if (col.functionParms().size() == expected)
    return false;
  col.setFunctor(&return_null);
  return true;
}

void Func_BitOp::setFunctorByParm(execplan::FunctionColumn& col, const execplan::SPTP& parm,
                                  Func_Int& return_uint64_from_uint64, Func_Int& return_uint64_from_sint64,
                                  Func_Int& return_uint64_generic) const
{
  if (parm->data()->resultType().isUnsignedInteger())
    col.setFunctor(&return_uint64_from_uint64);
  else if (parm->data()->resultType().isSignedInteger())
    col.setFunctor(&return_uint64_from_sint64);
  else
    col.setFunctor(&return_uint64_generic);
}

bool Func_BitOp::fixForBitShift(execplan::FunctionColumn& col, Func_Int& return_uint64_from_uint64,
                                Func_Int& return_uint64_from_sint64, Func_Int& return_uint64_generic) const
{
  if (validateArgCount(col, 2))
    return false;
  // The functor detection is done using functionParms()[0] only.
  // This is how MariaDB performs it.
  setFunctorByParm(col, col.functionParms()[0], return_uint64_from_uint64, return_uint64_from_sint64,
                   return_uint64_generic);
  return validateBitOperandTypeOrError(col, *this, 0) || validateBitOperandTypeOrError(col, *this, 1);
}

bool Func_BitOp::fixForBitOp2(execplan::FunctionColumn& col, Func_Int& return_uint64_from_uint64_uint64,
                              Func_Int& return_uint64_from_sint64_sint64,
                              Func_Int& return_uint64_generic) const
{
  if (validateArgCount(col, 2))
    return false;

  if (col.functionParms()[0]->data()->resultType().isUnsignedInteger() &&
      col.functionParms()[1]->data()->resultType().isUnsignedInteger())
  {
    col.setFunctor(&return_uint64_from_uint64_uint64);
    return false;
  }
  if (col.functionParms()[0]->data()->resultType().isSignedInteger() &&
      col.functionParms()[1]->data()->resultType().isSignedInteger())
  {
    col.setFunctor(&return_uint64_from_sint64_sint64);
    return false;
  }
  col.setFunctor(&return_uint64_generic);
  return validateBitOperandTypeOrError(col, *this, 0) || validateBitOperandTypeOrError(col, *this, 1);
}

//
// BITAND
//

template <class TA, class TB>
class Func_bitand_return_uint64 : public Func_bitand
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 2);
    Arg2Lazy<TA, TB> args(row, parm, *this, operationColType.getTimeZone());
    return (int64_t)(args.a & args.b).nullSafeValue(isNull);
  }
};

bool Func_bitand::fix(execplan::FunctionColumn& col) const
{
  static Func_bitand_return_uint64<ParmTUInt64, ParmTUInt64> return_uint64_from_uint64_uint64;
  static Func_bitand_return_uint64<ParmTSInt64, ParmTSInt64> return_uint64_from_sint64_sint64;
  static Func_bitand_return_uint64<BitOperandGeneric, BitOperandGeneric> return_uint64_generic;
  return fixForBitOp2(col, return_uint64_from_uint64_uint64, return_uint64_from_sint64_sint64,
                      return_uint64_generic);
}

//
// LEFT SHIFT
//

template <class TA>
class Func_leftshift_return_uint64 : public Func_leftshift
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 2);
    Arg2Eager<TA, BitOperandGenericShiftAmount> args(row, parm, *this, operationColType.getTimeZone());
    return (int64_t)args.a.MariaDBShiftLeft(args.b).nullSafeValue(isNull);
  }
};

bool Func_leftshift::fix(execplan::FunctionColumn& col) const
{
  static Func_leftshift_return_uint64<ParmTUInt64> return_uint64_from_uint64;
  static Func_leftshift_return_uint64<ParmTSInt64> return_uint64_from_sint64;
  static Func_leftshift_return_uint64<BitOperandGeneric> return_uint64_generic;
  return fixForBitShift(col, return_uint64_from_uint64, return_uint64_from_sint64, return_uint64_generic);
}

//
// RIGHT SHIFT
//

template <class TA>
class Func_rightshift_return_uint64 : public Func_rightshift
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 2);
    Arg2Eager<TA, BitOperandGenericShiftAmount> args(row, parm, *this, operationColType.getTimeZone());
    return (int64_t)args.a.MariaDBShiftRight(args.b).nullSafeValue(isNull);
  }
};

bool Func_rightshift::fix(execplan::FunctionColumn& col) const
{
  static Func_rightshift_return_uint64<ParmTUInt64> return_uint64_from_uint64;
  static Func_rightshift_return_uint64<ParmTSInt64> return_uint64_from_sint64;
  static Func_rightshift_return_uint64<BitOperandGeneric> return_uint64_generic;
  return fixForBitShift(col, return_uint64_from_uint64, return_uint64_from_sint64, return_uint64_generic);
}

//
// BIT OR
//

uint64_t Func_bitor::getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
{
  return static_cast<uint64_t>(getIntVal(row, fp, isNull, op_ct));
}

template <class TA, class TB>
class Func_bitor_return_uint64 : public Func_bitor
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 2);
    Arg2Lazy<TA, TB> args(row, parm, *this, operationColType.getTimeZone());
    return (int64_t)(args.a | args.b).nullSafeValue(isNull);
  }
};

bool Func_bitor::fix(execplan::FunctionColumn& col) const
{
  static Func_bitor_return_uint64<ParmTUInt64, ParmTUInt64> return_uint64_from_uint64_uint64;
  static Func_bitor_return_uint64<ParmTSInt64, ParmTSInt64> return_uint64_from_sint64_sint64;
  static Func_bitor_return_uint64<BitOperandGeneric, BitOperandGeneric> return_uint64_generic;
  return fixForBitOp2(col, return_uint64_from_uint64_uint64, return_uint64_from_sint64_sint64,
                      return_uint64_generic);
}

//
// BIT XOR
//

template <class TA, class TB>
class Func_bitxor_return_uint64 : public Func_bitxor
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 2);
    Arg2Eager<TA, TB> args(row, parm, *this, operationColType.getTimeZone());
    return (int64_t)(args.a ^ args.b).nullSafeValue(isNull);
  }
};

bool Func_bitxor::fix(execplan::FunctionColumn& col) const
{
  static Func_bitxor_return_uint64<ParmTUInt64, ParmTUInt64> return_uint64_from_uint64_uint64;
  static Func_bitxor_return_uint64<ParmTSInt64, ParmTSInt64> return_uint64_from_sint64_sint64;
  static Func_bitxor_return_uint64<BitOperandGeneric, BitOperandGeneric> return_uint64_generic;
  return fixForBitOp2(col, return_uint64_from_uint64_uint64, return_uint64_from_sint64_sint64,
                      return_uint64_generic);
}

//
// BIT COUNT
//

inline int64_t bitCount(uint64_t val)
{
  // Refer to Hacker's Delight Chapter 5
  // for the bit counting algo used here
  val = val - ((val >> 1) & 0x5555555555555555);
  val = (val & 0x3333333333333333) + ((val >> 2) & 0x3333333333333333);
  val = (val + (val >> 4)) & 0x0F0F0F0F0F0F0F0F;
  val = val + (val >> 8);
  val = val + (val >> 16);
  val = val + (val >> 32);

  return (int64_t)(val & 0x000000000000007F);
}

template <class TA>
class Func_bit_count_return_uint64 : public Func_bit_count
{
 public:
  int64_t getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                    CalpontSystemCatalog::ColType& operationColType) override
  {
    idbassert(parm.size() == 1);
    return bitCount((uint64_t)TA(row, parm[0], *this, operationColType.getTimeZone()).nullSafeValue(isNull));
  }
};

bool Func_bit_count::fix(execplan::FunctionColumn& col) const
{
  static Func_bit_count_return_uint64<ParmTUInt64> return_uint64_from_uint64;
  static Func_bit_count_return_uint64<ParmTSInt64> return_uint64_from_sint64;
  static Func_bit_count_return_uint64<BitOperandGeneric> return_uint64_generic;
  if (validateArgCount(col, 1))
    return false;
  setFunctorByParm(col, col.functionParms()[0], return_uint64_from_uint64, return_uint64_from_sint64,
                   return_uint64_generic);
  return validateBitOperandTypeOrError(col, *this, 0);
}

}  // namespace funcexp
// vim:ts=4 sw=4:
