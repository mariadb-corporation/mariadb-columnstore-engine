/* Copyright (C) 2021 MariaDB Corporation.

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

#ifndef NUMERICLITERAL_H
#define NUMERICLITERAL_H

#include "genericparser.h"
#include "mcs_datatype.h"

namespace literal
{
using datatypes::DataCondition;
using genericparser::Parser;
using utils::ConstString;

typedef uint32_t scale_t;

template <class A>
class Converter : public Parser, public A
{
 public:
  Converter(const char* str, size_t length, DataCondition& error)
   : Parser(str, length), A(&Parser::skipLeadingSpaces())
  {
    if (Parser::syntaxError())
    {
      /*
        Non-recoverable syntax error happened. The parser parsed the first part
        of a combined rule (and therefore shifted the tokenizer position)
        but then failed to parse the rule till the end.

        For example in the <signed numeric literal>:
        ''    -  empty string
        '+'   -  sign was not followed by a digit or period,        expect '+1'
        '.'   -  period was not followed by a digit,                expect '.1'
        '1e'  -  exponent marker was not followed by <exponent>,    expect '1e1'
        '1e+' -  in <exponent>, <sign> was not followed by a digit, expect '1e+1'
      */
      error |= (DataCondition::X_INVALID_CHARACTER_VALUE_FOR_CAST);
    }
  }
  Converter(const std::string& str, DataCondition& error) : Converter(str.data(), str.length(), error)
  {
  }
};

/*

SQL Standard definition for <cast specification>
related to character string to exact number conversion
======================================================
Abbreviations:
- TD - the target data type
- SD - the datatype of the source value
- SV - the source value

8) If TD is exact numeric, then
a) If SD is exact numeric or approximate numeric, then
Case:

  i) If there is a representation of SV in the data type TD that does not lose
     any leading significant digits after rounding or truncating if necessary,
     then TV is that representation. The choice of whether to round or truncate
     is implementation-defined. (NoteAI)
  ii) Otherwise, an exception condition is raised:
       data exception -- numeric value out of range. (NoteAII)

b) If SD is character string, then SV is replaced by SV with any leading
   or trailing <space>s removed. (NoteB)
Case:

  i) If SV does not comprise a <signed numeric literal> as defined by the rules
     for <literal> in Subclause "<literal>", then an exception condition is raised:
       data exception - invalid character value for cast. (NoteBI)
  ii) Otherwise, let LT be that <signed numeric literal>.
      The <cast specification> is equivalent to CAST ( LT AS TD )


Implementation details
======================
NoteAI
----
The implementation defined choice whether to round or truncate is
"round away from zero".

NoteAII
-----
When the "numeric value out of range" state is found, it is signalled
to the caller, and the returned value is adjusted according to the TD range.
The caller later decides whether to raise an error or to use the adjusted value.

NoteB
-----
The implementation removes only leading spaces. The caller can
check if any trailing spaces are left by the parser.

NoteBI
------
The implementation stops on the first character that does not
conform to the <signed numeric literal> syntax. The caller can
check if any trailing garbage characters are left by the parser.


Grammar
=======

<signed numeric literal>    ::=   [ <sign> ] <unsigned numeric literal>

<unsigned numeric literal>  ::=   <exact numeric literal> [ E <exponent> ]

<exact numeric literal>    ::=
                   <unsigned integer> [ <period> [ <unsigned integer> ] ]
               |   <period> <unsigned integer>

<sign>    ::=   <plus sign> | <minus sign>

<exponent>    ::=   <signed integer>

<signed integer>    ::=   [ <sign> ] <unsigned integer>

<unsigned integer>    ::=   <digit> ...

*/

//
//  Terminal symbols
//

class Period : public ConstString
{
 public:
  explicit Period(Parser* p) : ConstString(p->tokenChar('.'))
  {
  }
  bool isNull() const
  {
    return mStr == nullptr;
  }
};

class ExponentMarker : public ConstString
{
 public:
  explicit ExponentMarker(Parser* p) : ConstString(p->tokenAnyCharOf('e', 'E'))
  {
  }
  bool isNull() const
  {
    return mStr == nullptr;
  }
};

class Sign : public ConstString
{
 public:
  explicit Sign() : ConstString(NULL, 0)
  {
  }
  explicit Sign(const ConstString& str) : ConstString(str)
  {
  }
  explicit Sign(Parser* p) : ConstString(p->tokenAnyCharOf('+', '-'))
  {
  }
  static Sign empty(Parser* p)
  {
    return Sign(p->tokStartConstString());
  }
  bool isNull() const
  {
    return mStr == nullptr;
  }
  bool negative() const
  {
    return eq('-');
  }
};

class Digits : public ConstString
{
 public:
  explicit Digits() : ConstString(NULL, 0)
  {
  }
  explicit Digits(const char* str, size_t length) : ConstString(str, length)
  {
  }
  explicit Digits(const ConstString& str) : ConstString(str)
  {
  }
  explicit Digits(Parser* p) : ConstString(p->tokenDigits())
  {
  }
  bool isNull() const
  {
    return mStr == nullptr;
  }

  void skipLeadingZeroDigits()
  {
    for (; mLength > 0 && mStr[0] == '0';)
    {
      mStr++;
      mLength--;
    }
  }
  void skipTrailingZeroDigits()
  {
    for (; mLength > 0 && mStr[mLength - 1] == '0';)
      mLength--;
  }
};

//
// Non-terminal symbols
//

// <unsigned integer>    ::=   <digit> ...
class UnsignedInteger : public Digits
{
 public:
  explicit UnsignedInteger() : Digits()
  {
  }
  explicit UnsignedInteger(const char* str, size_t length) : Digits(str, length)
  {
  }
  explicit UnsignedInteger(const ConstString& str) : Digits(str)
  {
  }
  explicit UnsignedInteger(Parser* p) : Digits(p)
  {
  }
  static UnsignedInteger empty(const Parser* p)
  {
    return UnsignedInteger(p->tokStartConstString());
  }
  UnsignedInteger left(size_t len) const
  {
    return UnsignedInteger(str(), length() > len ? len : length());
  }

  template <typename T>
  T toXIntPositiveContinue(T start, DataCondition& error) const
  {
    const char* e = end();
    T val = start;
    for (const char* s = mStr; s < e; s++)
    {
      constexpr T cutoff = datatypes::numeric_limits<T>::max() / 10;
      if (val > cutoff)
      {
        error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
        return datatypes::numeric_limits<T>::max();
      }
      val *= 10;
      T newval = val + (s[0] - '0');
      if (newval < val)
      {
        error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
        return datatypes::numeric_limits<T>::max();
      }
      val = newval;
    }
    return val;
  }
  template <typename T>
  T toXIntPositive(DataCondition& error) const
  {
    return toXIntPositiveContinue<T>(0, error);
  }

  template <typename T>
  T toSIntNegativeContinue(T start, DataCondition& error) const
  {
    const char* e = end();
    T val = start;
    for (const char* s = mStr; s < e; s++)
    {
      constexpr T cutoff = datatypes::numeric_limits<T>::min() / 10;
      if (val < cutoff)
      {
        error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
        return datatypes::numeric_limits<T>::min();
      }
      val *= 10;
      T newval = val - (s[0] - '0');
      if (newval > val)
      {
        error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
        return datatypes::numeric_limits<T>::min();
      }
      val = newval;
    }
    return val;
  }
  template <typename T>
  T toSIntNegative(DataCondition& error) const
  {
    return toSIntNegativeContinue<T>(0, error);
  }

  template <typename T>
  T toXIntPositiveRoundAwayFromZeroContinue(T start, bool round, DataCondition& error) const
  {
    T val = toXIntPositiveContinue<T>(start, error);
    if (val == datatypes::numeric_limits<T>::max() && round)
    {
      error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
      return val;
    }
    return val + round;
  }
  template <typename T>
  T toXIntPositiveRoundAwayFromZero(bool round, DataCondition& error) const
  {
    return toXIntPositiveRoundAwayFromZeroContinue<T>(0, round, error);
  }
};

// <signed integer> := [<sign>] <unsigned integer>
class SignedInteger : public Parser::DD2OM<Sign, UnsignedInteger>
{
 public:
  using DD2OM::DD2OM;
  bool isNull() const
  {
    return UnsignedInteger::isNull();
  }

  template <typename T>
  T abs(DataCondition& error) const
  {
    return toXIntPositive<T>(error);
  }

  template <typename T>
  T toSInt(DataCondition& error) const
  {
    return negative() ? toSIntNegative<T>(error) : toXIntPositive<T>(error);
  }
};

// E <signed integer>
class EExponent : public Parser::UD2MM<ExponentMarker, SignedInteger>
{
 public:
  using UD2MM::UD2MM;
};

// <period> <unsigned integer>
class ExactUnsignedNumericLiteralFractionAlone : public Parser::UD2MM<Period, UnsignedInteger>
{
 public:
  using UD2MM::UD2MM;
};

// <period> [ <unsigned integer> ]
class PeriodOptUnsignedInteger : public Parser::UD2MO<Period, UnsignedInteger>
{
 public:
  using UD2MO::UD2MO;
  static PeriodOptUnsignedInteger empty(Parser* p)
  {
    return PeriodOptUnsignedInteger(UnsignedInteger(p->tokStartConstString()));
  }
  const PeriodOptUnsignedInteger& fraction() const
  {
    return *this;
  }
};

// <integral unsigned integer> := <unsigned integer>
class IntegralUnsignedInteger : public UnsignedInteger
{
 public:
  explicit IntegralUnsignedInteger(Parser* p) : UnsignedInteger(p)
  {
  }
  const UnsignedInteger& integral() const
  {
    return *this;
  }
};

// <integral unsigned integer> [ <period> [ <unsigned integer> ] ]

class ExactUnsignedNumericLiteralIntegralOptFraction
 : public Parser::DD2MO<IntegralUnsignedInteger, PeriodOptUnsignedInteger>
{
 public:
  using DD2MO::DD2MO;
};

// A container for integral and fractional parts
class UnsignedIntegerDecimal
{
 protected:
  UnsignedInteger mIntegral;
  UnsignedInteger mFraction;

 public:
  explicit UnsignedIntegerDecimal(const UnsignedInteger& intg, const UnsignedInteger& frac)
   : mIntegral(intg), mFraction(frac)
  {
  }
  explicit UnsignedIntegerDecimal(const ExactUnsignedNumericLiteralFractionAlone& rhs) : mFraction(rhs)
  {
  }
  explicit UnsignedIntegerDecimal(const ExactUnsignedNumericLiteralIntegralOptFraction& rhs)
   : mIntegral(rhs.integral()), mFraction(rhs.fraction())
  {
  }

  size_t IntFracDigits() const
  {
    return mIntegral.length() + mFraction.length();
  }

  bool isNull() const
  {
    return mIntegral.isNull() && mFraction.isNull();
  }

  void normalize()
  {
    mIntegral.skipLeadingZeroDigits();
    mFraction.skipTrailingZeroDigits();
  }

  template <typename T>
  T toXIntPositive(DataCondition& error) const
  {
    T val = mIntegral.toXIntPositive<T>(error);
    return mFraction.toXIntPositiveContinue<T>(val, error);
  }

  template <typename T>
  T toXIntPositiveRoundAwayFromZero(bool roundUp, DataCondition& error) const
  {
    T val = mIntegral.toXIntPositive<T>(error);
    return mFraction.toXIntPositiveRoundAwayFromZeroContinue<T>(val, roundUp, error);
  }

  template <typename T>
  T toXIntPositiveScaleUp(size_t scale, DataCondition& error) const
  {
    T val = toXIntPositive<T>(error);
    if (val == datatypes::numeric_limits<T>::max())
      return val;
    for (; scale; scale--)
    {
      constexpr T cutoff = datatypes::numeric_limits<T>::max() / 10;
      if (val > cutoff)
      {
        error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
        return datatypes::numeric_limits<T>::max();
      }
      val *= 10;
    }
    return val;
  }

  template <typename T>
  T toXIntPositiveRound(DataCondition& error) const
  {
    bool roundUp = mFraction.length() && mFraction.str()[0] >= '5';
    return mIntegral.toXIntPositiveRoundAwayFromZero<T>(roundUp, error);
  }

  template <typename T>
  T toXIntPositiveRoundExp(uint64_t absExp, bool negExp, DataCondition& error) const
  {
    if (absExp == 0)
      return toXIntPositiveRound<T>(error);

    if (negExp)
    {
      if (mIntegral.length() == absExp)  // 567.8e-3 -> 0.5678 -> 1
        return mIntegral.str()[0] >= '5' ? 1 : 0;
      if (mIntegral.length() < absExp)  // 123e-4 -> 0.0123
        return 0;
      // mIntegral.length() > absExp: 5678.8e-3 -> 5.6788 -> 6
      size_t diff = mIntegral.length() - absExp;
      const UnsignedInteger tmp(mIntegral.str(), diff);
      bool roundUp = mIntegral.str()[diff] >= '5';
      return tmp.toXIntPositiveRoundAwayFromZero<T>(roundUp, error);
    }

    // Positive exponent: 123.456e2
    if (mFraction.length() >= absExp)  // 123.456e2 -> 12345.6 -> 12346
    {
      bool roundUp = mFraction.length() > absExp && mFraction.str()[absExp] >= '5';
      UnsignedIntegerDecimal tmp(mIntegral, mFraction.left(absExp));
      return tmp.toXIntPositiveRoundAwayFromZero<T>(roundUp, error);
    }

    // Pad int+frac with right zeros 123.4e3 -> 123400
    size_t diff = absExp - mFraction.length();
    return toXIntPositiveScaleUp<T>(diff, error);
  }
};

// <exact unsigned numeric literal> :=
//           <period> [ <unsigned integer> ]
//         | <unsigned integer> [ <period> [ <unsigned integer> ] ]

class ExactUnsignedNumericLiteral
 : public Parser::Choice2<UnsignedIntegerDecimal, ExactUnsignedNumericLiteralFractionAlone,
                          ExactUnsignedNumericLiteralIntegralOptFraction>
{
 public:
  using Choice2::Choice2;
};

// <unsigned numeric literal>  ::=  <exact numeric literal> [ E <exponent> ]

class UnsignedNumericLiteral : public Parser::DM2MO<ExactUnsignedNumericLiteral, EExponent>
{
 public:
  using DM2MO::DM2MO;
  void normalize()
  {
    ExactUnsignedNumericLiteral::normalize();
    mB.skipLeadingZeroDigits();
  }
  const SignedInteger& exponent() const
  {
    return mB;
  }

  template <typename T>
  T toXIntPositiveRound(DataCondition& error) const
  {
    size_t availableDigits = IntFracDigits();
    if (!availableDigits)
      return 0;
    T absexp = exponent().abs<T>(error);
    return ExactUnsignedNumericLiteral::toXIntPositiveRoundExp<T>(absexp, exponent().negative(), error);
  }

  template <typename T>
  T toPackedDecimalPositive(scale_t scale, DataCondition& error) const
  {
    size_t availableDigits = IntFracDigits();
    if (!availableDigits)
      return 0;
    int64_t exp = exponent().toSInt<int64_t>(error);
    if (exp <= datatypes::numeric_limits<int64_t>::max() - scale)
      exp += scale;
    if (exp < 0)
    {
      if (exp == datatypes::numeric_limits<int64_t>::min())
        exp++;  // Avoid undefined behaviour in the unary minus below:
      return ExactUnsignedNumericLiteral::toXIntPositiveRoundExp<T>((uint64_t)-exp, true, error);
    }
    return ExactUnsignedNumericLiteral::toXIntPositiveRoundExp<T>((uint64_t)exp, false, error);
  }
};

// <signed numeric literal>    ::=   [ <sign> ] <unsigned numeric literal>
class SignedNumericLiteral : public Parser::DD2OM<Sign, UnsignedNumericLiteral>
{
 public:
  using DD2OM::DD2OM;
  bool isNull() const
  {
    return UnsignedNumericLiteral::isNull();
  }

  template <typename T>
  T toUIntXRound() const
  {
    if (negative())
      return 0;
    return UnsignedNumericLiteral::toXIntPositiveRound<T>();
  }

  template <typename T>
  T toPackedUDecimal(scale_t scale, DataCondition& error) const
  {
    if (negative())
      return 0;
    return UnsignedNumericLiteral::toPackedDecimalPositive<T>(scale, error);
  }

  template <typename T>
  T toPackedSDecimal(scale_t scale, DataCondition& error) const
  {
    if (!negative())
      return UnsignedNumericLiteral::toPackedDecimalPositive<T>(scale, error);
    typedef typename datatypes::make_unsigned<T>::type UT;
    UT absval = UnsignedNumericLiteral::toPackedDecimalPositive<UT>(scale, error);
    if (absval >= (UT)datatypes::numeric_limits<T>::min())
    {
      error |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
      return datatypes::numeric_limits<T>::min();
    }
    return -(T)absval;
  }
};

}  // namespace literal

#endif  // NUMERICLITERAL_H
