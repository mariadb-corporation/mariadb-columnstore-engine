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

/***********************************************************************
 *   $Id: arithmeticoperator.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include <iostream>

#include "bytestream.h"
#include "arithmeticoperator.h"
#include "objectreader.h"

using namespace std;

namespace execplan
{
/**
 * Constructors/Destructors
 */
ArithmeticOperator::ArithmeticOperator() : Operator(), fDecimalOverflowCheck(false)
{
}

ArithmeticOperator::ArithmeticOperator(const string& operatorName)
 : Operator(operatorName), fDecimalOverflowCheck(false)
{
}

ArithmeticOperator::ArithmeticOperator(const ArithmeticOperator& rhs)
 : Operator(rhs), fTimeZone(rhs.timeZone()), fDecimalOverflowCheck(rhs.getOverflowCheck())
{
}

ArithmeticOperator::~ArithmeticOperator()
{
}

/**
 * Operations
 */

/**
 * friend function
 */
ostream& operator<<(ostream& output, const ArithmeticOperator& rhs)
{
  output << rhs.toString();
  output << "opType=" << rhs.operationType().colDataType << endl;
  output << "decimalOverflowCheck=" << rhs.getOverflowCheck() << endl;
  return output;
}

/**
 * The serialization interface
 */
void ArithmeticOperator::serialize(messageqcpp::ByteStream& b) const
{
  b << (ObjectReader::id_t)ObjectReader::ARITHMETICOPERATOR;
  messageqcpp::ByteStream::octbyte timeZone = fTimeZone;
  b << timeZone;
  const messageqcpp::ByteStream::byte tmp = fDecimalOverflowCheck;
  b << tmp;
  Operator::serialize(b);
}

void ArithmeticOperator::unserialize(messageqcpp::ByteStream& b)
{
  ObjectReader::checkType(b, ObjectReader::ARITHMETICOPERATOR);
  messageqcpp::ByteStream::octbyte timeZone;
  b >> timeZone;
  fTimeZone = timeZone;
  messageqcpp::ByteStream::byte tmp;
  b >> tmp;
  fDecimalOverflowCheck = tmp;
  Operator::unserialize(b);
}

bool ArithmeticOperator::operator==(const ArithmeticOperator& t) const
{
  if (data() != t.data())
    return false;

  if (timeZone() != t.timeZone())
    return false;

  return true;
}

bool ArithmeticOperator::operator==(const TreeNode* t) const
{
  const ArithmeticOperator* o;

  o = dynamic_cast<const ArithmeticOperator*>(t);

  if (o == NULL)
    return false;

  return *this == *o;
}

bool ArithmeticOperator::operator!=(const ArithmeticOperator& t) const
{
  return (!(*this == t));
}

bool ArithmeticOperator::operator!=(const TreeNode* t) const
{
  return (!(*this == t));
}

void ArithmeticOperator::adjustResultType(const CalpontSystemCatalog::ColType& m)
{
  if (m.colDataType != CalpontSystemCatalog::DECIMAL && m.colDataType != CalpontSystemCatalog::UDECIMAL)
  {
    fResultType = m;
  }
  else
  {
    CalpontSystemCatalog::ColType n;
    n.colDataType = CalpontSystemCatalog::LONGDOUBLE;
    n.scale = m.scale;  // @bug5736, save the original decimal scale
    n.precision = -1;   // @bug5736, indicate this double is for decimal math
    n.colWidth = sizeof(long double);
    fResultType = n;
  }
}

}  // namespace execplan
