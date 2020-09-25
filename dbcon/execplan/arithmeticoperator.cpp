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
ArithmeticOperator::ArithmeticOperator() : Operator()
{
}

ArithmeticOperator::ArithmeticOperator(const string& operatorName): Operator(operatorName)
{
}

ArithmeticOperator::ArithmeticOperator(const ArithmeticOperator& rhs):
    Operator(rhs),
    fTimeZone(rhs.timeZone())
{
}

ArithmeticOperator:: ~ArithmeticOperator()
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
    return output;
}

/**
 * The serialization interface
 */
void ArithmeticOperator::serialize(messageqcpp::ByteStream& b) const
{
    b << (ObjectReader::id_t) ObjectReader::ARITHMETICOPERATOR;
    b << fTimeZone;
    Operator::serialize(b);
}

void ArithmeticOperator::unserialize(messageqcpp::ByteStream& b)
{
    ObjectReader::checkType(b, ObjectReader::ARITHMETICOPERATOR);
    b >> fTimeZone;
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

#if 0
void ArithmeticOperator::operationType(const Type& l, const Type& r)
{
    if (l.colDataType == execplan::CalpontSystemCatalog::DECIMAL)
    {
        switch (r.colDataType)
        {
            case execplan::CalpontSystemCatalog::DECIMAL:
            {
                // should follow the result type that MySQL gives
                fOperationType = fResultType;
                break;
            }

            case execplan::CalpontSystemCatalog::INT:
            case execplan::CalpontSystemCatalog::MEDINT:
            case execplan::CalpontSystemCatalog::TINYINT:
            case execplan::CalpontSystemCatalog::BIGINT:
                fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
                fOperationType.scale = l.scale;
                break;

            default:
                fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        }
    }
    else if (r.colDataType == execplan::CalpontSystemCatalog::DECIMAL)
    {
        switch (l.colDataType)
        {
            case execplan::CalpontSystemCatalog::DECIMAL:
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
                fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
                fOperationType.scale = r.scale;
                break;

            default:
                fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
        }
    }
    else if ((l.colDataType == execplan::CalpontSystemCatalog::INT ||
              l.colDataType == execplan::CalpontSystemCatalog::MEDINT ||
              l.colDataType == execplan::CalpontSystemCatalog::TINYINT ||
              l.colDataType == execplan::CalpontSystemCatalog::BIGINT) &&
             (r.colDataType == execplan::CalpontSystemCatalog::INT ||
              r.colDataType == execplan::CalpontSystemCatalog::MEDINT ||
              r.colDataType == execplan::CalpontSystemCatalog::TINYINT ||
              r.colDataType == execplan::CalpontSystemCatalog::BIGINT))
        fOperationType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
    else
        fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
}
#endif

void ArithmeticOperator::adjustResultType(const CalpontSystemCatalog::ColType& m)
{
    if (m.colDataType != CalpontSystemCatalog::DECIMAL)
    {
        fResultType = m;
    }
    else
    {
        CalpontSystemCatalog::ColType n;
        n.colDataType = CalpontSystemCatalog::LONGDOUBLE;
        n.scale = m.scale; // @bug5736, save the original decimal scale
        n.precision = -1;  // @bug5736, indicate this double is for decimal math
        n.colWidth = sizeof(long double);
        fResultType = n;
    }
}

}  // namespace
