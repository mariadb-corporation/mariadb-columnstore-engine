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
*   $Id: predicateoperator.h 9667 2013-07-08 16:37:10Z bpaul $
*
*
***********************************************************************/
/** @file */

#ifndef PREDICATEOPERATOR_H
#define PREDICATEOPERATOR_H
#include <string>
#include <sstream>
#if defined(_MSC_VER)
#include <malloc.h>
#elif defined(__FreeBSD__)
#include <cstdlib>
#else
#include <alloca.h>
#endif
#include <cstring>
#include <cmath>
#include <boost/regex.hpp>

#include "expressionparser.h"
#include "returnedcolumn.h"
#include "dataconvert.h"
#include "utils_utf8.h"

namespace messageqcpp
{
class ByteStream;
}

namespace execplan
{

class PredicateOperator : public Operator
{

public:
    PredicateOperator();
    PredicateOperator(const std::string& operatorName);
    PredicateOperator(const PredicateOperator& rhs);
    virtual ~PredicateOperator();


    /** return a copy of this pointer
     *
     * deep copy of this pointer and return the copy
     */
    inline virtual PredicateOperator* clone() const
    {
        return new PredicateOperator (*this);
    }

    /**
     * The serialization interface
     */
    virtual void serialize(messageqcpp::ByteStream&) const;
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
    bool operator==(const PredicateOperator& t) const;

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
    bool operator!=(const PredicateOperator& t) const;

    /***********************************************************
     *                  F&E framework                          *
     ***********************************************************/
    inline virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ReturnedColumn* lop, ReturnedColumn* rop);
    void setOpType(Type& l, Type& r);

private:
    template <typename result_t>
    inline bool numericCompare(result_t op1, result_t op2);
    inline bool strCompare(const std::string& op1, const std::string& op2);
    // MCOL-1559
    inline bool strTrimCompare(const std::string& op1, const std::string& op2);
};

inline bool PredicateOperator::getBoolVal(rowgroup::Row& row, bool& isNull, ReturnedColumn* lop, ReturnedColumn* rop)
{
    // like operator. both sides are string.
    if (fOp == OP_LIKE || fOp == OP_NOTLIKE)
    {
        SP_CNX_Regex regex = rop->regex();

        // Ugh. The strings returned by getStrVal have null padding out to the col width. boost::regex
        //  considers these nulls significant, but they're not in the pattern, so we need to strip
        //   them off...
        const std::string& v = lop->getStrVal(row, isNull);
//        char* c = (char*)alloca(v.length() + 1);
//        memcpy(c, v.c_str(), v.length());
//        c[v.length()] = 0;
//        std::string vv(c);

        if (regex)
        {
#ifdef POSIX_REGEX
            bool ret = regexec(regex.get(), v.c_str(), 0, NULL, 0) == 0;
#else
            bool ret = boost::regex_match(v.c_str(), *regex);
#endif
            return (((fOp == OP_LIKE) ? ret : !ret) && !isNull);
        }
        else
        {
#ifdef POSIX_REGEX
            regex_t regex;
            std::string str = dataconvert::DataConvert::constructRegexp(rop->getStrVal(row, isNull));
            regcomp(&regex, str.c_str(), REG_NOSUB | REG_EXTENDED);
            bool ret = regexec(&regex, v.c_str(), 0, NULL, 0) == 0;
            regfree(&regex);
#else
            boost::regex regex(dataconvert::DataConvert::constructRegexp(rop->getStrVal(row, isNull)));
            bool ret = boost::regex_match(v.c_str(), regex);
#endif
            return (((fOp == OP_LIKE) ? ret : !ret) && !isNull);
        }
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

            return numericCompare(val1,  rop->getIntVal(row, isNull)) && !isNull;
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

            return numericCompare(val1,  rop->getUintVal(row, isNull)) && !isNull;
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
//            return strCompare(val1, rop->getStrVal(row, isNull)) && !isNull;

        }

        //FIXME: ???
        case execplan::CalpontSystemCatalog::VARBINARY:
        case execplan::CalpontSystemCatalog::BLOB:
            return false;
            break;

        default:
        {
            std::ostringstream oss;
            oss << "invalid predicate operation type: " << fOperationType.colDataType;
            throw logging::InvalidOperationExcept(oss.str());
        }
    }

    return false;
}


template <typename result_t>
inline bool PredicateOperator::numericCompare(result_t op1, result_t op2)
{
    switch (fOp)
    {
        case OP_EQ:
            return op1 == op2;

        case OP_NE:
            return op1 != op2;

        case OP_GT:
            return op1 > op2;

        case OP_GE:
            return op1 >= op2;

        case OP_LT:
            return op1 < op2;

        case OP_LE:
            return op1 <= op2;

        default:
        {
            std::ostringstream oss;
            oss << "invalid predicate operation: " << fOp;
            throw logging::InvalidOperationExcept(oss.str());
        }
    }
}

inline bool PredicateOperator::strCompare(const std::string& op1, const std::string& op2)
{
    switch (fOp)
    {
        case OP_EQ:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) == 0;

        case OP_NE:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) != 0;

        case OP_GT:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) > 0;

        case OP_GE:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) >= 0;

        case OP_LT:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) < 0;

        case OP_LE:
            return funcexp::utf8::idb_strcoll(op1.c_str(), op2.c_str()) <= 0;

        default:
        {
            std::ostringstream oss;
            oss << "Non support predicate operation: " << fOp;
            throw logging::InvalidOperationExcept(oss.str());
        }
    }
}

inline bool PredicateOperator::strTrimCompare(const std::string& op1, const std::string& op2)
{
    switch (fOp)
    {
        case OP_EQ:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) == 0;

        case OP_NE:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) != 0;

        case OP_GT:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) > 0;

        case OP_GE:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) >= 0;

        case OP_LT:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) < 0;

        case OP_LE:
            return funcexp::utf8::idb_strtrimcoll(op1, op2) <= 0;

        default:
        {
            std::ostringstream oss;
            oss << "Non support predicate operation: " << fOp;
            throw logging::InvalidOperationExcept(oss.str());
        }
    }
}

std::ostream& operator<<(std::ostream& os, const PredicateOperator& rhs);
}

#endif //PREDICATEOPERATOR_H

