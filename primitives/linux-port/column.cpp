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

/*****************************************************************************
 * $Id: column.cpp 2103 2013-06-04 17:53:38Z dcathey $
 *
 ****************************************************************************/
#include <iostream>
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <cmath>
#ifndef _MSC_VER
#include <pthread.h>
#else
#endif
using namespace std;

#include <boost/scoped_array.hpp>
using namespace boost;

#include "primitiveprocessor.h"
#include "messagelog.h"
#include "messageobj.h"
#include "we_type.h"
#include "stats.h"
#include "primproc.h"
#include "dataconvert.h"
using namespace logging;
using namespace dbbc;
using namespace primitives;
using namespace primitiveprocessor;
using namespace execplan;

namespace
{

inline uint64_t order_swap(uint64_t x)
{
    uint64_t ret = (x >> 56) |
                   ((x << 40) & 0x00FF000000000000ULL) |
                   ((x << 24) & 0x0000FF0000000000ULL) |
                   ((x << 8)  & 0x000000FF00000000ULL) |
                   ((x >> 8)  & 0x00000000FF000000ULL) |
                   ((x >> 24) & 0x0000000000FF0000ULL) |
                   ((x >> 40) & 0x000000000000FF00ULL) |
                   (x << 56);
    return ret;
}

template <int W>
inline string fixChar(int64_t intval);

template <class T>
inline int  compareBlock(  const void* a, const void* b )
{
    return ( (*(T*)a) - (*(T*)b) );
}

//this function is out-of-band, we don't need to inline it
void logIt(int mid, int arg1, const string& arg2 = string())
{
    MessageLog logger(LoggingID(28));
    logging::Message::Args args;
    Message msg(mid);

    args.add(arg1);

    if (arg2.length() > 0)
        args.add(arg2);

    msg.format(args);
    logger.logErrorMessage(msg);
}

//FIXME: what are we trying to accomplish here? It looks like we just want to count
// the chars in a string arg?
p_DataValue convertToPDataValue(const void* val, int W)
{
    p_DataValue dv;
    string str;

    if (8 == W)
        str = fixChar<8>(*reinterpret_cast<const int64_t*>(val));
    else
        str = reinterpret_cast<const char*>(val);

    dv.len = static_cast<int>(str.length());
    dv.data = reinterpret_cast<const uint8_t*>(val);
    return dv;
}


template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2;

        case COMPARE_EQ:
            return val1 == val2;

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_NE:
            return val1 != val2;

        case COMPARE_GE:
            return val1 >= val2;

        default:
            logIt(34, COP, "colCompare");
            return false;						// throw an exception here?
    }
}

template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP, uint8_t rf)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && (rf & 0x01));

        case COMPARE_LE:
            return val1 < val2 || (val1 == val2 && rf ^ 0x80);

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf ^ 0x01);

        case COMPARE_GT:
            return val1 > val2 || (val1 == val2 && (rf & 0x80));

        default:
            logIt(34, COP, "colCompare_l");
            return false;						// throw an exception here?
    }
}

bool isLike(const char* val, const idb_regex_t* regex)
{
    if (!regex)
        throw runtime_error("PrimitiveProcessor::isLike: Missing regular expression for LIKE operator");

#ifdef POSIX_REGEX
    return (regexec(&regex->regex, val, 0, NULL, 0) == 0);
#else
    return regex_match(val, regex->regex);
#endif
}

//@bug 1828  Like must be a string compare.
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf, const idb_regex_t* regex)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && rf != 0);

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf == 0);

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_LIKE:
        case COMPARE_NLIKE:
        {
            /* LIKE comparisons are string comparisons so we reverse the order again.
            	Switching the order twice is probably as efficient as evaluating a guard.  */
            char tmp[9];
            val1 = order_swap(val1);
            memcpy(tmp, &val1, 8);
            tmp[8] = '\0';
            return (COP & COMPARE_NOT ? !isLike(tmp, regex) : isLike(tmp, regex));
        }

        default:
            logIt(34, COP, "colCompare_l");
            return false;						// throw an exception here?
    }
}

template<int>
inline bool isEmptyVal(uint8_t type, const void* val8);

template<>
inline bool isEmptyVal<8>(uint8_t type, const void* ival)
{
    const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return (joblist::DOUBLEEMPTYROW == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return (*val == joblist::CHAR8EMPTYROW);

        case CalpontSystemCatalog::UBIGINT:
            return (joblist::UBIGINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::BIGINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<4>(uint8_t type, const void* ival)
{
    const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return (joblist::FLOATEMPTYROW == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR4EMPTYROW == *val);

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return (joblist::UINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::INTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<2>(uint8_t type, const void* ival)
{
    const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR2EMPTYROW == *val);

        case CalpontSystemCatalog::USMALLINT:
            return (joblist::USMALLINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::SMALLINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<1>(uint8_t type, const void* ival)
{
    const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (*val == joblist::CHAR1EMPTYROW);

        case CalpontSystemCatalog::UTINYINT:
            return (*val == joblist::UTINYINTEMPTYROW);

        default:
            break;
    }

    return (*val == joblist::TINYINTEMPTYROW);
}

template<int>
inline bool isNullVal(uint8_t type, const void* val8);

template<>
inline bool isNullVal<8>(uint8_t type, const void* ival)
{
    const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return (joblist::DOUBLENULL == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            //@bug 339 might be a token here
            //TODO: what's up with the second const here?
            return (*val == joblist::CHAR8NULL || 0xFFFFFFFFFFFFFFFELL == *val);

        case CalpontSystemCatalog::UBIGINT:
            return (joblist::UBIGINTNULL == *val);

        default:
            break;
    }

    return (joblist::BIGINTNULL == *val);
}

template<>
inline bool isNullVal<4>(uint8_t type, const void* ival)
{
    const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return (joblist::FLOATNULL == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return (joblist::CHAR4NULL == *val);

        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::DATENULL == *val);

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return (joblist::UINTNULL == *val);

        default:
            break;
    }

    return (joblist::INTNULL == *val);
}

template<>
inline bool isNullVal<2>(uint8_t type, const void* ival)
{
    const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR2NULL == *val);

        case CalpontSystemCatalog::USMALLINT:
            return (joblist::USMALLINTNULL == *val);

        default:
            break;
    }

    return (joblist::SMALLINTNULL == *val);
}

template<>
inline bool isNullVal<1>(uint8_t type, const void* ival)
{
    const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (*val == joblist::CHAR1NULL);

        case CalpontSystemCatalog::UTINYINT:
            return (joblist::UTINYINTNULL == *val);

        default:
            break;
    }

    return (*val == joblist::TINYINTNULL);
}

/* A generic isNullVal */
inline bool isNullVal(uint32_t length, uint8_t type, const uint8_t* val8)
{
    switch (length)
    {
        case 8:
            return isNullVal<8>(type, val8);

        case 4:
            return isNullVal<4>(type, val8);

        case 2:
            return isNullVal<2>(type, val8);

        case 1:
            return isNullVal<1>(type, val8);
    };

    return false;
}

// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
inline bool isMinMaxValid(const NewColRequestHeader* in)
{
    if (in->NVALS != 0)
    {
        return false;
    }
    else
    {
        switch (in->DataType)
        {
            case CalpontSystemCatalog::CHAR:
                return (in->DataSize < 9);

            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
                return (in->DataSize < 8);

            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::BIGINT:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
                return true;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
                return (in->DataSize <= 8);

            default:
                return false;
        }
    }
}

//char(8) values lose their null terminator
template <int W>
inline string fixChar(int64_t intval)
{
    char chval[W + 1];
    memcpy(chval, &intval, W);
    chval[W] = '\0';

    return string(chval);
}

inline bool colCompare(int64_t val1, int64_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, const idb_regex_t& regex, bool isNull = false)
{
// 	cout << "comparing " << hex << val1 << " to " << val2 << endl;

    if (COMPARE_NIL == COP) return false;

    //@bug 425 added isNull condition
    else if ( !isNull && (type == CalpontSystemCatalog::FLOAT || type == CalpontSystemCatalog::DOUBLE))
    {
        double dVal1, dVal2;

        if (type == CalpontSystemCatalog::FLOAT)
        {
            dVal1 = *((float*) &val1);
            dVal2 = *((float*) &val2);
        }
        else
        {
            dVal1 = *((double*) &val1);
            dVal2 = *((double*) &val2);
        }

        return colCompare_(dVal1, dVal2, COP);
    }

    else if ( (type == CalpontSystemCatalog::CHAR || type == CalpontSystemCatalog::VARCHAR ||
               type == CalpontSystemCatalog::TEXT) && !isNull )
    {
        if (!regex.used && !rf)
        {
            // MCOL-1246 Trim trailing whitespace for matching, but not for
            // regex
            dataconvert::DataConvert::trimWhitespace(val1);
            dataconvert::DataConvert::trimWhitespace(val2);
            return colCompare_(order_swap(val1), order_swap(val2), COP);
        }
        else
            return colStrCompare_(order_swap(val1), order_swap(val2), COP, rf, &regex);
    }

    /* isNullVal should work on the normalized value on little endian machines */
    else
    {
        bool val2Null = isNullVal(width, type, (uint8_t*) &val2);

        if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
            return colCompare_(val1, val2, COP, rf);
        else
            return false;
    }
}

inline bool colCompareUnsigned(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, const idb_regex_t& regex, bool isNull = false)
{
// 	cout << "comparing unsigned" << hex << val1 << " to " << val2 << endl;

    if (COMPARE_NIL == COP) return false;

    /* isNullVal should work on the normalized value on little endian machines */
    bool val2Null = isNullVal(width, type, (uint8_t*) &val2);

    if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
        return colCompare_(val1, val2, COP, rf);
    else
        return false;
}


// Reads one ColValue from the input data.
// Returns true on success, false on EOF.
template<typename T, int W>
inline bool nextColValue(
    int64_t* result,            // Put here the value read
    int type,
    const uint16_t* ridArray,
    int ridSize,                // Number of values in ridArray
    int* index,
    bool* isNull,
    bool* isEmpty,
    uint16_t* rid,
    uint8_t OutputType,
    const T* srcArray,
    unsigned srcSize)
{
    auto i = *index;

    if (ridArray == NULL)
    {
        while (static_cast<unsigned>(i) < srcSize &&
                isEmptyVal<W>(type, &srcArray[i]) &&
                (OutputType & OT_RID))                      //// the only way to get Empty value
        {
            i++;
        }

        if (static_cast<unsigned>(i) >= srcSize)
            return false;

        *rid = i;
    }
    else
    {
        while (i < ridSize &&
                isEmptyVal<W>(type, &srcArray[ridArray[i]]))
        {
            i++;
        }

        if (i >= ridSize)
            return false;

        *rid = ridArray[i];
    }

    // at this point, nextRid is the index to return, and index is...
    //   if RIDs are not specified, nextRid + 1,
    //	 if RIDs are specified, it's the next index in the rid array.
    //Bug 838, tinyint null problem
#if 0
    if (type == CalpontSystemCatalog::FLOAT)
    {
        // convert the float to a 64-bit type, return that w/o conversion
        double dTmp = (double) * ((float*) &srcArray[*rid]);
        *result = *((int64_t*) &dTmp);
    }
    else
        *result = srcArray[*rid];
#endif

    *index = i+1;
    *result = srcArray[*rid];
    *isNull = isNullVal<W>(type, result);
    *isEmpty = isEmptyVal<W>(type, result);
    return true;
}


// Append value to the output buffer with debug-time check for buffer overflow
template<typename T>
inline void checkedWriteValue(
    void* out,
    unsigned outSize,
    unsigned* outPos,
    const T* src,
    int errSubtype)
{
#ifdef PRIM_DEBUG

    if (sizeof(T) > outSize - *outPos)
    {
        logIt(35, errSubtype);
        throw logic_error("PrimitiveProcessor::checkedWriteValue(): output buffer is too small");   //// is it OK to change errmsg?
    }

#endif

    uint8_t* out8 = reinterpret_cast<uint8_t*>(out);
    memcpy(out8 + *outPos, src, sizeof(T));
    *outPos += sizeof(T);
}

template<typename T>
inline void writeColValue(
    uint8_t OutputType,
    NewColResultHeader* out,
    unsigned outSize,
    unsigned* written,
    uint16_t rid,
    const T* srcArray)
{
    if (OutputType & OT_RID)
    {
        checkedWriteValue(out, outSize, written, &rid, 1);
        out->RidFlags |= (1 << (rid >> 10)); // set the (row/1024)'th bit
    }

    if (OutputType & OT_TOKEN || OutputType & OT_DATAVALUE)
    {
        checkedWriteValue(out, outSize, written, &srcArray[rid], 2);
    }

    out->NVALS++;   //// Can be computed at the end from *written value
}


// Compile column filter from BLOB into structure optimized for fast filtering.
// Returns the compiled filter.
template<typename T>                // C++ integer type corresponding to colType
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter_T
    (const uint8_t* filterString,   // Filter represented as BLOB
    uint32_t colType,               // Column datatype as ColDataType
    uint32_t filterCount,           // Number of filter elements contained in filterString
    uint32_t BOP)                   // Operation (and/or/xor/none) that combines all filter elements
{
    const uint32_t colWidth = sizeof(T);  // Sizeof of the column to be filtered

    boost::shared_ptr<ParsedColumnFilter> ret;  // Place for building the value to return
    if (filterCount == 0)
        return ret;

    // Allocate the compiled filter structure with space for filterCount filters.
    // No need to init arrays since they will be filled on the fly.
    ret.reset(new ParsedColumnFilter());
    ret->columnFilterMode = TWO_ARRAYS;
    ret->prestored_argVals.reset(new int64_t[filterCount]);
    ret->prestored_cops.reset(new uint8_t[filterCount]);
    ret->prestored_rfs.reset(new uint8_t[filterCount]);
    ret->prestored_regex.reset(new idb_regex_t[filterCount]);

    // Parse the filter predicates and insert them into argVals and cops
    for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
    {
        // Size of single filter element in filterString BLOB
        const uint32_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + colWidth;
        // Pointer to ColArgs structure representing argIndex'th element in the BLOB
        auto args = reinterpret_cast<const ColArgs*>(filterString + (argIndex * filterSize));
        ret->prestored_cops[argIndex] = args->COP;
        ret->prestored_rfs[argIndex] = args->rf;

#if 0
        if (colType == CalpontSystemCatalog::FLOAT)
        {
            double dTmp;

            dTmp = (double) * ((const float*) args->val);
            ret->prestored_argVals[argIndex] = *((int64_t*) &dTmp);
        }
        else
#else
        ret->prestored_argVals[argIndex] = *reinterpret_cast<const T*>(args->val);
#endif

//      cout << "inserted* " << hex << ret->prestored_argVals[argIndex] << dec <<
//        " COP = " << (int) ret->prestored_cops[argIndex] << endl;

        bool useRegex = ((COMPARE_LIKE & args->COP) != 0);
        ret->prestored_regex[argIndex].used = useRegex;

        if (useRegex)
        {
            p_DataValue dv = convertToPDataValue(&ret->prestored_argVals[argIndex], colWidth);
            if (PrimitiveProcessor::convertToRegexp(&ret->prestored_regex[argIndex], &dv))
                throw runtime_error("PrimitiveProcessor::parseColumnFilter(): Could not create regular expression for LIKE operator");
            ++ret->likeOps;     //// used nowhere
        }
    }


    /*  Decide which structure to use.  I think the only cases where we can use the set
        are when NOPS > 1, BOP is OR, and every COP is ==,
        and when NOPS > 1, BOP is AND, and every COP is !=.

        If there were no predicates that violate the condition for using a set,
        insert argVals into a set.
    */
    if (filterCount > 1)
    {
        for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
        {
            auto cop = ret->prestored_cops[argIndex];

            if ((BOP == BOP_OR && cop != COMPARE_EQ) ||
                (BOP == BOP_AND && cop != COMPARE_NE) ||
                (cop == COMPARE_NIL))
            {
                goto skipConversion;
            }
        }

        ret->columnFilterMode = UNORDERED_SET;
        ret->prestored_set.reset(new prestored_set_t());

        // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
        for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
            if (ret->prestored_rfs[argIndex] == 0)
                ret->prestored_set->insert(ret->prestored_argVals[argIndex]);

        ret->prestored_argVals.reset();
        ret->prestored_cops.reset();
        ret->prestored_rfs.reset();
        ret->prestored_regex.reset();

        skipConversion:;
    }

    return ret;
}


// Return true if curValue matches the filter represented by all those arrays
template<int W>
inline bool matchingColValue(
    int64_t curValue,
    bool isNull,
    CalpontSystemCatalog::ColDataType DataType,  // Column datatype
    uint32_t filterBOP,             // Operation (and/or/xor/none) that combines all filter elements
    prestored_set_t* filterSet,     // Set of values for simple filters (any of values / none of them)
    uint32_t filterCount,           // Number of filter elements, each described by one entry in the following arrays:
    uint8_t* filterCmpOps,          //   comparison operation
    int64_t* filterValues,          //   value to compare to
    uint8_t* filterRFs,             //   ?
    idb_regex_t* filterRegexes)     //   regex for string-LIKE operation
{
    if (filterSet)    // implies columnFilterMode == UNORDERED_SET
    {
        /* bug 1920: ignore NULLs in the set and in the column data */
        if (!(isNull && filterBOP == BOP_AND))
        {
            bool found = (filterSet->find(curValue) != filterSet->end());  //// Check on uint32/64 types!

            // Assume that we have either  BOP_OR && COMPARE_EQ  or  BOP_AND && COMPARE_NE
            if (filterBOP == BOP_OR?  found  :  !found)
                return true;
        }
        return false;
    }
    else
    {
        auto unsignedCurValue = static_cast<uint64_t>(curValue);
        auto unsignedFilterValues = reinterpret_cast<uint64_t*>(filterValues);

        for (int argIndex = 0; argIndex < filterCount; argIndex++)
        {
            bool cmp;
            if (isUnsigned(DataType))
            {
                cmp = colCompareUnsigned(unsignedCurValue, unsignedFilterValues[argIndex], filterCmpOps[argIndex],
                                         filterRFs[argIndex], DataType, W, filterRegexes[argIndex], isNull);
            }
            else
            {
                cmp = colCompare(curValue, filterValues[argIndex], filterCmpOps[argIndex],
                                 filterRFs[argIndex], DataType, W, filterRegexes[argIndex], isNull);
            }

            // Short-circuit the filter evaluation - true || ... == true, false && ... = false
            if (filterBOP == BOP_OR  &&  cmp == true)
                return true;
            else if (filterBOP == BOP_AND  &&  cmp == false)
                return false;
        }

        // BOP_AND can get here only if all filters returned true, BOP_OR - only if they all returned false
        return (filterBOP == BOP_AND  ||  filterCount == 0);
    }
}


// Copy data matching parsedColumnFilter from input to output.
// Input is srcArray[srcSize], optionally accessed in the order defined by ridArray[ridSize].
// Output is BLOB out[outSize], written starting at offset *written, which is updated afterward.
template<typename T, int W>
static void filterColumnData(
    NewColRequestHeader* in,
    NewColResultHeader* out,
    unsigned outSize,
    unsigned* written,
    uint16_t* ridArray,
    int ridSize,                // Number of values in ridArray
    int* srcArray16,
    unsigned srcSize,
    boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
    const T* srcArray = reinterpret_cast<const T*>(srcArray16);

    CalpontSystemCatalog::ColDataType DataType = (CalpontSystemCatalog::ColDataType) in->DataType;  // Column datatype
    uint32_t filterCount = in->NOPS;           // Number of elements in the filter
    uint32_t filterBOP = in->BOP;              // Operation (and/or/xor/none) that combines all filter elements
    bool isStringDataType = (W > 1) && (DataType == CalpontSystemCatalog::CHAR ||
                                        DataType == CalpontSystemCatalog::VARCHAR ||
                                        DataType == CalpontSystemCatalog::BLOB ||
                                        DataType == CalpontSystemCatalog::TEXT );

    // If no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL)
        parsedColumnFilter = parseColumnFilter_T<T>((uint8_t*)in + sizeof(NewColRequestHeader), in->DataType, in->NOPS, in->BOP);

    // For better speed, we cache parsedColumnFilter fields into local variables
    auto filterValues  = parsedColumnFilter->prestored_argVals.get();
    auto filterCmpOps  = parsedColumnFilter->prestored_cops.get();
    auto filterRFs     = parsedColumnFilter->prestored_rfs.get();
    auto filterSet     = parsedColumnFilter->prestored_set.get();
    auto filterRegexes = parsedColumnFilter->prestored_regex.get();

    // Set boolean indicating whether to capture the min and max values
    out->ValidMinMax = isMinMaxValid(in);

    if (out->ValidMinMax)
    {
        if (isUnsigned(DataType))
        {
            out->Min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
            out->Max = 0;
        }
        else
        {
            out->Min = numeric_limits<int64_t>::max();
            out->Max = numeric_limits<int64_t>::min();
        }
    }
    else
    {
        out->Min = 0;
        out->Max = 0;
    }

    // Loop-local variables
    int64_t curValue = 0;
    uint16_t rid = 0;
    int nextRidIndex = 0;
    bool isNull = false, isEmpty = false;
    idb_regex_t placeholderRegex;
    placeholderRegex.used = false;

    // Loop over the column values, storing those matching the filter, and updating the min..max range
    while (nextColValue<T,W>(&curValue, DataType, ridArray, ridSize, &nextRidIndex, &isNull, &isEmpty,
                             &rid, in->OutputType, srcArray, srcSize))
    {
        if (matchingColValue<W>(curValue, isNull, DataType, filterBOP, filterSet, filterCount,
                                filterCmpOps, filterValues, filterRFs, filterRegexes))
        {
            writeColValue<T>(in->OutputType, out, outSize, written, rid, srcArray);
        }

        // Update the min and max if necessary.  Ignore nulls.
        if (out->ValidMinMax && !isNull && !isEmpty)
        {
            if (isStringDataType)
            {
                if (colCompare(out->Min, curValue, COMPARE_GT, false, DataType, W, placeholderRegex))
                    out->Min = curValue;

                if (colCompare(out->Max, curValue, COMPARE_LT, false, DataType, W, placeholderRegex))
                    out->Max = curValue;
            }
            else if (isUnsigned(DataType))
            {
                if (static_cast<uint64_t>(out->Min) > static_cast<uint64_t>(curValue))
                    out->Min = curValue;

                if (static_cast<uint64_t>(out->Max) < static_cast<uint64_t>(curValue))
                    out->Max = curValue;
            }
            else
            {
                if (out->Min > curValue)
                    out->Min = curValue;

                if (out->Max < curValue)
                    out->Max = curValue;
            }
        }
    }
}

} //namespace anon

namespace primitives
{

void PrimitiveProcessor::p_Col(NewColRequestHeader* in, NewColResultHeader* out,
                               unsigned outSize, unsigned* written)
{
    void *outp = static_cast<void*>(out);
    memcpy(outp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
    out->NVALS = 0;
    out->LBID = in->LBID;
    out->ism.Command = COL_RESULTS;
    out->OutputType = in->OutputType;
    out->RidFlags = 0;
    *written = sizeof(NewColResultHeader);
    unsigned itemsPerBlock = logicalBlockMode ? BLOCK_SIZE
                                              : BLOCK_SIZE / in->DataSize;

    //...Initialize I/O counts;
    out->CacheIO    = 0;
    out->PhysicalIO = 0;

#if 0

    // short-circuit the actual block scan for testing
    if (out->LBID >= 802816)
    {
        out->ValidMinMax = false;
        out->Min = 0;
        out->Max = 0;
        return;
    }

#endif

    auto markEvent = [&] (char eventChar)
    {
        if (fStatsPtr)
#ifdef _MSC_VER
            fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, eventChar);
#else
            fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, eventChar);
#endif
    };

    markEvent('B');

    // Prepare index array
    uint16_t* ridArray = 0;
    int ridSize = in->NVALS;                // Number of values in ridArray
    if (ridSize > 0)
    {
        const uint32_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + in->DataSize;
        ridArray = reinterpret_cast<uint16_t*>((uint8_t*)in + sizeof(NewColRequestHeader) + (in->NOPS * filterSize));

        if (1 == in->sort )
        {
            qsort(ridArray, ridSize, sizeof(uint16_t), compareBlock<uint16_t>);
            markEvent('O');
        }
    }

    if (isUnsigned((CalpontSystemCatalog::ColDataType)in->DataType))
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< uint8_t,1>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<uint16_t,2>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<uint32_t,4>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<uint64_t,8>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }
    else
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< int8_t,1>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<int16_t,2>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<int32_t,4>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<int64_t,8>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }

    markEvent('K');  //// does it make sense?
    markEvent('C');
}


// Compile column filter from BLOB into structure optimized for fast filtering.
// Returns the compiled filter.
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter
    (const uint8_t* filterString,   // Filter represented as BLOB
    uint32_t colWidth,              // Sizeof of the column to be filtered
    uint32_t colType,               // Column datatype as ColDataType
    uint32_t filterCount,           // Number of filter elements contained in filterString
    uint32_t BOP)                   // Operation (and/or/xor/none) that combines all filter elements
{
    // Dispatch by the column type to make it faster
    if (isUnsigned((CalpontSystemCatalog::ColDataType)colType))
    {
        switch (colWidth)
        {
            case 1:  return parseColumnFilter_T< uint8_t>(filterString, colType, filterCount, BOP);
            case 2:  return parseColumnFilter_T<uint16_t>(filterString, colType, filterCount, BOP);
            case 4:  return parseColumnFilter_T<uint32_t>(filterString, colType, filterCount, BOP);
            case 8:  return parseColumnFilter_T<uint64_t>(filterString, colType, filterCount, BOP);
        }
    }
    else
    {
        switch (colWidth)
        {
            case 1:  return parseColumnFilter_T< int8_t>(filterString, colType, filterCount, BOP);
            case 2:  return parseColumnFilter_T<int16_t>(filterString, colType, filterCount, BOP);
            case 4:  return parseColumnFilter_T<int32_t>(filterString, colType, filterCount, BOP);
            case 8:  return parseColumnFilter_T<int64_t>(filterString, colType, filterCount, BOP);
        }
    }

    return NULL;   ///// throw error ???
}

} // namespace primitives
// vim:ts=4 sw=4:

