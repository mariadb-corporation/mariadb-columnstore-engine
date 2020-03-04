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
// Column filtering is dispatched 4-way based on the column type,
// which defines implementation of comparison operations for the column values
enum ENUM_KIND {KIND_DEFAULT,   // compared as signed integers
                KIND_UNSIGNED,  // compared as unsigned integers
                KIND_FLOAT,     // compared as floating-point numbers
                KIND_TEXT};     // whitespace-trimmed and then compared as signed integers


/*****************************************************************************
 *** AUXILIARY FUNCTIONS *****************************************************
 *****************************************************************************/

// File-local event logging helper
void logIt(int mid, int arg1, const char* arg2 = NULL)
{
    MessageLog logger(LoggingID(28));
    logging::Message::Args args;
    Message msg(mid);

    args.add(arg1);

    if (arg2 && *arg2)
        args.add(arg2);

    msg.format(args);
    logger.logErrorMessage(msg);
}

// Reverse the byte order
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

// char(8) values lose their null terminator
template <int W>
inline string fixChar(int64_t intval)
{
    char chval[W + 1];
    memcpy(chval, &intval, W);
    chval[W] = '\0';

    return string(chval);
}

//FIXME: what are we trying to accomplish here? It looks like we just want to count
// the chars in a string arg?
inline p_DataValue convertToPDataValue(const void* val, int W)
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


/*****************************************************************************
 *** NULL/EMPTY VALUES FOR EVERY COLUMN TYPE/WIDTH ***************************
 *****************************************************************************/

// Bit pattern representing EMPTY value for given column type/width
template<int W>
uint64_t getEmptyValue(uint8_t type);

template<>
uint64_t getEmptyValue<8>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return joblist::DOUBLEEMPTYROW;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR8EMPTYROW;

        case CalpontSystemCatalog::UBIGINT:
            return joblist::UBIGINTEMPTYROW;

        default:
            return joblist::BIGINTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<4>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return joblist::FLOATEMPTYROW;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR4EMPTYROW;

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return joblist::UINTEMPTYROW;

        default:
            return joblist::INTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<2>(uint8_t type)
{
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
            return joblist::CHAR2EMPTYROW;

        case CalpontSystemCatalog::USMALLINT:
            return joblist::USMALLINTEMPTYROW;

        default:
            return joblist::SMALLINTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<1>(uint8_t type)
{
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
            return joblist::CHAR1EMPTYROW;

        case CalpontSystemCatalog::UTINYINT:
            return joblist::UTINYINTEMPTYROW;

        default:
            return joblist::TINYINTEMPTYROW;
    }
}


// Bit pattern representing NULL value for given column type/width
template<int W>
uint64_t getNullValue(uint8_t type);

template<>
uint64_t getNullValue<8>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return joblist::DOUBLENULL;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR8NULL;

        case CalpontSystemCatalog::UBIGINT:
            return joblist::UBIGINTNULL;

        default:
            return joblist::BIGINTNULL;
    }
}

template<>
uint64_t getNullValue<4>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return joblist::FLOATNULL;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR4NULL;

        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::DATENULL;

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return joblist::UINTNULL;

        default:
            return joblist::INTNULL;
    }
}

template<>
uint64_t getNullValue<2>(uint8_t type)
{
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
            return joblist::CHAR2NULL;

        case CalpontSystemCatalog::USMALLINT:
            return joblist::USMALLINTNULL;

        default:
            return joblist::SMALLINTNULL;
    }
}

template<>
uint64_t getNullValue<1>(uint8_t type)
{
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
            return joblist::CHAR1NULL;

        case CalpontSystemCatalog::UTINYINT:
            return joblist::UTINYINTNULL;

        default:
            return joblist::TINYINTNULL;
    }
}

// Check whether val is NULL (or alternative NULL bit pattern for 64-bit string types)
template<ENUM_KIND KIND, typename T>
inline bool isNullValue(int64_t val, T NULL_VALUE)
{
    //@bug 339 might be a token here
    //TODO: what's up with the alternative NULL here?
    uint64_t ALT_NULL_VALUE = 0xFFFFFFFFFFFFFFFELL;

    constexpr int W = sizeof(T);
    return (static_cast<T>(val) == NULL_VALUE) ||
           ((KIND_TEXT == KIND)  &&  (W == 8)  &&  (val == ALT_NULL_VALUE));
}


/*****************************************************************************
 *** COMPARISON OPERATIONS FOR COLUMN VALUES *********************************
 *****************************************************************************/

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
            logIt(34, COP, "colCompare_");
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
            logIt(34, COP, "colStrCompare_");
            return false;						//TODO:  throw an exception here?
    }
}


// Compare two column values using given comparison operation,
// taking into account all rules about NULL values, string trimming and so on
template<ENUM_KIND KIND, int W, bool isNull = false>
inline bool colCompare(
    int64_t val1,
    int64_t val2,
    uint8_t COP,
    uint8_t rf,
    const idb_regex_t& regex,
    bool isVal2Null = false)
{
// 	cout << "comparing " << hex << val1 << " to " << val2 << endl;

    if (COMPARE_NIL == COP) return false;

    //@bug 425 added isNull condition
    else if (KIND_FLOAT == KIND  &&  !isNull)
    {
        double dVal1, dVal2;

        if (W == 4)     //// instead, we can just compare the floats directly
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

    else if (KIND_TEXT == KIND  &&  !isNull)
    {
        if (!regex.used && !rf)
        {
            // MCOL-1246 Trim trailing whitespace for matching, but not for regex
            dataconvert::DataConvert::trimWhitespace(val1);
            dataconvert::DataConvert::trimWhitespace(val2);
            return colCompare_(order_swap(val1), order_swap(val2), COP);
        }
        else
            return colStrCompare_(order_swap(val1), order_swap(val2), COP, rf, &regex);
    }

    //// outdated comment?
    // isNullVal should work on the normalized value on little endian machines
    else
    {
        if (isNull == isVal2Null || (isVal2Null && COP == COMPARE_NE))
        {
            if (KIND_UNSIGNED == KIND)
            {
                uint64_t uval1 = val1, uval2 = val2;
                return colCompare_(uval1, uval2, COP, rf);
            }
            else
                return colCompare_(val1, val2, COP, rf);
        }
        else
            return false;
    }
}


/*****************************************************************************
 *** FILTER/READ/WRITE A COLUMN VALUE ****************************************
 *****************************************************************************/

// Return true if curValue matches the filter represented by all those arrays
template<ENUM_KIND KIND, int W, bool isNull = false, typename T>
inline bool matchingColValue(
    // Value description
    int64_t curValue,               // The value (isNull - is the value null?)
    // Filter description
    uint32_t filterBOP,             // Operation (and/or/xor/none) that combines all filter elements
    prestored_set_t* filterSet,     // Set of values for simple filters (any of values / none of them)
    uint32_t filterCount,           // Number of filter elements, each described by one entry in the following arrays:
    uint8_t* filterCOPs,            //   comparison operation
    int64_t* filterValues,          //   value to compare to
    uint8_t* filterRFs,
    idb_regex_t* filterRegexes,     //   regex for string-LIKE comparison operation
    T NULL_VALUE)                   // Bit pattern representing NULL value for this column type/width
{
    if (filterSet)    // implies columnFilterMode == UNORDERED_SET
    {
        /* bug 1920: ignore NULLs in the set and in the column data */
        if (!(isNull && filterBOP == BOP_AND))
        {
            bool found = (filterSet->find(curValue) != filterSet->end());

            // Assume that we have either  BOP_OR && COMPARE_EQ  or  BOP_AND && COMPARE_NE
            if (filterBOP == BOP_OR?  found  :  !found)
                return true;
        }
        return false;
    }
    else if (filterBOP == BOP_AND)
    {
        for (int argIndex = 0; argIndex < filterCount; argIndex++)
        {
            auto filterValue = filterValues[argIndex];
            bool cmp = colCompare<KIND, W, isNull>(curValue, filterValue, filterCOPs[argIndex],
                                           filterRFs[argIndex], filterRegexes[argIndex],
                                           isNullValue<KIND,T>(filterValue, NULL_VALUE));

            // Short-circuit the filter evaluation - false && ... = false
            if (cmp == false)
                return false;
        }

        // We can get here only if all filters returned true
        return true;
    }
    else  // Otherwise we assume either BOP_OR or filterCount<=1
    {
        for (int argIndex = 0; argIndex < filterCount; argIndex++)
        {
            auto filterValue = filterValues[argIndex];
            bool cmp = colCompare<KIND, W, isNull>(curValue, filterValue, filterCOPs[argIndex],
                                           filterRFs[argIndex], filterRegexes[argIndex],
                                           isNullValue<KIND,T>(filterValue, NULL_VALUE));

            // Short-circuit the filter evaluation - true || ... == true
            if (cmp == true)
                return true;
        }

        // We can get here only if all filters returned false
        return (filterCount == 0);
    }
}


// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
bool isMinMaxValid(const NewColRequestHeader* in)
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


// Read one ColValue from the input data.
// Return true on success, false on EOF.
// Values are read from srcArray either in natural order or in the order defined by ridArray.
// Empty values are skipped, unless ridArray==0 && !(OutputType & OT_RID).
template<typename T, int W>
inline bool nextColValue(
    int64_t* result,            // Place for the value returned
    bool* isEmpty,              // ... and flag whether it's EMPTY
    int* index,                 // Successive index either in srcArray (going from 0 to srcSize-1) or ridArray (0..ridSize-1)
    uint16_t* rid,              // Index in srcArray of the value returned
    const T* srcArray,          // Input array
    const unsigned srcSize,     // ... and its size
    const uint16_t* ridArray,   // Optional array of indexes into srcArray, that defines the read order
    const int ridSize,          // ... and its size
    const uint8_t OutputType,   // Used to decide whether to skip EMPTY values
    T EMPTY_VALUE)
{
    auto i = *index;    // local copy of *index to speed up loops
    T value;            // value to be written into *result, local for the same reason

    if (ridArray)
    {
        // Read next non-empty value in the order defined by ridArray
        for( ; ; i++)
        {
            if (i >= ridSize)
                return false;

            value = srcArray[ridArray[i]];

            if (value != EMPTY_VALUE)
                break;
        }

        *rid = ridArray[i];
        *isEmpty = false;
    }
    else if (OutputType & OT_RID)
    {
        // Read next non-empty value in the natural order
        for( ; ; i++)
        {
            if (i >= srcSize)
                return false;

            value = srcArray[i];

            if (value != EMPTY_VALUE)
                break;
        }

        *rid = i;
        *isEmpty = false;
    }
    else
    {
        // Read next value in the natural order
        if (i >= srcSize)
            return false;

        *rid = i;
        value = srcArray[i];
        *isEmpty = (value == EMPTY_VALUE);
    }

    //// outdated comment
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
    *result = value;
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
        throw logic_error("PrimitiveProcessor::checkedWriteValue(): output buffer is too small");
    }

#endif

    uint8_t* out8 = reinterpret_cast<uint8_t*>(out);
    memcpy(out8 + *outPos, src, sizeof(T));
    *outPos += sizeof(T);
}


// Write the value index in srcArray and/or the value itself, depending on bits in OutputType,
// into the output buffer and update the output pointer.
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

    if (OutputType & (OT_TOKEN | OT_DATAVALUE))
    {
        checkedWriteValue(out, outSize, written, &srcArray[rid], 2);
    }

    out->NVALS++;   //TODO: Can be computed at the end from *written value
}


/*****************************************************************************
 *** COMPILE A COLUMN FILTER / RUN DATA THROUGH IT ***************************
 *****************************************************************************/

// Compile column filter from BLOB into structure optimized for fast filtering.
// Return the compiled filter.
template<typename T>                // C++ integer type corresponding to colType
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter_T(
    const uint8_t* filterString,    // Filter represented as BLOB
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
                throw runtime_error("PrimitiveProcessor::parseColumnFilter_T(): Could not create regular expression for LIKE operator");
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
        // Check that all COPs are of right kind that depends on BOP
        for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
        {
            auto cop = ret->prestored_cops[argIndex];

            if (! (BOP == BOP_OR  && cop == COMPARE_EQ) ||
                  (BOP == BOP_AND && cop == COMPARE_NE))
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


// Copy data matching parsedColumnFilter from input to output.
// Input is srcArray[srcSize], optionally accessed in the order defined by ridArray[ridSize].
// Output is BLOB out[outSize], written starting at offset *written, which is updated afterward.
template<typename T, ENUM_KIND KIND>
void filterColumnData(
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
    constexpr int W = sizeof(T);
    const T* srcArray = reinterpret_cast<const T*>(srcArray16);

    // Cache some structure fields in local vars
    auto DataType = (CalpontSystemCatalog::ColDataType) in->DataType;  // Column datatype
    uint32_t filterCount = in->NOPS;        // Number of elements in the filter
    uint32_t filterBOP   = in->BOP;         // Operation (and/or/xor/none) that combines all filter elements
    uint8_t  OutputType  = in->OutputType;

    // If no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL  &&  filterCount > 0)
        parsedColumnFilter = parseColumnFilter_T<T>((uint8_t*)in + sizeof(NewColRequestHeader), in->DataType, in->NOPS, in->BOP);

    // Cache parsedColumnFilter fields in local vars
    auto filterValues  = (filterCount==0? NULL : parsedColumnFilter->prestored_argVals.get());
    auto filterCOPs    = (filterCount==0? NULL : parsedColumnFilter->prestored_cops.get());
    auto filterRFs     = (filterCount==0? NULL : parsedColumnFilter->prestored_rfs.get());
    auto filterSet     = (filterCount==0? NULL : parsedColumnFilter->prestored_set.get());
    auto filterRegexes = (filterCount==0? NULL : parsedColumnFilter->prestored_regex.get());

    // Bit patterns in srcArray[i] representing an empty row and the NULL value
    T EMPTY_VALUE = static_cast<T>(getEmptyValue<W>(DataType));
    T NULL_VALUE  = static_cast<T>(getNullValue <W>(DataType));

    // Precompute filter results for EMPTY and NULL values
    bool isEmptyValueMatches = matchingColValue<KIND, W, false>(EMPTY_VALUE, filterBOP, filterSet, filterCount,
                                    filterCOPs, filterValues, filterRFs, filterRegexes, NULL_VALUE);

    bool isNullValueMatches = matchingColValue<KIND, W, true>(NULL_VALUE, filterBOP, filterSet, filterCount,
                                    filterCOPs, filterValues, filterRFs, filterRegexes, NULL_VALUE);

    // Boolean indicating whether to capture the min and max values
    bool ValidMinMax = isMinMaxValid(in);
    // Real type of values captured in Min/Max
    using VALTYPE = typename std::conditional<KIND_UNSIGNED == KIND, uint64_t, int64_t>::type;
    // Local vars to capture the min and max values
    auto Min = static_cast<int64_t>(numeric_limits<VALTYPE>::max());
    auto Max = static_cast<int64_t>(numeric_limits<VALTYPE>::min());

    // Loop-local variables
    int64_t curValue = 0;
    uint16_t rid = 0;
    bool isEmpty = false;
    idb_regex_t placeholderRegex;
    placeholderRegex.used = false;

    // Loop over the column values, storing those matching the filter, and updating the min..max range
    for (int i = 0;
         nextColValue<T,W>(&curValue, &isEmpty,
                           &i, &rid,
                           srcArray, srcSize, ridArray, ridSize,
                           OutputType, EMPTY_VALUE); )
    {
        if (isEmpty)
        {
            // If EMPTY values match the filter, write curValue to the output buffer
            if (isEmptyValueMatches)
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
        }
        else if (isNullValue<KIND,T>(curValue, NULL_VALUE))
        {
            // If NULL values match the filter, write curValue to the output buffer
            if (isNullValueMatches)
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
        }
        else
        {
            // If curValue matches the filter, write it to the output buffer
            if (matchingColValue<KIND, W, false>(curValue, filterBOP, filterSet, filterCount,
                                filterCOPs, filterValues, filterRFs, filterRegexes, NULL_VALUE))
            {
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
            }

            // Update Min and Max if necessary.  EMPTY/NULL values can't appear here.
            if (ValidMinMax)
            {
                if ((KIND_TEXT == KIND) && (W > 1))     //// but for filtering, we go through trimWhitespace() even with W==1
                {
                    if (colCompare<KIND, W>(Min, curValue, COMPARE_GT, false, placeholderRegex))
                        Min = curValue;

                    if (colCompare<KIND, W>(Max, curValue, COMPARE_LT, false, placeholderRegex))
                        Max = curValue;
                }
                else    //// Without (W>1) above, we can use colCompare<KIND,W> for all types
                {
                    if (static_cast<VALTYPE>(Min) > static_cast<VALTYPE>(curValue))
                        Min = curValue;

                    if (static_cast<VALTYPE>(Max) < static_cast<VALTYPE>(curValue))
                        Max = curValue;
                }
            }
        }
    }


    // Store Min/Max values captured to *out
    out->ValidMinMax = ValidMinMax;
    if (ValidMinMax)
    {
        out->Min = Min;
        out->Max = Max;
    }
    else
    {
        out->Min = 0;   //// can we just save arbitrary data here when ValidMinMax == false?
        out->Max = 0;
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

    // Prepare ridArray (the row index array)
    uint16_t* ridArray = 0;
    int ridSize = in->NVALS;                // Number of values in ridArray
    if (ridSize > 0)
    {
        int filterSize = sizeof(uint8_t) + sizeof(uint8_t) + in->DataSize;
        ridArray = reinterpret_cast<uint16_t*>((uint8_t*)in + sizeof(NewColRequestHeader) + (in->NOPS * filterSize));

        if (1 == in->sort )
        {
            std::sort(ridArray, ridArray + ridSize);
            markEvent('O');
        }
    }

    auto DataType = (CalpontSystemCatalog::ColDataType) in->DataType;

    // Dispatch filtering by the column datatype/width in order to make it faster
    if (DataType == CalpontSystemCatalog::FLOAT)
    {
        idbassert(in->DataSize == 4);
        filterColumnData<int32_t, KIND_FLOAT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    }
    else if (DataType == CalpontSystemCatalog::DOUBLE)
    {
        idbassert(in->DataSize == 8);
        filterColumnData<int64_t, KIND_FLOAT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    }
    else if (DataType == CalpontSystemCatalog::CHAR ||
             DataType == CalpontSystemCatalog::VARCHAR ||
             DataType == CalpontSystemCatalog::TEXT)
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< int8_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<int16_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<int32_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<int64_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }
    else if (isUnsigned(DataType))
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< uint8_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<uint16_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<uint32_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<uint64_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }
    else
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< int8_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<int16_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<int32_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<int64_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }

    markEvent('C');
}


// Compile column filter from BLOB into structure optimized for fast filtering.
// Returns the compiled filter.
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter(
    const uint8_t* filterString,    // Filter represented as BLOB
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

    logIt(36, colType*100 + colWidth, "parseColumnFilter");
    return NULL;   //FIXME: support for wider columns
}

} // namespace primitives
// vim:ts=4 sw=4:

