#include <my_config.h>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string.h>
#include <tr1/unordered_map>

#include "idb_mysql.h"

namespace
{
inline bool isNumeric(int type, const char* attr)
{
    if (type == INT_RESULT || type == REAL_RESULT || type == DECIMAL_RESULT)
    {
        return true;
    }
#if _MSC_VER
    if (_strnicmp("NULL", attr, 4) == 0))
#else
    if (strncasecmp("NULL", attr, 4) == 0)
#endif
    {
        return true;
    }
    return false;
}

inline long double cvtArgToLDouble(int t, const char* v)
{
    long double d = 0.0;

    switch (t)
    {
        case INT_RESULT:
            d = (long double)(*((long long*)v));
            break;

        case REAL_RESULT:
            d = *((long double*)v);
            break;

        case DECIMAL_RESULT:
        case STRING_RESULT:
            d = strtold(v, 0);
            break;

        case ROW_RESULT:
            break;
    }

    return d;
}
}

/****************************************************************************
 * UDF function moda
 * MariaDB's UDF function creation guideline needs to be followed.
 * 
 * moda returns the value with the greatest number of occurances in
 * the dataset with ties being broken by:
 * 1) closest to AVG
 * 2) smallest value
 */
extern "C"
{

struct moda_data
{
    int64_t   cnt;
    long double sum;
    std::tr1::unordered_map<long double, uint32_t> modeMap;
};
 
#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool moda_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
    struct moda_data* data;
    if (args->arg_count != 1)
    {
        strcpy(message,"moda() requires one argument");
        return 1;
    }
    if (!isNumeric(args->arg_type[0], args->attributes[0]))
    {
        strcpy(message,"moda() with a non-numeric argument");
        return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;
    
//    if (!(data = (struct moda_data*) malloc(sizeof(struct moda_data))))
    data = new moda_data;

    data->cnt = 0;
    data->sum = 0.0;

    initid->ptr = (char*)data;
    return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_deinit(UDF_INIT* initid)
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->modeMap.clear();
    delete data;
}   

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                char* message __attribute__((unused)))
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->cnt = 0;
    data->sum = 0.0;
    data->modeMap.clear();
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void
moda_add(UDF_INIT* initid, UDF_ARGS* args,
            char* is_null,
            char* message __attribute__((unused)))
{
    // Test for NULL
    if (args->args[0] == 0)
    {
        return;
    }
    struct moda_data* data = (struct moda_data*)initid->ptr;
    long double val = cvtArgToLDouble(args->arg_type[0], args->args[0]);
    data->sum += val;
    ++data->cnt;
    data->modeMap[val]++;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void
moda_remove(UDF_INIT* initid, UDF_ARGS* args,
            char* is_null,
            char* message __attribute__((unused)))
{
    // Test for NULL
    if (args->args[0] == 0)
    {
        return;
    }
    struct moda_data* data = (struct moda_data*)initid->ptr;
    long double val = cvtArgToLDouble(args->arg_type[0], args->args[0]);
    data->sum -= val;
    --data->cnt;
    data->modeMap[val]--;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
char* moda(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
                  char* is_null, char* error __attribute__((unused)))
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    long double avg = data->cnt ? data->sum / data->cnt : 0;
    long double maxCnt = 0.0;
    long double val = 0.0;

    typename std::tr1::unordered_map<long double, uint32_t>::iterator iter;

    for (iter = data->modeMap.begin(); iter != data->modeMap.end(); ++iter)
    {
        if (iter->second > maxCnt)
        {
            val = iter->first;
            maxCnt = iter->second;
        }
        else if (iter->second == maxCnt)
        {
            // Tie breaker: choose the closest to avg. If still tie, choose smallest
            if ((abs(val-avg) > abs(iter->first-avg))
            || ((abs(val-avg) == abs(iter->first-avg)) && (abs(val) > abs(iter->first))))
            {
                val = iter->first;
            }
        }
    }
    std::ostringstream oss;
    oss << val;
    return const_cast<char*>(oss.str().c_str());
}

} // Extern "C"
