#include <my_config.h>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string.h>
#include <tr1/unordered_map>
#include <algorithm>

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

struct moda_data
{
    long double fSum;
    uint64_t    fCount;
    enum Item_result    fReturnType; 
    std::tr1::unordered_map<int64_t, uint32_t> mapINT;
    std::tr1::unordered_map<double, uint32_t> mapREAL;
    std::tr1::unordered_map<long double, uint32_t> mapDECIMAL;
    void clear()
    {
        fSum = 0.0;
        fCount = 0;
        mapINT.clear();
        mapREAL.clear();
        mapDECIMAL.clear();
    }
};

}

extern "C"
{

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

    data = new moda_data;
    data->fReturnType = args->arg_type[0];
    data->fCount = 0;
    data->fSum = 0.0;
    initid->ptr = (char*)data;
    return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_deinit(UDF_INIT* initid)
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->clear();
    delete data;
}   

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                char* message __attribute__((unused)))
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->clear();
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_add(UDF_INIT* initid, 
              UDF_ARGS* args,
              char* is_null,
              char* message __attribute__((unused)))
{
    // Test for NULL
    if (args->args[0] == 0)
    {
        return;
    }

    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->fCount++;

    switch (args->arg_type[0])
    {
        case INT_RESULT:
        {
            int64_t val = *((int64_t*)args->args[0]);
            data->fSum += (long double)val;
            data->mapINT[val]++;
            break;
        }
        case REAL_RESULT:
        {
            double val = *((double*)args->args[0]);
            data->fSum += val;
            data->mapREAL[val]++;
            break;
        }
        case DECIMAL_RESULT:
        case STRING_RESULT:
        {
            long double val = strtold(args->args[0], 0);
            data->fSum += val;
            data->mapDECIMAL[val]++;
            break;
        }
        default:
            break;
    }
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_remove(UDF_INIT* initid, UDF_ARGS* args,
                 char* is_null,
                 char* message __attribute__((unused)))
{
    // Test for NULL
    if (args->args[0] == 0)
    {
        return;
    }
    struct moda_data* data = (struct moda_data*)initid->ptr;
    data->fCount--;

    switch (args->arg_type[0])
    {
        case INT_RESULT:
        {
            int64_t val = *((int64_t*)args->args[0]);
            data->fSum -= (long double)val;
            data->mapINT[val]--;
            break;
        }
        case REAL_RESULT:
        {
            double val = *((double*)args->args[0]);
            data->fSum -= val;
            data->mapREAL[val]--;
            break;
        }
        case DECIMAL_RESULT:
        case STRING_RESULT:
        {
            long double val = strtold(args->args[0], 0);
            data->fSum -= val;
            data->mapDECIMAL[val]--;
            break;
        }
        default:
            break;
    }
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
char* moda(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
           char* is_null, char* error __attribute__((unused)))
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    uint32_t maxCnt = 0.0;

    switch (args->arg_type[0])
    {
        case INT_RESULT:
        {
            typename std::tr1::unordered_map<int64_t, uint32_t>::iterator iter;
            int64_t avg = (int64_t)data->fCount ? data->fSum / data->fCount : 0;
            int64_t val = 0.0;
            for (iter = data->mapINT.begin(); iter != data->mapINT.end(); ++iter)
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
            break;
        }
        case REAL_RESULT:
        {
            typename std::tr1::unordered_map<double, uint32_t>::iterator iter;
            double avg = data->fCount ? data->fSum / data->fCount : 0;
            double val = 0.0;
            for (iter = data->mapREAL.begin(); iter != data->mapREAL.end(); ++iter)
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
            break;
        }
        case DECIMAL_RESULT:
        case STRING_RESULT:
        {
            typename std::tr1::unordered_map<long double, uint32_t>::iterator iter;
            long double avg = data->fCount ? data->fSum / data->fCount : 0;
            long double val = 0.0;
            for (iter = data->mapDECIMAL.begin(); iter != data->mapDECIMAL.end(); ++iter)
            {
                if (iter->second > maxCnt)
                {
                    val = iter->first;
                    maxCnt = iter->second;
                }
                else if (iter->second == maxCnt)
                {
                    long double thisVal = iter->first;
                    // Tie breaker: choose the closest to avg. If still tie, choose smallest
                    if ((abs(val-avg) > abs(thisVal-avg))
                    || ((abs(val-avg) == abs(thisVal-avg)) && (abs(val) > abs(thisVal))))
                    {
                        val = thisVal;
                    }
                }
            }
            std::ostringstream oss;
            oss << val;
            return const_cast<char*>(oss.str().c_str());
            break;
        }
        default:
            break;
    }
    return NULL;
}

} // Extern "C"
