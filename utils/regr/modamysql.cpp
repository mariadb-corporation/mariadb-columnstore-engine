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

template<class T>
inline T cvtArg(int t, char* v, uint32_t len)
{
    T d = 0.0;
    
    if (t == DECIMAL_RESULT || t == STRING_RESULT)
    {
        // NULL terminate the string
        // This can be a problem if Server has adjoining non zero terminated buffers
        *(v+len) = 0;
        d = strtold(v, 0);
    }
    else
        d = *((T*)v);

    return d;
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

struct moda_data
{
    long double fSum;
    uint64_t    fCount;
    void*       fMap; // Will be of type unordered_map<>
    enum Item_result    fReturnType; 
//    Moda_impl_base* modaImpl; // A pointer to one of the Moda_impl_T concrete classes
    
    template<class T>
    std::tr1::unordered_map<T, uint32_t>* getMap() 
    {
        if (!fMap)
        {
            // Just in time creation
            fMap = new std::tr1::unordered_map<T, uint32_t>;
        }
        return (std::tr1::unordered_map<T, uint32_t>*)fMap;
    }
    
    template<class T>
    void clear()
    {
        fSum = 0.0;
        fCount = 0;
        getMap<T>()->clear();
    }
};


class Moda_impl_base
{
public:
    // Defaults OK
    Moda_impl_base() {};
    virtual ~Moda_impl_base() {};

    virtual my_bool moda_init(UDF_INIT* initid, 
                              UDF_ARGS* args, 
                              char* message);
    virtual void moda_deinit(UDF_INIT* initid);
    virtual void moda_clear(UDF_INIT* initid);
    virtual void moda_add(UDF_INIT* initid, 
                          UDF_ARGS* args,
                          char* is_null,
                          char* message __attribute__((unused)));
    virtual void moda_remove(UDF_INIT* initid, 
                             UDF_ARGS* args,
                             char* is_null,
                             char* message __attribute__((unused)));
    virtual char* moda(UDF_INIT* initid, 
                       UDF_ARGS* args __attribute__((unused)),
                       char* is_null, 
                       char* error __attribute__((unused)));
};

template<class T>
class Moda_impl : public Moda_impl_base
{
public:
    // Defaults OK
    Moda_impl() {};
    virtual ~Moda_impl() {};

    my_bool moda_init(UDF_INIT* initid, 
                      UDF_ARGS* args, 
                      char* message)
    {
        return (my_bool)TRUE;
    }

    void moda_deinit(UDF_INIT* initid)
    {
        struct moda_data* data = (struct moda_data*)initid->ptr;
        std::tr1::unordered_map<T, uint32_t>* map = data->getMap<T>();
        data->clear<T>();
        delete map;
        delete data;
    }   
    
    void moda_clear(UDF_INIT* initid)
    {
        struct moda_data* data = (struct moda_data*)initid->ptr;
        data->clear<T>();
    }

    void moda_add(UDF_INIT* initid, 
                  UDF_ARGS* args,
                  char* is_null,
                  char* message __attribute__((unused)))
    {
        struct moda_data* data = (struct moda_data*)initid->ptr;
        std::tr1::unordered_map<T, uint32_t>* map = data->getMap<T>();
        T val = cvtArg<T>(args->arg_type[0], args->args[0], args->lengths[0]);
        data->fSum += val;
        ++data->fCount;
        (*map)[val]++;
    }

    void moda_remove(UDF_INIT* initid, 
                     UDF_ARGS* args,
                     char* is_null,
                     char* message __attribute__((unused)))
    {
        struct moda_data* data = (struct moda_data*)initid->ptr;
        std::tr1::unordered_map<T, uint32_t>* map = data->getMap<T>();
        long double val = cvtArg<T>(args->arg_type[0], args->args[0], args->lengths[0]);
        data->fSum -= val;
        --data->fCount;
        (*map)[val]--;
    }

    char* moda(UDF_INIT* initid, 
               UDF_ARGS* args __attribute__((unused)),
               char* is_null, char* 
               error __attribute__((unused)))
    {
        struct moda_data* data = (struct moda_data*)initid->ptr;
        std::tr1::unordered_map<T, uint32_t>* map = data->getMap<T>();
        long double avg = data->fCount ? data->fSum / data->fCount : 0;
        long double maxCnt = 0.0;
        T val = 0.0;

        typename std::tr1::unordered_map<T, uint32_t>::iterator iter;

        for (iter = map->begin(); iter != map->end(); ++iter)
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
};

Moda_impl<int64_t>     moda_impl_int64;
Moda_impl<double>      moda_impl_double;
Moda_impl<long double> moda_impl_longdouble;

Moda_impl_base* getImpl(UDF_INIT* initid)
{
    struct moda_data* data = (struct moda_data*)initid->ptr;
    if (!data)
        return NULL;
    if (data->modaImpl)
        return data->modaImpl;
    
    switch (data->fReturnType)
    {
        case INT_RESULT:
            data->modaImpl = &moda_impl_int64;
            break;
        case REAL_RESULT:
            data->modaImpl = &moda_impl_double;
            break;
        case DECIMAL_RESULT:
        case STRING_RESULT:
            data->modaImpl = &moda_impl_int64;
            break;
        default:
            data->modaImpl = NULL;
    }
    return data->modaImpl;
}

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
    getImpl(initid);
    return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_deinit(UDF_INIT* initid)
{
    Moda_impl_base* impl = getImpl(initid);
    if (impl)
        impl->moda_deinit(initid);
}   

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void moda_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                char* message __attribute__((unused)))
{
    Moda_impl_base* impl = getImpl(initid);
    if (impl)
        impl->moda_clear(initid);
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
    Moda_impl_base* impl = getImpl(initid);
    if (impl)
        impl->moda_add(initid, args, is_null, message);
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
    Moda_impl_base* impl = getImpl(initid);
    if (impl)
        impl->moda_remove(initid, args, is_null, message);
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
char* moda(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
           char* is_null, char* error __attribute__((unused)))
{
    Moda_impl_base* impl = getImpl(initid);
    if (impl)
        return impl->moda(initid, args, is_null, error);
    return NULL;
}

} // Extern "C"
