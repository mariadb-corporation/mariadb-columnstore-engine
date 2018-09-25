#include <my_config.h>
#include <cmath>
#include <iostream>
#include <sstream>
using namespace std;

#include "idb_mysql.h"

namespace
{
inline double cvtArgToDouble(int t, const char* v)
{
    double d = 0.0;

    switch (t)
    {
        case INT_RESULT:
            d = (double)(*((long long*)v));
            break;

        case REAL_RESULT:
            d = *((double*)v);
            break;

        case DECIMAL_RESULT:
        case STRING_RESULT:
            d = strtod(v, 0);
            break;

        case ROW_RESULT:
            break;
    }

    return d;
}
inline long long cvtArgToInt(int t, const char* v)
{
    long long ll = 0;

    switch (t)
    {
        case INT_RESULT:
            ll = *((long long*)v);
            break;

        case REAL_RESULT:
            ll = (long long)(*((double*)v));
            break;

        case DECIMAL_RESULT:
        case STRING_RESULT:
            ll = strtoll(v, 0, 0);
            break;

        case ROW_RESULT:
            break;
    }

    return ll;
}
inline string cvtArgToString(int t, const char* v)
{
    string str;

    switch (t)
    {
        case INT_RESULT:
        {
            long long ll;
            ll = *((long long*)v);
            ostringstream oss;
            oss << ll;
            str = oss.str();
            break;
        }

        case REAL_RESULT:
        {
            double d;
            d = *((double*)v);
            ostringstream oss;
            oss << d;
            str = oss.str();
            break;
        }

        case DECIMAL_RESULT:
        case STRING_RESULT:
            str = v;
            break;

        case ROW_RESULT:
            break;
    }

    return str;
}
}

/****************************************************************************
 * UDF function interface for MariaDB connector to recognize is defined in
 * this section. MariaDB's UDF function creation guideline needs to be followed.
 *
 * Three interface need to be defined on the connector for each UDF function.
 *
 * XXX_init: To allocate the necessary memory for the UDF function and validate
 *           the input.
 * XXX_deinit: To clean up the memory.
 * XXX: The function implementation.
 * Detailed instruction can be found at MariaDB source directory:
 * ~/sql/udf_example.cc.
 *
 * Please note that the implementation of the function defined on the connector
 * will only be called when all the input arguments are constant. e.g.,
 * mcs_add(2,3). That way, the function does not run in a distributed fashion
 * and could be slow. If there is a need for the UDF function to run with
 * pure constant input, then one needs to put a implementation in the XXX
 * body, which is very similar to the ones in getXXXval API. If there's no
 * such need for a given UDF, then the XXX interface can just return a dummy
 * result because this function will never be called.
 */
extern "C"
{

//=======================================================================

    /**
     * regr_avgx connector stub
     */
    struct regr_avgx_data
    {
      double	sumx;
      int64_t   cnt;
    };
     
    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    my_bool regr_avgx_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
    {
    	struct regr_avgx_data* data;
    	if (args->arg_count != 2)
    	{
    		strcpy(message,"regr_avgx() requires two arguments");
    		return 1;
    	}

    	if (!(data = (struct regr_avgx_data*) malloc(sizeof(struct regr_avgx_data))))
    	{
    		strmov(message,"Couldn't allocate memory");
    		return 1;
    	}
    	data->sumx	= 0;
        data->cnt = 0;

    	initid->ptr = (char*)data;
    	return 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void regr_avgx_deinit(UDF_INIT* initid)
    {
    	free(initid->ptr);
    }	

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_avgx_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                  char* message __attribute__((unused)))
    {
    	struct regr_avgx_data* data = (struct regr_avgx_data*)initid->ptr;
    	data->sumx = 0;
        data->cnt = 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_avgx_add(UDF_INIT* initid, UDF_ARGS* args,
                char* is_null,
                char* message __attribute__((unused)))
    {
        // Test for NULL in x and y
        if (args->args[0] == 0 || args->args[1] == 0)
        {
            return;
        }
    	struct regr_avgx_data* data = (struct regr_avgx_data*)initid->ptr;
    	double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
        ++data->cnt;
    	data->sumx += xval;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    double regr_avgx(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
    				  char* is_null, char* error __attribute__((unused)))
    {
    	struct regr_avgx_data* data = (struct regr_avgx_data*)initid->ptr;
    	return data->sumx / data->cnt;
    }

//=======================================================================

    /**
     * regr_avgy connector stub
     */
    struct regr_avgy_data
    {
      double	sumy;
      int64_t   cnt;
    };
     
    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    my_bool regr_avgy_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
    {
    	struct regr_avgy_data* data;
    	if (args->arg_count != 2)
    	{
    		strcpy(message,"regr_avgy() requires two arguments");
    		return 1;
    	}

    	if (!(data = (struct regr_avgy_data*) malloc(sizeof(struct regr_avgy_data))))
    	{
    		strmov(message,"Couldn't allocate memory");
    		return 1;
    	}
    	data->sumy	= 0;
        data->cnt = 0;

    	initid->ptr = (char*)data;
    	return 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void regr_avgy_deinit(UDF_INIT* initid)
    {
    	free(initid->ptr);
    }	

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_avgy_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                  char* message __attribute__((unused)))
    {
    	struct regr_avgy_data* data = (struct regr_avgy_data*)initid->ptr;
    	data->sumy = 0;
        data->cnt = 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_avgy_add(UDF_INIT* initid, UDF_ARGS* args,
                char* is_null,
                char* message __attribute__((unused)))
    {
        // Test for NULL in x and y
        if (args->args[0] == 0 || args->args[1] == 0)
        {
            return;
        }
    	struct regr_avgy_data* data = (struct regr_avgy_data*)initid->ptr;
    	double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
        ++data->cnt;
    	data->sumy += yval;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    double regr_avgy(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
    				  char* is_null, char* error __attribute__((unused)))
    {
    	struct regr_avgy_data* data = (struct regr_avgy_data*)initid->ptr;
    	return data->sumy / data->cnt;
    }

//=======================================================================

    /**
     * regr_count connector stub
     */
    struct regr_count_data
    {
      int64_t   cnt;
    };
     
    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    my_bool regr_count_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
    {
    	struct regr_count_data* data;
    	if (args->arg_count != 2)
    	{
    		strcpy(message,"regr_count() requires two arguments");
    		return 1;
    	}

    	if (!(data = (struct regr_count_data*) malloc(sizeof(struct regr_count_data))))
    	{
    		strmov(message,"Couldn't allocate memory");
    		return 1;
    	}
        data->cnt = 0;

    	initid->ptr = (char*)data;
    	return 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void regr_count_deinit(UDF_INIT* initid)
    {
    	free(initid->ptr);
    }	

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_count_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                  char* message __attribute__((unused)))
    {
    	struct regr_count_data* data = (struct regr_count_data*)initid->ptr;
        data->cnt = 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_count_add(UDF_INIT* initid, UDF_ARGS* args,
                char* is_null,
                char* message __attribute__((unused)))
    {
        // Test for NULL in x and y
        if (args->args[0] == 0 || args->args[1] == 0)
        {
            return;
        }
    	struct regr_count_data* data = (struct regr_count_data*)initid->ptr;
        ++data->cnt;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    long long regr_count(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
    				  char* is_null, char* error __attribute__((unused)))
    {
    	struct regr_count_data* data = (struct regr_count_data*)initid->ptr;
    	return data->cnt;
    }

//=======================================================================

    /**
     * regr_slope connector stub
     */
    struct regr_slope_data
    {
        int64_t   cnt;
        double      sumx;
        double      sumx2;  // sum of (x squared)
        double      sumy;
        double      sumxy;  // sum of (x*y)
    };
     
    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    my_bool regr_slope_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
    {
    	struct regr_slope_data* data;
    	if (args->arg_count != 2)
    	{
    		strcpy(message,"regr_slope() requires two arguments");
    		return 1;
    	}

    	if (!(data = (struct regr_slope_data*) malloc(sizeof(struct regr_slope_data))))
    	{
    		strmov(message,"Couldn't allocate memory");
    		return 1;
    	}
        data->cnt = 0;
        data->sumx = 0.0;
        data->sumx2 = 0.0;
        data->sumy = 0.0;

    	initid->ptr = (char*)data;
    	return 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void regr_slope_deinit(UDF_INIT* initid)
    {
    	free(initid->ptr);
    }	

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_slope_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                  char* message __attribute__((unused)))
    {
    	struct regr_slope_data* data = (struct regr_slope_data*)initid->ptr;
        data->cnt = 0;
        data->sumx = 0.0;
        data->sumx2 = 0.0;
        data->sumy = 0.0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_slope_add(UDF_INIT* initid, UDF_ARGS* args,
                char* is_null,
                char* message __attribute__((unused)))
    {
        // Test for NULL in x and y
        if (args->args[0] == 0 || args->args[1] == 0)
        {
            return;
        }
    	struct regr_slope_data* data = (struct regr_slope_data*)initid->ptr;
    	double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    	double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
        data->sumy += yval;
        data->sumx += xval;
        data->sumx2 += xval*xval;
        data->sumxy += xval*yval;
        ++data->cnt;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    double regr_slope(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
    				  char* is_null, char* error __attribute__((unused)))
    {
    	struct regr_slope_data* data = (struct regr_slope_data*)initid->ptr;
        double N = data->cnt;
        if (N > 0)
        {
            double sumx = data->sumx;
            double sumy = data->sumy;
            double sumx2 = data->sumx2;
            double sumxy = data->sumxy;
            double variance = (N * sumx2) - (sumx * sumx);
            if (variance)
            {
                return ((N * sumxy) - (sumx * sumy)) / variance;
            }
        }
        *is_null = 1;
    	return 0;
    }

//=======================================================================

    /**
     * regr_intercept connector stub
     */
    struct regr_intercept_data
    {
        int64_t   cnt;
        double      sumx;
        double      sumx2;  // sum of (x squared)
        double      sumy;
        double      sumxy;  // sum of (x*y)
    };
     
    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    my_bool regr_intercept_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
    {
    	struct regr_intercept_data* data;
    	if (args->arg_count != 2)
    	{
    		strcpy(message,"regr_intercept() requires two arguments");
    		return 1;
    	}

    	if (!(data = (struct regr_intercept_data*) malloc(sizeof(struct regr_intercept_data))))
    	{
    		strmov(message,"Couldn't allocate memory");
    		return 1;
    	}
        data->cnt = 0;
        data->sumx = 0.0;
        data->sumx2 = 0.0;
        data->sumy = 0.0;

    	initid->ptr = (char*)data;
    	return 0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void regr_intercept_deinit(UDF_INIT* initid)
    {
    	free(initid->ptr);
    }	

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_intercept_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                  char* message __attribute__((unused)))
    {
    	struct regr_intercept_data* data = (struct regr_intercept_data*)initid->ptr;
        data->cnt = 0;
        data->sumx = 0.0;
        data->sumx2 = 0.0;
        data->sumy = 0.0;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    void
    regr_intercept_add(UDF_INIT* initid, UDF_ARGS* args,
                char* is_null,
                char* message __attribute__((unused)))
    {
        // Test for NULL in x and y
        if (args->args[0] == 0 || args->args[1] == 0)
        {
            return;
        }
    	struct regr_intercept_data* data = (struct regr_intercept_data*)initid->ptr;
    	double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    	double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
        data->sumy += yval;
        data->sumx += xval;
        data->sumx2 += xval*xval;
        data->sumxy += xval*yval;
        ++data->cnt;
    }

    #ifdef _MSC_VER
    __declspec(dllexport)
    #endif
    double regr_intercept(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
    				  char* is_null, char* error __attribute__((unused)))
    {
    	struct regr_intercept_data* data = (struct regr_intercept_data*)initid->ptr;
        double N = data->cnt;
        if (N > 0)
        {
            double sumx = data->sumx;
            double sumy = data->sumy;
            double sumx2 = data->sumx2;
            double sumxy = data->sumxy;
            double variance = (N * sumx2) - (sumx * sumx);
            if (variance)
            {
                double slope = ((N * sumxy) - (sumx * sumy)) / variance;
                return (sumy - (slope * sumx)) / N;
            }
        }
        *is_null = 1;
    	return 0;
    }
}
// vim:ts=4 sw=4:

