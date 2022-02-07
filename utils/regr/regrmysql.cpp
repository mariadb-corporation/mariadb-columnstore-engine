#include <my_config.h>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string.h>
using namespace std;

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

inline double cvtArgToDouble(int t, const char* v)
{
  double d = 0.0;

  switch (t)
  {
    case INT_RESULT: d = (double)(*((long long*)v)); break;

    case REAL_RESULT: d = *((double*)v); break;

    case DECIMAL_RESULT:
    case STRING_RESULT: d = strtod(v, 0); break;

    case ROW_RESULT: break;
  }

  return d;
}
}  // namespace

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
   * regr_avgx
   */
  struct regr_avgx_data
  {
    long double sumx;
    int64_t cnt;
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_avgx_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_avgx_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_avgx() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_avgx() with a non-numeric independant (second) argument");
      return 1;
    }
    if (initid->decimals != DECIMAL_NOT_SPECIFIED)
    {
      initid->decimals += 4;
    }

    if (!(data = (struct regr_avgx_data*)malloc(sizeof(struct regr_avgx_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->sumx = 0;
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
      void regr_avgx_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                           char* message __attribute__((unused)))
  {
    struct regr_avgx_data* data = (struct regr_avgx_data*)initid->ptr;
    data->sumx = 0;
    data->cnt = 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_avgx_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
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
      double regr_avgx(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                       char* error __attribute__((unused)))
  {
    struct regr_avgx_data* data = (struct regr_avgx_data*)initid->ptr;
    double valOut = 0;
    if (data->cnt > 0)
    {
      valOut = static_cast<double>(data->sumx / data->cnt);
    }
    else
    {
      *is_null = 1;
    }

    return valOut;
  }

  //=======================================================================

  /**
   * regr_avgy
   */
  struct regr_avgy_data
  {
    long double sumy;
    int64_t cnt;
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_avgy_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_avgy_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_avgy() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0])))
    {
      strcpy(message, "regr_avgy() with a non-numeric dependant (first) argument");
      return 1;
    }

    if (initid->decimals != DECIMAL_NOT_SPECIFIED)
    {
      initid->decimals += 4;
    }

    if (!(data = (struct regr_avgy_data*)malloc(sizeof(struct regr_avgy_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->sumy = 0;
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
      void regr_avgy_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                           char* message __attribute__((unused)))
  {
    struct regr_avgy_data* data = (struct regr_avgy_data*)initid->ptr;
    data->sumy = 0;
    data->cnt = 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_avgy_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
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
      double regr_avgy(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                       char* error __attribute__((unused)))
  {
    struct regr_avgy_data* data = (struct regr_avgy_data*)initid->ptr;
    double valOut = 0;
    if (data->cnt > 0)
    {
      valOut = static_cast<double>(data->sumy / data->cnt);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }

  //=======================================================================

  /**
   * regr_count
   */
  struct regr_count_data
  {
    int64_t cnt;
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_count_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_count_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_count() requires two arguments");
      return 1;
    }

    if (!(data = (struct regr_count_data*)malloc(sizeof(struct regr_count_data))))
    {
      strmov(message, "Couldn't allocate memory");
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
      void regr_count_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                            char* message __attribute__((unused)))
  {
    struct regr_count_data* data = (struct regr_count_data*)initid->ptr;
    data->cnt = 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_count_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
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
      long long regr_count(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                           char* error __attribute__((unused)))
  {
    struct regr_count_data* data = (struct regr_count_data*)initid->ptr;
    return data->cnt;
  }

  //=======================================================================

  /**
   * regr_slope
   */
  struct regr_slope_data
  {
    int64_t cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
    long double sumy;
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_slope_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_slope_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_slope() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_slope() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct regr_slope_data*)malloc(sizeof(struct regr_slope_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;

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
      void regr_slope_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                            char* message __attribute__((unused)))
  {
    struct regr_slope_data* data = (struct regr_slope_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_slope_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
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
    data->sumx2 += xval * xval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_slope(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                        char* error __attribute__((unused)))
  {
    struct regr_slope_data* data = (struct regr_slope_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    *is_null = 1;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumx2 = data->sumx2;
      long double sumxy = data->sumxy;
      long double covar_pop = N * sumxy - sumx * sumy;
      long double var_pop = N * sumx2 - sumx * sumx;
      if (var_pop > 0)
      {
        valOut = static_cast<double>(covar_pop / var_pop);
        *is_null = 0;
      }
    }
    return valOut;
  }

  //=======================================================================

  /**
   * regr_intercept
   */
  struct regr_intercept_data
  {
    int64_t cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
    long double sumy;
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_intercept_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_intercept_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_intercept() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_intercept() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;
    if (!(data = (struct regr_intercept_data*)malloc(sizeof(struct regr_intercept_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;

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
      void regr_intercept_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                                char* message __attribute__((unused)))
  {
    struct regr_intercept_data* data = (struct regr_intercept_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_intercept_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
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
    data->sumx2 += xval * xval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_intercept(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                            char* error __attribute__((unused)))
  {
    struct regr_intercept_data* data = (struct regr_intercept_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    *is_null = 1;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumx2 = data->sumx2;
      long double sumxy = data->sumxy;
      long double numerator = sumy * sumx2 - sumx * sumxy;
      long double var_pop = (N * sumx2) - (sumx * sumx);
      if (var_pop > 0)
      {
        valOut = static_cast<double>(numerator / var_pop);
        *is_null = 0;
      }
    }
    return valOut;
  }

  //=======================================================================

  /**
   * regr_r2
   */
  struct regr_r2_data
  {
    int64_t cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
    long double sumy;
    long double sumy2;  // sum of (y squared)
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_r2_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_r2_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_r2() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_r2() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct regr_r2_data*)malloc(sizeof(struct regr_r2_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;
    data->sumxy = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_r2_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_r2_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                         char* message __attribute__((unused)))
  {
    struct regr_r2_data* data = (struct regr_r2_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_r2_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct regr_r2_data* data = (struct regr_r2_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumy += yval;
    data->sumx += xval;
    data->sumx2 += xval * xval;
    data->sumy2 += yval * yval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_r2(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                     char* error __attribute__((unused)))
  {
    struct regr_r2_data* data = (struct regr_r2_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumx2 = data->sumx2;
      long double sumy2 = data->sumy2;
      long double sumxy = data->sumxy;
      long double var_popx = (sumx2 - (sumx * sumx / N)) / N;
      if (var_popx <= 0)  // Catch -0
      {
        // When var_popx is 0, NULL is the result.
        *is_null = 1;
        return 0;
      }
      long double var_popy = (sumy2 - (sumy * sumy / N)) / N;
      if (var_popy <= 0)  // Catch -0
      {
        // When var_popy is 0, 1 is the result
        return 1;
      }
      long double std_popx = sqrt(var_popx);
      long double std_popy = sqrt(var_popy);
      long double covar_pop = (sumxy - ((sumx * sumy) / N)) / N;
      long double corr = covar_pop / (std_popy * std_popx);
      valOut = static_cast<double>(corr * corr);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }

  //=======================================================================

  /**
   * corr
   */
  struct corr_data
  {
    int64_t cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
    long double sumy;
    long double sumy2;  // sum of (y squared)
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool corr_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct corr_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "corr() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "corr() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct corr_data*)malloc(sizeof(struct corr_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;
    data->sumxy = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void corr_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void corr_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                      char* message __attribute__((unused)))
  {
    struct corr_data* data = (struct corr_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void corr_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct corr_data* data = (struct corr_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumy += yval;
    data->sumx += xval;
    data->sumx2 += xval * xval;
    data->sumy2 += yval * yval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double corr(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                  char* error __attribute__((unused)))
  {
    struct corr_data* data = (struct corr_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumx2 = data->sumx2;
      long double sumy2 = data->sumy2;
      long double sumxy = data->sumxy;
      long double var_popx = (sumx2 - (sumx * sumx / N)) / N;
      if (var_popx <= 0)  // Catch -0
      {
        // When var_popx is 0, NULL is the result.
        *is_null = 1;
        return 0;
      }
      long double var_popy = (sumy2 - (sumy * sumy / N)) / N;
      if (var_popy <= 0)  // Catch -0
      {
        // When var_popy is 0, 1 is the result
        return 1;
      }
      long double std_popx = sqrt(var_popx);
      long double std_popy = sqrt(var_popy);
      long double covar_pop = (sumxy - ((sumx * sumy) / N)) / N;
      long double corr = covar_pop / (std_popy * std_popx);
      return static_cast<double>(corr);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }

  //=======================================================================

  /**
   * regr_sxx
   */
  struct regr_sxx_data
  {
    int64_t cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_sxx_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_sxx_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_sxx() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_avgx() with a non-numeric independant (second) argument");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct regr_sxx_data*)malloc(sizeof(struct regr_sxx_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxx_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxx_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                          char* message __attribute__((unused)))
  {
    struct regr_sxx_data* data = (struct regr_sxx_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxx_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
                        char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct regr_sxx_data* data = (struct regr_sxx_data*)initid->ptr;
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumx += xval;
    data->sumx2 += xval * xval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_sxx(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                      char* error __attribute__((unused)))
  {
    struct regr_sxx_data* data = (struct regr_sxx_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumx2 = data->sumx2;
      long double sxx = (sumx2 - (sumx * sumx / N));
      if (sxx < 0)  // catch -0
        sxx = 0;
      valOut = static_cast<double>(sxx);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }
  //=======================================================================

  /**
   * regr_syy
   */
  struct regr_syy_data
  {
    int64_t cnt;
    long double sumy;
    long double sumy2;  // sum of (y squared)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_syy_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_syy_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_syy() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0])))
    {
      strcpy(message, "regr_syy() with a non-numeric dependant (first) argument");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct regr_syy_data*)malloc(sizeof(struct regr_syy_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_syy_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_syy_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                          char* message __attribute__((unused)))
  {
    struct regr_syy_data* data = (struct regr_syy_data*)initid->ptr;
    data->cnt = 0;
    data->sumy = 0.0;
    data->sumy2 = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_syy_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
                        char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct regr_syy_data* data = (struct regr_syy_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    data->sumy += yval;
    data->sumy2 += yval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_syy(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                      char* error __attribute__((unused)))
  {
    struct regr_syy_data* data = (struct regr_syy_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumy = data->sumy;
      long double sumy2 = data->sumy2;
      long double syy = (sumy2 - (sumy * sumy / N));
      if (syy < 0)  // might be -0
        syy = 0;
      valOut = static_cast<double>(syy);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }

  //=======================================================================

  /**
   * regr_sxy
   */
  struct regr_sxy_data
  {
    int64_t cnt;
    long double sumx;
    long double sumy;
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool regr_sxy_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct regr_sxy_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "regr_sxy() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "regr_sxy() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct regr_sxy_data*)malloc(sizeof(struct regr_sxy_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxy_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxy_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                          char* message __attribute__((unused)))
  {
    struct regr_sxy_data* data = (struct regr_sxy_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void regr_sxy_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
                        char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct regr_sxy_data* data = (struct regr_sxy_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumy += yval;
    data->sumx += xval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double regr_sxy(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                      char* error __attribute__((unused)))
  {
    struct regr_sxy_data* data = (struct regr_sxy_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumxy = data->sumxy;
      long double regr_sxy = (sumxy - ((sumx * sumy) / N));
      valOut = static_cast<double>(regr_sxy);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }

  //=======================================================================

  /**
   * covar_pop
   */
  struct covar_pop_data
  {
    int64_t cnt;
    long double sumx;
    long double sumy;
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool covar_pop_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct covar_pop_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "covar_pop() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "covar_pop() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct covar_pop_data*)malloc(sizeof(struct covar_pop_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_pop_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_pop_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                           char* message __attribute__((unused)))
  {
    struct covar_pop_data* data = (struct covar_pop_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_pop_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
                         char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct covar_pop_data* data = (struct covar_pop_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumy += yval;
    data->sumx += xval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double covar_pop(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                       char* error __attribute__((unused)))
  {
    struct covar_pop_data* data = (struct covar_pop_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumxy = data->sumxy;
      long double covar_pop = (sumxy - ((sumx * sumy) / N)) / N;
      valOut = static_cast<double>(covar_pop);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }
  //=======================================================================

  /**
   * covar_samp
   */
  struct covar_samp_data
  {
    int64_t cnt;
    long double sumx;
    long double sumy;
    long double sumxy;  // sum of (x*y)
  };

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool covar_samp_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct covar_samp_data* data;
    if (args->arg_count != 2)
    {
      strcpy(message, "covar_samp() requires two arguments");
      return 1;
    }
    if (!(isNumeric(args->arg_type[0], args->attributes[0]) &&
          isNumeric(args->arg_type[1], args->attributes[1])))
    {
      strcpy(message, "covar_samp() with non-numeric arguments");
      return 1;
    }

    initid->decimals = DECIMAL_NOT_SPECIFIED;

    if (!(data = (struct covar_samp_data*)malloc(sizeof(struct covar_samp_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;

    initid->ptr = (char*)data;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_samp_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_samp_clear(UDF_INIT* initid, char* is_null __attribute__((unused)),
                            char* message __attribute__((unused)))
  {
    struct covar_samp_data* data = (struct covar_samp_data*)initid->ptr;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void covar_samp_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null,
                          char* message __attribute__((unused)))
  {
    // Test for NULL in x and y
    if (args->args[0] == 0 || args->args[1] == 0)
    {
      return;
    }
    struct covar_samp_data* data = (struct covar_samp_data*)initid->ptr;
    double yval = cvtArgToDouble(args->arg_type[0], args->args[0]);
    double xval = cvtArgToDouble(args->arg_type[1], args->args[1]);
    data->sumy += yval;
    data->sumx += xval;
    data->sumxy += xval * yval;
    ++data->cnt;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      double covar_samp(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                        char* error __attribute__((unused)))
  {
    struct covar_samp_data* data = (struct covar_samp_data*)initid->ptr;
    double N = data->cnt;
    double valOut = 0;
    if (N > 0)
    {
      long double sumx = data->sumx;
      long double sumy = data->sumy;
      long double sumxy = data->sumxy;
      long double covar_samp = (sumxy - ((sumx * sumy) / N)) / (N - 1);
      valOut = static_cast<double>(covar_samp);
    }
    else
    {
      *is_null = 1;
    }
    return valOut;
  }
}
// vim:ts=4 sw=4:
