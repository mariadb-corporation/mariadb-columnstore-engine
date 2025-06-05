#include <my_config.h>
#include <cmath>
#include <iostream>
#include <sstream>
using namespace std;

#include "idb_mysql.h"
#include "bloom_funcs.h"
using namespace bloom_funcs;

// Debugging purposes
#include <fstream>
#include <bitset>

namespace
{
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

template<typename T>
static inline void lg(T data)
{
  std::ofstream log("/tmp/bc_udf.log", std::ios::app);
  log << data << "\n";
}

static inline void logBloomFilter(auto& bloomFilter, string_view s)
{
  std::ofstream log("/tmp/bloom_and_udfmysql.log", std::ios::app);
  
  log << "STUB Bloom filter, " << s << "\n";
  for (const auto& v : bloomFilter)
  {
    log << std::bitset<8>(v);
  }
  log << "\n" << "bloom filter size: " << bloomFilter.size() << "\n";
  log << "\n\n";

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
  /**
   * MCS_ADD connector stub
   */
  my_bool mcs_add_init(UDF_INIT* /*initid*/, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 2)
    {
      strcpy(message, "mcs_add() requires two argument");
      return 1;
    }

    return 0;
  }

  void mcs_add_deinit(UDF_INIT* /*initid*/)
  {
  }

  double mcs_add(UDF_INIT* /*initid*/, UDF_ARGS* args, char* /*is_null*/, char* /*error*/)
  {
    double op1, op2;

    op1 = cvtArgToDouble(args->arg_type[0], args->args[0]);
    op2 = cvtArgToDouble(args->arg_type[1], args->args[1]);

    return op1 + op2;
  }

  /**
   * MCS_ISNULL connector stub
   */

  my_bool mcs_isnull_init(UDF_INIT* /*initid*/, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 1)
    {
      strcpy(message, "mcs_isnull() requires one argument");
      return 1;
    }

    return 0;
  }

  void mcs_isnull_deinit(UDF_INIT* /*initid*/)
  {
  }

  long long mcs_isnull(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    return 0;
  }

  /**
   * ALLNULL connector stub
   */
  struct allnull_data
  {
    ulonglong totalQuantity;
    ulonglong totalNulls;
  };

  my_bool allnull_init(UDF_INIT* initid, UDF_ARGS* /*args*/, char* message)
  {
    struct allnull_data* data;
    //	if (args->arg_count != 1)
    //	{
    //		strcpy(message,"allnull() requires one argument");
    //		return 1;
    //	}

    if (!(data = (struct allnull_data*)malloc(sizeof(struct allnull_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }

    data->totalQuantity = 0;
    data->totalNulls = 0;

    initid->ptr = (char*)data;

    return 0;
  }

  void allnull_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

  long long allnull(UDF_INIT* initid, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    struct allnull_data* data = (struct allnull_data*)initid->ptr;
    return data->totalQuantity > 0 && data->totalNulls == data->totalQuantity;
  }

  void allnull_clear(UDF_INIT* initid, char* /*is_null*/, char* /*message*/)
  {
    struct allnull_data* data = (struct allnull_data*)initid->ptr;
    data->totalQuantity = 0;
    data->totalNulls = 0;
  }

  void allnull_add(UDF_INIT* initid, UDF_ARGS* args, char* /*is_null*/, char* /*message*/)
  {
    struct allnull_data* data = (struct allnull_data*)initid->ptr;
    const char* word = args->args[0];
    data->totalQuantity++;

    if (!word)
    {
      data->totalNulls++;
    }
  }

  /**
   * SSQ connector stub
   */
  struct ssq_data
  {
    double sumsq;
  };

  my_bool ssq_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct ssq_data* data;

    if (args->arg_count != 1)
    {
      strcpy(message, "ssq() requires one argument");
      return 1;
    }

    if (!(data = (struct ssq_data*)malloc(sizeof(struct ssq_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }

    data->sumsq = 0;

    initid->ptr = (char*)data;
    return 0;
  }

  void ssq_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

  void ssq_clear(UDF_INIT* initid, char* /*is_null*/, char* /*message*/)
  {
    struct ssq_data* data = (struct ssq_data*)initid->ptr;
    data->sumsq = 0;
  }

  void ssq_add(UDF_INIT* initid, UDF_ARGS* args, char* /*is_null*/, char* /*message*/)
  {
    struct ssq_data* data = (struct ssq_data*)initid->ptr;
    double val = cvtArgToDouble(args->arg_type[0], args->args[0]);
    data->sumsq = val * val;
  }

  long long ssq(UDF_INIT* initid, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    struct ssq_data* data = (struct ssq_data*)initid->ptr;
    return data->sumsq;
  }

  //=======================================================================

  /**
   * MEDIAN connector stub
   */
  my_bool median_init(UDF_INIT* /*initid*/, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 1)
    {
      strcpy(message, "median() requires one argument");
      return 1;
    }

    /*
            if (!(data = (struct ssq_data*) malloc(sizeof(struct ssq_data))))
            {
                    strmov(message,"Couldn't allocate memory");
                    return 1;
            }
            data->sumsq	= 0;

            initid->ptr = (char*)data;
    */
    return 0;
  }

  void median_deinit(UDF_INIT* /*initid*/)
  {
    //	free(initid->ptr);
  }

  void median_clear(UDF_INIT* /*initid*/, char* /*is_null*/, char* /*message*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	data->sumsq = 0;
  }

  void median_add(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*message*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	double val = cvtArgToDouble(args->arg_type[0], args->args[0]);
    //	data->sumsq = val*val;
  }

  long long median(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	return data->sumsq;
    return 0;
  }

  /**
   * avg_mode connector stub
   */
  my_bool avg_mode_init(UDF_INIT* /*initid*/, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 1)
    {
      strcpy(message, "avg_mode() requires one argument");
      return 1;
    }

    /*
            if (!(data = (struct ssq_data*) malloc(sizeof(struct ssq_data))))
            {
                    strmov(message,"Couldn't allocate memory");
                    return 1;
            }
            data->sumsq	= 0;

            initid->ptr = (char*)data;
    */
    return 0;
  }

  void avg_mode_deinit(UDF_INIT* /*initid*/)
  {
    //	free(initid->ptr);
  }

  void avg_mode_clear(UDF_INIT* /*initid*/, char* /*is_null*/, char* /*message*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	data->sumsq = 0;
  }

  void avg_mode_add(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*message*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	double val = cvtArgToDouble(args->arg_type[0], args->args[0]);
    //	data->sumsq = val*val;
  }

  long long avg_mode(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    //	struct ssq_data* data = (struct ssq_data*)initid->ptr;
    //	return data->sumsq;
    return 0;
  }

  //=======================================================================

  /**
   * avgx connector stub. Exactly the same functionality as the
   * built in avg() function. Use to test the performance of the
   * API
   */
  struct avgx_data
  {
    double sumx;
    int64_t cnt;
  };

  my_bool avgx_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    struct avgx_data* data;
    if (args->arg_count != 1)
    {
      strcpy(message, "avgx() requires one argument");
      return 1;
    }

    if (!(data = (struct avgx_data*)malloc(sizeof(struct avgx_data))))
    {
      strmov(message, "Couldn't allocate memory");
      return 1;
    }
    data->sumx = 0;
    data->cnt = 0;

    initid->ptr = (char*)data;
    return 0;
  }

  void avgx_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

  void avgx_clear(UDF_INIT* initid, char* /*is_null*/, char* /*message*/)
  {
    struct avgx_data* data = (struct avgx_data*)initid->ptr;
    data->sumx = 0;
    data->cnt = 0;
  }

  void avgx_add(UDF_INIT* initid, UDF_ARGS* args, char* /*is_null*/, char* /*message*/)
  {
    // TODO test for NULL in x and y
    struct avgx_data* data = (struct avgx_data*)initid->ptr;
    double xval = cvtArgToDouble(args->arg_type[1], args->args[0]);
    ++data->cnt;
    data->sumx += xval;
  }

  long long avgx(UDF_INIT* initid, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    struct avgx_data* data = (struct avgx_data*)initid->ptr;
    return data->sumx / data->cnt;
  }

  /**
   * distinct_count connector stub
   */
  my_bool distinct_count_init(UDF_INIT* /*initid*/, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 1)
    {
      strcpy(message, "distinct_count() requires one argument");
      return 1;
    }

    return 0;
  }

  void distinct_count_deinit(UDF_INIT* /*initid*/)
  {
    //	free(initid->ptr);
  }

  void distinct_count_clear(UDF_INIT* /*initid*/, char* /*is_null*/, char* /*message*/)
  {
  }

  void distinct_count_add(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*message*/)
  {
  }

  long long distinct_count(UDF_INIT* /*initid*/, UDF_ARGS* /*args*/, char* /*is_null*/, char* /*error*/)
  {
    return 0;
  }

  // BLOOM_AGG connector stub
    my_bool bloom_agg_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 3)
    {
      strcpy(message, "bloom_agg() requires 3 arguments: max_elem_count, false pos rate, column");
      return 1;
    }

    // lg("max elem and fp rate: ");
    // lg(*reinterpret_cast<uint64_t*>(args->args[0]));
    // lg(*reinterpret_cast<uint64_t*>(args->args[1]));
    
    auto* data = new BloomData(
      *reinterpret_cast<uint64_t*>(args->args[0]),
      *reinterpret_cast<uint64_t*>(args->args[1])
    );

    //lg("Hash func count: ");
    //lg(data->fHashFuncCount);

    
    initid->ptr = (char*)data;
    
    //logBloomFilter(data->bloomFilter);
    initid->max_length = data->bloomFilter.size();

    return 0;
  }

    void bloom_agg_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

    void bloom_agg_clear(UDF_INIT* initid __attribute__((unused)), char* is_null __attribute__((unused)),
                     char* message __attribute__((unused)))
  {
  }

    void bloom_agg_add(UDF_INIT* initid, UDF_ARGS* args, char* is_null __attribute__((unused)), char* message __attribute__((unused)))
  {
    struct BloomData* data = (struct BloomData*)initid->ptr;
    auto val = *reinterpret_cast<const uint64_t*>(args->args[2]);;

    addValueToBloomFilter(val, *data);
  }

    char* bloom_agg(UDF_INIT *initid __attribute__((unused)),
               UDF_ARGS *args __attribute__((unused)), char *result, unsigned long *length,
               char *is_null, char *error __attribute__((unused)))
  {
    struct BloomData* data = (struct BloomData*)initid->ptr;

    if (data->bloomFilter.empty()) 
    {
      *is_null = 1;
      return result;
    }

    //logBloomFilter(data->bloomFilter);

    *is_null = 0;
    *length = data->bloomFilter.size();

    memcpy(result, data->bloomFilter.data(), *length);
    return result;
  }

  // MCS_bloom_contains connector stub
      my_bool bloom_contains_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 3)
    {
      strcpy(message, "bloom_contains() requires three arguments: bloom_agg, column, max_element_count");
      return 1;
    }

    initid->max_length = 8;
    
    const char* rawData = args->args[0];
    unsigned long rawDataSize = args->lengths[0];
    // lg("before constructing bloom filter");
    // lg("rawData pointer: " + std::to_string(reinterpret_cast<uintptr_t>(rawData)));
    // lg("rawDataSize: " + std::to_string(rawDataSize));
    if (!rawData || rawDataSize == 0) {
        strcpy(message, "bloom_contains(): Invalid bloom filter data");
        return 1;
    }
    std::vector<uint8_t> bloomFilter(reinterpret_cast<const uint8_t*>(rawData),
                            reinterpret_cast<const uint8_t*>(rawData) + rawDataSize);


    // lg("after constructing bloom filter");

    auto maxElemCount = *reinterpret_cast<const uint64_t*>(args->args[2]);

    auto* data = new BloomData();
    data->bloomFilter = bloomFilter;
    data->fHashFuncCount = getHashFuncCount(maxElemCount, bloomFilter.size() * blockSize);
    //lg(data->fHashFuncCount);

    //logBloomFilter(data->bloomFilter);

    initid->ptr = (char*)data;

    return 0;
  }

    void bloom_contains_deinit(UDF_INIT* initid)
  {
    free(initid->ptr);
  }

    void bloom_contains_clear(UDF_INIT* initid __attribute__((unused)), char* is_null __attribute__((unused)),
                     char* message __attribute__((unused)))
  {
  }

    void bloom_contains_add(UDF_INIT* initid __attribute__((unused)), UDF_ARGS* args __attribute__((unused)), char* is_null __attribute__((unused)), char* message __attribute__((unused)))
  {
  }

    long long bloom_contains(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)), char* is_null,
                    char* error __attribute__((unused)))
  {
    struct BloomData* data = (struct BloomData*)initid->ptr;

    if (data->bloomFilter.empty()) 
    {
      *is_null = 1;
      return 0;
    }
    
    *is_null = 0;

    // auto val = static_cast<uint64_t>(*args->args[1]);
    auto val = *reinterpret_cast<const int64_t*>(args->args[1]);

    // std::ofstream log("/tmp/bloom_udf.log", std::ios::app);
    // log << "Value: " << val << "\n";

    // logBloomFilter(data->bloomFilter, "BLOOM_CONTAINS");
    // lg(data->fHashFuncCount);
    // lg("bloom filter size: ");
    // lg(data->bloomFilter.size());
    // lg(val);

    return bloomFilterContains(val, data->bloomFilter, data->fHashFuncCount);
  }

   // BLOOM_AND connector stub
    my_bool bloom_and_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 2)
    {
      strcpy(message, "bloom_agg() requires two arguments");
      return 1;
    }

    if (args->lengths[0] != args->lengths[1])
    {
      strcpy(message, "bloom_agg() arguments are of different lengths");
      return 1;
    }

    initid->max_length = args->lengths[0];

    initid->max_length = args->lengths[0];  
      
    // Allocate buffer for the result  
    if (!(initid->ptr = (char*)malloc(args->lengths[0])))  
    {  
        strcpy(message, "Couldn't allocate memory for result buffer");  
        return 1;  
    }  

    return 0;
  }

    void bloom_and_deinit(UDF_INIT* initid __attribute__((unused)))
{
    if (initid->ptr)  
    {  
        free(initid->ptr);  
        initid->ptr = nullptr;  
    }  
  }

    void bloom_and_clear(UDF_INIT* initid __attribute__((unused)), char* is_null __attribute__((unused)),
                     char* message __attribute__((unused)))
  {
  }

    void bloom_and_add(UDF_INIT* initid __attribute__((unused)), UDF_ARGS* args __attribute__((unused)), char* is_null __attribute__((unused)), char* message __attribute__((unused)))
  {
  }

    char* bloom_and(UDF_INIT *initid __attribute__((unused)),
               UDF_ARGS *args, char *result, unsigned long *length,
               char *is_null, char *error __attribute__((unused)))
  {
    if (args->lengths[0] != args->lengths[1] || args->lengths[0] == 0 || args->lengths[1] == 0)
    {
      *is_null = 1;
      return result;
    }

    *length = args->lengths[0];  
      
    uint8_t* out = reinterpret_cast<uint8_t*>(initid->ptr);  
    const uint8_t* a = reinterpret_cast<const uint8_t*>(args->args[0]);  
    const uint8_t* b = reinterpret_cast<const uint8_t*>(args->args[1]);  
      
    for (unsigned long i = 0; i < *length; ++i)  
    {  
        out[i] = a[i] & b[i];  
    }  
      
    return initid->ptr;
  }



}
