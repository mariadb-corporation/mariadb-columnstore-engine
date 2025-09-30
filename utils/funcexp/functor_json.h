#pragma once

#include <string>
#define PREFER_MY_CONFIG_H
#include <mariadb.h>
#include <mysql.h>
#include <my_sys.h>
#include <json_lib.h>

#include "collation.h"
#include "functor_bool.h"
#include "functor_int.h"
#include "functor_str.h"

namespace funcexp
{
// The json_path_t wrapper include some flags
struct JSONPath
{
 public:
  JSONPath() : constant(false), parsed(false), currStep(nullptr)
  {
  }
  json_path_t p{};
  bool constant;  // check if the argument is constant
  bool parsed;    // check if the argument is parsed
  json_path_step_t* currStep;
};

class Func_json
{
  protected:
    json_engine_t jsEg;
#if MYSQL_VERSION_ID >= 120200
    bool path_inited;
    int jsEg_stack[JSON_DEPTH_LIMIT];
#endif
  public:
    Func_json()
    {
#if MYSQL_VERSION_ID >= 120200
      path_inited= false;
      initJsonArray(NULL, &jsEg.stack, sizeof(int), &jsEg_stack[0],
                    MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
    }
};

class Func_json_no_multipath : public Func_json
{
  protected:
    JSONPath path;
#if MYSQL_VERSION_ID >= 120200
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];
#endif

public:

  void init_json_path_array()
  {
#if MYSQL_VERSION_ID >= 120200
    if (!path_inited)
    {
      initJsonArray(NULL, &path.p.steps, sizeof(json_path_step_t), &p_steps[0], 
                    MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
    path_inited= true;
    }
#endif

    return;
  }

  Func_json_no_multipath()
  {
#if MYSQL_VERSION_ID >= 120200
      init_json_path_array();
#endif
  }
};

class Func_json_multipath : public Func_json
{
  protected:
  std::vector<JSONPath> paths;
#if MYSQL_VERSION_ID >= 120200
  std::vector<std::vector<json_path_step_t>> p_steps_arr;
#endif

  public:

  inline void initJSPaths(std::vector<JSONPath>& paths, FunctionParm& fp,
                          const int start, const int step)
  {
    if (paths.empty())
      for (size_t i = start; i < fp.size(); i += step)
        paths.emplace_back();
  }

#if MYSQL_VERSION_ID >= 120200
  void init_json_multipath_array(bool& path_inited, std::vector<JSONPath>& paths,
                                std::vector<std::vector<json_path_step_t>>& p_steps_arr)
  {
    if (!path_inited)
    {
      for (size_t i=0; i<paths.size(); i++)
      {
        JSONPath& path = paths[i];
        initJsonArray(NULL, &path.p.steps, sizeof(json_path_step_t), &p_steps_arr[i],  MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
      }
      path_inited= true;
    }
    return;
  }
#endif

  Func_json_multipath() {}

  Func_json_multipath(FunctionParm& fp, int start, int end)
  {
    initJSPaths(paths, fp, start, end);
#if MYSQL_VERSION_ID >= 120200
  p_steps_arr = std::vector<std::vector<json_path_step_t>>(fp.size(),
                   std::vector<json_path_step_t>(JSON_DEPTH_LIMIT));
  init_json_multipath_array(path_inited, paths, p_steps_arr);
#endif
  }

  ~Func_json_multipath() {}
};

class JSONEgWrapper : public json_engine_t
{
 public:
#if MYSQL_VERSION_ID >= 120200
  JSONEgWrapper(CHARSET_INFO* cs, const uchar* str, const uchar* end, int *buffer)
#else
  JSONEgWrapper(CHARSET_INFO* cs, const uchar* str, const uchar* end)
#endif
  {
#if MYSQL_VERSION_ID >= 120200
    mem_root_dynamic_array_init(NULL, PSI_INSTRUMENT_MEM,
                                &this->stack, sizeof(int), buffer,
                                JSON_DEPTH_DEFAULT, JSON_DEPTH_INC, MYF(0));
#endif
    json_scan_start(this, cs, str, end);
  }
  JSONEgWrapper(const std::string& str, CHARSET_INFO* cs)
   : 
#if MYSQL_VERSION_ID >= 120200
   JSONEgWrapper(cs, (const uchar*)str.data(), (const uchar*)str.data() + str.size(), NULL)
#else
   JSONEgWrapper(cs, (const uchar*)str.data(), (const uchar*)str.data() + str.size())
#endif
  {
  }
  bool checkAndGetScalar(std::string& ret, int* error);
  bool checkAndGetComplexVal(std::string& ret, int* error);
};

class JSONPathWrapper : public JSONPath
{
 private:
 json_path_t p;
#if MYSQL_VERSION_ID >= 120200
   int je_stack[JSON_DEPTH_LIMIT];
   MEM_ROOT_DYNAMIC_ARRAY array;
   json_path_step_t p_steps[JSON_DEPTH_LIMIT];
   int arrayCounters[JSON_DEPTH_LIMIT];
#endif

 protected:
  virtual ~JSONPathWrapper() = default;
  virtual bool checkAndGetValue(JSONEgWrapper* je, std::string& ret, int* error) = 0;

 public:
  JSONPathWrapper()
  {
#if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &array, sizeof(int), &je_stack[0], MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
    initJsonArray(NULL, &p.steps, sizeof(json_path_step_t), &p_steps, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
    initJsonArray(NULL, &array, sizeof(int), &arrayCounters, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  bool extract(std::string& ret, rowgroup::Row& row, execplan::SPTP& funcParmJS,
               execplan::SPTP& funcParmPath);
};
/** @brief Func_json_valid class
 */
class Func_json_valid : public Func_Bool, public Func_json
{
 public:
  Func_json_valid() : Func_Bool("json_valid")
  {
  }
  ~Func_json_valid() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_depth class
 */
class Func_json_depth : public Func_Int, public Func_json
{
 public:
  Func_json_depth() : Func_Int("json_depth")
  {
  }
  ~Func_json_depth() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_length class
 */
class Func_json_length : public Func_Int, public Func_json_no_multipath
{
 public:
  Func_json_length() : Func_Int("json_length"), Func_json_no_multipath()
  {}
  ~Func_json_length() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_equals class
 */
class Func_json_equals : public Func_Bool, public Func_json
{
 private:
#if MYSQL_VERSION_ID >= 120200
   MEM_ROOT_DYNAMIC_ARRAY array;
   struct json_norm_value *buffer_array[JSON_DEPTH_LIMIT];
#endif

 public:
  Func_json_equals() : Func_Bool("json_equals")
  {
#if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &array, sizeof(struct json_norm_value*), buffer_array, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_equals() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_normalize class
 */
class Func_json_normalize : public Func_Str, public Func_json
{
private:
#if MYSQL_VERSION_ID >= 120200
  MEM_ROOT_DYNAMIC_ARRAY array;
  struct json_norm_value *buffer_array[JSON_DEPTH_LIMIT];
#endif
 public:
  Func_json_normalize() : Func_Str("json_normalize")
  {
#if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &array, sizeof(struct json_norm_value*), buffer_array, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_normalize() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_type class
 */
class Func_json_type : public Func_Str, public Func_json
{
 public:
  Func_json_type() : Func_Str("json_type")
  {
  }
  ~Func_json_type() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_object class
 */
class Func_json_object : public Func_Str
{
 public:
  Func_json_object() : Func_Str("json_object")
  {
  }
  ~Func_json_object() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_array class
 */
class Func_json_array : public Func_Str
{
 public:
  Func_json_array() : Func_Str("json_array")
  {
  }
  ~Func_json_array() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_keys class
 */
class Func_json_keys : public Func_Str, public Func_json_no_multipath
{
 public:
  Func_json_keys() : Func_Str("json_keys"), Func_json_no_multipath()
  {
  }
  ~Func_json_keys() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_exists class
 */
class Func_json_exists : public Func_Bool, public Func_json_no_multipath
{
 public:
  Func_json_exists() : Func_Bool("json_exists"), Func_json_no_multipath()
  {}
  ~Func_json_exists() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_quote class
 */
class Func_json_quote : public Func_Str, public Func_json_no_multipath
{
 public:
  Func_json_quote() : Func_Str("json_quote"), Func_json_no_multipath()
  {}
  ~Func_json_quote() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_unquote class
 */
class Func_json_unquote : public Func_Str, public Func_json_no_multipath
{
 public:
  Func_json_unquote() : Func_Str("json_unquote"), Func_json_no_multipath()
  {}
  ~Func_json_unquote() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_format class
 */
class Func_json_format : public Func_Str, public Func_json
{
 public:
  enum FORMATS
  {
    NONE,
    COMPACT,
    LOOSE,
    DETAILED
  };

 protected:
  FORMATS fmt;

 public:
  Func_json_format() : Func_Str("json_detailed"), fmt(DETAILED)
  {
  }
  explicit Func_json_format(FORMATS format) : fmt(format)
  {
    assert(format != NONE);
    switch (format)
    {
      case DETAILED: Func_Str::Func::funcName("json_detailed"); break;
      case LOOSE: Func_Str::Func::funcName("json_loose"); break;
      case COMPACT: Func_Str::Func::funcName("json_compact"); break;
      default: break;
    }
  }
  ~Func_json_format() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_merge_preserve class
 */
class Func_json_merge : public Func_Str, public Func_json
{
 private:
  json_engine_t jsEg2;
#if MYSQL_VERSION_ID >= 120200
  int jsEg2_stack[JSON_DEPTH_LIMIT];
#endif
 public:
  Func_json_merge() : Func_Str("json_merge_preserve")
  {
#if MYSQL_VERSION_ID >= 120200
     initJsonArray(NULL, &jsEg2.stack, sizeof(int), &jsEg2_stack, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_merge() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_merge_patch class
 */
class Func_json_merge_patch : public Func_Str, public Func_json
{
 private:
  json_engine_t jsEg2;
#if MYSQL_VERSION_ID >= 120200
  int jsEg2_stack[JSON_DEPTH_LIMIT];
#endif

 public:
  Func_json_merge_patch() : Func_Str("json_merge_patch")
  {
  #if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &jsEg2.stack, sizeof(int), &jsEg2_stack, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
  #endif
  }
  ~Func_json_merge_patch() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_value class
 */
class Func_json_value : public Func_Str
{
 public:
  Func_json_value() : Func_Str("json_value")
  {}
  ~Func_json_value() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_query class
 */
class Func_json_query : public Func_Str
{
 public:
  Func_json_query() : Func_Str("json_query")
  {
  }
  ~Func_json_query() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_contains class
 */
class Func_json_contains : public Func_Bool, public Func_json_no_multipath
{
 protected:
  bool arg2Const;
  bool arg2Parsed;  // argument 2 is a constant or has been parsed
  utils::NullString arg2Val;
  json_engine_t valEg;
#if MYSQL_VERSION_ID >= 120200
  int valEg_stack[JSON_DEPTH_LIMIT];
#endif

 public:
  Func_json_contains() : Func_Bool("json_contains"), arg2Const(false),
                         arg2Parsed(false), arg2Val("")
  {
#if MYSQL_VERSION_ID >= 120200
  initJsonArray(NULL, &valEg.stack, sizeof(int), &valEg_stack, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_contains() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_array_append class
 */
class Func_json_array_append : public Func_Str, public Func_json_multipath
{
 public:
  Func_json_array_append() : Func_Str("json_array_append"), Func_json_multipath()
  {}
  Func_json_array_append(FunctionParm& fp) : Func_Str("json_array_append"),
                                            Func_json_multipath(fp, 1, 2)
  {}
  ~Func_json_array_append() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;

 private:
  static const int padding = 8;
};
/** @brief Func_json_array_insert class
 */
class Func_json_array_insert : public Func_Str, public Func_json_multipath
{
 public:
  Func_json_array_insert() : Func_Str("json_array_insert"), Func_json_multipath()
  {}
  Func_json_array_insert(FunctionParm& fp) : Func_Str("json_array_insert"),
                                             Func_json_multipath(fp, 1, 2)
  {}
  ~Func_json_array_insert() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_insert class
 */
class Func_json_insert : public Func_json_multipath, public Func_Str
{
 public:
  enum MODE
  {
    NONE,
    INSERT,
    REPLACE,
    SET
  };

 protected:
  MODE mode;

 public:
  Func_json_insert() : Func_json_multipath(), Func_Str("json_insert"), mode(INSERT)
  {}
  Func_json_insert(FunctionParm& fp) :Func_json_multipath(fp, 1, 2),
                                            Func_Str("json_insert"), mode(INSERT)
  {}

  explicit Func_json_insert(MODE m) : mode(m)
  {
    assert(m != NONE);
    switch (m)
    {
      case INSERT: Func_Str::Func::funcName("json_insert"); break;
      case REPLACE: Func_Str::Func::funcName("json_replace"); break;
      case SET: Func_Str::Func::funcName("json_set"); break;
      default: break;
    }
  }
  explicit Func_json_insert(FunctionParm& fp, MODE m) : Func_json_multipath(fp, 1, 2), mode(m)
  {
    assert(m != NONE);
    switch (m)
    {
      case INSERT: Func_Str::Func::funcName("json_insert"); break;
      case REPLACE: Func_Str::Func::funcName("json_replace"); break;
      case SET: Func_Str::Func::funcName("json_set"); break;
      default: break;
    }
  }

  ~Func_json_insert() override = default;

  MODE getMode() const
  {
    return mode;
  }

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_remove class
 */
class Func_json_remove : public Func_Str, public Func_json_multipath
{
 public:
  Func_json_remove() : Func_Str("json_remove"), Func_json_multipath()
  {}
  Func_json_remove(FunctionParm& fp) : Func_Str("json_remove"), Func_json_multipath(fp, 1, 1)
  {}

  ~Func_json_remove() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_contains_path class
 */
class Func_json_contains_path : public Func_json_multipath, public Func_Bool
{
 protected:
  std::vector<bool> hasFound;
  bool isModeOne;
  bool isModeConst;
  bool isModeParsed;
#if MYSQL_VERSION_ID >= 120200
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];
#endif
  json_path_t p;

 public:
  Func_json_contains_path()
   : Func_json_multipath(), Func_Bool("json_contains_path"),
     isModeOne(false), isModeConst(false), isModeParsed(false)
  {}
  Func_json_contains_path(FunctionParm& fp)
   : Func_json_multipath(fp, 2, 1), Func_Bool("json_contains_path"),
     isModeOne(false), isModeConst(false), isModeParsed(false)
  {
#if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &p.steps, sizeof(json_path_step_t), &p_steps, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_contains_path() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_overlaps class
 */
class Func_json_overlaps : public Func_Bool, public Func_json_no_multipath
{
 private:
  json_engine_t jsEg2;
#if MYSQL_VERSION_ID >= 120200
  int jsEg2_stack[JSON_DEPTH_LIMIT];
#endif

 public:
  Func_json_overlaps() : Func_Bool("json_overlaps"), Func_json_no_multipath()
  {
#if MYSQL_VERSION_ID >= 120200
  initJsonArray(NULL, &jsEg2.stack, sizeof(int), &jsEg2_stack, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }
  ~Func_json_overlaps() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_search class
 */
class Func_json_search : public Func_json_multipath, public Func_Str
{
 protected:
  bool isModeParsed;
  bool isModeConst;
  bool isModeOne;
  int escape;
  json_path_t p, savPath;
#if MYSQL_VERSION_ID >= 120200
  json_path_step_t p_steps[JSON_DEPTH_LIMIT],
                   savPath_steps[JSON_DEPTH_LIMIT];
#endif

 public:
  Func_json_search()
   : Func_json_multipath(), Func_Str("json_search"), isModeParsed(false),
     isModeConst(false), isModeOne(false), escape('\\')
  {
  }

  Func_json_search(FunctionParm& fp)
   : Func_json_multipath(fp, 4, 1), Func_Str("json_search"),
     isModeParsed(false), isModeConst(false), isModeOne(false), escape('\\')
  {
#if MYSQL_VERSION_ID >= 120200
  initJsonArray(NULL, &savPath.steps, sizeof(json_path_step_t), &savPath_steps, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
  initJsonArray(NULL, &p.steps, sizeof(json_path_step_t), &p_steps, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }

  ~Func_json_search() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;

 private:
  int cmpJSValWild(json_engine_t* jsEg, const utils::NullString& cmpStr, const CHARSET_INFO* cs);
};
/** @brief Func_json_extract_string class
 */
class Func_json_extract : public Func_Str, public Func_json_multipath
{
  json_engine_t savJSEg;
#if MYSQL_VERSION_ID >= 120200
  int savJSEg_stack[JSON_DEPTH_LIMIT];
  json_path_step_t p_steps[JSON_DEPTH_LIMIT];
#endif
  json_path_t p;

 public:
  Func_json_extract() :Func_Str("json_extract"), Func_json_multipath()
  {}
  Func_json_extract(FunctionParm& fp) : Func_Str("json_extract"),
                                        Func_json_multipath(fp, 1, 1)
  {
#if MYSQL_VERSION_ID >= 120200
    initJsonArray(NULL, &savJSEg.stack, sizeof(int),
                  &savJSEg_stack, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
    initJsonArray(NULL, &p.steps, sizeof(json_path_step_t),
                  &p_steps, MY_INIT_BUFFER_USED | MY_BUFFER_NO_RESIZE);
#endif
  }

  ~Func_json_extract() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& type) override;

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& type) override;

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& type) override;

 private:
  int doExtract(rowgroup::Row& row, FunctionParm& fp, json_value_types* type, std::string& retJS,
                bool compareWhole);
};
}  // namespace funcexp
