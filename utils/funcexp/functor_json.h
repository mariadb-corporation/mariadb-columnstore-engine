#pragma once

#include <string>
#define PREFER_MY_CONFIG_H
#include <mariadb.h>
#include <mysql.h>
#include <my_sys.h>
#include "collation.h"
#include "functor_bool.h"
#include "functor_int.h"
#include "functor_str.h"

namespace funcexp
{
// Local replacement for former json_lib json_value_types
enum json_value_types
{
  JSON_VALUE_NULL = 0,
  JSON_VALUE_OBJECT,
  JSON_VALUE_ARRAY,
  JSON_VALUE_STRING,
  JSON_VALUE_NUMBER,
  JSON_VALUE_TRUE,
  JSON_VALUE_FALSE,
  JSON_VALUE_UNINITIALIZED
};

/** @brief Func_json_valid class
 */
class Func_json_valid : public Func_Bool
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
class Func_json_depth : public Func_Int
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
class Func_json_length : public Func_Int
{
 public:
  Func_json_length() : Func_Int("json_length")
  {
  }
  ~Func_json_length() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_equals class
 */
class Func_json_equals : public Func_Bool
{
 public:
  Func_json_equals() : Func_Bool("json_equals")
  {
  }
  ~Func_json_equals() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_normalize class
 */
class Func_json_normalize : public Func_Str
{
 public:
  Func_json_normalize() : Func_Str("json_normalize")
  {
  }
  ~Func_json_normalize() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_type class
 */
class Func_json_type : public Func_Str
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
class Func_json_keys : public Func_Str
{
 public:
  Func_json_keys() : Func_Str("json_keys")
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
class Func_json_exists : public Func_Bool
{
 public:
  Func_json_exists() : Func_Bool("json_exists")
  {
  }
  ~Func_json_exists() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_quote class
 */
class Func_json_quote : public Func_Str
{
 public:
  Func_json_quote() : Func_Str("json_quote")
  {
  }
  ~Func_json_quote() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_unquote class
 */
class Func_json_unquote : public Func_Str
{
 public:
  Func_json_unquote() : Func_Str("json_unquote")
  {
  }
  ~Func_json_unquote() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_format class
 */
class Func_json_format : public Func_Str
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
class Func_json_merge : public Func_Str
{
 public:
  Func_json_merge() : Func_Str("json_merge_preserve")
  {
  }
  ~Func_json_merge() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_merge_patch class
 */
class Func_json_merge_patch : public Func_Str
{
 public:
  Func_json_merge_patch() : Func_Str("json_merge_patch")
  {
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
  {
  }
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
class Func_json_contains : public Func_Bool
{
 protected:
  bool arg2Const;
  bool arg2Parsed;  // argument 2 is a constant or has been parsed
  utils::NullString arg2Val;

 public:
  Func_json_contains() : Func_Bool("json_contains"), arg2Const(false), arg2Parsed(false), arg2Val("")
  {
  }
  ~Func_json_contains() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_array_append class
 */
class Func_json_array_append : public Func_Str
{
 public:
  Func_json_array_append() : Func_Str("json_array_append")
  {
  }
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
class Func_json_array_insert : public Func_Str
{
 public:
  Func_json_array_insert() : Func_Str("json_array_insert")
  {
  }
  ~Func_json_array_insert() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_insert class
 */
class Func_json_insert : public Func_Str
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
  Func_json_insert() : Func_Str("json_insert"), mode(INSERT)
  {
  }
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
class Func_json_remove : public Func_Str
{
 public:
  Func_json_remove() : Func_Str("json_remove")
  {
  }
  ~Func_json_remove() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_contains_path class
 */
class Func_json_contains_path : public Func_Bool
{
 protected:
  std::vector<bool> hasFound;
  bool isModeOne;
  bool isModeConst;
  bool isModeParsed;

 public:
  Func_json_contains_path()
   : Func_Bool("json_contains_path"), isModeOne(false), isModeConst(false), isModeParsed(false)
  {
  }
  ~Func_json_contains_path() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};

/** @brief Func_json_overlaps class
 */
class Func_json_overlaps : public Func_Bool
{
 public:
  Func_json_overlaps() : Func_Bool("json_overlaps")
  {
  }
  ~Func_json_overlaps() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_search class
 */
class Func_json_search : public Func_Str
{
 protected:
  bool isModeParsed;
  bool isModeConst;
  bool isModeOne;
  int escape;

 public:
  Func_json_search()
   : Func_Str("json_search"), isModeParsed(false), isModeConst(false), isModeOne(false), escape('\\')
  {
  }
  ~Func_json_search() override = default;

  execplan::CalpontSystemCatalog::ColType operationType(
      FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) override;

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& type) override;
};
/** @brief Func_json_extract_string class
 */
class Func_json_extract : public Func_Str
{
 public:
  Func_json_extract() : Func_Str("json_extract")
  {
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
