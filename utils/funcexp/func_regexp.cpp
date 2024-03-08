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

/****************************************************************************
 * $Id: func_regexp.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "utils/pcre2/jpcre2.hpp"

#include "functor_bool.h"
#include "functor_str.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
#include "constantcolumn.h"
using namespace execplan;

#include "rowgroup.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace
{

struct RegExpParams
{
  std::string expression;
  std::string pattern;
};

inline RegExpParams getEpressionAndPattern(rowgroup::Row& row, funcexp::FunctionParm& pm, bool& isNull,
                                           CalpontSystemCatalog::ColType& ct, long timeZone)
{
  string expr;
  string pattern;

  switch (pm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::VARCHAR:  // including CHAR'
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      expr = pm[0]->data()->getStrVal(row, isNull).safeString("");
      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      expr = dataconvert::DataConvert::dateToString(pm[0]->data()->getDateIntVal(row, isNull));
      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      expr = dataconvert::DataConvert::datetimeToString(pm[0]->data()->getDatetimeIntVal(row, isNull));
      // strip off micro seconds
      expr = expr.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      expr = dataconvert::DataConvert::timestampToString(pm[0]->data()->getTimestampIntVal(row, isNull),
                                                         timeZone);
      // strip off micro seconds
      expr = expr.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      expr = dataconvert::DataConvert::timeToString(pm[0]->data()->getTimeIntVal(row, isNull));
      // strip off micro seconds
      expr = expr.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal d = pm[0]->data()->getDecimalVal(row, isNull);

      if (pm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
      {
        expr = d.toString(true);
      }
      else
      {
        expr = d.toString();
      }

      break;
    }

    default:
    {
      std::ostringstream oss;
      oss << "regexp: datatype of " << execplan::colDataTypeToString(ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  switch (pm[1]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::VARCHAR:  // including CHAR'
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      pattern = pm[1]->data()->getStrVal(row, isNull).safeString("");
      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      pattern = dataconvert::DataConvert::dateToString(pm[1]->data()->getDateIntVal(row, isNull));
      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      pattern = dataconvert::DataConvert::datetimeToString(pm[1]->data()->getDatetimeIntVal(row, isNull));
      // strip off micro seconds
      pattern = pattern.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      pattern = dataconvert::DataConvert::timestampToString(pm[1]->data()->getTimestampIntVal(row, isNull),
                                                            timeZone);
      // strip off micro seconds
      pattern = pattern.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      pattern = dataconvert::DataConvert::timeToString(pm[1]->data()->getTimeIntVal(row, isNull));
      // strip off micro seconds
      pattern = pattern.substr(0, 19);
      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal d = pm[1]->data()->getDecimalVal(row, isNull);

      if (pm[1]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
      {
        pattern = d.toString(true);
      }
      else
      {
        pattern = d.toString();
      }
      break;
    }

    default:
    {
      std::ostringstream oss;
      oss << "regexp: datatype of " << execplan::colDataTypeToString(ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return RegExpParams{expr, pattern};
}
}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_regexp_replace::operationType(FunctionParm& fp,
                                                                 CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp_substr::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp_instr::operationType(FunctionParm& fp,
                                                               CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp::operationType(FunctionParm& fp,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

using jp = jpcre2::select<char>;

struct PCREOptions
{
  jpcre2::Uint flags = 0;
  CHARSET_INFO* library_charset = &my_charset_utf8mb3_general_ci;
  bool conversion_is_needed = false;
};

inline bool areSameCharsets(CHARSET_INFO* cs1, CHARSET_INFO* cs2)
{
  return (cs1->cs_name.str == cs2->cs_name.str);
}

PCREOptions pcreOptions(execplan::CalpontSystemCatalog::ColType& ct)
{
  CHARSET_INFO* cs = ct.getCharset();
  PCREOptions options;

  // TODO use system variable instead if hardcode default_regex_flags_pcre(_current_thd());

  jpcre2::Uint defaultFlags =
      PCRE2_DOTALL | PCRE2_DUPNAMES | PCRE2_EXTENDED | PCRE2_EXTENDED_MORE | PCRE2_MULTILINE | PCRE2_UNGREEDY;

  options.flags = (cs != &my_charset_bin ? (PCRE2_UTF | PCRE2_UCP) : 0) |
                  ((cs->state & (MY_CS_BINSORT | MY_CS_CSSORT)) ? 0 : PCRE2_CASELESS) | defaultFlags;

  // Convert text data to utf-8.
  options.library_charset = cs == &my_charset_bin ? &my_charset_bin : &my_charset_utf8mb3_general_ci;
  options.conversion_is_needed = (cs != &my_charset_bin) && !areSameCharsets(cs, options.library_charset);
  return options;
}

/*
  returns the string subject with all occurrences of the regular expression pattern replaced by
  the string replace. If no occurrences are found, then subject is returned as is.
  https://mariadb.com/kb/en/regexp_replace/
*/
std::string Func_regexp_replace::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                           execplan::CalpontSystemCatalog::ColType& ct)

{
  RegExpParams param = getEpressionAndPattern(row, fp, isNull, ct, ct.getTimeZone());

  if (isNull)
    return std::string{};

  const auto& replace_with = fp[2]->data()->getStrVal(row, isNull);

  if (replace_with.isNull())
    return param.expression;

  const PCREOptions& options = pcreOptions(ct);
  jp::Regex re(param.pattern, options.flags);

  return re.replace(param.expression, replace_with.unsafeStringRef(), "g");
}

/*
  Returns the part of the string subject that matches the regular expression pattern, or an empty string if
  pattern was not found. https://mariadb.com/kb/en/regexp_substr/
*/
std::string Func_regexp_substr::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& ct)

{
  RegExpParams param = getEpressionAndPattern(row, fp, isNull, ct, ct.getTimeZone());

  if (isNull)
    return std::string{};

  const PCREOptions& options = pcreOptions(ct);
  jp::Regex re(param.pattern, options.flags);
  jp::RegexMatch rm(&re);
  jp::VecNum vec_num;

  size_t count = rm.setSubject(param.expression).setNumberedSubstringVector(&vec_num).match();

  if (count == 0)
    return std::string{};

  return vec_num[0][0];
}

/*
  Returns the position of the first occurrence of the regular expression pattern in the string subject, or 0
  if pattern was not found. https://mariadb.com/kb/en/regexp_instr/
*/
std::string Func_regexp_instr::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& ct)

{
  RegExpParams param = getEpressionAndPattern(row, fp, isNull, ct, ct.getTimeZone());

  if (isNull)
    return std::string{};

  const PCREOptions& options = pcreOptions(ct);
  jp::Regex re(param.pattern, options.flags);
  jp::RegexMatch rm(&re);
  jpcre2::VecOff vec_soff;

  size_t count = rm.setSubject(param.expression).setMatchStartOffsetVector(&vec_soff).match();

  if (count == 0)
    return "0";

  size_t offset = vec_soff[0];
  size_t charNumber = ct.getCharset()->numchars(param.expression.c_str(), param.expression.c_str() + offset);

  return std::to_string(charNumber + 1);
}

/*
  https://mariadb.com/kb/en/regexp/
*/
bool Func_regexp::getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             CalpontSystemCatalog::ColType& ct)
{
  RegExpParams param = getEpressionAndPattern(row, fp, isNull, ct, ct.getTimeZone());

  if (isNull)
    return false;

  const PCREOptions& options = pcreOptions(ct);
  jp::Regex re(param.pattern, options.flags);
  return re.match(param.expression);
}

}  // namespace funcexp
