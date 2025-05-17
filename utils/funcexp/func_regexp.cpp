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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "utils/pcre2/jpcre2.hpp"
#pragma GCC diagnostic pop

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

using jp = jpcre2::select<char>;

struct PCREOptions
{
  PCREOptions(execplan::CalpontSystemCatalog::ColType& ct);

  datatypes::Charset dataCharset = my_charset_utf8mb3_general_ci;
  datatypes::Charset libraryCharset = my_charset_utf8mb3_general_ci;
  jpcre2::Uint flags = 0;
  bool conversionIsNeeded = false;
};

PCREOptions::PCREOptions(execplan::CalpontSystemCatalog::ColType& ct)
{
  datatypes::Charset cs = ct.getCharset();
  datatypes::Charset myCharsetBin = my_charset_bin;

  // TODO use system variable instead if hardcode default_regex_flags_pcre(_current_thd());
  // PCRE2_DOTALL | PCRE2_DUPNAMES | PCRE2_EXTENDED | PCRE2_EXTENDED_MORE | PCRE2_MULTILINE | PCRE2_UNGREEDY;

  jpcre2::Uint defaultFlags = 0;

  flags = (cs != myCharsetBin ? (PCRE2_UTF | PCRE2_UCP) : 0) |
          ((cs.getCharset().state & (MY_CS_BINSORT | MY_CS_CSSORT)) ? 0 : PCRE2_CASELESS) | defaultFlags;

  // Convert text data to utf-8.
  dataCharset = cs;
  libraryCharset = cs == myCharsetBin ? my_charset_bin : my_charset_utf8mb3_general_ci;
}

struct RegExpParams
{
  std::string expression;
  std::string pattern;
  RegExpParams& CharsetFix(const PCREOptions options)
  {
    if (options.conversionIsNeeded)
      return *this;

    expression = options.libraryCharset.convert(expression, options.dataCharset);
    pattern = options.libraryCharset.convert(pattern, options.dataCharset);
    return *this;
  }
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
CalpontSystemCatalog::ColType Func_regexp_replace::operationType(
    FunctionParm& fp, CalpontSystemCatalog::ColType& /*resultType*/)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp_substr::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& /*resultType*/)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp_instr::operationType(FunctionParm& fp,
                                                               CalpontSystemCatalog::ColType& /*resultType*/)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

CalpontSystemCatalog::ColType Func_regexp::operationType(FunctionParm& /*fp*/,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
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

  const auto& replaceWith = fp[2]->data()->getStrVal(row, isNull);

  if (replaceWith.isNull())
    return param.expression;

  PCREOptions options(ct);
  param.CharsetFix(options);
  jp::Regex re(param.pattern, options.flags);

  const auto& replaceWithStr = replaceWith.unsafeStringRef();
  if (options.conversionIsNeeded)
  {
    const auto& convertedReplaceToken = options.libraryCharset.convert(replaceWithStr, options.dataCharset);
    return re.replace(param.expression, convertedReplaceToken, "g");
  }

  return re.replace(param.expression, replaceWithStr, "g");
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

  PCREOptions options(ct);
  param.CharsetFix(options);

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

  PCREOptions options(ct);
  param.CharsetFix(options);

  jp::Regex re(param.pattern, options.flags);
  jp::RegexMatch rm(&re);
  jpcre2::VecOff vec_soff;

  size_t count = rm.setSubject(param.expression).setMatchStartOffsetVector(&vec_soff).match();

  if (count == 0)
    return "0";

  size_t offset = vec_soff[0];
  size_t charNumber = options.libraryCharset.getCharset().numchars(param.expression.c_str(),
                                                                   param.expression.c_str() + offset);

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

  PCREOptions options(ct);
  param.CharsetFix(options);

  jp::Regex re(param.pattern, options.flags);
  return re.match(param.expression);
}

}  // namespace funcexp
