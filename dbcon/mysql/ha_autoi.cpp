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

/*
 * $Id$
 */
#include <regex>
#include <string>
#include <boost/algorithm/string.hpp>
#include <utils/loggingcpp/idberrorinfo.h>

bool parseAutoincrementTableComment(std::string comment, uint64_t& startValue, std::string& columnName)
{
  boost::algorithm::to_upper(comment);
  std::regex compat("[[:space:]]*AUTOINCREMENT[[:space:]]*=[[:space:]]*", std::regex_constants::extended);
  bool autoincrement = false;
  columnName = "";
  std::match_results<std::string::const_iterator> what;
  std::string::const_iterator start, end;
  start = comment.begin();
  end = comment.end();
  std::regex_constants::match_flag_type flags = std::regex_constants::match_default;

  if (std::regex_search(start, end, what, compat, flags))
  {
    if (what[0].matched)
    {
      // string params (what[0].first, what[0].second);
      std::string params(&(*(what[0].second)));
      unsigned i = params.find_first_of(",");

      if (i <= params.length())
      {
        // check whether there is more autoincrement column
        std::string restComment = params.substr(i + 1, params.length());
        start = restComment.begin();
        end = restComment.end();

        if (std::regex_search(start, end, what, compat, flags))
        {
          if (what[0].matched)
            throw runtime_error(logging::IDBErrorInfo::instance()->errorMsg(ERR_INVALID_NUMBER_AUTOINCREMENT));
        }

        columnName = params.substr(0, i);
        string startVal = params.substr(i + 1, params.length());
        // get rid of possible empty space
        i = startVal.find_first_not_of(" ");

        if (i <= startVal.length())
        {
          startVal = startVal.substr(i, startVal.length());
          //; is the seperator between compression and autoincrement comments.
          i = startVal.find_first_of(";");

          if (i <= startVal.length())
          {
            startVal = startVal.substr(0, i);
          }

          i = startVal.find_last_not_of(" ");

          if (i <= startVal.length())
          {
            startVal = startVal.substr(0, i + 1);
          }

          errno = 0;
          char* ep = NULL;
          const char* str = startVal.c_str();
          startValue = strtoull(str, &ep, 10);

          //  (no digits) || (more chars)  || (other errors & value = 0)
          if ((ep == str) || (*ep != '\0') || (errno != 0))
          {
            throw runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_INVALID_START_VALUE));
          }
        }
      }
      else
      {
        columnName = params;
      }

      autoincrement = true;
    }
  }

  if (columnName.compare("") != 0)
  {
    // get rid of possible empty space
    unsigned i = columnName.find_last_not_of(" ");

    if (i <= columnName.length())
    {
      columnName = columnName.substr(0, i + 1);
    }
  }

  return autoincrement;
}

bool parseAutoincrementColumnComment(std::string comment, uint64_t& startValue)
{
  boost::algorithm::to_upper(comment);
  std::regex compat("[[:space:]]*AUTOINCREMENT[[:space:]]*", std::regex_constants::extended);
  bool autoincrement = false;
  std::match_results<std::string::const_iterator> what;
  std::string::const_iterator start, end;
  start = comment.begin();
  end = comment.end();
  std::regex_constants::match_flag_type flags = std::regex_constants::match_default;

  if (std::regex_search(start, end, what, compat, flags))
  {
    if (what[0].matched)
    {
      std::string params(&(*(what[0].second)));
      unsigned i = params.find_first_of(",");

      if (i <= params.length())
      {
        std::string startVal = params.substr(i + 1, params.length());
        // get rid of possible empty space
        i = startVal.find_first_not_of(" ");

        if (i <= startVal.length())
        {
          startVal = startVal.substr(i, startVal.length());
          //; is the seperator between compression and autoincrement comments.
          i = startVal.find_first_of(";");

          if (i <= startVal.length())
          {
            startVal = startVal.substr(0, i);
          }

          i = startVal.find_last_not_of(" ");

          if (i <= startVal.length())
          {
            startVal = startVal.substr(0, i + 1);
          }

          errno = 0;
          char* ep = NULL;
          const char* str = startVal.c_str();
          startValue = strtoll(str, &ep, 10);

          //  (no digits) || (more chars)  || (other errors & value = 0)
          if ((ep == str) || (*ep != '\0') || (errno != 0))
          {
            throw runtime_error(logging::IDBErrorInfo::instance()->errorMsg(ERR_INVALID_START_VALUE));
          }
        }
      }
      else
      {
        startValue = 1;
      }

      autoincrement = true;
    }
  }

  return autoincrement;
}

