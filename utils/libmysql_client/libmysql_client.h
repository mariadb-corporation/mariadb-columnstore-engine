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

#ifndef UTILS_LIBMYSQL_CL_H
#define UTILS LIBMYSQL_CL_H

#include <my_config.h>
#include <mysql.h>

#include <boost/scoped_array.hpp>

namespace utils
{
class LibMySQL
{
 public:
  LibMySQL();
  ~LibMySQL();

  // init:   host          port        username      passwd         db
  int init(const char*, unsigned int, const char*, const char*, const char*);

  // run the query
  int run(const char* q, bool resultExpected = true);

  void handleMySqlError(const char*, int);

  MYSQL* getMySqlCon()
  {
    return fCon;
  }
  int getFieldCount()
  {
    return mysql_num_fields(fRes);
  }
  int getRowCount()
  {
    return mysql_num_rows(fRes);
  }
  char** nextRow()
  {
    char** row = mysql_fetch_row(fRes);
    fieldLengths = mysql_fetch_lengths(fRes);
    fFields = mysql_fetch_fields(fRes);
    return row;
  }
  long getFieldLength(int field)
  {
    return fieldLengths[field];
  }
  MYSQL_FIELD* getField(int field)
  {
    return &fFields[field];
  }
  const std::string& getError()
  {
    return fErrStr;
  }
  unsigned int getErrno()
  {
    return mysql_errno(fCon);
  }
  const char* getErrorMsg()
  {
    return mysql_error(fCon);
  }

 private:
  MYSQL* fCon;
  MYSQL_RES* fRes;
  MYSQL_FIELD* fFields;
  std::string fErrStr;
  unsigned long* fieldLengths;
};

}  // namespace utils

#endif  // UTILS_LIBMYSQL_CL_H

// vim:ts=4 sw=4:
