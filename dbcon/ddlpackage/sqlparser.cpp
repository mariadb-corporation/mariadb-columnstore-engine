/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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

/***********************************************************************
 *   $Id: sqlparser.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

#include <fstream>
#include <errno.h>

#define DDLPKGSQLPARSER_DLLEXPORT
#include "sqlparser.h"
#undef DDLPKGSQLPARSER_DLLEXPORT

#ifdef _MSC_VER
#include "ddl-gram-win.h"
#else
#include "ddl-gram.h"
#endif

void scanner_finish(void* yyscanner);
void scanner_init(const char* str, void* yyscanner);
int ddllex_init_extra(void* user_defined, void** yyscanner);
int ddllex_destroy(void* yyscanner);
int ddlparse(ddlpackage::pass_to_bison* x);
void set_schema(std::string schema);
namespace ddlpackage
{
using namespace std;

SqlParser::SqlParser() : fStatus(-1), fDebug(false), x(&fParseTree)
{
}

void SqlParser::SetDebug(bool debug)
{
  fDebug = debug;
}

void SqlParser::setDefaultSchema(std::string schema)
{
  x.fDBSchema = schema;
}

void SqlParser::setDefaultCharset(const CHARSET_INFO* default_charset)
{
  x.default_table_charset = default_charset;
}

int SqlParser::Parse(const char* sqltext)
{
  ddllex_init_extra(&scanData, &x.scanner);
  scanner_init(sqltext, x.scanner);
  fStatus = ddlparse(&x);
  return fStatus;
}

const ParseTree& SqlParser::GetParseTree(void)
{
  if (!Good())
  {
    throw logic_error("The ParseTree is invalid");
  }

  return fParseTree;
}

bool SqlParser::Good()
{
  return fStatus == 0;
}

SqlParser::~SqlParser()
{
  scanner_finish(x.scanner);  // free scanner allocated memory
  ddllex_destroy(x.scanner);
}

SqlFileParser::SqlFileParser() : SqlParser()
{
}

int SqlFileParser::Parse(const string& sqlfile)
{
  fStatus = -1;

  ifstream ifsql;
  ifsql.open(sqlfile.c_str());

  if (!ifsql.is_open())
  {
    perror(sqlfile.c_str());
    return fStatus;
  }

  char sqlbuf[1024 * 1024];
  unsigned length;
  ifsql.seekg(0, ios::end);
  length = ifsql.tellg();
  ifsql.seekg(0, ios::beg);

  if (length > sizeof(sqlbuf) - 1)
  {
    throw length_error("SqlFileParser has file size hard limit of 16K.");
  }

  std::streamsize rcount;
  rcount = ifsql.readsome(sqlbuf, sizeof(sqlbuf) - 1);

  if (rcount < 0)
    return fStatus;

  sqlbuf[rcount] = 0;

  // cout << endl << sqlfile << "(" << rcount << ")" << endl;
  // cout << "----------------------" << endl;
  // cout << sqlbuf << endl;

  return SqlParser::Parse(sqlbuf);
}
}  // namespace ddlpackage
