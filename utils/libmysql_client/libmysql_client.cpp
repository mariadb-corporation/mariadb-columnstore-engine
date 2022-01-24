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

#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include "errorids.h"
#include "exceptclasses.h"
#include "configcpp.h"

#include "libmysql_client.h"

namespace utils
{
LibMySQL::LibMySQL() : fCon(NULL), fRes(NULL)
{
}

LibMySQL::~LibMySQL()
{
  if (fRes)
  {
    mysql_free_result(fRes);
  }

  fRes = NULL;

  if (fCon)
  {
    mysql_close(fCon);
  }

  fCon = NULL;
}

int LibMySQL::init(const char* h, unsigned int p, const char* u, const char* w, const char* d)
{
  int ret = 0;

  fCon = mysql_init(NULL);

  config::Config* cf = config::Config::makeConfig();
  const string TLSCA = cf->getConfig("CrossEngineSupport", "TLSCA");
  const string TLSClientCert = cf->getConfig("CrossEngineSupport", "TLSClientCert");
  const string TLSClientKey = cf->getConfig("CrossEngineSupport", "TLSClientKey");

  if (!(TLSCA.empty() || TLSClientCert.empty() || TLSClientKey.empty()))
  {
    mysql_ssl_set(fCon, TLSClientKey.c_str(), TLSClientCert.c_str(), TLSCA.c_str(), NULL, NULL);
  }

  if (fCon != NULL)
  {
    unsigned int tcp_option = MYSQL_PROTOCOL_TCP;
    mysql_options(fCon, MYSQL_OPT_PROTOCOL, &tcp_option);

    if (mysql_real_connect(fCon, h, u, w, d, p, NULL, 0) == NULL)
    {
      fErrStr = "fatal error running mysql_real_connect() in libmysql_client lib";
      ret = mysql_errno(fCon);
    }
    else
    {
      mysql_set_character_set(fCon, "utf8");
    }
  }
  else
  {
    fErrStr = "fatal error running mysql_init() in libmysql_client lib";
    ret = -1;
  }

  return ret;
}

int LibMySQL::run(const char* query, bool resultExpected)
{
  int ret = 0;

  if (mysql_real_query(fCon, query, strlen(query)) != 0)
  {
    fErrStr = "fatal error runing mysql_real_query() in libmysql_client lib";
    ret = -1;
    return ret;
  }

  fRes = mysql_use_result(fCon);

  if (fRes == NULL && resultExpected)
  {
    fErrStr = "fatal error running mysql_use_result() or empty result set in libmysql_client lib";
    ret = -1;
  }

  return ret;
}

void LibMySQL::handleMySqlError(const char* errStr, int errCode)
{
  ostringstream oss;

  if (getErrno())
  {
    oss << errStr << " (" << getErrno() << ")";
    oss << " (" << getErrorMsg() << ")";
  }
  else
  {
    oss << errStr << " (" << errCode << ")";
    oss << " (unknown)";
  }

  throw logging::IDBExcept(oss.str(), logging::ERR_CROSS_ENGINE_CONNECT);

  return;
}

}  // namespace utils
