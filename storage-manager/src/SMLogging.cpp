/* Copyright (C) 2019 MariaDB Corporation

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

#include <stdarg.h>
#include <unistd.h>
#include "SMLogging.h"

using namespace std;

namespace
{
storagemanager::SMLogging* smLog = NULL;
boost::mutex m;
};  // namespace

namespace storagemanager
{
SMLogging::SMLogging()
{
  // TODO: make this configurable
  setlogmask(LOG_UPTO(LOG_DEBUG));
  openlog("StorageManager", LOG_PID | LOG_NDELAY | LOG_PERROR | LOG_CONS, LOG_LOCAL1);
}

SMLogging::~SMLogging()
{
  syslog(LOG_INFO, "CloseLog");
  closelog();
}

SMLogging* SMLogging::get()
{
  if (smLog)
    return smLog;
  boost::mutex::scoped_lock s(m);
  if (smLog)
    return smLog;
  smLog = new SMLogging();
  return smLog;
}

void SMLogging::log(int priority, const char* format, ...)
{
  va_list args;
  va_start(args, format);

#ifdef DEBUG
  va_list args2;
  va_copy(args2, args);
  vfprintf(stderr, format, args2);
  fprintf(stderr, "\n");
  va_end(args2);
#endif
  vsyslog(priority, format, args);

  va_end(args);
}

}  // namespace storagemanager
