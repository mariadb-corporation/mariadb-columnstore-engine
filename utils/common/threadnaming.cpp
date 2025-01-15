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

#include <sys/prctl.h>
#include "threadnaming.h"

namespace utils
{
void setThreadName(const char* threadName)
{
#ifdef __FreeBSD__
  pthread_set_name_np(pthread_self(), threadName);
#else
  prctl(PR_SET_NAME, threadName, 0, 0, 0);
#endif
}

std::string getThreadName()
{
  char buf[32];
#ifdef __FreeBSD__
  pthread_get_name_np(pthread_self(), buf, sizeof(buf));
#else
  prctl(PR_GET_NAME, buf, 0, 0, 0);
#endif
  return std::string(buf);
}
}  // namespace utils
