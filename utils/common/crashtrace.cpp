/* Copyright (C) 2018 MariaDB Corporation

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

#include <execinfo.h>
#include <errno.h>
#include <cstdio>
#include <ctime>
#include <cstring>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include "mcsconfig.h"

void fatalHandler(int sig)
{
  char filename[128];
  void* addrs[128];
  snprintf(filename, 128, "%s/trace/%s.%d.log", MCSLOGDIR, program_invocation_short_name, getpid());
  FILE* logfile = fopen(filename, "w");
  char s[30];
  struct tm tim;
  time_t now;
  now = time(NULL);
  tim = *(localtime(&now));
  strftime(s, 30, "%F %T", &tim);
  fprintf(logfile, "Date/time: %s\n", s);
  fprintf(logfile, "Signal: %d\n\n", sig);
  fflush(logfile);
  int fd = fileno(logfile);
  int count = backtrace(addrs, sizeof(addrs) / sizeof(addrs[0]));
  backtrace_symbols_fd(addrs, count, fd);
  fclose(logfile);
  struct sigaction sigact;
  memset(&sigact, 0, sizeof(sigact));
  sigact.sa_handler = SIG_DFL;
  sigaction(sig, &sigact, NULL);
  raise(sig);
}
