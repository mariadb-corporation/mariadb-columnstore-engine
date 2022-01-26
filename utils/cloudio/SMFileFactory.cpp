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

#include <sys/stat.h>
#include <fcntl.h>
#include "SMFileFactory.h"
#include "SMDataFile.h"
#include "SMComm.h"
#include "BufferedFile.h"
#include "IDBDataFile.h"

using namespace std;

namespace idbdatafile
{
IDBDataFile* SMFileFactory::open(const char* filename, const char* mode, unsigned opts, unsigned colWidth)
{
  // TODO, test whether this breaks anything.
  // if (opts & IDBDataFile::USE_TMPFILE)
  //    return new BufferedFile(filename, mode, opts);

  bool _read = false;
  bool _write = false;
  bool create = false;
  bool truncate = false;
  bool append = false;

  // strip 'b' chars from mode
  char newmode[8] = {'\0'};  // there'd better not be 7 chars in the mode string
  int i = 0;
  for (const char* c = mode; *c != '\0' && i < 8; c++)
    if (*c != 'b')
      newmode[i++] = *c;
  if (i == 8)
  {
    errno = EINVAL;
    return NULL;
  }

  // parse the new mode string
  if (newmode[0] == 'r')
  {
    _read = true;
    if (newmode[1] == '+')
      _write = true;
  }
  else if (newmode[0] == 'w')
  {
    _write = true;
    truncate = true;
    create = true;
    if (newmode[1] == '+')
      _read = true;
  }
  else if (newmode[0] == 'a')
  {
    _write = true;
    create = true;
    append = true;
    if (newmode[1] == '+')
      _read = true;
  }
  else
  {
    errno = EINVAL;
    return NULL;
  }

  // turn newmode into posix flags
  uint posix_flags = 0;
  if (_read && _write)
    posix_flags |= O_RDWR;
  else if (_read)
    posix_flags |= O_RDONLY;
  else if (_write)
    posix_flags |= O_WRONLY;

  posix_flags |= (create ? O_CREAT : 0);
  posix_flags |= (truncate ? O_TRUNC : 0);
  posix_flags |= (append ? O_APPEND : 0);

  SMComm* comm = SMComm::get();
  struct stat _stat;
  int err = comm->open(filename, posix_flags, &_stat);
  if (err)
    return NULL;

  SMDataFile* ret = new SMDataFile(filename, posix_flags, _stat);
  return ret;
}

}  // namespace idbdatafile
