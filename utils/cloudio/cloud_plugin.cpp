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

#include "IDBFactory.h"
#include "SMFileFactory.h"
#include "SMFileSystem.h"

using namespace idbdatafile;

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreturn-type-c-linkage"
#endif

extern "C"
{
  FileFactoryEnt plugin_instance()
  {
    return FileFactoryEnt(IDBDataFile::CLOUD, "cloud", new SMFileFactory(), new SMFileSystem());
  }
}

#ifdef __clang__
#pragma clang diagnostic pop
#endif
