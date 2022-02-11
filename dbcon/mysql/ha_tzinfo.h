/* Copyright (C) 2021 MariaDB Corporation

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

#ifndef HA_TZINFO
#define HA_TZINFO

//#undef LOG_INFO
#include <my_config.h>
#include "idb_mysql.h"
#include "dataconvert.h"
using namespace dataconvert;

namespace cal_impl_if
{
TIME_ZONE_INFO* my_tzinfo_find(THD* thd, const String* name);

}

#endif
