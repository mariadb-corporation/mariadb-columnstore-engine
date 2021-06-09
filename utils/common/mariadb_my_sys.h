/*
   Copyright (C) 2020 MariaDB Corporation

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
#ifndef MARIADB_MY_SYS_H_INCLUDED
#define MARIADB_MY_SYS_H_INCLUDED

// These are the common headers needed to use the MariaDB mysys library.

// This must be included after any boost headers, or anything that includes
// boost headers. <mariadb.h> and boost are not friends.

#ifndef TEST_MCSCONFIG_H
#error mcscofig.h was not included
#endif

#include "mcsconfig_conflicting_defs_remember.h"
#include "mcsconfig_conflicting_defs_undef.h"

#include <mariadb.h>
#undef set_bits  // mariadb.h defines set_bits, which is incompatible with boost
#include <my_sys.h>

#include "mcsconfig_conflicting_defs_restore.h"

#endif
