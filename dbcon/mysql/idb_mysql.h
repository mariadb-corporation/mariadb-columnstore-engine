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
//One include file to deal with all the MySQL pollution of the
//  global namespace
//
// Don't include ANY mysql headers anywhere except here!
#ifndef IDB_MYSQL_H__
#define IDB_MYSQL_H__

#ifdef _MSC_VER
#include <stdint.h>
#if _MSC_VER >= 1800
template <class T> bool isnan(T);
#endif
#endif

#define MYSQL_SERVER 1 //needed for definition of struct THD in mysql_priv.h
#define USE_CALPONT_REGEX

#undef LOG_INFO

#ifdef _DEBUG
#ifndef _MSC_VER
#ifndef SAFE_MUTEX
#define SAFE_MUTEX
#endif
#ifndef SAFEMALLOC
#define SAFEMALLOC
#endif
#endif
#ifndef ENABLED_DEBUG_SYNC
#define ENABLED_DEBUG_SYNC
#endif
//#define INFINIDB_DEBUG
#define DBUG_ON 1
#undef  DBUG_OFF
#else
#undef SAFE_MUTEX
#undef SAFEMALLOC
#undef ENABLED_DEBUG_SYNC
#undef DBUG_ON
#define DBUG_OFF 1
#endif
#ifdef _MSC_VER
#define MYSQL_DYNAMIC_PLUGIN
#define DONT_DEFINE_VOID
#ifdef ETIMEDOUT
#undef ETIMEDOUT
#endif
#endif

#include "sql_plugin.h"
#include "sql_table.h"
#include "sql_select.h"
#include "mysqld_error.h"
#include "item_windowfunc.h"
#include "sql_cte.h"

// Now clean up the pollution as best we can...
#undef min
#undef max
#undef UNKNOWN
#undef test
#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_mutex_wait
#undef pthread_mutex_timedwait
#undef pthread_mutex_t
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_mutex_trylock
#undef sleep
#undef getpid
#undef SIZEOF_LONG
#undef PACKAGE_VERSION
#undef PACKAGE_TARNAME
#undef PACKAGE_STRING
#undef PACKAGE_NAME
#undef PACKAGE_BUGREPORT
#undef DEBUG
#undef set_bits

namespace
{
inline char* idb_mysql_query_str(THD* thd)
{
#if MYSQL_VERSION_ID >= 50172
    return thd->query();
#else
    return thd->query;
#endif
}
}

#endif
// vim:ts=4 sw=4:

