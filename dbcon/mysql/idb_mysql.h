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
#include "tztime.h"
#include "derived_handler.h"
#include "select_handler.h"

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

class MIGR
{
    public:
        enum infinidb_state
          {
            INFINIDB_INIT_CONNECT = 0,      // intend to use to drop leftover vtable when logon. not being used now.
            INFINIDB_INIT,
            INFINIDB_CREATE_VTABLE,
            INFINIDB_ALTER_VTABLE,
            INFINIDB_SELECT_VTABLE,
            INFINIDB_DROP_VTABLE,
            INFINIDB_DISABLE_VTABLE,
            INFINIDB_REDO_PHASE1,       // post process requires to re-create vtable
            INFINIDB_ORDER_BY,          // for InfiniDB handler to ignore the 2nd scan for order by
            INFINIDB_REDO_QUERY,        // redo query with the normal mysql path
            INFINIDB_ERROR_REDO_PHASE1,
            INFINIDB_ERROR = 32,
          };
          struct INFINIDB_VTABLE
          {
            String original_query;
            String create_vtable_query;
            String alter_vtable_query;
            String select_vtable_query;
            String drop_vtable_query;
            String insert_vtable_query;
            infinidb_state vtable_state;  // flag for InfiniDB MySQL virtual table structure
            bool autoswitch;
            bool has_order_by;
            bool duplicate_field_name; // @bug 1928. duplicate field name in create_phase will be ingored.
            bool call_sp;
            bool override_largeside_estimate;
            void* cal_conn_info;
            bool isUnion;
            bool impossibleWhereOnUnion;
            bool isInsertSelect;
            bool isUpdateWithDerive;
            bool isInfiniDBDML; // default false
            bool hasInfiniDBTable; // default false
            bool isNewQuery;
            INFINIDB_VTABLE() : cal_conn_info(NULL) {init();}
            void init()
            {
                //vtable_state = INFINIDB_INIT_CONNECT;
                vtable_state = INFINIDB_DISABLE_VTABLE;
                autoswitch = false;
                has_order_by = false;
                duplicate_field_name = false;
                call_sp = false;
                override_largeside_estimate = false;
                isUnion = false;
                impossibleWhereOnUnion = false;
                isUpdateWithDerive = false;
                isInfiniDBDML = false;
                hasInfiniDBTable = false;
                isNewQuery = true;
            }
          };

      static INFINIDB_VTABLE infinidb_vtable;                  // InfiniDB custom structure

};


#endif
// vim:ts=4 sw=4:

