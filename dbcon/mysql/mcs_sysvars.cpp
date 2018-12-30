/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporaton

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

#include <my_config.h>
#include "idb_mysql.h"
#include "mcs_sysvars.h"

const char* mcs_compression_type_names[] = {
    "NO_COMPRESSION",
    "SNAPPY",
    NullS
};

static TYPELIB mcs_compression_type_names_lib = {
    array_elements(mcs_compression_type_names) - 1,
    "mcs_compression_type_names",
    mcs_compression_type_names,
    NULL
};

// compression type
static MYSQL_THDVAR_ENUM(
    compression_type,
    PLUGIN_VAR_RQCMDARG,
    "Controls compression type for create tables. Possible values are: "
    "NO_COMPRESSION segment files aren't compressed; "
    "SNAPPY segment files are Snappy compressed (default);",
    NULL,
    NULL,
    SNAPPY,
    &mcs_compression_type_names_lib);

// original query
static MYSQL_THDVAR_STR(
  original_query, /* name */
  PLUGIN_VAR_MEMALLOC |
  PLUGIN_VAR_RQCMDARG,
  "Original query text", /* comment */
  NULL, /* check */
  NULL, /* update */
  NULL /* def */
);

// fe_conn_info pointer
static MYSQL_THDVAR_ULONGLONG(
    fe_conn_info_ptr,
    PLUGIN_VAR_NOSYSVAR | PLUGIN_VAR_NOCMDOPT,
    "FrontEnd connection structure pointer. For internal usage.",
    NULL,
    NULL,
    0,
    0,
    ~0U,
    1
);

st_mysql_sys_var* mcs_system_variables[] =
{
  MYSQL_SYSVAR(compression_type),
  MYSQL_SYSVAR(original_query),
  MYSQL_SYSVAR(fe_conn_info_ptr),
  NULL
};

const char* get_original_query(THD* thd) {
    return THDVAR(thd, original_query);
}

void set_original_query(THD* thd, char* query) {
    THDVAR(thd, original_query) = query;
}

void* get_fe_conn_info_ptr()
{
    return ( current_thd == NULL ) ? NULL :
        (void*)THDVAR(current_thd, fe_conn_info_ptr);
}

void set_fe_conn_info_ptr(void* ptr)
{
    if ( current_thd == NULL )
    {
        return;
    }
    
    THDVAR(current_thd, fe_conn_info_ptr) = (uint64_t)(ptr);
}

/*ha_calpont* get_legacy_handler(mcs_handler_info mcs_hndl_ptr)
{
    //MCOL-1101 Add handler type check
    //hndl_ptr.hndl_type == LEGACY )
    ha_calpont* hndl;
    if ( mcs_hndl_ptr.hndl_ptr != NULL )
    {
        hndl = (ha_calpont*)(mcs_hndl_ptr.hndl_ptr);
    }
    else
    {
        hndl = new ha_calpont();
        hndl->fe_conn_info = (void*)THDVAR(current_thd, fe_conn_info_ptr);
    }
    
    return hndl;
}*/

mcs_compression_type_t get_compression_type(THD* thd) {
    return (mcs_compression_type_t) THDVAR(thd, compression_type);
}
