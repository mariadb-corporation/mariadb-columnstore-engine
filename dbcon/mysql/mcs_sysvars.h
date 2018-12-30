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

#ifndef MCS_SYSVARS_H__
#define MCS_SYSVARS_H__

#include <my_config.h>
#include "idb_mysql.h"

extern st_mysql_sys_var* mcs_system_variables[];

enum mcs_handler_types_t
{
    SELECT,
    GROUP_BY,
    LEGACY
};

struct mcs_handler_info
{
    mcs_handler_info() : hndl_ptr(NULL), hndl_type(LEGACY) { };
    mcs_handler_info(mcs_handler_types_t type) : hndl_ptr(NULL), hndl_type(type) { };
    mcs_handler_info(void* ptr, mcs_handler_types_t type) : hndl_ptr(ptr), hndl_type(type) { };
    ~mcs_handler_info() { };
    void* hndl_ptr;
    mcs_handler_types_t hndl_type;
};

// compression_type
enum mcs_compression_type_t {
    NO_COMPRESSION = 0,
    SNAPPY = 2
};

// simple setters/getters
const char* get_original_query(THD* thd);
void set_original_query(THD* thd, char* query);
mcs_compression_type_t get_compression_type(THD* thd);

void* get_fe_conn_info_ptr();
void set_fe_conn_info_ptr(void* ptr);

#endif
