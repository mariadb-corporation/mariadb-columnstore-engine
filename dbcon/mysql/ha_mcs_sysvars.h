/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
#include "mcsconfig.h"

extern st_mysql_sys_var* mcs_system_variables[];
extern st_mysql_show_var mcs_status_variables[];
extern char cs_version[];
extern char cs_commit_hash[];

// compression_type
enum mcs_compression_type_t
{
  NO_COMPRESSION = 0,
  SNAPPY = 2,
#ifdef HAVE_LZ4
  LZ4 = 3
#endif
};

// use_import_for_batchinsert mode
enum class mcs_use_import_for_batchinsert_mode_t
{
  OFF = 0,
  ON = 1,
  ALWAYS = 2
};

// select_handler mode
enum class mcs_select_handler_mode_t
{
  OFF = 0,
  ON = 1,
  AUTO = 2
};

// simple setters/getters
const char* get_original_query(THD* thd);
void set_original_query(THD* thd, char* query);

mcs_compression_type_t get_compression_type(THD* thd);
void set_compression_type(THD* thd, ulong value);

void* get_fe_conn_info_ptr(THD* thd = NULL);
void set_fe_conn_info_ptr(void* ptr, THD* thd = NULL);

ulonglong get_original_optimizer_flags(THD* thd = NULL);
void set_original_optimizer_flags(ulonglong ptr, THD* thd = NULL);

ulonglong get_original_option_bits(THD* thd = NULL);
void set_original_option_bits(ulonglong value, THD* thd = NULL);

mcs_select_handler_mode_t get_select_handler_mode(THD* thd);
void set_select_handler_mode(THD* thd, ulong value);

bool get_derived_handler(THD* thd);
void set_derived_handler(THD* thd, bool value);

bool get_group_by_handler(THD* thd);
void set_group_by_handler(THD* thd, bool value);

bool get_select_handler_in_stored_procedures(THD* thd);
void set_select_handler_in_stored_procedures(THD* thd, bool value);

uint get_orderby_threads(THD* thd);
void set_orderby_threads(THD* thd, uint value);

bool get_use_decimal_scale(THD* thd);
void set_use_decimal_scale(THD* thd, bool value);

ulong get_decimal_scale(THD* thd);
void set_decimal_scale(THD* thd, ulong value);

bool get_ordered_only(THD* thd);
void set_ordered_only(THD* thd, bool value);

ulong get_string_scan_threshold(THD* thd);
void set_string_scan_threshold(THD* thd, ulong value);

ulong get_stringtable_threshold(THD* thd);
void set_stringtable_threshold(THD* thd, ulong value);

ulong get_diskjoin_smallsidelimit(THD* thd);
void set_diskjoin_smallsidelimit(THD* thd, ulong value);

ulong get_diskjoin_largesidelimit(THD* thd);
void set_diskjoin_largesidelimit(THD* thd, ulong value);

ulong get_diskjoin_bucketsize(THD* thd);
void set_diskjoin_bucketsize(THD* thd, ulong value);

ulong get_um_mem_limit(THD* thd);
void set_um_mem_limit(THD* thd, ulong value);

bool get_varbin_always_hex(THD* thd);
void set_varbin_always_hex(THD* thd, bool value);

bool get_double_for_decimal_math(THD* thd);
void set_double_for_decimal_math(THD* thd, bool value);

bool get_decimal_overflow_check(THD* thd);
void set_decimal_overflow_check(THD* thd, bool value);

ulong get_local_query(THD* thd);
void set_local_query(THD* thd, ulong value);

mcs_use_import_for_batchinsert_mode_t get_use_import_for_batchinsert_mode(THD* thd);
void set_use_import_for_batchinsert_mode(THD* thd, ulong value);

ulong get_import_for_batchinsert_delimiter(THD* thd);
void set_import_for_batchinsert_delimiter(THD* thd, ulong value);

ulong get_import_for_batchinsert_enclosed_by(THD* thd);
void set_import_for_batchinsert_enclosed_by(THD* thd, ulong value);

bool get_replication_slave(THD* thd);
void set_replication_slave(THD* thd, bool value);

bool get_cache_inserts(THD* thd);
void set_cache_inserts(THD* thd, bool value);

bool get_cache_use_import(THD* thd);
void set_cache_use_import(THD* thd, bool value);

ulonglong get_cache_flush_threshold(THD* thd);
void set_cache_flush_threshold(THD* thd, ulonglong value);

#endif
