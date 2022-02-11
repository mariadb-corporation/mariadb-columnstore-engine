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

#include <my_config.h>
#include "idb_mysql.h"
#include "ha_mcs_sysvars.h"
#include "mcsconfig.h"

const char* mcs_compression_type_names[] = {"SNAPPY",  // 0
                                            "SNAPPY",  // 1
                                            "SNAPPY",  // 2
#ifdef HAVE_LZ4
                                            "LZ4",  // 3
#endif
                                            NullS};

static TYPELIB mcs_compression_type_names_lib = {array_elements(mcs_compression_type_names) - 1,
                                                 "mcs_compression_type_names", mcs_compression_type_names,
                                                 NULL};

// compression type
static MYSQL_THDVAR_ENUM(compression_type, PLUGIN_VAR_RQCMDARG,
                         "Controls compression algorithm for create tables. Possible values are: "
                         "SNAPPY segment files are Snappy compressed (default);"
#ifdef HAVE_LZ4
                         "LZ4 segment files are LZ4 compressed;",
#endif
                         NULL,                              // check
                         NULL,                              // update
                         1,                                 // default
                         &mcs_compression_type_names_lib);  // values lib

// fe_conn_info pointer
static MYSQL_THDVAR_ULONGLONG(fe_conn_info_ptr, PLUGIN_VAR_NOSYSVAR | PLUGIN_VAR_NOCMDOPT,
                              "FrontEnd connection structure pointer. For internal usage.", NULL, NULL, 0, 0,
                              ~0U, 1);

// optimizer flags vault
static MYSQL_THDVAR_ULONGLONG(original_optimizer_flags, PLUGIN_VAR_NOSYSVAR | PLUGIN_VAR_NOCMDOPT,
                              "Vault for original optimizer flags. For internal usage.", NULL, NULL, 0, 0,
                              ~0U, 1);

static MYSQL_THDVAR_ULONGLONG(original_option_bits, PLUGIN_VAR_NOSYSVAR | PLUGIN_VAR_NOCMDOPT,
                              "Storage for thd->variables.option_bits. For internal usage.", NULL, NULL, 0, 0,
                              ~0U, 1);

const char* mcs_select_handler_mode_values[] = {"OFF", "ON", "AUTO", NullS};

static TYPELIB mcs_select_handler_mode_values_lib = {array_elements(mcs_select_handler_mode_values) - 1,
                                                     "mcs_select_handler_mode_values",
                                                     mcs_select_handler_mode_values, NULL};

static MYSQL_THDVAR_ENUM(select_handler, PLUGIN_VAR_RQCMDARG,
                         "Set the MCS select_handler to Disabled, Enabled, or Automatic",
                         NULL,                                // check
                         NULL,                                // update
                         1,                                   // default
                         &mcs_select_handler_mode_values_lib  // values lib
);

static MYSQL_THDVAR_BOOL(derived_handler, PLUGIN_VAR_NOCMDARG, "Enable/Disable the MCS derived_handler", NULL,
                         NULL, 1);

static MYSQL_THDVAR_BOOL(group_by_handler, PLUGIN_VAR_NOCMDARG, "Enable/Disable the MCS group_by_handler",
                         NULL, NULL, 1);

static MYSQL_THDVAR_BOOL(select_handler_in_stored_procedures, PLUGIN_VAR_NOCMDARG,
                         "Enable/Disable the MCS select_handler for Stored Procedures", NULL, NULL, 1);

static MYSQL_THDVAR_UINT(orderby_threads, PLUGIN_VAR_RQCMDARG,
                         "Number of parallel threads used by ORDER BY. (default to 16)", NULL, NULL, 16, 0,
                         2048, 1);

// legacy system variables
static MYSQL_THDVAR_ULONG(decimal_scale, PLUGIN_VAR_RQCMDARG,
                          "The default decimal precision for calculated column sub-operations ", NULL, NULL,
                          8, 0, 18, 1);

static MYSQL_THDVAR_BOOL(varbin_always_hex, PLUGIN_VAR_NOCMDARG,
                         "Always display/process varbinary columns as if they have been hexified.", NULL,
                         NULL, 0);

static MYSQL_THDVAR_BOOL(use_decimal_scale, PLUGIN_VAR_NOCMDARG,
                         "Enable/disable the MCS decimal scale to be used internally", NULL, NULL, 0);

static MYSQL_THDVAR_BOOL(
    double_for_decimal_math, PLUGIN_VAR_NOCMDARG,
    "Enable/disable for ColumnStore to replace DECIMAL with DOUBLE in arithmetic operation.", NULL, NULL, 0);

static MYSQL_THDVAR_BOOL(decimal_overflow_check, PLUGIN_VAR_NOCMDARG,
                         "Enable/disable for ColumnStore to check for overflow in arithmetic operation.",
                         NULL, NULL, 0);

static MYSQL_THDVAR_BOOL(ordered_only, PLUGIN_VAR_NOCMDARG,
                         "Always use the first table in the from clause as the large side "
                         "table for joins",
                         NULL, NULL, 0);

static MYSQL_THDVAR_ULONG(string_scan_threshold, PLUGIN_VAR_RQCMDARG,
                          "Max number of blocks in a dictionary file to be scanned for filtering", NULL, NULL,
                          10, 1, ~0U, 1);

static MYSQL_THDVAR_ULONG(stringtable_threshold, PLUGIN_VAR_RQCMDARG,
                          "The minimum width of a string column to be stored in a string table", NULL, NULL,
                          20, 9, ~0U, 1);

static MYSQL_THDVAR_ULONG(diskjoin_smallsidelimit, PLUGIN_VAR_RQCMDARG,
                          "The maximum amount of disk space in MB to use per query for storing "
                          "'small side' tables for a disk-based join. (0 = unlimited)",
                          NULL, NULL, 0, 0, ~0U, 1);

static MYSQL_THDVAR_ULONG(diskjoin_largesidelimit, PLUGIN_VAR_RQCMDARG,
                          "The maximum amount of disk space in MB to use per join for storing "
                          "'large side' table data for a disk-based join. (0 = unlimited)",
                          NULL, NULL, 0, 0, ~0U, 1);

static MYSQL_THDVAR_ULONG(diskjoin_bucketsize, PLUGIN_VAR_RQCMDARG,
                          "The maximum size in MB of each 'small side' table in memory.", NULL, NULL, 100, 1,
                          ~0U, 1);

static MYSQL_THDVAR_ULONG(um_mem_limit, PLUGIN_VAR_RQCMDARG,
                          "Per user Memory limit(MB). Switch to disk-based JOIN when limit is reached", NULL,
                          NULL, 0, 0, ~0U, 1);

static MYSQL_THDVAR_ULONG(local_query, PLUGIN_VAR_RQCMDARG,
                          "Enable/disable the ColumnStore local PM query only feature.", NULL, NULL, 0, 0, 2,
                          1);

static MYSQL_THDVAR_ULONG(import_for_batchinsert_delimiter, PLUGIN_VAR_RQCMDARG,
                          "ASCII value of the delimiter used by LDI and INSERT..SELECT",
                          NULL,  // check
                          NULL,  // update
                          7,     // default
                          0,     // min
                          127,   // max
                          1      // block size
);

static MYSQL_THDVAR_ULONG(import_for_batchinsert_enclosed_by, PLUGIN_VAR_RQCMDARG,
                          "ASCII value of the quote symbol used by batch data ingestion",
                          NULL,  // check
                          NULL,  // update
                          17,    // default
                          17,    // min
                          127,   // max
                          1      // block size
);

const char* mcs_use_import_for_batchinsert_mode_values[] = {"OFF", "ON", "ALWAYS", NullS};

static TYPELIB mcs_use_import_for_batchinsert_mode_values_lib = {
    array_elements(mcs_use_import_for_batchinsert_mode_values) - 1,
    "mcs_use_import_for_batchinsert_mode_values", mcs_use_import_for_batchinsert_mode_values, NULL};

static MYSQL_THDVAR_ENUM(use_import_for_batchinsert, PLUGIN_VAR_RQCMDARG,
                         "LOAD DATA INFILE and INSERT..SELECT will use cpimport internally",
                         NULL,                                            // check
                         NULL,                                            // update
                         1,                                               // default
                         &mcs_use_import_for_batchinsert_mode_values_lib  // values lib
);

static MYSQL_THDVAR_BOOL(replication_slave, PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
                         "Allow this MariaDB server to apply replication changes to ColumnStore", NULL, NULL,
                         0);

static MYSQL_THDVAR_BOOL(cache_inserts, PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
                         "Perform cache-based inserts to ColumnStore", NULL, NULL, 0);

static MYSQL_THDVAR_BOOL(cache_use_import, PLUGIN_VAR_RQCMDARG,
                         "Use cpimport for the cache flush into ColumnStore", NULL, NULL, 0);

static MYSQL_THDVAR_ULONGLONG(cache_flush_threshold, PLUGIN_VAR_RQCMDARG,
                              "Threshold on the number of rows in the cache to trigger a flush", NULL, NULL,
                              500000, 1, 1000000000, 1);

st_mysql_sys_var* mcs_system_variables[] = {MYSQL_SYSVAR(compression_type),
                                            MYSQL_SYSVAR(fe_conn_info_ptr),
                                            MYSQL_SYSVAR(original_optimizer_flags),
                                            MYSQL_SYSVAR(original_option_bits),
                                            MYSQL_SYSVAR(select_handler),
                                            MYSQL_SYSVAR(derived_handler),
                                            MYSQL_SYSVAR(group_by_handler),
                                            MYSQL_SYSVAR(select_handler_in_stored_procedures),
                                            MYSQL_SYSVAR(orderby_threads),
                                            MYSQL_SYSVAR(decimal_scale),
                                            MYSQL_SYSVAR(use_decimal_scale),
                                            MYSQL_SYSVAR(ordered_only),
                                            MYSQL_SYSVAR(string_scan_threshold),
                                            MYSQL_SYSVAR(stringtable_threshold),
                                            MYSQL_SYSVAR(diskjoin_smallsidelimit),
                                            MYSQL_SYSVAR(diskjoin_largesidelimit),
                                            MYSQL_SYSVAR(diskjoin_bucketsize),
                                            MYSQL_SYSVAR(um_mem_limit),
                                            MYSQL_SYSVAR(double_for_decimal_math),
                                            MYSQL_SYSVAR(decimal_overflow_check),
                                            MYSQL_SYSVAR(local_query),
                                            MYSQL_SYSVAR(use_import_for_batchinsert),
                                            MYSQL_SYSVAR(import_for_batchinsert_delimiter),
                                            MYSQL_SYSVAR(import_for_batchinsert_enclosed_by),
                                            MYSQL_SYSVAR(varbin_always_hex),
                                            MYSQL_SYSVAR(replication_slave),
                                            MYSQL_SYSVAR(cache_inserts),
                                            MYSQL_SYSVAR(cache_use_import),
                                            MYSQL_SYSVAR(cache_flush_threshold),
                                            NULL};

st_mysql_show_var mcs_status_variables[] = {{"columnstore_version", (char*)&cs_version, SHOW_CHAR},
                                            {"columnstore_commit_hash", (char*)&cs_commit_hash, SHOW_CHAR},
                                            {0, 0, (enum_mysql_show_type)0}};

void* get_fe_conn_info_ptr(THD* thd)
{
  return (current_thd == NULL && thd == NULL) ? NULL : (void*)THDVAR(current_thd, fe_conn_info_ptr);
}

void set_fe_conn_info_ptr(void* ptr, THD* thd)
{
  if (current_thd == NULL && thd == NULL)
  {
    return;
  }

  THDVAR(current_thd, fe_conn_info_ptr) = (uint64_t)(ptr);
}

ulonglong get_original_optimizer_flags(THD* thd)
{
  return THDVAR(current_thd, original_optimizer_flags);
}

void set_original_optimizer_flags(ulonglong ptr, THD* thd)
{
  if (current_thd == NULL && thd == NULL)
  {
    return;
  }

  THDVAR(current_thd, original_optimizer_flags) = (uint64_t)(ptr);
}

ulonglong get_original_option_bits(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, original_option_bits);
}

void set_original_option_bits(ulonglong value, THD* thd)
{
  if (thd == NULL)
  {
    return;
  }

  THDVAR(thd, original_option_bits) = (uint64_t)(value);
}

mcs_select_handler_mode_t get_select_handler_mode(THD* thd)
{
  return (thd == NULL) ? mcs_select_handler_mode_t::ON
                       : (mcs_select_handler_mode_t)THDVAR(thd, select_handler);
}
void set_select_handler_mode(THD* thd, ulong value)
{
  THDVAR(thd, select_handler) = value;
}

bool get_derived_handler(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, derived_handler);
}
void set_derived_handler(THD* thd, bool value)
{
  THDVAR(thd, derived_handler) = value;
}

bool get_group_by_handler(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, group_by_handler);
}
void set_group_by_handler(THD* thd, bool value)
{
  THDVAR(thd, group_by_handler) = value;
}

bool get_select_handler_in_stored_procedures(THD* thd)
{
  return (thd == NULL) ? true : THDVAR(thd, select_handler_in_stored_procedures);
}
void set_select_handler_in_stored_procedures(THD* thd, bool value)
{
  THDVAR(thd, select_handler_in_stored_procedures) = value;
}

void set_compression_type(THD* thd, ulong value)
{
  THDVAR(thd, compression_type) = value;
}

mcs_compression_type_t get_compression_type(THD* thd)
{
  return (mcs_compression_type_t)THDVAR(thd, compression_type);
}

uint get_orderby_threads(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, orderby_threads);
}
void set_orderby_threads(THD* thd, uint value)
{
  THDVAR(thd, orderby_threads) = value;
}

bool get_use_decimal_scale(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, use_decimal_scale);
}
void set_use_decimal_scale(THD* thd, bool value)
{
  THDVAR(thd, use_decimal_scale) = value;
}

ulong get_decimal_scale(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, decimal_scale);
}
void set_decimal_scale(THD* thd, ulong value)
{
  THDVAR(thd, decimal_scale) = value;
}

bool get_ordered_only(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, ordered_only);
}
void set_ordered_only(THD* thd, bool value)
{
  THDVAR(thd, ordered_only) = value;
}

ulong get_string_scan_threshold(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, string_scan_threshold);
}
void set_string_scan_threshold(THD* thd, ulong value)
{
  THDVAR(thd, string_scan_threshold) = value;
}

ulong get_stringtable_threshold(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, stringtable_threshold);
}
void set_stringtable_threshold(THD* thd, ulong value)
{
  THDVAR(thd, stringtable_threshold) = value;
}

ulong get_diskjoin_smallsidelimit(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, diskjoin_smallsidelimit);
}
void set_diskjoin_smallsidelimit(THD* thd, ulong value)
{
  THDVAR(thd, diskjoin_smallsidelimit) = value;
}

ulong get_diskjoin_largesidelimit(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, diskjoin_largesidelimit);
}
void set_diskjoin_largesidelimit(THD* thd, ulong value)
{
  THDVAR(thd, diskjoin_largesidelimit) = value;
}

ulong get_diskjoin_bucketsize(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, diskjoin_bucketsize);
}
void set_diskjoin_bucketsize(THD* thd, ulong value)
{
  THDVAR(thd, diskjoin_bucketsize) = value;
}

ulong get_um_mem_limit(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, um_mem_limit);
}
void set_um_mem_limit(THD* thd, ulong value)
{
  THDVAR(thd, um_mem_limit) = value;
}

bool get_varbin_always_hex(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, varbin_always_hex);
}
void set_varbin_always_hex(THD* thd, bool value)
{
  THDVAR(thd, varbin_always_hex) = value;
}

bool get_double_for_decimal_math(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, double_for_decimal_math);
}
void set_double_for_decimal_math(THD* thd, bool value)
{
  THDVAR(thd, double_for_decimal_math) = value;
}

bool get_decimal_overflow_check(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, decimal_overflow_check);
}
void set_decimal_overflow_check(THD* thd, bool value)
{
  THDVAR(thd, decimal_overflow_check) = value;
}

ulong get_local_query(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, local_query);
}
void set_local_query(THD* thd, ulong value)
{
  THDVAR(thd, local_query) = value;
}

mcs_use_import_for_batchinsert_mode_t get_use_import_for_batchinsert_mode(THD* thd)
{
  return (thd == NULL) ? mcs_use_import_for_batchinsert_mode_t::ON
                       : (mcs_use_import_for_batchinsert_mode_t)THDVAR(thd, use_import_for_batchinsert);
}
void set_use_import_for_batchinsert_mode(THD* thd, ulong value)
{
  THDVAR(thd, use_import_for_batchinsert) = value;
}

ulong get_import_for_batchinsert_delimiter(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, import_for_batchinsert_delimiter);
}
void set_import_for_batchinsert_delimiter(THD* thd, ulong value)
{
  THDVAR(thd, import_for_batchinsert_delimiter) = value;
}

ulong get_import_for_batchinsert_enclosed_by(THD* thd)
{
  return (thd == NULL) ? 0 : THDVAR(thd, import_for_batchinsert_enclosed_by);
}
void set_import_for_batchinsert_enclosed_by(THD* thd, ulong value)
{
  THDVAR(thd, import_for_batchinsert_enclosed_by) = value;
}

bool get_replication_slave(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, replication_slave);
}
void set_replication_slave(THD* thd, bool value)
{
  THDVAR(thd, replication_slave) = value;
}

bool get_cache_inserts(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, cache_inserts);
}
void set_cache_inserts(THD* thd, bool value)
{
  THDVAR(thd, cache_inserts) = value;
}

bool get_cache_use_import(THD* thd)
{
  return (thd == NULL) ? false : THDVAR(thd, cache_use_import);
}
void set_cache_use_import(THD* thd, bool value)
{
  THDVAR(thd, cache_use_import) = value;
}

ulonglong get_cache_flush_threshold(THD* thd)
{
  return (thd == NULL) ? 500000 : THDVAR(thd, cache_flush_threshold);
}

void set_cache_flush_threshold(THD* thd, ulonglong value)
{
  THDVAR(thd, cache_flush_threshold) = value;
}
