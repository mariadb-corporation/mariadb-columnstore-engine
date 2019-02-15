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
#include "ha_mcs_sysvars.h"

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
    "Controls compression algorithm for create tables. Possible values are: "
    "NO_COMPRESSION segment files aren't compressed; "
    "SNAPPY segment files are Snappy compressed (default);",
    NULL, // check
    NULL, // update
    1, //default
    &mcs_compression_type_names_lib); // values lib

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

// legacy system variables
static MYSQL_THDVAR_ULONG(
    decimal_scale,
    PLUGIN_VAR_RQCMDARG,
    "The default decimal precision for calculated column sub-operations ",
    NULL,
    NULL,
    8,
    0,
    18,
    1
);

static MYSQL_THDVAR_BOOL(
    varbin_always_hex,
    PLUGIN_VAR_NOCMDARG,
    "Always display/process varbinary columns as if they have been hexified.",
    NULL,
    NULL,
    0
);

static MYSQL_THDVAR_BOOL(
    use_decimal_scale,
    PLUGIN_VAR_NOCMDARG,
    "Enable/disable the MCS decimal scale to be used internally",
    NULL,
    NULL,
    0
);

static MYSQL_THDVAR_BOOL(
    double_for_decimal_math,
    PLUGIN_VAR_NOCMDARG,
    "Enable/disable the InfiniDB to replace DECIMAL with DOUBLE in arithmetic operation.",
    NULL,
    NULL,
    0
);

static MYSQL_THDVAR_BOOL(
    ordered_only,
    PLUGIN_VAR_NOCMDARG,
    "Always use the first table in the from clause as the large side "
    "table for joins",
    NULL,
    NULL,
    0
);

static MYSQL_THDVAR_ULONG(
    string_scan_threshold,
    PLUGIN_VAR_RQCMDARG,
    "Max number of blocks in a dictionary file to be scanned for filtering",
    NULL,
    NULL,
    10,
    1,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    stringtable_threshold,
    PLUGIN_VAR_RQCMDARG,
    "The minimum width of a string column to be stored in a string table",
    NULL,
    NULL,
    20,
    9,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    diskjoin_smallsidelimit,
    PLUGIN_VAR_RQCMDARG,
    "The maximum amount of disk space in MB to use per query for storing "
    "'small side' tables for a disk-based join. (0 = unlimited)",
    NULL,
    NULL,
    0,
    0,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    diskjoin_largesidelimit,
    PLUGIN_VAR_RQCMDARG,
    "The maximum amount of disk space in MB to use per join for storing "
    "'large side' table data for a disk-based join. (0 = unlimited)",
    NULL,
    NULL,
    0,
    0,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    diskjoin_bucketsize,
    PLUGIN_VAR_RQCMDARG,
    "The maximum size in MB of each 'small side' table in memory.",
    NULL,
    NULL,
    100,
    1,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    um_mem_limit,
    PLUGIN_VAR_RQCMDARG,
    "Per user Memory limit(MB). Switch to disk-based JOIN when limit is reached",
    NULL,
    NULL,
    0,
    0,
    ~0U,
    1
);

static MYSQL_THDVAR_ULONG(
    local_query,
    PLUGIN_VAR_RQCMDARG,
    "Enable/disable the Infinidb local PM query only feature.",
    NULL,
    NULL,
    0,
    0,
    2,
    1
);

static MYSQL_THDVAR_ULONG(
    import_for_batchinsert_delimiter,
    PLUGIN_VAR_RQCMDARG,
    "ASCII value of the delimiter used by LDI and INSERT..SELECT",
    NULL, // check
    NULL, // update
    7, // default
    0, // min
    127, // max
    1 // block size
);

static MYSQL_THDVAR_ULONG(
    import_for_batchinsert_enclosed_by,
    PLUGIN_VAR_RQCMDARG,
    "ASCII value of the quote symbol used by batch data ingestion",
    NULL, // check
    NULL, // update
    17, // default
    17, // min
    127, // max
    1 // block size
);

static MYSQL_THDVAR_BOOL(
    use_import_for_batchinsert,
    PLUGIN_VAR_NOCMDARG,
    "LOAD DATA INFILE and INSERT..SELECT will use cpimport internally",
    NULL, // check
    NULL, // update
    1 // default
);

static MYSQL_THDVAR_BOOL(
    use_legacy_sysvars,
    PLUGIN_VAR_NOCMDARG,
    "Control CS behavior using legacy * sysvars",
    NULL, // check
    NULL, // update
    0 // default
);

st_mysql_sys_var* mcs_system_variables[] =
{
  MYSQL_SYSVAR(compression_type),
  MYSQL_SYSVAR(fe_conn_info_ptr),
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
  MYSQL_SYSVAR(local_query),
  MYSQL_SYSVAR(use_import_for_batchinsert),
  MYSQL_SYSVAR(import_for_batchinsert_delimiter),
  MYSQL_SYSVAR(import_for_batchinsert_enclosed_by),
  MYSQL_SYSVAR(use_legacy_sysvars),
  MYSQL_SYSVAR(varbin_always_hex),
  NULL
};

void* get_fe_conn_info_ptr(THD* thd)
{
    return ( current_thd == NULL && thd == NULL ) ? NULL :
        (void*)THDVAR(current_thd, fe_conn_info_ptr);
}

void set_fe_conn_info_ptr(void* ptr, THD* thd)
{
    if ( current_thd == NULL && thd == NULL)
    {
        return;
    }
    
    THDVAR(current_thd, fe_conn_info_ptr) = (uint64_t)(ptr);
}

bool get_use_legacy_sysvars(THD* thd)
{
    return ( thd == NULL ) ? false : THDVAR(thd, use_legacy_sysvars);
}

void set_use_legacy_sysvars(THD* thd, bool value)
{
    THDVAR(thd, use_legacy_sysvars) = value;
}

void set_compression_type(THD* thd, ulong value)
{
    THDVAR(thd, compression_type) = value;
}

mcs_compression_type_t get_compression_type(THD* thd) {
    return (mcs_compression_type_t) THDVAR(thd, compression_type);
}

bool get_use_decimal_scale(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? false : thd->variables.infinidb_use_decimal_scale;
    else
        return ( thd == NULL ) ? false : THDVAR(thd, use_decimal_scale);
}
void set_use_decimal_scale(THD* thd, bool value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_use_decimal_scale = value;
    else
        THDVAR(thd, use_decimal_scale) = value;
}

ulong get_decimal_scale(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_decimal_scale;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, decimal_scale);
}
void set_decimal_scale(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_decimal_scale = value;
    else
        THDVAR(thd, decimal_scale) = value;
}

bool get_ordered_only(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? false : thd->variables.infinidb_ordered_only;
    else
        return ( thd == NULL ) ? false : THDVAR(thd, ordered_only);
}
void set_ordered_only(THD* thd, bool value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_ordered_only = value;
    else
        THDVAR(thd, ordered_only) = value;
}

ulong get_string_scan_threshold(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_string_scan_threshold;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, string_scan_threshold);
}
void set_string_scan_threshold(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_string_scan_threshold = value;
    else
        THDVAR(thd, string_scan_threshold) = value;
}

ulong get_stringtable_threshold(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_stringtable_threshold;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, stringtable_threshold);
}
void set_stringtable_threshold(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_stringtable_threshold = value;
    else
        THDVAR(thd, stringtable_threshold) = value;
}

ulong get_diskjoin_smallsidelimit(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_diskjoin_smallsidelimit;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, diskjoin_smallsidelimit);
}
void set_diskjoin_smallsidelimit(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_diskjoin_smallsidelimit = value;
    else
        THDVAR(thd, diskjoin_smallsidelimit) = value;
}

ulong get_diskjoin_largesidelimit(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_diskjoin_largesidelimit;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, diskjoin_largesidelimit);
}
void set_diskjoin_largesidelimit(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_diskjoin_largesidelimit = value;
    else
        THDVAR(thd, diskjoin_largesidelimit) = value;
}

ulong get_diskjoin_bucketsize(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_diskjoin_bucketsize;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, diskjoin_bucketsize);
}
void set_diskjoin_bucketsize(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_diskjoin_bucketsize = value;
    else
        THDVAR(thd, diskjoin_bucketsize) = value;
}

ulong get_um_mem_limit(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_um_mem_limit;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, um_mem_limit);
}
void set_um_mem_limit(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_um_mem_limit = value;
    else
        THDVAR(thd, um_mem_limit) = value;
}

bool get_varbin_always_hex(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? false : thd->variables.infinidb_varbin_always_hex;
    else
        return ( thd == NULL ) ? false : THDVAR(thd, varbin_always_hex);
}
void set_varbin_always_hex(THD* thd, bool value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_varbin_always_hex = value;
    else
        THDVAR(thd, varbin_always_hex) = value;
}

bool get_double_for_decimal_math(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? false : thd->variables.infinidb_double_for_decimal_math;
    else
        return ( thd == NULL ) ? false : THDVAR(thd, double_for_decimal_math);
}
void set_double_for_decimal_math(THD* thd, bool value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_double_for_decimal_math = value;
    else
        THDVAR(thd, double_for_decimal_math) = value;
}

ulong get_local_query(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_local_query;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, local_query);
}
void set_local_query(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_local_query = value;
    else
        THDVAR(thd, local_query) = value;
}

bool get_use_import_for_batchinsert(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? false : thd->variables.infinidb_use_import_for_batchinsert;
    else
        return ( thd == NULL ) ? false : THDVAR(thd, use_import_for_batchinsert);
}
void set_use_import_for_batchinsert(THD* thd, bool value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_use_import_for_batchinsert = value;
    else
        THDVAR(thd, use_import_for_batchinsert) = value;
}

ulong get_import_for_batchinsert_delimiter(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_import_for_batchinsert_delimiter;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, import_for_batchinsert_delimiter);
}
void set_import_for_batchinsert_delimiter(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_import_for_batchinsert_delimiter = value;
    else
        THDVAR(thd, import_for_batchinsert_delimiter) = value;
}

ulong get_import_for_batchinsert_enclosed_by(THD* thd)
{
    if(get_use_legacy_sysvars(thd))
        return ( thd == NULL ) ? 0 : thd->variables.infinidb_import_for_batchinsert_enclosed_by;
    else
        return ( thd == NULL ) ? 0 : THDVAR(thd, import_for_batchinsert_enclosed_by);
}
void set_import_for_batchinsert_enclosed_by(THD* thd, ulong value)
{
    if(get_use_legacy_sysvars(thd))
        thd->variables.infinidb_import_for_batchinsert_enclosed_by = value;
    else
        THDVAR(thd, import_for_batchinsert_enclosed_by) = value;
}
