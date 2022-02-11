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

#ifndef HA_MCS_IMPL_H__
#define HA_MCS_IMPL_H__

#include "idb_mysql.h"

#ifdef NEED_CALPONT_EXTERNS
// Forward declaration.
struct mcs_handler_info;
extern int ha_mcs_impl_discover_existence(const char* schema, const char* name);
extern int ha_mcs_impl_create(const char* name, TABLE* table_arg, HA_CREATE_INFO* create_info);
extern int ha_mcs_impl_delete_table(const char* name);
extern int ha_mcs_impl_analyze(THD* thd, TABLE* table);
extern int ha_mcs_impl_open(const char* name, int mode, uint32_t test_if_locked);
extern int ha_mcs_impl_close(void);
extern int ha_mcs_impl_rnd_next(uchar* buf, TABLE* table, long timeZone);
extern int ha_mcs_impl_rnd_end(TABLE* table, bool is_derived_hand = false);
extern int ha_mcs_impl_write_row(const uchar* buf, TABLE* table, uint64_t rows_changed, long timeZone);
extern void ha_mcs_impl_start_bulk_insert(ha_rows rows, TABLE* table, bool is_cache_insert = false);
extern int ha_mcs_impl_end_bulk_insert(bool abort, TABLE* table);
extern int ha_mcs_impl_rename_table(const char* from, const char* to);
extern int ha_mcs_impl_commit(handlerton* hton, THD* thd, bool all);
extern int ha_mcs_impl_rollback(handlerton* hton, THD* thd, bool all);
extern int ha_mcs_impl_close_connection(handlerton* hton, THD* thd);
extern COND* ha_mcs_impl_cond_push(COND* cond, TABLE* table, std::vector<COND*>&);
extern int ha_mcs_impl_update_row();
extern int ha_mcs_impl_direct_update_delete_rows(bool execute, ha_rows* affected_rows,
                                                 const std::vector<COND*>& condStack);
extern int ha_mcs_impl_delete_row();
extern int ha_mcs_impl_rnd_pos(uchar* buf, uchar* pos);
extern int ha_mcs_impl_pushdown_init(mcs_handler_info* handler_info, TABLE* table);
extern int ha_mcs_impl_select_next(uchar* buf, TABLE* table, long timeZone);
extern int ha_mcs_impl_group_by_init(mcs_handler_info* handler_info, TABLE* table);
extern int ha_mcs_impl_group_by_next(TABLE* table, long timeZone);
extern int ha_mcs_impl_group_by_end(TABLE* table);

#endif

#ifdef NEED_CALPONT_INTERFACE
#include "ha_mcs_impl_if.h"
#include "calpontsystemcatalog.h"
#include "ha_mcs.h"
extern int ha_mcs_impl_rename_table_(const char* from, const char* to, cal_impl_if::cal_connection_info& ci);
extern int ha_mcs_impl_write_row_(const uchar* buf, TABLE* table, cal_impl_if::cal_connection_info& ci,
                                  ha_rows& rowsInserted);
extern int ha_mcs_impl_write_batch_row_(const uchar* buf, TABLE* table, cal_impl_if::cal_connection_info& ci,
                                        long timeZone);
extern int ha_mcs_impl_write_last_batch(TABLE* table, cal_impl_if::cal_connection_info& ci, bool abort);
extern int ha_mcs_impl_commit_(handlerton* hton, THD* thd, bool all, cal_impl_if::cal_connection_info& ci);
extern int ha_mcs_impl_rollback_(handlerton* hton, THD* thd, bool all, cal_impl_if::cal_connection_info& ci);
extern int ha_mcs_impl_close_connection_(handlerton* hton, THD* thd, cal_impl_if::cal_connection_info& ci);
extern int ha_mcs_impl_delete_table_(const char* db, const char* name, cal_impl_if::cal_connection_info& ci);
extern int ha_mcs_impl_create_(const char* name, TABLE* table_arg, HA_CREATE_INFO* create_info,
                               cal_impl_if::cal_connection_info& ci);
extern std::string ha_mcs_impl_markpartition_(execplan::CalpontSystemCatalog::TableName tableName,
                                              uint32_t partition);
extern std::string ha_mcs_impl_restorepartition_(execplan::CalpontSystemCatalog::TableName tableName,
                                                 uint32_t partition);
extern std::string ha_mcs_impl_droppartition_(execplan::CalpontSystemCatalog::TableName tableName,
                                              uint32_t partition);

extern std::string ha_mcs_impl_viewtablelock(cal_impl_if::cal_connection_info& ci,
                                             execplan::CalpontSystemCatalog::TableName& tablename);
extern std::string ha_mcs_impl_cleartablelock(cal_impl_if::cal_connection_info& ci, uint64_t tableLockID);
#endif

#endif
