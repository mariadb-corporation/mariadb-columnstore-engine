/*
   Copyright (c) 2019-20 MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef HA_MCS_PUSH
#define HA_MCS_PUSH

#define PREFER_MY_CONFIG_H
#include "idb_mysql.h"
#include "ha_mcs.h"
#include "ha_mcs_sysvars.h"
#define NEED_CALPONT_EXTERNS
#include "ha_mcs_impl.h"
#include "ha_mcs_impl_if.h"
#include "ha_mcs_opt_rewrites.h"

void mutate_optimizer_flags(THD* thd_);
void restore_optimizer_flags(THD* thd_);

enum mcs_handler_types_t
{
  SELECT,
  DERIVED,
  GROUP_BY,
  LEGACY
};

struct mcs_handler_info
{
  mcs_handler_info() : hndl_ptr(NULL), hndl_type(LEGACY){};
  mcs_handler_info(mcs_handler_types_t type) : hndl_ptr(NULL), hndl_type(type){};
  mcs_handler_info(void* ptr, mcs_handler_types_t type) : hndl_ptr(ptr), hndl_type(type){};
  ~mcs_handler_info(){};
  void* hndl_ptr;
  mcs_handler_types_t hndl_type;
};

/*@brief  group_by_handler class*/
/***********************************************************
 * DESCRIPTION:
 * Provides server with group_by_handler API methods.
 * One should read comments in server/sql/group_by_handler.h
 * Attributes:
 * select - attribute contains all GROUP BY, HAVING, ORDER items and calls it
 *              an extended SELECT list according to comments in
 *              server/sql/group_handler.cc.
 *              So the temporary table for
 *              select count(*) from b group by a having a > 3 order by a
 *              will have 4 columns not 1.
 *              However server ignores all NULLs used in
 *              GROUP BY, HAVING, ORDER.
 * select_list_descr - contains Item description returned by Item->print()
 *              that is used in lookup for corresponding columns in
 *              extended SELECT list.
 * table_list - contains all tables involved. Must be CS tables only.
 * distinct - looks like a useless thing for now. Couldn't get it set by server.
 * where - where items.
 * group_by - group by ORDER items.
 * order_by - order by ORDER items.
 * having - having Item.
 * Methods:
 * init_scan - get plan and send it to ExeMgr. Get the execution result.
 * next_row - get a row back from sm.
 * end_scan - finish and clean the things up.
 ***********************************************************/
class ha_mcs_group_by_handler : public group_by_handler
{
 private:
  long time_zone;

 public:
  ha_mcs_group_by_handler(THD* thd_arg, Query* query);
  ~ha_mcs_group_by_handler();
  int init_scan() override;
  int next_row() override;
  int end_scan() override;

  List<Item>* select;
  TABLE_LIST* table_list;
  bool distinct;
  Item* where;
  ORDER* group_by;
  ORDER* order_by;
  Item* having;
};

/*@brief  derived_handler class*/
/***********************************************************
 * DESCRIPTION:
 * derived_handler API methods. Could be used by the server
 * tp process sub-queries.
 * More details in server/sql/dervied_handler.h
 * COLUMNSTORE_SHARE* hton share
 * tbl in the constructor is the list of the tables involved.
 * Methods:
 * init_scan - get plan and send it to ExeMgr. Get the execution result.
 * next_row - get a row back from sm.
 * end_scan - finish and clean the things up.
 ***********************************************************/
class ha_columnstore_derived_handler : public derived_handler
{
 private:
  COLUMNSTORE_SHARE* share;
  long time_zone;

 public:
  ha_columnstore_derived_handler(THD* thd_arg, TABLE_LIST* tbl);
  ~ha_columnstore_derived_handler();
  int init_scan() override;
  int next_row() override;
  int end_scan() override;
  void print_error(int, unsigned long) override;
};

/*@brief select_handler class*/
/***********************************************************
 * DESCRIPTION:
 * select_handler API methods. Could be used by the server
 * tp pushdown the whole query described by SELECT_LEX.
 * More details in server/sql/select_handler.h
 * COLUMNSTORE_SHARE* hton share
 * sel in the constructor is the semantic tree for the query.
 * Methods:
 * init_scan - get plan and send it to ExeMgr. Get the execution result.
 * next_row - get a row back from sm.
 * end_scan - finish and clean the things up.
 ***********************************************************/
class ha_columnstore_select_handler : public select_handler
{
 private:
  COLUMNSTORE_SHARE* share;
  bool prepared;
  bool scan_ended;
  long time_zone;

 public:
  bool scan_initialized;
  int pushdown_init_rc;
  // MCOL-4525 Store the original TABLE_LIST::outer_join value in a hash map.
  // This will be used to restore to the original state later in case
  // query execution fails using the select_handler.
  cal_impl_if::TableOuterJoinMap tableOuterJoinMap;
  ha_columnstore_select_handler(THD* thd_arg, SELECT_LEX* sel);
  ~ha_columnstore_select_handler();
  int init_scan() override;
  int next_row() override;
  int end_scan() override;
  bool prepare() override;
};

#endif
