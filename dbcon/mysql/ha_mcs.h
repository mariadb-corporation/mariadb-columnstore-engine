/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation
   Copyright (C) 2019 MariaDB Corporation

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
#ifndef HA_MCS_H__
#define HA_MCS_H__

#include <my_config.h>
#include "idb_mysql.h"
#include "ha_mcs_sysvars.h"
#include "ha_maria.h"

extern handlerton* mcs_hton;

/** @brief
  COLUMNSTORE_SHARE is a structure that will be shared among all open handlers.
  This example implements the minimum of what you will probably need.
*/
typedef struct st_mcs_share
{
  char* table_name;
  uint32_t table_name_length, use_count;
  pthread_mutex_t mutex;
  THR_LOCK lock;
} COLUMNSTORE_SHARE;

/** @brief
  Class definition for the storage engine
*/
class ha_mcs : public handler
{
  THR_LOCK_DATA lock;        ///< MySQL lock
  COLUMNSTORE_SHARE* share;  ///< Shared lock info
  ulonglong int_table_flags;
  // We are using a vector here to mimick the stack functionality
  // using push_back() and pop_back()
  // as apparently there is a linker error on the std::stack<COND*>::pop()
  // call on Ubuntu18.
  std::vector<COND*> condStack;
  int m_lock_type;
  long time_zone;

  int impl_external_lock(THD* thd, TABLE* table, int lock_type);
  int impl_rnd_init(TABLE* table, const std::vector<COND*>& condStack);

 public:
  ha_mcs(handlerton* hton, TABLE_SHARE* table_arg);
  virtual ~ha_mcs()
  {
  }

  /** @brief
    The name that will be used for display purposes.
   */
  const char* table_type() const override
  {
    return "ColumnStore";
  }

  /** @brief
    The name of the index type that will be used for display.
    Don't implement this method unless you really have indexes.
   */
  // const char *index_type(uint32_t inx) { return "HASH"; }

  /** @brief
    The file extensions.
   */
  const char** bas_ext() const;

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override
  {
    return int_table_flags;
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint32_t inx, uint32_t part, bool all_parts) const override
  {
    return 0;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint32_t max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint32_t max_supported_record_length() const override
  {
    return HA_MAX_REC_LENGTH;
  }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  virtual double scan_time() override
  {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    Analyze table command.
  */
  int analyze(THD* thd, HA_CHECK_OPT* check_opt) override;

  /*
    Everything below are methods that we implement in ha_example.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */

  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int open(const char* name, int mode, uint32_t test_if_locked) override;  // required

  // MCOL-4282 This function is called by open_tables in sql_base.cc.
  // We mutate the optimizer flags here for prepared statements as this
  // handler function is called before JOIN::prepare, and we need to
  // disable the default optimizer flags before JOIN::prepare (which is
  // called during "PREPARE stmt FROM ..." SQL) is called.
  // Sequence of SQL statements that will lead to this execution path
  // for prepared statements:
  //   CREATE TABLE t1 (a int, b int) engine=columnstore;
  //   INSERT INTO t1 VALUES (1, 2), (2, 4), (3, 1);
  //   PREPARE stmt1 FROM "SELECT * FROM t1";
  //   EXECUTE stmt1;

  int discover_check_version() override;

  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int close(void) override;  // required

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(const uchar* buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  void start_bulk_insert(ha_rows rows, uint flags = 0) override;
  void start_bulk_insert_from_cache(ha_rows rows, uint flags = 0);

  /**@bug 2461 - Overloaded end_bulk_insert.  MariaDB uses the abort bool, mysql does not. */
  int end_bulk_insert() override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar* old_data, const uchar* new_data) override;
  int direct_update_rows_init(List<Item>* update_fields) override;
  int direct_update_rows(ha_rows* update_rows);
  int direct_update_rows(ha_rows* update_rows, ha_rows* found_rows) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar* buf) override;
  int direct_delete_rows_init() override;
  int direct_delete_rows(ha_rows* deleted_rows) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar* buf, const uchar* key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar* buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar* buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar* buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar* buf) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override;  // required
  int rnd_end() override;
  int rnd_next(uchar* buf) override;             ///< required
  int rnd_pos(uchar* buf, uchar* pos) override;  ///< required
  int reset() override;
  void position(const uchar* record) override;  ///< required
  int info(uint32_t) override;                  ///< required
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD* thd, int lock_type) override;  ///< required
  int delete_all_rows(void) override;
  ha_rows records_in_range(uint32_t inx, const key_range* min_key, const key_range* max_key,
                           page_range* res) override;
  int delete_table(const char* from) override;
  int rename_table(const char* from, const char* to) override;
  int create(const char* name, TABLE* form,
             HA_CREATE_INFO* create_info) override;  ///< required

  THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** to,
                             enum thr_lock_type lock_type) override;  ///< required
  const COND* cond_push(const COND* cond) override;
  void cond_pop() override;
  uint8 table_cache_type() override
  {
    return HA_CACHE_TBL_NOCACHE;
  }

  int repair(THD* thd, HA_CHECK_OPT* check_opt) override;
  bool is_crashed() const override;

  bool isReadOnly() const
  {
    return m_lock_type == F_RDLCK;
  }
};

class ha_mcs_cache_share
{
  ha_mcs_cache_share* next; /* Next open share */
  const char* name;
  uint open_count;

 public:
  ulonglong cached_rows;
  THR_LOCK org_lock;
  friend ha_mcs_cache_share* find_cache_share(const char* name, ulonglong cached_rows);
  void close();
};

class ha_mcs_cache : public ha_mcs
{
  typedef ha_mcs parent;
  int original_lock_type;
  bool insert_command, cache_locked;
  enum_sql_command sql_command;

  // True if this handler belongs to either calpontsys.systable or
  // calpontsys.syscolumn system catalog tables
  bool isSysCatTable;

  // True if the ColumnStore table is not cached (i.e. when the table
  // was created with columnstore_cache_inserts=OFF).
  bool isCacheDisabled;

  bool isCacheEnabled() const
  {
    return (get_cache_inserts(current_thd) && !isSysCatTable && !isCacheDisabled);
  }

 public:
  uint lock_counter;
  ha_maria* cache_handler;
  ha_mcs_cache_share* share;

  ha_mcs_cache(handlerton* hton, TABLE_SHARE* table_arg, MEM_ROOT* mem_root);
  ~ha_mcs_cache();

  /*
    The following functions duplicates calls to derived handler and
    cache handler
  */

  int create(const char* name, TABLE* table_arg, HA_CREATE_INFO* ha_create_info);
  int open(const char* name, int mode, uint open_flags);
  int delete_table(const char* name);
  int rename_table(const char* from, const char* to);
  int delete_all_rows(void);
  int close(void);

  uint lock_count(void) const;
  THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** to, enum thr_lock_type lock_type);
  int external_lock(THD* thd, int lock_type);
  int repair(THD* thd, HA_CHECK_OPT* check_opt);
  bool is_crashed() const;

  /*
    Write row uses cache_handler, for normal inserts, otherwise derived
    handler
  */
  int write_row(const uchar* buf);
  void start_bulk_insert(ha_rows rows, uint flags);
  int end_bulk_insert();

  /* Cache functions */
  void free_locks();
  ha_rows num_rows_cached();
  int flush_insert_cache();
  friend my_bool get_status_and_flush_cache(void* param, my_bool concurrent_insert);
  friend my_bool cache_start_trans(void* param);
};

#endif  // HA_MCS_H__
