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

#include "ha_mcs.h"
#include "maria_def.h"
#include <typeinfo>
#include "columnstoreversion.h"

#include "ha_mcs_pushdown.h"
#define NEED_CALPONT_EXTERNS
#include "ha_mcs_impl.h"
#include "is_columnstore.h"
#include "ha_mcs_version.h"

#ifndef COLUMNSTORE_MATURITY
#define COLUMNSTORE_MATURITY MariaDB_PLUGIN_MATURITY_STABLE
#endif

#define CACHE_PREFIX "#cache#"

handlerton* mcs_hton = NULL;
// This is the maria handlerton that we need for the cache
static handlerton* mcs_maria_hton = NULL;
char cs_version[25];
char cs_commit_hash[41];  // a commit hash is 40 characters

// handlers creation function for hton.
// Look into ha_mcs_pushdown.* for more details.
group_by_handler* create_columnstore_group_by_handler(THD* thd, Query* query);

derived_handler* create_columnstore_derived_handler(THD* thd, TABLE_LIST* derived);

select_handler* create_columnstore_select_handler(THD* thd, SELECT_LEX* sel);

/* Variables for example share methods */

/*
   Hash used to track the number of open tables; variable for example share
   methods
*/
static HASH mcs_open_tables;

#ifndef _MSC_VER
/* The mutex used to init the hash; variable for example share methods */
pthread_mutex_t mcs_mutex;
#endif

#ifdef DEBUG_ENTER
#undef DEBUG_ENTER
#endif
#ifdef DEBUG_RETURN
#undef DEBUG_ENTER
#endif
#define DEBUG_RETURN return

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

static uchar* mcs_get_key(COLUMNSTORE_SHARE* share, size_t* length, my_bool not_used __attribute__((unused)))
{
  *length = share->table_name_length;
  return (uchar*)share->table_name;
}

// This one is unused
int mcs_discover(handlerton* hton, THD* thd, TABLE_SHARE* share)
{
  DBUG_ENTER("mcs_discover");
  DBUG_PRINT("mcs_discover", ("db: '%s'  name: '%s'", share->db.str, share->table_name.str));
#ifdef INFINIDB_DEBUG
  fprintf(stderr, "mcs_discover()\n");
#endif

  uchar* frm_data = NULL;
  size_t frm_len = 0;
  int error = 0;

  if (!ha_mcs_impl_discover_existence(share->db.str, share->table_name.str))
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);

  error = share->read_frm_image((const uchar**)&frm_data, &frm_len);

  if (error)
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);

  my_errno = share->init_from_binary_frm_image(thd, 1, frm_data, frm_len);

  my_free(frm_data);
  DBUG_RETURN(my_errno);
}

// This f() is also unused
int mcs_discover_existence(handlerton* hton, const char* db, const char* table_name)
{
  return ha_mcs_impl_discover_existence(db, table_name);
}

static int mcs_commit(handlerton* hton, THD* thd, bool all)
{
  int rc;
  try
  {
    rc = ha_mcs_impl_commit(hton, thd, all);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  return rc;
}

static int mcs_rollback(handlerton* hton, THD* thd, bool all)
{
  int rc;
  try
  {
    rc = ha_mcs_impl_rollback(hton, thd, all);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  return rc;
}

static int mcs_close_connection(handlerton* hton, THD* thd)
{
  int rc;
  try
  {
    rc = ha_mcs_impl_close_connection(hton, thd);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  return rc;
}

ha_mcs::ha_mcs(handlerton* hton, TABLE_SHARE* table_arg)
 : handler(hton, table_arg)
 , int_table_flags(HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE | HA_TABLE_SCAN_ON_INDEX |
                   HA_CAN_TABLE_CONDITION_PUSHDOWN | HA_CAN_DIRECT_UPDATE_AND_DELETE)
 , m_lock_type(F_UNLCK)
{
  const char* timeZone = current_thd->variables.time_zone->get_name()->ptr();
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &time_zone);
}

/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char* ha_mcs_exts[] = {NullS};

const char** ha_mcs::bas_ext() const
{
  return ha_mcs_exts;
}

int ha_mcs::analyze(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_mcs::analyze");

  int rc;
  try
  {
    rc = ha_mcs_impl_analyze(thd, table);
  }
  catch (std::runtime_error& e)
  {
    thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_mcs::open(const char* name, int mode, uint32_t test_if_locked)
{
  DBUG_ENTER("ha_mcs::open");

  bool isPS = current_thd->stmt_arena &&
              (current_thd->stmt_arena->is_stmt_prepare() || current_thd->stmt_arena->is_stmt_execute());

  // MCOL-4282 See the description for discover_check_version() in ha_mcs.h
  // for why we need to mutate optimizer flags here. Sequence of SQL
  // statements that will lead to this execution path for prepared
  // statements:
  //   CREATE TABLE t1 (a int, b int) engine=columnstore;
  //   PREPARE stmt1 FROM "SELECT * FROM t1";
  //   EXECUTE stmt1;
  if (isPS)
    mutate_optimizer_flags(current_thd);

  int rc;
  try
  {
    rc = ha_mcs_impl_open(name, mode, test_if_locked);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

int ha_mcs::discover_check_version()
{
  bool isPS = current_thd->stmt_arena &&
              (current_thd->stmt_arena->is_stmt_prepare() || current_thd->stmt_arena->is_stmt_execute());

  if (isPS)
    mutate_optimizer_flags(current_thd);

  return 0;
}

/**
  @brief
  Closes a table. We call the free_share() function to free any resources
  that we have allocated in the "shared" structure.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_mcs::close(void)
{
  DBUG_ENTER("ha_mcs::close");

  int rc;
  try
  {
    rc = ha_mcs_impl_close();
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

    @details
  Example of this would be:
    @code
    @endcode
*/

int ha_mcs::write_row(const uchar* buf)
{
  DBUG_ENTER("ha_mcs::write_row");
  int rc;
  try
  {
    rc = ha_mcs_impl_write_row(buf, table, rows_changed, time_zone);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

void ha_mcs::start_bulk_insert(ha_rows rows, uint flags)
{
  DBUG_ENTER("ha_mcs::start_bulk_insert");
  try
  {
    ha_mcs_impl_start_bulk_insert(rows, table);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
  }
  DBUG_VOID_RETURN;
}

void ha_mcs::start_bulk_insert_from_cache(ha_rows rows, uint flags)
{
  DBUG_ENTER("ha_mcs::start_bulk_insert_from_cache");
  try
  {
    ha_mcs_impl_start_bulk_insert(rows, table, true);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
  }
  DBUG_VOID_RETURN;
}

int ha_mcs::end_bulk_insert()
{
  DBUG_ENTER("ha_mcs::end_bulk_insert");
  int rc;
  try
  {
    rc = ha_mcs_impl_end_bulk_insert(false, table);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

    @details
   @code
    @endcode

    @see
*/
int ha_mcs::update_row(const uchar* old_data, const uchar* new_data)
{
  DBUG_ENTER("ha_mcs::update_row");
  int rc;
  try
  {
    rc = ha_mcs_impl_update_row();
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
    Durect UPDATE/DELETE are the features that allows engine to run UPDATE
    or DELETE on its own. There are number of limitations that dissalows
    the feature.

    @details
   @code
    @endcode

    @see
      mysql_update()/mysql_delete
*/
int ha_mcs::direct_update_rows_init(List<Item>* update_fields)
{
  DBUG_ENTER("ha_mcs::direct_update_rows_init");
  DBUG_RETURN(0);
}

int ha_mcs::direct_update_rows(ha_rows* update_rows)
{
  DBUG_ENTER("ha_mcs::direct_update_rows");
  int rc;
  try
  {
    rc = ha_mcs_impl_direct_update_delete_rows(false, update_rows, condStack);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

int ha_mcs::direct_update_rows(ha_rows* update_rows, ha_rows* found_rows)
{
  DBUG_ENTER("ha_mcs::direct_update_rows");
  int rc;
  try
  {
    rc = ha_mcs_impl_direct_update_delete_rows(false, update_rows, condStack);
    *found_rows = *update_rows;
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

int ha_mcs::direct_delete_rows_init()
{
  DBUG_ENTER("ha_mcs::direct_delete_rows_init");
  DBUG_RETURN(0);
}

int ha_mcs::direct_delete_rows(ha_rows* deleted_rows)
{
  DBUG_ENTER("ha_mcs::direct_delete_rows");
  int rc;
  try
  {
    rc = ha_mcs_impl_direct_update_delete_rows(true, deleted_rows, condStack);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}
/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_mcs::delete_row(const uchar* buf)
{
  DBUG_ENTER("ha_mcs::delete_row");
  int rc;
  try
  {
    rc = ha_mcs_impl_delete_row();
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_mcs::index_read_map(uchar* buf, const uchar* key, key_part_map keypart_map __attribute__((unused)),
                           enum ha_rkey_function find_flag __attribute__((unused)))
{
  DBUG_ENTER("ha_mcs::index_read");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_mcs::index_next(uchar* buf)
{
  DBUG_ENTER("ha_mcs::index_next");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_mcs::index_prev(uchar* buf)
{
  DBUG_ENTER("ha_mcs::index_prev");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  index_first() asks for the first key in the index.

    @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

    @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mcs::index_first(uchar* buf)
{
  DBUG_ENTER("ha_mcs::index_first");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  index_last() asks for the last key in the index.

    @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

    @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mcs::index_last(uchar* buf)
{
  DBUG_ENTER("ha_mcs::index_last");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.

    @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mcs::rnd_init(bool scan)
{
  DBUG_ENTER("ha_mcs::rnd_init");

  int rc = 0;
  if (scan)
  {
    try
    {
      rc = impl_rnd_init(table, condStack);
    }
    catch (std::runtime_error& e)
    {
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
      rc = ER_INTERNAL_ERROR;
    }
  }

  DBUG_RETURN(rc);
}

int ha_mcs::rnd_end()
{
  DBUG_ENTER("ha_mcs::rnd_end");

  int rc;
  try
  {
    rc = ha_mcs_impl_rnd_end(table);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

    @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mcs::rnd_next(uchar* buf)
{
  DBUG_ENTER("ha_mcs::rnd_next");

  int rc;
  try
  {
    rc = ha_mcs_impl_rnd_next(buf, table, time_zone);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
    @code
  my_store_ptr(ref, ref_length, current_position);
    @endcode

    @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

    @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
// @TODO: Implement position() and rnd_pos() and remove HA_NO_BLOBS from table_flags
// This would require us to add a psuedo-column of some sort for a primary index. This
// would only be used in rare cases of ORDER BY, so the slow down would be ok and would
// allow for implementing blobs (is that the same as varbinary?). Perhaps using
// lbid and offset as key would work, or something. We also need to add functionality
// to retrieve records quickly by this "key"
void ha_mcs::position(const uchar* record)
{
  DBUG_ENTER("ha_mcs::position");
  DBUG_VOID_RETURN;
}

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

    @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_mcs::rnd_pos(uchar* buf, uchar* pos)
{
  DBUG_ENTER("ha_mcs::rnd_pos");
  int rc;
  try
  {
    rc = ha_mcs_impl_rnd_pos(buf, pos);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

    @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
    @code
  if (records < 2)
    records = 2;
    @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

    @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_mcs::info(uint32_t flag)
{
  DBUG_ENTER("ha_mcs::info");
  // @bug 1635. Raise this number magically fix the filesort crash issue. May need to twist
  // the number again if the issue re-occurs
  stats.records = 2000;
#ifdef INFINIDB_DEBUG
  puts("info");
#endif
  DBUG_RETURN(0);
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_mcs::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_mcs::extra");
#ifdef INFINIDB_DEBUG
  {
    const char* hefs;

    switch (operation)
    {
      case HA_EXTRA_NO_READCHECK: hefs = "HA_EXTRA_NO_READCHECK"; break;

      case HA_EXTRA_CACHE: hefs = "HA_EXTRA_CACHE"; break;

      case HA_EXTRA_NO_CACHE: hefs = "HA_EXTRA_NO_CACHE"; break;

      case HA_EXTRA_NO_IGNORE_DUP_KEY: hefs = "HA_EXTRA_NO_IGNORE_DUP_KEY"; break;

      case HA_EXTRA_PREPARE_FOR_RENAME: hefs = "HA_EXTRA_PREPARE_FOR_RENAME"; break;

      default: hefs = "UNKNOWN ENUM!"; break;
    }

    fprintf(stderr, "ha_mcs::extra(\"%s\", %d: %s)\n", table->s->table_name.str, operation, hefs);
  }
#endif
  DBUG_RETURN(0);
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

    @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

    @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_mcs::delete_all_rows()
{
  DBUG_ENTER("ha_mcs::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

    @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

    @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_mcs::external_lock(THD* thd, int lock_type)
{
  DBUG_ENTER("ha_mcs::external_lock");

  int rc;
  try
  {
    //@Bug 2526 Only register the transaction when autocommit is off
    if ((thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
      trans_register_ha(thd, true, mcs_hton, 0);

    rc = impl_external_lock(thd, table, lock_type);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

    @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

    @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

    @see
  get_lock_data() in lock.cc
*/

THR_LOCK_DATA** ha_mcs::store_lock(THD* thd, THR_LOCK_DATA** to, enum thr_lock_type lock_type)
{
  // if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  //  lock.type=lock_type;
  //*to++= &lock;
#ifdef INFINIDB_DEBUG
  puts("store_lock");
#endif
  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

    @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

    @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_mcs::delete_table(const char* name)
{
  DBUG_ENTER("ha_mcs::delete_table");
  /* This is not implemented but we want someone to be able that it works. */

  int rc;

  try
  {
    rc = ha_mcs_impl_delete_table(name);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_mcs::rename_table(const char* from, const char* to)
{
  DBUG_ENTER("ha_mcs::rename_table ");
  int rc;
  try
  {
    rc = ha_mcs_impl_rename_table(from, to);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_mcs::records_in_range(uint32_t inx, const key_range* min_key, const key_range* max_key,
                                 page_range* res)
{
  DBUG_ENTER("ha_mcs::records_in_range");
  DBUG_RETURN(10);  // low number to force index usage
}

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_mcs::create(const char* name, TABLE* table_arg, HA_CREATE_INFO* create_info)
{
  DBUG_ENTER("ha_mcs::create");

  int rc;
  try
  {
    rc = ha_mcs_impl_create(name, table_arg, create_info);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    rc = ER_INTERNAL_ERROR;
  }
  DBUG_RETURN(rc);
}

const COND* ha_mcs::cond_push(const COND* cond)
{
  DBUG_ENTER("ha_mcs::cond_push");
  COND* ret_cond = NULL;
  try
  {
    ret_cond = ha_mcs_impl_cond_push(const_cast<COND*>(cond), table, condStack);
  }
  catch (std::runtime_error& e)
  {
    current_thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
  }
  DBUG_RETURN(ret_cond);
}

void ha_mcs::cond_pop()
{
  DBUG_ENTER("ha_mcs::cond_pop");

  THD* thd = current_thd;

  if ((((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI) ||
       ((thd->lex)->sql_command == SQLCOM_DELETE) || ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI)) &&
      !condStack.empty())
  {
    condStack.pop_back();
  }

  DBUG_VOID_RETURN;
}

int ha_mcs::reset()
{
  DBUG_ENTER("ha_mcs::reset");

  if (!condStack.empty())
  {
    condStack.clear();
  }

  // Restore the optimizer flags which were mutated earlier in
  // ha_mcs::open/ha_mcs::discover_check_version
  restore_optimizer_flags(current_thd);

  DBUG_RETURN(0);
}

int ha_mcs::repair(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_mcs::repair");
  DBUG_ASSERT(!(int_table_flags & HA_CAN_REPAIR));
  DBUG_RETURN(HA_ADMIN_NOT_IMPLEMENTED);
}

bool ha_mcs::is_crashed() const
{
  DBUG_ENTER("ha_mcs::is_crashed");
  DBUG_RETURN(0);
}

/*
  Implementation of ha_mcs_cache, ha_mcs_cache is an insert cache for ColumnStore to
  speed up inserts.

  The idea is that inserts are first stored in storage engine that is
  fast for inserts, like MyISAM or Aria, and in case of select,
  update, delete then the rows are first flushed to ColumnStore before
  the original requests is made.

  The table used for the cache is the original table name prefixed with #cache#
*/

/*
  TODO:
  - Add create flag to myisam_open that will ensure that on open and recovery
    it restores only as many rows as stored in the header.
    (Can be fast as we have number of rows and file size stored in the header)
  - Commit to the cache is now done per statement. Should be changed to be per
    transaction.  Should not be impossible to do as we only have to update
    key_file_length and data_file_length on commit.
  - On recovery, check all #cache# tables to see if the last stored commit
    is already in ColumnStore. If yes, truncate the cache.

  Things to consider:

    Current implementation is doing a syncronization tables as part of
    thd->get_status(), which is the function to be called when server
    has got a lock of all used tables. This ensure that the row
    visibility is the same for all tables.
    The disadvantage of this is that we have to always take a write lock
    for the cache table. In case of read transactions, this lock is released
    in free_locks() as soon as we get the table lock.
    Another alternative would be to assume that if there are no cached rows
    during the call to 'store_lock', then we can ignore any new rows added.
    This would allows us to avoid write locks for the cached table, except
    for inserts or if there are rows in the cache. The disadvanteg would be
    that we would not see any rows inserted while we are trying to get the
    lock.
*/

my_bool get_status_and_flush_cache(void* param, my_bool concurrent_insert);

/*
  Create a name for the cache table
*/

static void create_cache_name(char* to, const char* name)
{
  uint dir_length = dirname_length(name);
  to = strnmov(to, name, dir_length);
  strxmov(to, CACHE_PREFIX, name + dir_length, NullS);
}

/*****************************************************************************
 THR_LOCK wrapper functions

  The idea of these is to highjack 'THR_LOCK->get_status() so that if this
  is called in a non-insert context then we will flush the cache.
  We also hijack THR_LOCK->start_trans() to free any locks on the cache
  if the command was not an insert command.
*****************************************************************************/

/*
  First call to get_status() will flush the cache if the command is not an
  insert
*/

my_bool get_status_and_flush_cache(void* param, my_bool concurrent_insert)
{
  if (current_thd->slave_thread && !get_replication_slave(current_thd))
    return (0);

  ha_mcs_cache* cache = (ha_mcs_cache*)param;
  int error;
  enum_sql_command sql_command = cache->table->in_use->lex->sql_command;

  cache->sql_command = sql_command;

  cache->insert_command = (sql_command == SQLCOM_INSERT || sql_command == SQLCOM_LOAD);
  /*
    Call first the original Aria get_status function
    All Aria get_status functions takes Maria handler as the parameter
  */
  if (cache->share->org_lock.get_status)
    (*cache->share->org_lock.get_status)(cache->cache_handler->file, concurrent_insert);

  /* If first get_status() call for this table, flush cache if needed */
  if (!cache->lock_counter++)
  {
    ha_rows num_rows = cache->num_rows_cached();
    if (((!cache->insert_command && num_rows != 0) || num_rows >= get_cache_flush_threshold(current_thd)) &&
        // In replication, LDI on a master comes as sql_command = SQLCOM_END
        // on the slave, if binlog_format != STATEMENT. See mysql_load
        // function in sql/sql_load.cc to know why. If this is the case,
        // make sure we don't flush the cache.
        (!current_thd->slave_thread || sql_command != SQLCOM_END))
    {
      if ((error = cache->flush_insert_cache()))
      {
        my_error(error, MYF(MY_WME | ME_FATAL), "Got error while trying to flush insert cache: %d", my_errno);
        return (1);
      }
    }
  }

  return (0);
}

/*
  start_trans() is called when all locks has been given
  If this was not an insert command then we can free the write lock on
  the cache table and also downgrade external lock for the cached table
  to F_READ
*/

my_bool cache_start_trans(void* param)
{
  ha_mcs_cache* cache = (ha_mcs_cache*)param;

  if (!cache->insert_command)
  {
    cache->free_locks();
    return 0;
  }

  return (*cache->share->org_lock.start_trans)(cache->cache_handler->file);
}

/* Pass through functions for all the THR_LOCK virtual functions */

static void cache_copy_status(void* to, void* from)
{
  ha_mcs_cache *to_cache = (ha_mcs_cache*)to, *from_cache = (ha_mcs_cache*)from;
  (*to_cache->share->org_lock.copy_status)(to_cache->cache_handler->file, from_cache->cache_handler->file);
}

static void cache_update_status(void* param)
{
  ha_mcs_cache* cache = (ha_mcs_cache*)param;
  (*cache->share->org_lock.update_status)(cache->cache_handler->file);
}

static void cache_restore_status(void* param)
{
  ha_mcs_cache* cache = (ha_mcs_cache*)param;
  (*cache->share->org_lock.restore_status)(cache->cache_handler->file);
}

static my_bool cache_check_status(void* param)
{
  ha_mcs_cache* cache = (ha_mcs_cache*)param;
  return (*cache->share->org_lock.check_status)(cache->cache_handler->file);
}

/*****************************************************************************
 ha_mcs_cache_share functions (Common storage for an open cache file)
*****************************************************************************/

static ha_mcs_cache_share* cache_share_list = 0;
static PSI_mutex_key key_LOCK_cache_share;
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_info all_mutexes[] = {
    {&key_LOCK_cache_share, "LOCK_cache_share", PSI_FLAG_GLOBAL},
};
#endif
static mysql_mutex_t LOCK_cache_share;

/*
  Find or create a share
*/

ha_mcs_cache_share* find_cache_share(const char* name, ulonglong cached_rows)
{
  ha_mcs_cache_share *pos, *share;
  mysql_mutex_lock(&LOCK_cache_share);
  for (pos = cache_share_list; pos; pos = pos->next)
  {
    if (!strcmp(pos->name, name))
    {
      pos->open_count++;
      mysql_mutex_unlock(&LOCK_cache_share);
      return (pos);
    }
  }
  if (!(share = (ha_mcs_cache_share*)my_malloc(PSI_NOT_INSTRUMENTED, sizeof(*share) + strlen(name) + 1,
                                               MYF(MY_FAE))))
  {
    mysql_mutex_unlock(&LOCK_cache_share);
    return 0;
  }
  share->name = (char*)(share + 1);
  share->open_count = 1;
  share->cached_rows = cached_rows;
  strmov((char*)share->name, name);
  share->next = cache_share_list;
  cache_share_list = share;
  mysql_mutex_unlock(&LOCK_cache_share);
  return share;
}

/*
  Decrement open counter and free share if there is no more users
*/

void ha_mcs_cache_share::close()
{
  ha_mcs_cache_share* pos;
  mysql_mutex_lock(&LOCK_cache_share);
  if (!--open_count)
  {
    ha_mcs_cache_share** prev = &cache_share_list;
    for (; (pos = *prev) != this; prev = &pos->next)
      ;
    *prev = next;
    my_free(this);
  }
  mysql_mutex_unlock(&LOCK_cache_share);
}

/*****************************************************************************
 ha_mcs_cache handler functions
*****************************************************************************/

static plugin_ref plugin_maria = NULL;

ha_mcs_cache::ha_mcs_cache(handlerton* hton, TABLE_SHARE* table_arg, MEM_ROOT* mem_root)
 : ha_mcs(mcs_hton, table_arg), isSysCatTable(false), isCacheDisabled(false)
{
  if (table_arg && table_arg->db.str && !strcasecmp(table_arg->db.str, "calpontsys") &&
      table_arg->table_name.str &&
      (!strcasecmp(table_arg->table_name.str, "syscolumn") ||
       !strcasecmp(table_arg->table_name.str, "systable")))
    isSysCatTable = true;

  if (get_cache_inserts(current_thd) && !isSysCatTable)
  {
    if (!plugin_maria)
    {
      LEX_CSTRING name = {STRING_WITH_LEN("Aria")};
      plugin_maria = ha_resolve_by_name(0, &name, 0);
      mcs_maria_hton = plugin_hton(plugin_maria);
      int error = mcs_maria_hton == NULL;  // Engine must exists!
      if (error)
        my_error(HA_ERR_INITIALIZATION, MYF(0), "Could not find storage engine %s", name.str);
    }

    assert(mcs_maria_hton);

    cache_handler = (ha_maria*)mcs_maria_hton->create(mcs_maria_hton, table_arg, mem_root);
    share = 0;
    lock_counter = 0;
    cache_locked = 0;
  }
}

ha_mcs_cache::~ha_mcs_cache()
{
  if (get_cache_inserts(current_thd) && !isSysCatTable && cache_handler)
  {
    delete cache_handler;
    cache_handler = NULL;
  }
}

/*
  The following functions duplicates calls to derived handler and
  cache handler
*/

int ha_mcs_cache::create(const char* name, TABLE* table_arg, HA_CREATE_INFO* ha_create_info)
{
  int error;
  char cache_name[FN_REFLEN + 8];
  DBUG_ENTER("ha_mcs_cache::create");

  if (isCacheEnabled())
  {
    create_cache_name(cache_name, name);
    /* Create a cached table */
    ha_choice save_transactional = ha_create_info->transactional;
    row_type save_row_type = ha_create_info->row_type;
    ha_create_info->transactional = HA_CHOICE_NO;
    ha_create_info->row_type = ROW_TYPE_DYNAMIC;

    if ((error = cache_handler->create(cache_name, table_arg, ha_create_info)))
      DBUG_RETURN(error);
    ha_create_info->transactional = save_transactional;
    ha_create_info->row_type = save_row_type;
  }

  /* Create the real table in ColumnStore */
  if ((error = parent::create(name, table_arg, ha_create_info)))
  {
    if (get_cache_inserts(current_thd) && !isSysCatTable)
      cache_handler->delete_table(cache_name);
    DBUG_RETURN(error);
  }

  DBUG_RETURN(0);
}

int ha_mcs_cache::open(const char* name, int mode, uint open_flags)
{
  int error, cache_error;
  DBUG_ENTER("ha_mcs_cache::open");

  if (get_cache_inserts(current_thd) && !isSysCatTable)
  {
    /* Copy table object to cache_handler */
    cache_handler->change_table_ptr(table, table->s);

    char cache_name[FN_REFLEN + 8];
    create_cache_name(cache_name, name);

    if (!(cache_error = cache_handler->open(cache_name, mode, open_flags)))
    {
      if (!(share = find_cache_share(name, cache_handler->file->state->records)))
      {
        cache_handler->close();
        DBUG_RETURN(ER_OUTOFMEMORY);
      }

      /* Fix lock so that it goes through get_status_and_flush() */
      THR_LOCK* lock = &cache_handler->file->s->lock;
      if (lock->get_status != &get_status_and_flush_cache)
      {
        mysql_mutex_lock(&cache_handler->file->s->intern_lock);

        /* The following lock is here just to establish mutex locking order */
        mysql_mutex_lock(&lock->mutex);
        mysql_mutex_unlock(&lock->mutex);

        if (lock->get_status != &get_status_and_flush_cache)
        {
          /* Remember original lock. Used by the THR_lock cache functions */
          share->org_lock = lock[0];
          if (lock->start_trans)
            lock->start_trans = &cache_start_trans;
          if (lock->copy_status)
            lock->copy_status = &cache_copy_status;
          if (lock->update_status)
            lock->update_status = &cache_update_status;
          if (lock->restore_status)
            lock->restore_status = &cache_restore_status;
          if (lock->check_status)
            lock->check_status = &cache_check_status;
          if (lock->restore_status)
            lock->restore_status = &cache_restore_status;
          lock->get_status = &get_status_and_flush_cache;
        }
        mysql_mutex_unlock(&cache_handler->file->s->intern_lock);
      }
      cache_handler->file->lock.status_param = (void*)this;
    }
    else if (cache_error == ENOENT)
    {
      if (!(error = parent::open(name, mode, open_flags)))
      {
        isCacheDisabled = true;
        DBUG_RETURN(0);
      }
      else
      {
        DBUG_RETURN(error);
      }
    }
    else
    {
      DBUG_RETURN(cache_error);
    }
  }

  if ((error = parent::open(name, mode, open_flags)))
  {
    if (get_cache_inserts(current_thd) && !isSysCatTable)
      cache_handler->close();
    DBUG_RETURN(error);
  }

  DBUG_RETURN(0);
}

int ha_mcs_cache::close()
{
  int error, error2;

  DBUG_ENTER("ha_mcs_cache::close()");

  if (isCacheEnabled())
  {
    error = cache_handler->close();
    if ((error2 = parent::close()))
      error = error2;
    ha_mcs_cache_share* org_share = share;
    if (org_share)
      org_share->close();
  }
  else
  {
    error = parent::close();
  }

  DBUG_RETURN(error);
}

/*
   Handling locking of the tables. In case of INSERT we have to lock both
   the cache handler and main table. If not, we only lock the main table
*/

uint ha_mcs_cache::lock_count(void) const
{
  /*
    If we are doing an insert or if we want to flush the cache, we have to lock
    both the Aria table and normal table.
  */
  if (isCacheEnabled())
    return 2;
  else
    return 1;
}

/**
   Store locks for the Aria table and ColumnStore table
*/

THR_LOCK_DATA** ha_mcs_cache::store_lock(THD* thd, THR_LOCK_DATA** to, enum thr_lock_type lock_type)
{
  if (isCacheEnabled())
    to = cache_handler->store_lock(thd, to, TL_WRITE);
  return parent::store_lock(thd, to, lock_type);
}

/**
   Do external locking of the tables
*/

int ha_mcs_cache::external_lock(THD* thd, int lock_type)
{
  int error = 0;
  DBUG_ENTER("ha_mcs_cache::external_lock");

  if (isCacheEnabled())
  {
    /*
      Reset lock_counter. This is ok as external_lock() is guaranteed to be
      called before first get_status()
    */
    lock_counter = 0;

    if (lock_type == F_UNLCK)
    {
      int error2;
      error = 0;
      if (cache_locked)
      {
        error = cache_handler->external_lock(thd, lock_type);
        cache_locked = 0;
      }
      if ((error2 = parent::external_lock(thd, lock_type)))
        error = error2;
      DBUG_RETURN(error);
    }

    /* Lock first with write lock to be able to do insert or flush table */
    original_lock_type = lock_type;
    lock_type = F_WRLCK;
    if ((error = cache_handler->external_lock(thd, lock_type)))
      DBUG_RETURN(error);
    if ((error = parent::external_lock(thd, lock_type)))
    {
      error = cache_handler->external_lock(thd, F_UNLCK);
      DBUG_RETURN(error);
    }

    cache_locked = 1;
  }
  else
  {
    error = parent::external_lock(thd, lock_type);
  }

  DBUG_RETURN(error);
}

int ha_mcs_cache::delete_table(const char* name)
{
  int cache_error = 0, error;

  DBUG_ENTER("ha_mcs_cache::delete_table");

  if (isCacheEnabled())
  {
    char cache_name[FN_REFLEN + 8];
    create_cache_name(cache_name, name);
    cache_error = cache_handler->delete_table(cache_name);

    if (cache_error && (cache_error != ENOENT))
      DBUG_RETURN(cache_error);
    else if (cache_error == ENOENT)
      cache_error = 0;
  }

  if ((error = parent::delete_table(name)))
    cache_error = error;

  DBUG_RETURN(cache_error);
}

int ha_mcs_cache::rename_table(const char* from, const char* to)
{
  int error = 0;

  DBUG_ENTER("ha_mcs_cache::rename_table");

  if (isCacheEnabled())
  {
    char cache_from[FN_REFLEN + 8], cache_to[FN_REFLEN + 8];
    create_cache_name(cache_from, from);
    create_cache_name(cache_to, to);
    if (!(error = cache_handler->rename_table(cache_from, cache_to)))
    {
      if ((error = parent::rename_table(from, to)))
      {
        cache_handler->rename_table(cache_to, cache_from);
        DBUG_RETURN(error);
      }
    }
    else if (error == ENOENT)
    {
      if ((error = parent::rename_table(from, to)))
      {
        DBUG_RETURN(error);
      }
    }
  }
  else
  {
    error = parent::rename_table(from, to);
  }

  DBUG_RETURN(error);
}

int ha_mcs_cache::delete_all_rows(void)
{
  int error = 0, error2;

  DBUG_ENTER("ha_mcs_cache::delete_all_rows");

  if (isCacheEnabled())
  {
    error = cache_handler->delete_all_rows();
    share->cached_rows = 0;
  }
  if ((error2 = parent::delete_all_rows()))
    error = error2;
  DBUG_RETURN(error);
}

bool ha_mcs_cache::is_crashed() const
{
  if (isCacheEnabled())
    return (cache_handler->is_crashed() || parent::is_crashed());
  else
    return parent::is_crashed();
}

/**
   After a crash, repair will be run on next open.

   There are two cases when repair is run:
   1) Automatically on open if the table is crashed
   2) When the user explicitely runs repair

   In the case of 1) we don't want to run repair on both tables as
   the repair can be a slow process. Instead we only run repair
   on the crashed tables. If no tables are marked crashed, we
   run repair on both tables.

   Repair on the cache table will delete the part of the cache that was
   not committed.

   key_file_length and data_file_length are updated last for a statement.
   When these are updated, we threat the cache as committed
*/

int ha_mcs_cache::repair(THD* thd, HA_CHECK_OPT* check_opt)
{
  int error = 0, error2;
  int something_crashed = is_crashed();
  DBUG_ENTER("ha_mcs_cache::repair");

  if (isCacheEnabled())
  {
    if (cache_handler->is_crashed() || !something_crashed)
    {
      /* Delete everything that was not already committed */
      mysql_file_chsize(cache_handler->file->dfile.file, cache_handler->file->s->state.state.data_file_length,
                        0, MYF(MY_WME));
      mysql_file_chsize(cache_handler->file->s->kfile.file,
                        cache_handler->file->s->state.state.key_file_length, 0, MYF(MY_WME));
      check_opt->flags |= T_AUTO_REPAIR;
      error = cache_handler->repair(thd, check_opt);
      share->cached_rows = cache_handler->file->state->records;
    }
  }

  if (parent::is_crashed() || !something_crashed)
    if ((error2 = parent::repair(thd, check_opt)))
      error = error2;

  DBUG_RETURN(error);
}

/**
   Write to cache handler or main table
*/
int ha_mcs_cache::write_row(const uchar* buf)
{
  if (current_thd->slave_thread && !get_replication_slave(current_thd))
    return (0);

  if (isCacheEnabled() && insert_command)
  {
    DBUG_ASSERT(share->cached_rows == cache_handler->file->state->records);
    share->cached_rows++;
    return cache_handler->write_row(buf);
  }

  return parent::write_row(buf);
}

void ha_mcs_cache::start_bulk_insert(ha_rows rows, uint flags)
{
  if (current_thd->slave_thread && !get_replication_slave(current_thd))
    return;

  if (isCacheEnabled())
  {
    if (insert_command)
    {
      bzero(&cache_handler->copy_info, sizeof(cache_handler->copy_info));
      return cache_handler->start_bulk_insert(rows, flags);
    }
    else if (sql_command == SQLCOM_INSERT_SELECT)
    {
      return parent::start_bulk_insert_from_cache(rows, flags);
    }
    // sql_command == SQLCOM_END:
    // See the comment in get_status_and_flush_cache
    else
    {
      return parent::start_bulk_insert(rows, flags);
    }
  }
  else
  {
    return parent::start_bulk_insert(rows, flags);
  }
}

int ha_mcs_cache::end_bulk_insert()
{
  if (current_thd->slave_thread && !get_replication_slave(current_thd))
    return (0);

  if (isCacheEnabled() && insert_command)
    return cache_handler->end_bulk_insert();
  return parent::end_bulk_insert();
}

/******************************************************************************
 ha_mcs_cache Plugin code
******************************************************************************/

static handler* ha_mcs_cache_create_handler(handlerton* hton, TABLE_SHARE* table, MEM_ROOT* mem_root)
{
  return new (mem_root) ha_mcs_cache(hton, table, mem_root);
}

/******************************************************************************
 ha_mcs Plugin code
******************************************************************************/

static int columnstore_init_func(void* p)
{
  DBUG_ENTER("columnstore_init_func");

  struct tm tm;
  time_t t;
  time(&t);
  localtime_r(&t, &tm);
  fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d ", tm.tm_year % 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
          tm.tm_min, tm.tm_sec);

  fprintf(stderr, "Columnstore: Started; Version: %s-%s\n", columnstore_version.c_str(),
          columnstore_release.c_str());

  strncpy(cs_version, columnstore_version.c_str(), sizeof(cs_version));
  cs_version[sizeof(cs_version) - 1] = 0;

  strncpy(cs_commit_hash, columnstore_commit_hash.c_str(), sizeof(cs_commit_hash));
  cs_commit_hash[sizeof(cs_commit_hash) - 1] = 0;

  mcs_hton = (handlerton*)p;

#ifndef _MSC_VER
  (void)pthread_mutex_init(&mcs_mutex, MY_MUTEX_INIT_FAST);
#endif
  (void)my_hash_init(PSI_NOT_INSTRUMENTED, &mcs_open_tables, system_charset_info, 32, 0, 0,
                     (my_hash_get_key)mcs_get_key, 0, 0);

  mcs_hton->create = ha_mcs_cache_create_handler;
  mcs_hton->panic = 0;
  mcs_hton->flags = HTON_CAN_RECREATE | HTON_NO_PARTITION;
  //  mcs_hton->discover_table = mcs_discover;
  //  mcs_hton->discover_table_existence = mcs_discover_existence;
  mcs_hton->commit = mcs_commit;
  mcs_hton->rollback = mcs_rollback;
  mcs_hton->close_connection = mcs_close_connection;
  mcs_hton->create_group_by = create_columnstore_group_by_handler;
  mcs_hton->create_derived = create_columnstore_derived_handler;
  mcs_hton->create_select = create_columnstore_select_handler;
  mcs_hton->db_type = DB_TYPE_AUTOASSIGN;

#ifdef HAVE_PSI_INTERFACE
  uint count = sizeof(all_mutexes) / sizeof(all_mutexes[0]);
  mysql_mutex_register("ha_mcs_cache", all_mutexes, count);
#else
  (void)key_LOCK_cache_share;
#endif
  mysql_mutex_init(key_LOCK_cache_share, &LOCK_cache_share, MY_MUTEX_INIT_FAST);

  DBUG_RETURN(0);
}

static int columnstore_done_func(void* p)
{
  DBUG_ENTER("columnstore_done_func");

  config::Config::deleteInstanceMap();
  my_hash_free(&mcs_open_tables);
#ifndef _MSC_VER
  pthread_mutex_destroy(&mcs_mutex);
#endif

  if (plugin_maria)
  {
    plugin_unlock(0, plugin_maria);
    plugin_maria = NULL;
  }

  mysql_mutex_destroy(&LOCK_cache_share);

  DBUG_RETURN(0);
}

struct st_mysql_storage_engine ha_mcs_cache_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

struct st_mysql_storage_engine columnstore_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

static struct st_mysql_information_schema is_columnstore_plugin_version = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

maria_declare_plugin(columnstore){
    MYSQL_STORAGE_ENGINE_PLUGIN, &columnstore_storage_engine,  "Columnstore",
    "MariaDB Corporation",       "ColumnStore storage engine", PLUGIN_LICENSE_GPL,
    columnstore_init_func,       columnstore_done_func,        MCSVERSIONHEX,
    mcs_status_variables,       /* status variables */
    mcs_system_variables,       /* system variables */
    PLUGIN_COLUMNSTORE_VERSION, /* string version */
    COLUMNSTORE_MATURITY        /* maturity */
},
    {MYSQL_INFORMATION_SCHEMA_PLUGIN, &is_columnstore_plugin_version, "COLUMNSTORE_COLUMNS",
     "MariaDB Corporation", "An information schema plugin to list ColumnStore columns", PLUGIN_LICENSE_GPL,
     is_columnstore_columns_plugin_init,
     // is_columnstore_tables_plugin_deinit,
     NULL, MCSVERSIONHEX, NULL, NULL, PLUGIN_COLUMNSTORE_VERSION, COLUMNSTORE_MATURITY},
    {MYSQL_INFORMATION_SCHEMA_PLUGIN, &is_columnstore_plugin_version, "COLUMNSTORE_TABLES",
     "MariaDB Corporation", "An information schema plugin to list ColumnStore tables", PLUGIN_LICENSE_GPL,
     is_columnstore_tables_plugin_init,
     // is_columnstore_tables_plugin_deinit,
     NULL, MCSVERSIONHEX, NULL, NULL, PLUGIN_COLUMNSTORE_VERSION, COLUMNSTORE_MATURITY},
    {MYSQL_INFORMATION_SCHEMA_PLUGIN, &is_columnstore_plugin_version, "COLUMNSTORE_FILES",
     "MariaDB Corporation", "An information schema plugin to list ColumnStore files", PLUGIN_LICENSE_GPL,
     is_columnstore_files_plugin_init,
     // is_columnstore_files_plugin_deinit,
     NULL, MCSVERSIONHEX, NULL, NULL, PLUGIN_COLUMNSTORE_VERSION, COLUMNSTORE_MATURITY},
    {MYSQL_INFORMATION_SCHEMA_PLUGIN, &is_columnstore_plugin_version, "COLUMNSTORE_EXTENTS",
     "MariaDB Corporation", "An information schema plugin to list ColumnStore extents", PLUGIN_LICENSE_GPL,
     is_columnstore_extents_plugin_init,
     // is_columnstore_extents_plugin_deinit,
     NULL, MCSVERSIONHEX, NULL, NULL, PLUGIN_COLUMNSTORE_VERSION,
     COLUMNSTORE_MATURITY} maria_declare_plugin_end;

/******************************************************************************
Implementation of write cache
******************************************************************************/

ha_rows ha_mcs_cache::num_rows_cached()
{
  return cache_handler->file->state->records;
}

/* Free write locks if this was not an insert */

void ha_mcs_cache::free_locks()
{
  /* Restart transaction for columnstore table */
  if (original_lock_type != F_WRLCK)
  {
    parent::external_lock(table->in_use, F_UNLCK);
    parent::external_lock(table->in_use, original_lock_type);
  }

  /* We don't need to lock cache_handler anymore as it's already flushed */
  cache_handler->external_lock(table->in_use, F_UNLCK);
  thr_unlock(&cache_handler->file->lock, 0);
  cache_locked = false;
}

/**
   Copy data from cache to ColumnStore

   Both tables are locked. The from table has also an exclusive lock to
   ensure that no one is inserting data to it while we are reading it.
*/

int ha_mcs_cache::flush_insert_cache()
{
  int error, error2;
  ha_maria* from = cache_handler;
  uchar* record = table->record[0];
  ulonglong copied_rows = 0;
  DBUG_ENTER("flush_insert_cache");

  DBUG_ASSERT(from->file->state->records == share->cached_rows);

  parent::start_bulk_insert_from_cache(from->file->state->records, 0);
  from->rnd_init(1);
  while (!(error = from->rnd_next(record)))
  {
    copied_rows++;
    if ((error = parent::write_row(record)))
      goto end;
    rows_changed++;
  }
  if (error == HA_ERR_END_OF_FILE)
    error = 0;
  DBUG_ASSERT(copied_rows == share->cached_rows);

end:
  from->rnd_end();
  if ((error2 = parent::end_bulk_insert()) && !error)
    error = error2;

  if (!error)
  {
    if (parent::ht->commit)
      error = parent::ht->commit(parent::ht, table->in_use, 1);
  }
  else
  {
    /* We can ignore the rollback error as we already have some other errors */
    if (parent::ht->rollback)
      parent::ht->rollback(parent::ht, table->in_use, 1);
  }

  DBUG_ASSERT(error == 0);
  if (!error)
  {
    /*
      Everything went fine, delete all rows from the cache and allow others
      to use it.
    */
    {
      /*
        We have to unlock the lock mutex as otherwise we get a conflict in
        mutex order. This is fine as we have a write lock on the mutex,
        so we will be able to get it again
      */
      mysql_mutex_unlock(&from->file->s->lock.mutex);
      from->delete_all_rows();
      share->cached_rows = 0;
      mysql_mutex_lock(&from->file->s->lock.mutex);
    }
  }
  DBUG_RETURN(error);
}
