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

#include "ha_calpont.h"

#define NEED_CALPONT_EXTERNS
#include "ha_calpont_impl.h"

static handler* calpont_create_handler(handlerton* hton,
                                       TABLE_SHARE* table,
                                       MEM_ROOT* mem_root);

static int calpont_commit(handlerton* hton, THD* thd, bool all);

static int calpont_rollback(handlerton* hton, THD* thd, bool all);
static int calpont_close_connection ( handlerton* hton, THD* thd );
handlerton* calpont_hton;

static group_by_handler*
create_calpont_group_by_handler(THD* thd, Query* query);

/* Variables for example share methods */

/*
   Hash used to track the number of open tables; variable for example share
   methods
*/
static HASH calpont_open_tables;

#ifndef _MSC_VER
/* The mutex used to init the hash; variable for example share methods */
pthread_mutex_t calpont_mutex;
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

static uchar* calpont_get_key(INFINIDB_SHARE* share, size_t* length,
                              my_bool not_used __attribute__((unused)))
{
    *length = share->table_name_length;
    return (uchar*) share->table_name;
}

int calpont_discover(handlerton* hton, THD* thd, TABLE_SHARE* share)
{
    DBUG_ENTER("calpont_discover");
    DBUG_PRINT("calpont_discover", ("db: '%s'  name: '%s'", share->db.str, share->table_name.str));
#ifdef INFINIDB_DEBUG
    fprintf(stderr, "calpont_discover()\n");
#endif

    uchar* frm_data = NULL;
    size_t frm_len = 0;
    int error = 0;

    if (!ha_calpont_impl_discover_existence(share->db.str, share->table_name.str))
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);

    error = share->read_frm_image((const uchar**)&frm_data, &frm_len);

    if (error)
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);

    my_errno = share->init_from_binary_frm_image(thd, 1, frm_data, frm_len);

    my_free(frm_data);
    DBUG_RETURN(my_errno);
}

int calpont_discover_existence(handlerton* hton, const char* db,
                               const char* table_name)
{
    return ha_calpont_impl_discover_existence(db, table_name);
}

static int columnstore_init_func(void* p)
{
    DBUG_ENTER("columnstore_init_func");

    struct tm tm;
    time_t t;

    time(&t);
    localtime_r(&t, &tm);
    fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d ",
            tm.tm_year % 100, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec);

    fprintf(stderr, "Columnstore: Started; Version: %s-%s\n", columnstore_version.c_str(), columnstore_release.c_str());

    calpont_hton = (handlerton*)p;
#ifndef _MSC_VER
    (void) pthread_mutex_init(&calpont_mutex, MY_MUTEX_INIT_FAST);
#endif
    (void) my_hash_init(&calpont_open_tables, system_charset_info, 32, 0, 0,
                        (my_hash_get_key) calpont_get_key, 0, 0);

    calpont_hton->state =   SHOW_OPTION_YES;
    calpont_hton->create =  calpont_create_handler;
    calpont_hton->flags =   HTON_CAN_RECREATE;
//  calpont_hton->discover_table= calpont_discover;
//  calpont_hton->discover_table_existence= calpont_discover_existence;
    calpont_hton->commit = calpont_commit;
    calpont_hton->rollback = calpont_rollback;
    calpont_hton->close_connection = calpont_close_connection;
    calpont_hton->create_group_by = create_calpont_group_by_handler;
    DBUG_RETURN(0);
}

static int infinidb_init_func(void* p)
{
    DBUG_ENTER("infinidb_init_func");

    struct tm tm;
    time_t t;

    time(&t);
    localtime_r(&t, &tm);
    fprintf(stderr, "%02d%02d%02d %2d:%02d:%02d ",
            tm.tm_year % 100, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec);

    fprintf(stderr, "Columnstore: Started; Version: %s-%s\n", columnstore_version.c_str(), columnstore_release.c_str());

    calpont_hton = (handlerton*)p;

    calpont_hton->state =   SHOW_OPTION_YES;
    calpont_hton->create =  calpont_create_handler;
    calpont_hton->flags =   HTON_CAN_RECREATE;
//  calpont_hton->discover_table= calpont_discover;
//  calpont_hton->discover_table_existence= calpont_discover_existence;
    calpont_hton->commit = calpont_commit;
    calpont_hton->rollback = calpont_rollback;
    calpont_hton->close_connection = calpont_close_connection;
    DBUG_RETURN(0);
}


static int columnstore_done_func(void* p)
{
    DBUG_ENTER("calpont_done_func");

    my_hash_free(&calpont_open_tables);
#ifndef _MSC_VER
    pthread_mutex_destroy(&calpont_mutex);
#endif
    DBUG_RETURN(0);
}

static int infinidb_done_func(void* p)
{
    DBUG_ENTER("calpont_done_func");

    DBUG_RETURN(0);
}

static handler* calpont_create_handler(handlerton* hton,
                                       TABLE_SHARE* table,
                                       MEM_ROOT* mem_root)
{
    return new (mem_root) ha_calpont(hton, table);
}

static int calpont_commit(handlerton* hton, THD* thd, bool all)
{
    int rc = ha_calpont_impl_commit( hton, thd, all);
    return rc;
}

static int calpont_rollback(handlerton* hton, THD* thd, bool all)
{
    int rc = ha_calpont_impl_rollback( hton, thd, all);
    return rc;
}

static int calpont_close_connection ( handlerton* hton, THD* thd )
{
    int rc = ha_calpont_impl_close_connection( hton, thd);
    return rc;
}

ha_calpont::ha_calpont(handlerton* hton, TABLE_SHARE* table_arg) :
    handler(hton, table_arg),
    int_table_flags(HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
                    HA_TABLE_SCAN_ON_INDEX |
                    HA_CAN_TABLE_CONDITION_PUSHDOWN)
//	int_table_flags(HA_NO_BLOBS | HA_BINLOG_STMT_CAPABLE)
{
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

static const char* ha_calpont_exts[] =
{
    NullS
};

const char** ha_calpont::bas_ext() const
{
    return ha_calpont_exts;
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

int ha_calpont::open(const char* name, int mode, uint32_t test_if_locked)
{
    DBUG_ENTER("ha_calpont::open");

    //if (!(share = get_share(name, table)))
    //  DBUG_RETURN(1);
    //thr_lock_data_init(&share->lock,&lock,NULL);

    int rc = ha_calpont_impl_open(name, mode, test_if_locked);

    DBUG_RETURN(rc);
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

int ha_calpont::close(void)
{
    DBUG_ENTER("ha_calpont::close");
    //DBUG_RETURN(free_share(share));

    int rc = ha_calpont_impl_close();

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
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
    @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

    @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_calpont::write_row(uchar* buf)
{
    DBUG_ENTER("ha_calpont::write_row");
    int rc = ha_calpont_impl_write_row(buf, table);

    DBUG_RETURN(rc);
}

void ha_calpont::start_bulk_insert(ha_rows rows, uint flags)
{
    DBUG_ENTER("ha_calpont::start_bulk_insert");
    ha_calpont_impl_start_bulk_insert(rows, table);
    DBUG_VOID_RETURN;
}

int ha_calpont::end_bulk_insert(bool abort)
{
    DBUG_ENTER("ha_calpont::end_bulk_insert");
    int rc = ha_calpont_impl_end_bulk_insert(abort, table);
    DBUG_RETURN(rc);
}

/**@bug 2461 - Overloaded end_bulk_insert.  MariaDB uses the abort bool, mysql does not. */
int ha_calpont::end_bulk_insert()
{
    DBUG_ENTER("ha_calpont::end_bulk_insert");
    int rc = ha_calpont_impl_end_bulk_insert(false, table);
    DBUG_RETURN(rc);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

    @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for example by doing:
    @code
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
    table->timestamp_field->set_time();
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
    @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

    @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_calpont::update_row(const uchar* old_data, uchar* new_data)
{

    DBUG_ENTER("ha_calpont::update_row");
    int rc = ha_calpont_impl_update_row();
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

int ha_calpont::delete_row(const uchar* buf)
{
    DBUG_ENTER("ha_calpont::delete_row");
    int rc = ha_calpont_impl_delete_row();
    DBUG_RETURN(rc);
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_calpont::index_read_map(uchar* buf, const uchar* key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
    DBUG_ENTER("ha_calpont::index_read");
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_calpont::index_next(uchar* buf)
{
    DBUG_ENTER("ha_calpont::index_next");
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_calpont::index_prev(uchar* buf)
{
    DBUG_ENTER("ha_calpont::index_prev");
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
int ha_calpont::index_first(uchar* buf)
{
    DBUG_ENTER("ha_calpont::index_first");
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
int ha_calpont::index_last(uchar* buf)
{
    DBUG_ENTER("ha_calpont::index_last");
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
int ha_calpont::rnd_init(bool scan)
{
    DBUG_ENTER("ha_calpont::rnd_init");

    int rc = ha_calpont_impl_rnd_init(table);

    DBUG_RETURN(rc);
}

int ha_calpont::rnd_end()
{
    DBUG_ENTER("ha_calpont::rnd_end");

    int rc = ha_calpont_impl_rnd_end(table);

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
int ha_calpont::rnd_next(uchar* buf)
{
    DBUG_ENTER("ha_calpont::rnd_next");

    int rc = ha_calpont_impl_rnd_next(buf, table);

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
void ha_calpont::position(const uchar* record)
{
    DBUG_ENTER("ha_calpont::position");
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
int ha_calpont::rnd_pos(uchar* buf, uchar* pos)
{
    DBUG_ENTER("ha_calpont::rnd_pos");
    int rc = ha_calpont_impl_rnd_pos(buf, pos);
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
int ha_calpont::info(uint32_t flag)
{
    DBUG_ENTER("ha_calpont::info");
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
int ha_calpont::extra(enum ha_extra_function operation)
{
    DBUG_ENTER("ha_calpont::extra");
#ifdef INFINIDB_DEBUG
    {
        const char* hefs;

        switch (operation)
        {
            case HA_EXTRA_NO_READCHECK:
                hefs = "HA_EXTRA_NO_READCHECK";
                break;

            case HA_EXTRA_CACHE:
                hefs = "HA_EXTRA_CACHE";
                break;

            case HA_EXTRA_NO_CACHE:
                hefs = "HA_EXTRA_NO_CACHE";
                break;

            case HA_EXTRA_NO_IGNORE_DUP_KEY:
                hefs = "HA_EXTRA_NO_IGNORE_DUP_KEY";
                break;

            case HA_EXTRA_PREPARE_FOR_RENAME:
                hefs = "HA_EXTRA_PREPARE_FOR_RENAME";
                break;

            default:
                hefs = "UNKNOWN ENUM!";
                break;
        }

        fprintf(stderr, "ha_calpont::extra(\"%s\", %d: %s)\n", table->s->table_name.str, operation, hefs);
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
int ha_calpont::delete_all_rows()
{
    DBUG_ENTER("ha_calpont::delete_all_rows");
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
int ha_calpont::external_lock(THD* thd, int lock_type)
{
    DBUG_ENTER("ha_calpont::external_lock");

    //@Bug 2526 Only register the transaction when autocommit is off
    if ((thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
        trans_register_ha( thd, true, calpont_hton);

    int rc = ha_calpont_impl_external_lock(thd, table, lock_type);
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

THR_LOCK_DATA** ha_calpont::store_lock(THD* thd,
                                       THR_LOCK_DATA** to,
                                       enum thr_lock_type lock_type)
{
    //if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
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
int ha_calpont::delete_table(const char* name)
{
    DBUG_ENTER("ha_calpont::delete_table");
    /* This is not implemented but we want someone to be able that it works. */

    int rc = ha_calpont_impl_delete_table(name);

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
int ha_calpont::rename_table(const char* from, const char* to)
{
    DBUG_ENTER("ha_calpont::rename_table ");
    int rc = ha_calpont_impl_rename_table(from, to);
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
ha_rows ha_calpont::records_in_range(uint32_t inx, key_range* min_key,
                                     key_range* max_key)
{
    DBUG_ENTER("ha_calpont::records_in_range");
    DBUG_RETURN(10);                         // low number to force index usage
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

int ha_calpont::create(const char* name, TABLE* table_arg,
                       HA_CREATE_INFO* create_info)
{
    DBUG_ENTER("ha_calpont::create");

    int rc = ha_calpont_impl_create(name, table_arg, create_info);
//  table_arg->s->write_frm_image();
    DBUG_RETURN(rc);
}

const COND* ha_calpont::cond_push(const COND* cond)
{
    DBUG_ENTER("ha_calpont::cond_push");
    DBUG_RETURN(ha_calpont_impl_cond_push(const_cast<COND*>(cond), table));
}


struct st_mysql_storage_engine columnstore_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

struct st_mysql_storage_engine infinidb_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

/*@brief  check_walk - It traverses filter conditions*/
/************************************************************
 * DESCRIPTION:
 * It traverses filter predicates looking for unsupported
 * JOIN types: non-equi JOIN, e.g  t1.c1 > t2.c2;
 * logical OR.
 * PARAMETERS:
 *    thd - THD pointer.
 *    derived - TABLE_LIST* to work with.
 * RETURN:
 *    derived_handler if possible
 *    NULL in other case
 ***********************************************************/
void check_walk(const Item* item, void* arg)
{
    bool* unsupported_feature  = static_cast<bool*>(arg);
    if ( *unsupported_feature )
        return;
    switch (item->type())
    {
        case Item::FUNC_ITEM:
        {
            const Item_func* ifp = static_cast<const Item_func*>(item);

            if ( ifp->functype() != Item_func::EQ_FUNC ) // NON-equi JOIN
            {
                if ( ifp->argument_count() == 2 &&
                    ifp->arguments()[0]->type() == Item::FIELD_ITEM &&
                    ifp->arguments()[1]->type() == Item::FIELD_ITEM )
                {
                    Item_field* left= static_cast<Item_field*>(ifp->arguments()[0]);
                    Item_field* right= static_cast<Item_field*>(ifp->arguments()[1]);

                    if ( left->field->table != right->field->table )
                    {
                        *unsupported_feature = true;
                        return;
                    }
                }
                else // IN + correlated subquery 
                {
                    if ( ifp->functype() == Item_func::NOT_FUNC
                        && ifp->arguments()[0]->type() == Item::EXPR_CACHE_ITEM )
                    {
                        check_walk(ifp->arguments()[0], arg);
                    }
                }
            }
            break;
        }
        
        case Item::EXPR_CACHE_ITEM: // IN + correlated subquery
        {
            const Item_cache_wrapper* icw = static_cast<const Item_cache_wrapper*>(item);
            if ( icw->get_orig_item()->type() == Item::FUNC_ITEM )
            {
                const Item_func *ifp = static_cast<const Item_func*>(icw->get_orig_item());
                if ( ifp->argument_count() == 2 &&
                    ( ifp->arguments()[0]->type() == Item::Item::SUBSELECT_ITEM
                    || ifp->arguments()[1]->type() == Item::Item::SUBSELECT_ITEM ))
                {
                    *unsupported_feature = true;
                    return;
                }
            }
            break;
        }

        case Item::COND_ITEM: // OR in cods is unsupported yet
        {
            Item_cond* icp = (Item_cond*)item;
            if ( is_cond_or(icp) )
            {
                *unsupported_feature = true;
            }
            break;
        }
        default:
        {
            break;
        }
    }
}

/*@brief  create_calpont_group_by_handler- Creates handler*/
/***********************************************************
 * DESCRIPTION:
 * Creates a group_by pushdown handler if there is no:
 *  non-equi JOIN, e.g * t1.c1 > t2.c2
 *  logical OR in the filter predicates
 *  Impossible WHERE
 *  Impossible HAVING
 * and there is either GROUP BY or aggregation function
 * exists at the top level.
 * Valid queries with the last two crashes the server if
 * processed.
 * Details are in server/sql/group_by_handler.h
 * PARAMETERS:
 *    thd - THD pointer
 *    query - Query structure LFM in group_by_handler.h
 * RETURN:
 *    group_by_handler if success
 *    NULL in other case
 ***********************************************************/
static group_by_handler*
create_calpont_group_by_handler(THD* thd, Query* query)
{
    ha_calpont_group_by_handler* handler = NULL;
    // same as thd->lex->current_select
    SELECT_LEX *select_lex = query->from->select_lex;

    // Create a handler if query is valid. See comments for details.
    if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE
        && ( thd->variables.infinidb_vtable_mode == 0
            || thd->variables.infinidb_vtable_mode == 2 )
        && ( query->group_by || select_lex->with_sum_func ) )
    {
        bool unsupported_feature = false;
        // revisit SELECT_LEX for all units
        for(TABLE_LIST* tl = query->from; !unsupported_feature && tl; tl = tl->next_global)
        {
            select_lex = tl->select_lex;
            // Correlation subquery. Comming soon so fail on this yet.
            unsupported_feature = select_lex->is_correlated;

            // Impossible HAVING or WHERE
            if ( ( !unsupported_feature && query->having && select_lex->having_value == Item::COND_FALSE )
                || ( select_lex->cond_count > 0
                    && select_lex->cond_value == Item::COND_FALSE ) )
            {
                unsupported_feature = true;
            }

            // Unsupported JOIN conditions
            if ( !unsupported_feature )
            {
                JOIN *join = select_lex->join;
                Item_cond *icp = 0;

                if (join != 0)
                    icp = reinterpret_cast<Item_cond*>(join->conds);

                if ( unsupported_feature == false
                    && icp )
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }

                // Optimizer could move some join conditions into where
                if (select_lex->where != 0)
                    icp = reinterpret_cast<Item_cond*>(select_lex->where);

                if ( unsupported_feature == false
                    && icp )
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }

            }
        } // unsupported features check ends here

        if ( !unsupported_feature )
        {
            handler = new ha_calpont_group_by_handler(thd, query);

            // Notify the server, that CS handles GROUP BY, ORDER BY and HAVING clauses.
            query->group_by = NULL;
            query->order_by = NULL;
            query->having = NULL;
        }
    }

    return handler;
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    query - Query describing structure
 ***********************************************************/
ha_calpont_group_by_handler::ha_calpont_group_by_handler(THD* thd_arg, Query* query)
        : group_by_handler(thd_arg, calpont_hton),
          select(query->select),
          table_list(query->from),
          distinct(query->distinct),
          where(query->where),
          group_by(query->group_by),
          order_by(query->order_by),
          having(query->having)
{
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY destructor
 ***********************************************************/
ha_calpont_group_by_handler::~ha_calpont_group_by_handler()
{
}

/***********************************************************
 * DESCRIPTION:
 * Makes the plan and prepares the data
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::init_scan()
{
    DBUG_ENTER("ha_calpont_group_by_handler::init_scan");

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    // MCOL-1052 Should be removed after cleaning the code up.
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;
    int rc = ha_calpont_impl_group_by_init(this, table);
    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Fetches a row and saves it to a temporary table.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::next_row()
{
    DBUG_ENTER("ha_calpont_group_by_handler::next_row");
    int rc = ha_calpont_impl_group_by_next(this, table);

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Shuts the scan down.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::end_scan()
{
    DBUG_ENTER("ha_calpont_group_by_handler::end_scan");

    int rc = ha_calpont_impl_group_by_end(this, table);

    DBUG_RETURN(rc);
}

mysql_declare_plugin(columnstore)
{
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &columnstore_storage_engine,
    "Columnstore",
    "MariaDB",
    "Columnstore storage engine",
    PLUGIN_LICENSE_GPL,
    columnstore_init_func,                        /* Plugin Init */
    columnstore_done_func,                        /* Plugin Deinit */
    0x0100 /* 1.0 */,
    NULL,                                         /* status variables */
    mcs_system_variables,                         /* system variables */
    NULL,                                         /* reserved */
    0                                             /* config flags */
},
{
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &infinidb_storage_engine,
    "InfiniDB",
    "MariaDB",
    "Columnstore storage engine (deprecated: use columnstore)",
    PLUGIN_LICENSE_GPL,
    infinidb_init_func,                           /* Plugin Init */
    infinidb_done_func,                            /* Plugin Deinit */
    0x0100 /* 1.0 */,
    NULL,                                         /* status variables */
    mcs_system_variables,                         /* system variables */
    NULL,                                         /* reserved */
    0                                             /* config flags */
}
mysql_declare_plugin_end;
maria_declare_plugin(columnstore)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &columnstore_storage_engine,
  "Columnstore",
  "MariaDB",
  "Columnstore storage engine",
  PLUGIN_LICENSE_GPL,
  columnstore_init_func,
  columnstore_done_func,
  0x0100, /* 1.0 */
  NULL,                          /* status variables                */
  mcs_system_variables,          /* system variables                */
  "1.0",                         /* string version */
  MariaDB_PLUGIN_MATURITY_STABLE /* maturity */
},
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &infinidb_storage_engine,
  "InfiniDB",
  "MariaDB",
  "Columnstore storage engine (deprecated: use columnstore)",
  PLUGIN_LICENSE_GPL,
  infinidb_init_func,
  infinidb_done_func,
  0x0100, /* 1.0 */
  NULL,                           /* status variables                */
  mcs_system_variables,           /* system variables                */
  "1.0",                          /* string version */
  MariaDB_PLUGIN_MATURITY_STABLE  /* maturity */
}
maria_declare_plugin_end;

