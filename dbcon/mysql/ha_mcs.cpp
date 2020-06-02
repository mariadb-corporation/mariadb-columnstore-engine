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

#include <typeinfo>
#include "ha_mcs.h"
#include "columnstoreversion.h"

#include "ha_mcs_pushdown.h"
#define NEED_CALPONT_EXTERNS
#include "ha_mcs_impl.h"
#include "is_columnstore.h"
#include "ha_mcs_version.h"

#ifndef COLUMNSTORE_MATURITY
#define COLUMNSTORE_MATURITY MariaDB_PLUGIN_MATURITY_STABLE
#endif

static handler* mcs_create_handler(handlerton* hton,
                                   TABLE_SHARE* table,
                                   MEM_ROOT* mem_root);

static int mcs_commit(handlerton* hton, THD* thd, bool all);

static int mcs_rollback(handlerton* hton, THD* thd, bool all);
static int mcs_close_connection(handlerton* hton, THD* thd );
handlerton* mcs_hton;
char cs_version[25];
char cs_commit_hash[41]; // a commit hash is 40 characters

// handlers creation function for hton.
// Look into ha_mcs_pushdown.* for more details.
group_by_handler*
create_columnstore_group_by_handler(THD* thd, Query* query);

derived_handler*
create_columnstore_derived_handler(THD* thd, TABLE_LIST *derived);

select_handler*
create_columnstore_select_handler(THD* thd, SELECT_LEX* sel);

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

static uchar* mcs_get_key(COLUMNSTORE_SHARE* share, size_t* length,
                          my_bool not_used __attribute__((unused)))
{
    *length = share->table_name_length;
    return (uchar*) share->table_name;
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
int mcs_discover_existence(handlerton* hton, const char* db,
                           const char* table_name)
{
    return ha_mcs_impl_discover_existence(db, table_name);
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

    strncpy(cs_version, columnstore_version.c_str(), sizeof(cs_version));
    cs_version[sizeof(cs_version) - 1] = 0;

    strncpy(cs_commit_hash, columnstore_commit_hash.c_str(), sizeof(cs_commit_hash));
    cs_commit_hash[sizeof(cs_commit_hash) - 1] = 0;

    mcs_hton = (handlerton*)p;
#ifndef _MSC_VER
    (void) pthread_mutex_init(&mcs_mutex, MY_MUTEX_INIT_FAST);
#endif
    (void) my_hash_init(PSI_NOT_INSTRUMENTED, &mcs_open_tables, system_charset_info, 32, 0, 0,
                        (my_hash_get_key) mcs_get_key, 0, 0);

    mcs_hton->create =  mcs_create_handler;
    mcs_hton->flags =   HTON_CAN_RECREATE;
//  mcs_hton->discover_table = mcs_discover;
//  mcs_hton->discover_table_existence = mcs_discover_existence;
    mcs_hton->commit = mcs_commit;
    mcs_hton->rollback = mcs_rollback;
    mcs_hton->close_connection = mcs_close_connection;
    mcs_hton->create_group_by = create_columnstore_group_by_handler;
    mcs_hton->create_derived = create_columnstore_derived_handler;
    mcs_hton->create_select = create_columnstore_select_handler;
    mcs_hton->db_type = DB_TYPE_AUTOASSIGN;
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
    DBUG_RETURN(0);
}

static handler* mcs_create_handler(handlerton* hton,
                                   TABLE_SHARE* table,
                                   MEM_ROOT* mem_root)
{
    return new (mem_root) ha_mcs(hton, table);
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

ha_mcs::ha_mcs(handlerton* hton, TABLE_SHARE* table_arg) :
    handler(hton, table_arg),
    int_table_flags(HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
                    HA_TABLE_SCAN_ON_INDEX |
                    HA_CAN_TABLE_CONDITION_PUSHDOWN |
                    HA_CAN_DIRECT_UPDATE_AND_DELETE)
{ }


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

static const char* ha_mcs_exts[] =
{
    NullS
};

const char** ha_mcs::bas_ext() const
{
    return ha_mcs_exts;
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
        rc = ha_mcs_impl_write_row(buf, table, rows_changed);
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
int ha_mcs::update_row(const uchar* old_data, uchar* new_data)
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
int ha_mcs::direct_update_rows_init(List<Item> *update_fields)
{
    DBUG_ENTER("ha_mcs::direct_update_rows_init");
    DBUG_RETURN(0);
}

int ha_mcs::direct_update_rows(ha_rows *update_rows)
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

int ha_mcs::direct_update_rows(ha_rows *update_rows, ha_rows *found_rows)
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

int ha_mcs::direct_delete_rows(ha_rows *deleted_rows)
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

int ha_mcs::index_read_map(uchar* buf, const uchar* key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
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
    if(scan)
    {
        try
        {
            rc = ha_mcs_impl_rnd_init(table, condStack);
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
        rc = ha_mcs_impl_rnd_next(buf, table);
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
            trans_register_ha( thd, true, mcs_hton, 0);

        rc = ha_mcs_impl_external_lock(thd, table, lock_type);
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

THR_LOCK_DATA** ha_mcs::store_lock(THD* thd,
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
ha_rows ha_mcs::records_in_range(uint32_t inx, key_range* min_key,
                                     key_range* max_key)
{
    DBUG_ENTER("ha_mcs::records_in_range");
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

int ha_mcs::create(const char* name, TABLE* table_arg,
                       HA_CREATE_INFO* create_info)
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

    if ((((thd->lex)->sql_command == SQLCOM_UPDATE) ||
         ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI) ||
         ((thd->lex)->sql_command == SQLCOM_DELETE) ||
         ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI)) &&
        !condStack.empty())
    {
        condStack.pop_back();
    }

    DBUG_VOID_RETURN;
}

struct st_mysql_storage_engine columnstore_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static struct st_mysql_information_schema is_columnstore_plugin_version =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };


maria_declare_plugin(columnstore)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &columnstore_storage_engine,
  "Columnstore",
  "MariaDB Corporation",
  "ColumnStore storage engine",
  PLUGIN_LICENSE_GPL,
  columnstore_init_func,
  columnstore_done_func,
  MCSVERSIONHEX,
  mcs_status_variables,          /* status variables                */
  mcs_system_variables,          /* system variables                */
  MCSVERSION,                    /* string version */
  COLUMNSTORE_MATURITY           /* maturity */
},
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_plugin_version,
    "COLUMNSTORE_COLUMNS",
    "MariaDB Corporation",
    "An information schema plugin to list ColumnStore columns",
    PLUGIN_LICENSE_GPL,
    is_columnstore_columns_plugin_init,
    //is_columnstore_tables_plugin_deinit,
    NULL,
    MCSVERSIONHEX,
    NULL,
    NULL,
    MCSVERSION,
    COLUMNSTORE_MATURITY
},
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_plugin_version,
    "COLUMNSTORE_TABLES",
    "MariaDB Corporation",
    "An information schema plugin to list ColumnStore tables",
    PLUGIN_LICENSE_GPL,
    is_columnstore_tables_plugin_init,
    //is_columnstore_tables_plugin_deinit,
    NULL,
    MCSVERSIONHEX,
    NULL,
    NULL,
    MCSVERSION,
    COLUMNSTORE_MATURITY
},
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_plugin_version,
    "COLUMNSTORE_FILES",
    "MariaDB Corporation",
    "An information schema plugin to list ColumnStore files",
    PLUGIN_LICENSE_GPL,
    is_columnstore_files_plugin_init,
    //is_columnstore_files_plugin_deinit,
    NULL,
    MCSVERSIONHEX,
    NULL,
    NULL,
    MCSVERSION,
    COLUMNSTORE_MATURITY
},
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_plugin_version,
    "COLUMNSTORE_EXTENTS",
    "MariaDB Corporation",
    "An information schema plugin to list ColumnStore extents",
    PLUGIN_LICENSE_GPL,
    is_columnstore_extents_plugin_init,
    //is_columnstore_extents_plugin_deinit,
    NULL,
    MCSVERSIONHEX,
    NULL,
    NULL,
    MCSVERSION,
    COLUMNSTORE_MATURITY
}
maria_declare_plugin_end;

