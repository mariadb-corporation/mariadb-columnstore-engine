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
/** @file ha_example.h

    @brief
  The ha_example engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/example/ha_example.cc.

    @note
  Please read ha_example.cc before reading this file.
  Reminder: The example storage engine implements all methods that are *required*
  to be implemented. For a full list of all methods that you can implement, see
  handler.h.

   @see
  /sql/handler.h and /storage/example/ha_example.cc
*/

// $Id: ha_calpont.h 9210 2013-01-21 14:10:42Z rdempsey $

#ifndef HA_CALPONT_H__
#define HA_CALPONT_H__
#include <my_config.h>
#include "idb_mysql.h"

extern handlerton* calpont_hton;

/** @brief
  EXAMPLE_SHARE is a structure that will be shared among all open handlers.
  This example implements the minimum of what you will probably need.
*/
typedef struct st_calpont_share
{
    char* table_name;
    uint32_t table_name_length, use_count;
    pthread_mutex_t mutex;
    THR_LOCK lock;
} INFINIDB_SHARE;

/** @brief
  Class definition for the storage engine
*/
class ha_calpont: public handler
{
    THR_LOCK_DATA lock;      ///< MySQL lock
    INFINIDB_SHARE* share;    ///< Shared lock info
    ulonglong int_table_flags;

public:
    ha_calpont(handlerton* hton, TABLE_SHARE* table_arg);
    ~ha_calpont()
    {
    }

    /** @brief
      The name that will be used for display purposes.
     */
    const char* table_type() const
    {
        return "ColumnStore";
    }

    /** @brief
      The name of the index type that will be used for display.
      Don't implement this method unless you really have indexes.
     */
    //const char *index_type(uint32_t inx) { return "HASH"; }

    /** @brief
      The file extensions.
     */
    const char** bas_ext() const;

    /** @brief
      This is a list of flags that indicate what functionality the storage engine
      implements. The current table flags are documented in handler.h
    */
    ulonglong table_flags() const
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
    ulong index_flags(uint32_t inx, uint32_t part, bool all_parts) const
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
    uint32_t max_supported_record_length() const
    {
        return HA_MAX_REC_LENGTH;
    }

    /** @brief
      Called in test_quick_select to determine if indexes should be used.
    */
    virtual double scan_time()
    {
        return (double) (stats.records + stats.deleted) / 20.0 + 10;
    }

    /*
      Everything below are methods that we implement in ha_example.cc.

      Most of these methods are not obligatory, skip them and
      MySQL will treat them as not implemented
    */

    /** @brief
      We implement this in ha_example.cc; it's a required method.
    */
    int open(const char* name, int mode, uint32_t test_if_locked);    // required

    /** @brief
      We implement this in ha_example.cc; it's a required method.
    */
    int close(void);                                              // required

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int write_row(uchar* buf);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    void start_bulk_insert(ha_rows rows, uint flags = 0) ;

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int end_bulk_insert(bool abort) ;

    /**@bug 2461 - Overloaded end_bulk_insert.  MariaDB uses the abort bool, mysql does not. */
    int end_bulk_insert() ;

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int update_row(const uchar* old_data, uchar* new_data);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int delete_row(const uchar* buf);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int index_read_map(uchar* buf, const uchar* key,
                       key_part_map keypart_map, enum ha_rkey_function find_flag);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int index_next(uchar* buf);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int index_prev(uchar* buf);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int index_first(uchar* buf);

    /** @brief
      We implement this in ha_example.cc. It's not an obligatory method;
      skip it and and MySQL will treat it as not implemented.
    */
    int index_last(uchar* buf);

    /** @brief
      Unlike index_init(), rnd_init() can be called two consecutive times
      without rnd_end() in between (it only makes sense if scan=1). In this
      case, the second call should prepare for the new table scan (e.g if
      rnd_init() allocates the cursor, the second call should position the
      cursor to the start of the table; no need to deallocate and allocate
      it again. This is a required method.
    */
    int rnd_init(bool scan);                                      //required
    int rnd_end();
    int rnd_next(uchar* buf);                                     ///< required
    int rnd_pos(uchar* buf, uchar* pos);                          ///< required
    void position(const uchar* record);                           ///< required
    int info(uint32_t);                                               ///< required
    int extra(enum ha_extra_function operation);
    int external_lock(THD* thd, int lock_type);                   ///< required
    int delete_all_rows(void);
    ha_rows records_in_range(uint32_t inx, key_range* min_key,
                             key_range* max_key);
    int delete_table(const char* from);
    int rename_table(const char* from, const char* to);
    int create(const char* name, TABLE* form,
               HA_CREATE_INFO* create_info);                      ///< required

    THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** to,
                               enum thr_lock_type lock_type);     ///< required
    const COND* cond_push(const COND* cond);
    uint8 table_cache_type()
    {
        return HA_CACHE_TBL_NOCACHE;
    }

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
class ha_calpont_group_by_handler: public group_by_handler
{
public:
    ha_calpont_group_by_handler(THD* thd_arg, Query* query);
    ~ha_calpont_group_by_handler();
    int init_scan();
    int next_row();
    int end_scan();

    List<Item>* select;
    TABLE_LIST* table_list;
    bool        distinct;
    Item*       where;
    ORDER*      group_by;
    ORDER*      order_by;
    Item*       having;
};
#endif //HA_CALPONT_H__

