/* Copyright (C) 2021 MariaDB Corporation

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

#include "ha_tzinfo.h"
#include "tzfile.h"
#include <my_sys.h>

/* needed for sql_base.h which contains open_system_tables_for_read*/
#ifndef TRUE
#define TRUE (1)  /* Logical true */
#define FALSE (0) /* Logical false */
#endif
#include <sql_base.h>

// static HASH tz_names;
// static HASH offset_tzs;
static MEM_ROOT tz_storage;
static uint tz_leapcnt = 0;
static LS_INFO* tz_lsis = 0;
static bool time_zone_tables_exist = 1;

namespace cal_impl_if
{
/*
  Names of tables (with their lengths) that are needed
  for dynamical loading of time zone descriptions.
*/

static const LEX_CSTRING tz_tables_names[MY_TZ_TABLES_COUNT] = {
    {STRING_WITH_LEN("time_zone_name")},
    {STRING_WITH_LEN("time_zone")},
    {STRING_WITH_LEN("time_zone_transition_type")},
    {STRING_WITH_LEN("time_zone_transition")}};

static void tz_init_table_list(TABLE_LIST* tz_tabs)
{
  for (int i = 0; i < MY_TZ_TABLES_COUNT; i++)
  {
    tz_tabs[i].init_one_table(&MYSQL_SCHEMA_NAME, tz_tables_names + i, NULL, TL_READ);
    if (i != MY_TZ_TABLES_COUNT - 1)
      tz_tabs[i].next_global = tz_tabs[i].next_local = &tz_tabs[i + 1];
    if (i != 0)
      tz_tabs[i].prev_global = &tz_tabs[i - 1].next_global;
  }
}

static my_bool prepare_tz_info(TIME_ZONE_INFO* sp, MEM_ROOT* storage)
{
  my_time_t cur_t = MY_TIME_T_MIN;
  my_time_t cur_l, end_t;
  my_time_t end_l = 0;
  my_time_t cur_max_seen_l = MY_TIME_T_MIN;
  long cur_offset, cur_corr, cur_off_and_corr;
  uint next_trans_idx, next_leap_idx;
  uint i;
  /*
    Temporary arrays where we will store tables. Needed because
    we don't know table sizes ahead. (Well we can estimate their
    upper bound but this will take extra space.)
  */
  my_time_t revts[TZ_MAX_REV_RANGES];
  REVT_INFO revtis[TZ_MAX_REV_RANGES];

  /*
    Let us setup fallback time type which will be used if we have not any
    transitions or if we have moment of time before first transition.
    We will find first non-DST local time type and use it (or use first
    local time type if all of them are DST types).
  */
  for (i = 0; i < sp->typecnt && sp->ttis[i].tt_isdst; i++)
    /* no-op */;
  if (i == sp->typecnt)
    i = 0;
  sp->fallback_tti = &(sp->ttis[i]);

  /*
    Let us build shifted my_time_t -> my_time_t map.
  */
  sp->revcnt = 0;

  /* Let us find initial offset */
  if (sp->timecnt == 0 || cur_t < sp->ats[0])
  {
    /*
      If we have not any transitions or t is before first transition we are using
      already found fallback time type which index is already in i.
    */
    next_trans_idx = 0;
  }
  else
  {
    /* cur_t == sp->ats[0] so we found transition */
    i = sp->types[0];
    next_trans_idx = 1;
  }

  cur_offset = sp->ttis[i].tt_gmtoff;

  /* let us find leap correction... unprobable, but... */
  for (next_leap_idx = 0; next_leap_idx < sp->leapcnt && cur_t >= sp->lsis[next_leap_idx].ls_trans;
       ++next_leap_idx)
    continue;

  if (next_leap_idx > 0)
    cur_corr = sp->lsis[next_leap_idx - 1].ls_corr;
  else
    cur_corr = 0;

  /* Iterate trough t space */
  while (sp->revcnt < TZ_MAX_REV_RANGES - 1)
  {
    cur_off_and_corr = cur_offset - cur_corr;

    /*
      We assuming that cur_t could be only overflowed downwards,
      we also assume that end_t won't be overflowed in this case.
    */
    if (cur_off_and_corr < 0 && cur_t < MY_TIME_T_MIN - cur_off_and_corr)
      cur_t = MY_TIME_T_MIN - cur_off_and_corr;

    cur_l = cur_t + cur_off_and_corr;

    /*
      Let us choose end_t as point before next time type change or leap
      second correction.
    */
    end_t = MY_MIN((next_trans_idx < sp->timecnt) ? sp->ats[next_trans_idx] - 1 : MY_TIME_T_MAX,
                   (next_leap_idx < sp->leapcnt) ? sp->lsis[next_leap_idx].ls_trans - 1 : MY_TIME_T_MAX);
    /*
      again assuming that end_t can be overlowed only in positive side
      we also assume that end_t won't be overflowed in this case.
    */
    if (cur_off_and_corr > 0 && end_t > MY_TIME_T_MAX - cur_off_and_corr)
      end_t = MY_TIME_T_MAX - cur_off_and_corr;

    end_l = end_t + cur_off_and_corr;

    if (end_l > cur_max_seen_l)
    {
      /* We want special handling in the case of first range */
      if (cur_max_seen_l == MY_TIME_T_MIN)
      {
        revts[sp->revcnt] = cur_l;
        revtis[sp->revcnt].rt_offset = cur_off_and_corr;
        revtis[sp->revcnt].rt_type = 0;
        sp->revcnt++;
        cur_max_seen_l = end_l;
      }
      else
      {
        if (cur_l > cur_max_seen_l + 1)
        {
          /* We have a spring time-gap and we are not at the first range */
          revts[sp->revcnt] = cur_max_seen_l + 1;
          revtis[sp->revcnt].rt_offset = revtis[sp->revcnt - 1].rt_offset;
          revtis[sp->revcnt].rt_type = 1;
          sp->revcnt++;
          if (sp->revcnt == TZ_MAX_TIMES + TZ_MAX_LEAPS + 1)
            break; /* That was too much */
          cur_max_seen_l = cur_l - 1;
        }

        /* Assume here end_l > cur_max_seen_l (because end_l>=cur_l) */

        revts[sp->revcnt] = cur_max_seen_l + 1;
        revtis[sp->revcnt].rt_offset = cur_off_and_corr;
        revtis[sp->revcnt].rt_type = 0;
        sp->revcnt++;
        cur_max_seen_l = end_l;
      }
    }

    if (end_t == MY_TIME_T_MAX || ((cur_off_and_corr > 0) && (end_t >= MY_TIME_T_MAX - cur_off_and_corr)))
      /* end of t space */
      break;

    cur_t = end_t + 1;

    /*
      Let us find new offset and correction. Because of our choice of end_t
      cur_t can only be point where new time type starts or/and leap
      correction is performed.
    */
    if (sp->timecnt != 0 && cur_t >= sp->ats[0]) /* else reuse old offset */
      if (next_trans_idx < sp->timecnt && cur_t == sp->ats[next_trans_idx])
      {
        /* We are at offset point */
        cur_offset = sp->ttis[sp->types[next_trans_idx]].tt_gmtoff;
        ++next_trans_idx;
      }

    if (next_leap_idx < sp->leapcnt && cur_t == sp->lsis[next_leap_idx].ls_trans)
    {
      /* we are at leap point */
      cur_corr = sp->lsis[next_leap_idx].ls_corr;
      ++next_leap_idx;
    }
  }

  /* check if we have had enough space */
  if (sp->revcnt == TZ_MAX_REV_RANGES - 1)
    return 1;

  /* set maximum end_l as finisher */
  revts[sp->revcnt] = end_l;

  /* Allocate arrays of proper size in sp and copy result there */
  if (!(sp->revts = (my_time_t*)alloc_root(storage, sizeof(my_time_t) * (sp->revcnt + 1))) ||
      !(sp->revtis = (REVT_INFO*)alloc_root(storage, sizeof(REVT_INFO) * sp->revcnt)))
    return 1;

  memcpy(sp->revts, revts, sizeof(my_time_t) * (sp->revcnt + 1));
  memcpy(sp->revtis, revtis, sizeof(REVT_INFO) * sp->revcnt);

  return 0;
}

static TIME_ZONE_INFO* tz_load_from_open_tables(const String* tz_name, TABLE_LIST* tz_tables)
{
  TABLE* table = 0;
  TIME_ZONE_INFO* tz_info = NULL;
  // Tz_names_entry *tmp_tzname;
  TIME_ZONE_INFO* return_val = 0;
  int res;
  uint tzid, ttid;
  my_time_t ttime;
  char buff[MAX_FIELD_WIDTH];
  uchar keybuff[32];
  Field* field;
  String abbr(buff, sizeof(buff), &my_charset_latin1);
  char* alloc_buff = NULL;
  char* tz_name_buff = NULL;
  /*
    Temporary arrays that are used for loading of data for filling
    TIME_ZONE_INFO structure
  */
  my_time_t ats[TZ_MAX_TIMES];
  uchar types[TZ_MAX_TIMES];
  TRAN_TYPE_INFO ttis[TZ_MAX_TYPES];
#ifdef ABBR_ARE_USED
  char chars[MY_MAX(TZ_MAX_CHARS + 1, (2 * (MY_TZNAME_MAX + 1)))];
#endif
  /*
    Used as a temporary tz_info until we decide that we actually want to
    allocate and keep the tz info and tz name in tz_storage.
  */
  TIME_ZONE_INFO tmp_tz_info;
  memset(&tmp_tz_info, 0, sizeof(TIME_ZONE_INFO));

  DBUG_ENTER("tz_load_from_open_tables");

  /*
    Let us find out time zone id by its name (there is only one index
    and it is specifically for this purpose).
  */
  table = tz_tables->table;
  tz_tables = tz_tables->next_local;
  table->field[0]->store(tz_name->ptr(), tz_name->length(), &my_charset_latin1);
  if (table->file->ha_index_init(0, 1))
    goto end;

  if (table->file->ha_index_read_map(table->record[0], table->field[0]->ptr, HA_WHOLE_KEY, HA_READ_KEY_EXACT))
  {
#ifdef EXTRA_DEBUG
    /*
      Most probably user has mistyped time zone name, so no need to bark here
      unless we need it for debugging.
    */
    sql_print_error("Can't find description of time zone '%.*b'", tz_name->length(), tz_name->ptr());
#endif
    goto end;
  }

  tzid = (uint)table->field[1]->val_int();

  (void)table->file->ha_index_end();

  /*
    Now we need to lookup record in mysql.time_zone table in order to
    understand whenever this timezone uses leap seconds (again we are
    using the only index in this table).
  */
  table = tz_tables->table;
  tz_tables = tz_tables->next_local;
  field = table->field[0];
  field->store((longlong)tzid, true);
  DBUG_ASSERT(field->key_length() <= sizeof(keybuff));
  field->get_key_image(keybuff, MY_MIN(field->key_length(), sizeof(keybuff)), Field::itRAW);
  if (table->file->ha_index_init(0, 1))
    goto end;

  if (table->file->ha_index_read_map(table->record[0], keybuff, HA_WHOLE_KEY, HA_READ_KEY_EXACT))
  {
    sql_print_error("Can't find description of time zone '%u'", tzid);
    goto end;
  }

  /* If Uses_leap_seconds == 'Y' */
  if (table->field[1]->val_int() == 1)
  {
    tmp_tz_info.leapcnt = tz_leapcnt;
    tmp_tz_info.lsis = tz_lsis;
  }

  (void)table->file->ha_index_end();

  /*
    Now we will iterate through records for out time zone in
    mysql.time_zone_transition_type table. Because we want records
    only for our time zone guess what are we doing?
    Right - using special index.
  */
  table = tz_tables->table;
  tz_tables = tz_tables->next_local;
  field = table->field[0];
  field->store((longlong)tzid, true);
  DBUG_ASSERT(field->key_length() <= sizeof(keybuff));
  field->get_key_image(keybuff, MY_MIN(field->key_length(), sizeof(keybuff)), Field::itRAW);
  if (table->file->ha_index_init(0, 1))
    goto end;

  res = table->file->ha_index_read_map(table->record[0], keybuff, (key_part_map)1, HA_READ_KEY_EXACT);
  while (!res)
  {
    ttid = (uint)table->field[1]->val_int();

    if (ttid >= TZ_MAX_TYPES)
    {
      sql_print_error(
          "Error while loading time zone description from "
          "mysql.time_zone_transition_type table: too big "
          "transition type id");
      goto end;
    }

    ttis[ttid].tt_gmtoff = (long)table->field[2]->val_int();
    ttis[ttid].tt_isdst = (table->field[3]->val_int() > 0);

#ifdef ABBR_ARE_USED
    // FIXME should we do something with duplicates here ?
    table->field[4]->val_str(&abbr, &abbr);
    if (tmp_tz_info.charcnt + abbr.length() + 1 > sizeof(chars))
    {
      sql_print_error(
          "Error while loading time zone description from "
          "mysql.time_zone_transition_type table: not enough "
          "room for abbreviations");
      goto end;
    }
    ttis[ttid].tt_abbrind = tmp_tz_info.charcnt;
    memcpy(chars + tmp_tz_info.charcnt, abbr.ptr(), abbr.length());
    tmp_tz_info.charcnt += abbr.length();
    chars[tmp_tz_info.charcnt] = 0;
    tmp_tz_info.charcnt++;

    DBUG_PRINT("info",
               ("time_zone_transition_type table: tz_id=%u tt_id=%u tt_gmtoff=%ld "
                "abbr='%s' tt_isdst=%u",
                tzid, ttid, ttis[ttid].tt_gmtoff, chars + ttis[ttid].tt_abbrind, ttis[ttid].tt_isdst));
#else
    DBUG_PRINT("info", ("time_zone_transition_type table: tz_id=%u tt_id=%u tt_gmtoff=%ld "
                        "tt_isdst=%u",
                        tzid, ttid, ttis[ttid].tt_gmtoff, ttis[ttid].tt_isdst));
#endif

    /* ttid is increasing because we are reading using index */
    DBUG_ASSERT(ttid >= tmp_tz_info.typecnt);

    tmp_tz_info.typecnt = ttid + 1;

    res = table->file->ha_index_next_same(table->record[0], keybuff, 4);
  }

  if (res != HA_ERR_END_OF_FILE)
  {
    sql_print_error(
        "Error while loading time zone description from "
        "mysql.time_zone_transition_type table");
    goto end;
  }

  (void)table->file->ha_index_end();

  /*
    At last we are doing the same thing for records in
    mysql.time_zone_transition table. Here we additionally need records
    in ascending order by index scan also satisfies us.
  */
  table = tz_tables->table;
  table->field[0]->store((longlong)tzid, true);
  if (table->file->ha_index_init(0, 1))
    goto end;

  res = table->file->ha_index_read_map(table->record[0], keybuff, (key_part_map)1, HA_READ_KEY_EXACT);
  while (!res)
  {
    ttime = (my_time_t)table->field[1]->val_int();
    ttid = (uint)table->field[2]->val_int();

    if (tmp_tz_info.timecnt + 1 > TZ_MAX_TIMES)
    {
      sql_print_error(
          "Error while loading time zone description from "
          "mysql.time_zone_transition table: "
          "too much transitions");
      goto end;
    }
    if (ttid + 1 > tmp_tz_info.typecnt)
    {
      sql_print_error(
          "Error while loading time zone description from "
          "mysql.time_zone_transition table: "
          "bad transition type id");
      goto end;
    }

    ats[tmp_tz_info.timecnt] = ttime;
    types[tmp_tz_info.timecnt] = ttid;
    tmp_tz_info.timecnt++;

    DBUG_PRINT("info",
               ("time_zone_transition table: tz_id: %u  tt_time: %lu  tt_id: %u", tzid, (ulong)ttime, ttid));

    res = table->file->ha_index_next_same(table->record[0], keybuff, 4);
  }

  /*
    We have to allow HA_ERR_KEY_NOT_FOUND because some time zones
    for example UTC have no transitons.
  */
  if (res != HA_ERR_END_OF_FILE && res != HA_ERR_KEY_NOT_FOUND)
  {
    sql_print_error(
        "Error while loading time zone description from "
        "mysql.time_zone_transition table");
    goto end;
  }

  (void)table->file->ha_index_end();
  table = 0;

  /*
    Let us check how correct our time zone description is. We don't check for
    tz->timecnt < 1 since it is ok for GMT.
  */
  if (tmp_tz_info.typecnt < 1)
  {
    sql_print_error("loading time zone without transition types");
    goto end;
  }

  /* Allocate memory for the timezone info and timezone name in tz_storage. */
  if (!(alloc_buff = (char*)alloc_root(&tz_storage, sizeof(TIME_ZONE_INFO) + tz_name->length() + 1)))
  {
    sql_print_error("Out of memory while loading time zone description");
    return 0;
  }

  /* Move the temporary tz_info into the allocated area */
  tz_info = (TIME_ZONE_INFO*)alloc_buff;
  memcpy(tz_info, &tmp_tz_info, sizeof(TIME_ZONE_INFO));
  tz_name_buff = alloc_buff + sizeof(TIME_ZONE_INFO);
  /*
    By writing zero to the end we guarantee that we can call ptr()
    instead of c_ptr() for time zone name.
  */
  strmake(tz_name_buff, tz_name->ptr(), tz_name->length());

  /*
    Now we will allocate memory and init TIME_ZONE_INFO structure.
  */
  if (!(alloc_buff = (char*)alloc_root(&tz_storage, ALIGN_SIZE(sizeof(my_time_t) * tz_info->timecnt) +
                                                        ALIGN_SIZE(tz_info->timecnt) +
#ifdef ABBR_ARE_USED
                                                        ALIGN_SIZE(tz_info->charcnt) +
#endif
                                                        sizeof(TRAN_TYPE_INFO) * tz_info->typecnt)))
  {
    sql_print_error("Out of memory while loading time zone description");
    goto end;
  }

  tz_info->ats = (my_time_t*)alloc_buff;
  memcpy(tz_info->ats, ats, tz_info->timecnt * sizeof(my_time_t));
  alloc_buff += ALIGN_SIZE(sizeof(my_time_t) * tz_info->timecnt);
  tz_info->types = (uchar*)alloc_buff;
  memcpy(tz_info->types, types, tz_info->timecnt);
  alloc_buff += ALIGN_SIZE(tz_info->timecnt);
#ifdef ABBR_ARE_USED
  tz_info->chars = alloc_buff;
  memcpy(tz_info->chars, chars, tz_info->charcnt);
  alloc_buff += ALIGN_SIZE(tz_info->charcnt);
#endif
  tz_info->ttis = (TRAN_TYPE_INFO*)alloc_buff;
  memcpy(tz_info->ttis, ttis, tz_info->typecnt * sizeof(TRAN_TYPE_INFO));

  /* Build reversed map. */
  if (prepare_tz_info(tz_info, &tz_storage))
  {
    sql_print_error("Unable to build mktime map for time zone");
    goto end;
  }

  /*
    Loading of time zone succeeded
  */
  return_val = tz_info;

end:

  if (table && table->file->inited)
    (void)table->file->ha_index_end();

  DBUG_RETURN(return_val);
}

TIME_ZONE_INFO* my_tzinfo_find(THD* thd, const String* name)
{
  TIME_ZONE_INFO* result_tzinfo = 0;
  long offset;
  DBUG_ENTER("my_tz_find");
  DBUG_PRINT("enter", ("time zone name='%s'\n", name ? ((String*)name)->c_ptr_safe() : "NULL"));

  if (!name || name->is_empty())
    DBUG_RETURN(0);

  if (!timeZoneToOffset(name->ptr(), name->length(), &offset))
  {
    return NULL;
  }
  else
  {
    result_tzinfo = 0;
    my_tz_init(thd, NULL, 0);
    if (time_zone_tables_exist)
    {
      TABLE_LIST tz_tables[MY_TZ_TABLES_COUNT];

      /*
        Allocate start_new_trans with malloc as it's > 4000 bytes and this
        function can be called deep inside a stored procedure
      */
      start_new_trans* new_trans = new start_new_trans(thd);
      tz_init_table_list(tz_tables);
      init_mdl_requests(tz_tables);
      if (!open_system_tables_for_read(thd, tz_tables))
      {
        result_tzinfo = tz_load_from_open_tables(name, tz_tables);
        thd->commit_whole_transaction_and_close_tables();
      }
      new_trans->restore_old_transaction();
      delete new_trans;
    }
  }

  DBUG_RETURN(result_tzinfo);
}

}  // namespace cal_impl_if
