/* Copyright (C) 2016-2023 MariaDB Corporation

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

#pragma once

#include "idb_mysql.h"

namespace ha_mcs_common
{

inline bool isMCSTable(TABLE* table_ptr)
{
#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)

  if (!(table_ptr->s && (*table_ptr->s->db_plugin)->name.str))
#else
  if (!(table_ptr->s && (table_ptr->s->db_plugin)->name.str))
#endif
    return true;

#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
  std::string engineName = (*table_ptr->s->db_plugin)->name.str;
#else
  std::string engineName = table_ptr->s->db_plugin->name.str;
#endif

  if (engineName == "Columnstore" || engineName == "Columnstore_cache")
    return true;
  return false;
}

inline bool isMultiUpdateStatement(const enum_sql_command& command)
{
  return command == SQLCOM_UPDATE_MULTI;
}

inline bool isUpdateStatement(const enum_sql_command& command)
{
  return command == SQLCOM_UPDATE || isMultiUpdateStatement(command);
}

inline bool isDeleteStatement(const enum_sql_command& command)
{
  return (command == SQLCOM_DELETE) || (command == SQLCOM_DELETE_MULTI);
}

inline bool isUpdateOrDeleteStatement(const enum_sql_command& command)
{
  return isUpdateStatement(command) || isDeleteStatement(command);
}

inline bool isDMLStatement(const enum_sql_command& command)
{
  return (command == SQLCOM_INSERT || command == SQLCOM_INSERT_SELECT || command == SQLCOM_TRUNCATE ||
          command == SQLCOM_LOAD || isUpdateOrDeleteStatement(command));
}

inline bool isForeignTableUpdate(THD* thd)
{
  LEX* lex = thd->lex;

  if (!isUpdateStatement(lex->sql_command))
    return false;

  Item_field* item;
  List_iterator_fast<Item> field_it(lex->first_select_lex()->item_list);

  while ((item = (Item_field*)field_it++))
  {
    if (item->field && item->field->table && !isMCSTable(item->field->table))
      return true;
  }

  return false;
}

// This function is different from isForeignTableUpdate()
// above as it only checks if any of the tables involved
// in the multi-table update statement is a foreign table,
// irrespective of whether the update is performed on the
// foreign table or not, as in isForeignTableUpdate().
inline bool isUpdateHasForeignTable(THD* thd)
{
  LEX* lex = thd->lex;

  if (!isUpdateStatement(lex->sql_command))
    return false;

  TABLE_LIST* table_ptr = lex->first_select_lex()->get_table_list();

  for (; table_ptr; table_ptr = table_ptr->next_local)
  {
    if (table_ptr->table && !isMCSTable(table_ptr->table))
      return true;
  }

  return false;
}

inline bool isMCSTableUpdate(THD* thd)
{
  LEX* lex = thd->lex;

  if (!isUpdateStatement(lex->sql_command))
    return false;

  Item_field* item;
  List_iterator_fast<Item> field_it(lex->first_select_lex()->item_list);

  while ((item = (Item_field*)field_it++))
  {
    if (item->field && item->field->table && isMCSTable(item->field->table))
      return true;
  }

  return false;
}

inline bool isMCSTableDelete(THD* thd)
{
  LEX* lex = thd->lex;

  if (!isDeleteStatement(lex->sql_command))
    return false;

  TABLE_LIST* table_ptr = lex->first_select_lex()->get_table_list();

  if (table_ptr && table_ptr->table && isMCSTable(table_ptr->table))
    return true;

  return false;
}

}  // namespace ha_mcs_common
