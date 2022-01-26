/* Copyright (C) 2019-20 MariaDB Corporation

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

#include "ha_mcs_opt_rewrites.h"

/*@brief  in_subselect_rewrite_walk - Rewrites Item_in_subselect*/
/************************************************************
 * DESCRIPTION:
 * It traverses filter predicates searching for
 * Item_in_subselect and rewrites it adding equi-join predicate
 * to finalise IN_2_EXISTS rewrite.
 * PARAMETERS:
 *    item_arg - Item to check.
 *    arg - bool to early return if predicate injection fails.
 * RETURN:
 ***********************************************************/
void in_subselect_rewrite_walk(const Item* item_arg, void* arg)
{
  bool* result = reinterpret_cast<bool*>(arg);
  if (*result)
    return;

  Item* item = const_cast<Item*>(item_arg);

  JOIN* join = nullptr;
  if (typeid(*item) == typeid(Item_in_subselect))
  {
    Item_in_subselect* sub = reinterpret_cast<Item_in_subselect*>(item);
    // MCS 1.4.3 doesn't support IN + subquery with UNION so
    // we safe to take this JOIN.
    join = sub->unit->first_select()->join;
    // Inject equi-JOIN predicates if needed.
    *result = sub->create_in_to_exists_cond(join);
    *result = (*result) ? *result : sub->inject_in_to_exists_cond(join);
    sub->unit->first_select()->prep_where = join->conds ? join->conds->copy_andor_structure(current_thd) : 0;
  }
  else if (typeid(*item) == typeid(Item_singlerow_subselect))
  {
    Item_singlerow_subselect* sub = reinterpret_cast<Item_singlerow_subselect*>(item);
    // MCS 1.4.3 doesn't support IN + subquery with UNION so
    // we safe to take this JOIN.
    join = sub->unit->first_select()->join;
  }
  else
  {
    // Exit for any but dedicated Items.
    return;
  }

  // Walk recursively to process nested IN ops.
  if (join->conds)
  {
    join->conds->traverse_cond(in_subselect_rewrite_walk, arg, Item::POSTFIX);
  }
}

// Sets SELECT_LEX::first_cond_optimization
void first_cond_optimization_flag_set(SELECT_LEX* select_lex)
{
  select_lex->first_cond_optimization = true;
}

// Unsets SELECT_LEX::first_cond_optimization
void first_cond_optimization_flag_unset(SELECT_LEX* select_lex)
{
  select_lex->first_cond_optimization = false;
}

/* @brief  first_cond_optimization_flag_toggle() - Sets/Unsets first_cond_optimization */
/************************************************************
 * DESCRIPTION:
 * This function traverses SELECT_LEX::table_list recursively
 * to set/unset SELECT_LEX::first_cond_optimization: a marker
 * to control optimizations executing PS. If set it allows to
 * apply optimizations. If unset, it disables optimizations.
 * PARAMETERS:
 *    select_lex - SELECT_LEX* that describes the query.
 *    func - Pointer to a function which either sets/unsets
 *           SELECT_LEX::first_cond_optimization
 ***********************************************************/
void first_cond_optimization_flag_toggle(SELECT_LEX* select_lex, void (*func)(SELECT_LEX*))
{
  for (TABLE_LIST* tl = select_lex->get_table_list(); tl; tl = tl->next_local)
  {
    if (tl->is_view_or_derived())
    {
      SELECT_LEX_UNIT* unit = tl->get_unit();

      if (unit)
      {
        for (SELECT_LEX* sl = unit->first_select(); sl; sl = sl->next_select())
        {
          first_cond_optimization_flag_toggle(sl, func);
        }
      }
    }
  }

  (*func)(select_lex);
}

/* @brief  in_subselect_rewrite - Rewrites Item_in_subselect */
/************************************************************
 * DESCRIPTION:
 * This function traverses TABLE_LISTs running in_subselect_rewrite_walk
 * PARAMETERS:
 *    select_lex - SELECT_LEX* that describes the query.
 * RETURN:
 *    bool to indicate predicate injection failures.
 ***********************************************************/
bool in_subselect_rewrite(SELECT_LEX* select_lex)
{
  bool result = false;
  TABLE_LIST* tbl;
  List_iterator_fast<TABLE_LIST> li(select_lex->leaf_tables);

  while (!result && (tbl = li++))
  {
    if (tbl->is_view_or_derived())
    {
      SELECT_LEX_UNIT* unit = tbl->get_unit();

      for (SELECT_LEX* sl = unit->first_select(); sl; sl = sl->next_select())
        result = in_subselect_rewrite(sl);
    }
  }

  if (select_lex->join && select_lex->join->conds)
  {
    select_lex->join->conds->traverse_cond(in_subselect_rewrite_walk, &result, Item::POSTFIX);
  }

  return result;
}
