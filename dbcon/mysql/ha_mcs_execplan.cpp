/* Copyright (C) 2014 InfiniDB, Inc.
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

#include <strings.h>

#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <time.h>
#include <cassert>
#include <vector>
#include <map>
#include <limits>

#include <string.h>

using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>

#include "errorids.h"
using namespace logging;

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include "idb_mysql.h"

#include "mcsv1_udaf.h"

#include "ha_mcs_impl_if.h"
#include "ha_mcs_sysvars.h"
#include "ha_subquery.h"
#include "ha_mcs_pushdown.h"
#include "ha_tzinfo.h"
using namespace cal_impl_if;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn_int.h"
#include "simplecolumn_uint.h"
#include "simplecolumn_decimal.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "selectfilter.h"
#include "existsfilter.h"
#include "groupconcatcolumn.h"
#include "outerjoinonfilter.h"
#include "intervalcolumn.h"
#include "udafcolumn.h"
using namespace execplan;

#include "funcexp.h"
#include "functor.h"
using namespace funcexp;

#include "vlarray.h"

const uint64_t AGG_BIT = 0x01;
const uint64_t SUB_BIT = 0x02;
const uint64_t AF_BIT = 0x04;
const uint64_t CORRELATED = 0x08;

// In certain cases, gp_walk is called recursively. When done so,
// we need to bookmark the rcWorkStack for those cases where a constant
// expression such as 1=1 is used in an if statement or function call.
// This is a seriously bad kludge for MariaDB bug 750.
//
// BM => BookMark
// HWM => HighWaterMark
class RecursionCounter
{
 private:
  RecursionCounter()
  {
  }

 public:
  RecursionCounter(gp_walk_info* gwip) : fgwip(gwip)
  {
    ++fgwip->recursionLevel;

    if (fgwip->recursionLevel > fgwip->recursionHWM)
    {
      fgwip->rcBookMarkStack.push(fgwip->rcWorkStack.size());
      fgwip->recursionHWM = fgwip->recursionLevel;
    }
  }
  ~RecursionCounter()
  {
    --fgwip->recursionLevel;

    if (fgwip->recursionLevel < fgwip->recursionHWM - 1)
    {
      fgwip->rcBookMarkStack.pop();
      --fgwip->recursionHWM;
    }
  }

  gp_walk_info* fgwip;
};

#include "ha_view.h"

namespace cal_impl_if
{
// This is taken from Item_cond::fix_fields in sql/item_cmpfunc.cc.
void calculateNotNullTables(const std::vector<COND*>& condList, table_map& not_null_tables)
{
  for (Item* item : condList)
  {
    if (item->can_eval_in_optimize() && !item->with_sp_var() && !cond_has_datetime_is_null(item))
    {
      if (item->eval_const_cond())
      {
      }
      else
      {
        not_null_tables = (table_map)0;
      }
    }
    else
    {
      not_null_tables |= item->not_null_tables();
    }
  }
}

// Recursively iterate through the join_list and store all non-null
// TABLE_LIST::on_expr items to a hash map keyed by the TABLE_LIST ptr.
// This is then used by convertOuterJoinToInnerJoin().
void buildTableOnExprList(List<TABLE_LIST>* join_list, TableOnExprList& tableOnExprList)
{
  TABLE_LIST* table;
  NESTED_JOIN* nested_join;
  List_iterator<TABLE_LIST> li(*join_list);

  while ((table = li++))
  {
    if ((nested_join = table->nested_join))
    {
      buildTableOnExprList(&nested_join->join_list, tableOnExprList);
    }

    if (table->on_expr)
      tableOnExprList[table].push_back(table->on_expr);
  }
}

// This is a trimmed down version of simplify_joins() in sql/sql_select.cc.
// Refer to that function for more details. But we have customized the
// original implementation in simplify_joins() to avoid any changes to
// the SELECT_LEX::JOIN::conds. Specifically, in some cases, simplify_joins()
// would create new Item_cond_and objects. We want to avoid such changes to
// the SELECT_LEX in a scenario where the select_handler execution has failed
// and we want to fallback to the server execution (MCOL-4525). Here, we mimick
// the creation of Item_cond_and using tableOnExprList and condList.
void convertOuterJoinToInnerJoin(List<TABLE_LIST>* join_list, TableOnExprList& tableOnExprList,
                                 std::vector<COND*>& condList, TableOuterJoinMap& tableOuterJoinMap)
{
  TABLE_LIST* table;
  NESTED_JOIN* nested_join;
  List_iterator<TABLE_LIST> li(*join_list);

  while ((table = li++))
  {
    table_map used_tables;
    table_map not_null_tables = (table_map)0;

    if ((nested_join = table->nested_join))
    {
      auto iter = tableOnExprList.find(table);

      if ((iter != tableOnExprList.end()) && !iter->second.empty())
      {
        convertOuterJoinToInnerJoin(&nested_join->join_list, tableOnExprList, tableOnExprList[table],
                                    tableOuterJoinMap);
      }

      nested_join->used_tables = (table_map)0;
      nested_join->not_null_tables = (table_map)0;

      convertOuterJoinToInnerJoin(&nested_join->join_list, tableOnExprList, condList, tableOuterJoinMap);

      used_tables = nested_join->used_tables;
      not_null_tables = nested_join->not_null_tables;
    }
    else
    {
      used_tables = table->get_map();

      if (!condList.empty())
      {
        if (condList.size() == 1)
        {
          not_null_tables = condList[0]->not_null_tables();
        }
        else
        {
          calculateNotNullTables(condList, not_null_tables);
        }
      }
    }

    if (table->embedding)
    {
      table->embedding->nested_join->used_tables |= used_tables;
      table->embedding->nested_join->not_null_tables |= not_null_tables;
    }

    if (!(table->outer_join & (JOIN_TYPE_LEFT | JOIN_TYPE_RIGHT)) || (used_tables & not_null_tables))
    {
      if (table->outer_join)
      {
        tableOuterJoinMap[table] = table->outer_join;
      }

      table->outer_join = 0;

      auto iter = tableOnExprList.find(table);

      if (iter != tableOnExprList.end() && !iter->second.empty())
      {
        // The original implementation in simplify_joins() creates
        // an Item_cond_and object here. Instead of doing so, we
        // append the table->on_expr to the condList and hence avoid
        // making any permanent changes to SELECT_LEX::JOIN::conds.
        condList.insert(condList.end(), tableOnExprList[table].begin(), tableOnExprList[table].end());
        iter->second.clear();
      }
    }
  }
}

CalpontSystemCatalog::TableAliasName makeTableAliasName(TABLE_LIST* table)
{
  return make_aliasview(
      (table->db.length ? table->db.str : ""), (table->table_name.length ? table->table_name.str : ""),
      (table->alias.length ? table->alias.str : ""), getViewName(table), true, lower_case_table_names);
}

//@bug5228. need to escape backtick `
string escapeBackTick(const char* str)
{
  if (!str)
    return "";

  string ret;

  for (uint32_t i = 0; str[i] != 0; i++)
  {
    if (str[i] == '`')
      ret.append("``");
    else
      ret.append(1, str[i]);
  }

  return ret;
}

void clearStacks(gp_walk_info& gwi)
{
  while (!gwi.rcWorkStack.empty())
    gwi.rcWorkStack.pop();

  while (!gwi.ptWorkStack.empty())
    gwi.ptWorkStack.pop();
}

bool nonConstFunc(Item_func* ifp)
{
  if (strcasecmp(ifp->func_name(), "rand") == 0 || strcasecmp(ifp->func_name(), "sysdate") == 0 ||
      strcasecmp(ifp->func_name(), "idblocalpm") == 0)
    return true;

  for (uint32_t i = 0; i < ifp->argument_count(); i++)
  {
    if (ifp->arguments()[i]->type() == Item::FUNC_ITEM && nonConstFunc(((Item_func*)ifp->arguments()[i])))
      return true;
  }

  return false;
}

/*@brief getColNameFromItem - builds a name from an Item    */
/***********************************************************
 * DESCRIPTION:
 * This f() looks for a first proper Item_ident and populate
 * ostream with schema, table and column names.
 * Used to build db.table.field tuple for debugging output
 * in getSelectPlan(). TBD getGroupPlan must use this also.
 * PARAMETERS:
 *   item               source Item*
 *   ostream            output stream
 * RETURNS
 *  void
 ***********************************************************/
void getColNameFromItem(std::ostringstream& ostream, Item* item)
{
  // Item_func doesn't have proper db.table.field values
  // inherited from Item_ident. TBD what is the valid output.
  // !!!dynamic_cast fails compilation
  ostream << "'";

  if (item->type() != Item::FIELD_ITEM)
  {
    ostream << "unknown db" << '.';
    ostream << "unknown table" << '.';
    ostream << "unknown field";
  }
  else
  {
    Item_ident* iip = reinterpret_cast<Item_ident*>(item);

    if (iip->db_name.str)
      ostream << iip->db_name.str << '.';
    else
      ostream << "unknown db" << '.';

    if (iip->table_name.str)
      ostream << iip->table_name.str << '.';
    else
      ostream << "unknown table" << '.';

    if (iip->field_name.length)
      ostream << iip->field_name.str;
    else
      ostream << "unknown field";
  }

  ostream << "'";
  return;
}

/*@brf sortItemIsInGroupRec - seeks for an item in grouping*/
/***********************************************************
 * DESCRIPTION:
 * This f() recursively traverses grouping items and looks
 * for an FUNC_ITEM, REF_ITEM or FIELD_ITEM.
 * f() is used by sortItemIsInGrouping().
 * PARAMETERS:
 *   sort_item          Item* used to build aggregation.
 *   group_item         GROUP BY item.
 * RETURNS
 *  bool
 ***********************************************************/
bool sortItemIsInGroupRec(Item* sort_item, Item* group_item)
{
  bool found = false;
  // If ITEM_REF::ref is NULL
  if (sort_item == NULL)
  {
    return found;
  }

  Item_func* ifp_sort = reinterpret_cast<Item_func*>(sort_item);

  // base cases for Item_field and Item_ref. The second arg is binary cmp switch
  found = group_item->eq(sort_item, false);
  if (!found && sort_item->type() == Item::REF_ITEM)
  {
    Item_ref* ifp_sort_ref = reinterpret_cast<Item_ref*>(sort_item);
    found = sortItemIsInGroupRec(*ifp_sort_ref->ref, group_item);
  }
  else if (!found && sort_item->type() == Item::FIELD_ITEM)
  {
    return found;
  }

  // seeking for a group_item match
  for (uint32_t i = 0; !found && i < ifp_sort->argument_count(); i++)
  {
    Item* ifp_sort_arg = ifp_sort->arguments()[i];
    if (ifp_sort_arg->type() == Item::FUNC_ITEM || ifp_sort_arg->type() == Item::FIELD_ITEM)
    {
      Item* ifp_sort_arg = ifp_sort->arguments()[i];
      found = sortItemIsInGroupRec(ifp_sort_arg, group_item);
    }
    else if (ifp_sort_arg->type() == Item::REF_ITEM)
    {
      // dereference the Item
      Item_ref* ifp_sort_ref = reinterpret_cast<Item_ref*>(ifp_sort_arg);
      found = sortItemIsInGroupRec(*ifp_sort_ref->ref, group_item);
    }
  }

  return found;
}

/*@brief check_sum_func_item - This traverses Item       */
/**********************************************************
 * DESCRIPTION:
 * This f() walks Item looking for the existence of
 * a Item::REF_ITEM, which references an item of
 * type Item::SUM_FUNC_ITEM
 * PARAMETERS:
 *    Item * Item to traverse
 * RETURN:
 *********************************************************/
void check_sum_func_item(const Item* item, void* arg)
{
  bool* found = reinterpret_cast<bool*>(arg);

  if (*found)
    return;

  if (item->type() == Item::REF_ITEM)
  {
    const Item_ref* ref_item = reinterpret_cast<const Item_ref*>(item);
    Item* ref_item_item = (Item*)*ref_item->ref;
    if (ref_item_item->type() == Item::SUM_FUNC_ITEM)
    {
      *found = true;
    }
  }
  else if (item->type() == Item::CONST_ITEM)
  {
    *found = true;
  }
}

/*@brief sortItemIsInGrouping- seeks for an item in grouping*/
/***********************************************************
 * DESCRIPTION:
 * This f() traverses grouping items and looks for an item.
 * only Item_fields, Item_func are considered. However there
 * could be Item_ref down the tree.
 * f() is used in sorting parsing by getSelectPlan().
 * PARAMETERS:
 *   sort_item          Item* used to build aggregation.
 *   groupcol           GROUP BY items from this unit.
 * RETURNS
 *  bool
 ***********************************************************/
bool sortItemIsInGrouping(Item* sort_item, ORDER* groupcol)
{
  bool found = false;

  if (sort_item->type() == Item::SUM_FUNC_ITEM)
  {
    found = true;
  }

  // A function that contains an aggregate function
  // can be included in the ORDER BY clause
  // e.g. select a, if (sum(b) > 1, 2, 1) from t1 group by 1 order by 2;
  if (sort_item->type() == Item::FUNC_ITEM)
  {
    Item_func* ifp = reinterpret_cast<Item_func*>(sort_item);
    ifp->traverse_cond(check_sum_func_item, &found, Item::POSTFIX);
  }
  else if (sort_item->type() == Item::CONST_ITEM || sort_item->type() == Item::WINDOW_FUNC_ITEM)
  {
    found = true;
  }

  for (; !found && groupcol; groupcol = groupcol->next)
  {
    Item* group_item = *(groupcol->item);
    found = (group_item->eq(sort_item, false)) ? true : false;
    // Detect aggregation functions first then traverse
    // if sort field is a Func and group field
    // is either Field or Func
    // Consider nonConstFunc() check here
    if (!found && sort_item->type() == Item::FUNC_ITEM &&
        (group_item->type() == Item::FUNC_ITEM || group_item->type() == Item::FIELD_ITEM))
    {
      found = sortItemIsInGroupRec(sort_item, group_item);
    }
  }

  return found;
}

/*@brief  buildAggFrmTempField- build aggr func from extSELECT list item*/
/***********************************************************
 * DESCRIPTION:
 * Server adds additional aggregation items to extended SELECT list and
 * references them in projection and HAVING. This f() finds
 * corresponding item in extSelAggColsItems and builds
 * ReturnedColumn using the item.
 * PARAMETERS:
 *    item          Item* used to build aggregation
 *    gwi           main structure
 * RETURNS
 *  ReturnedColumn* if corresponding Item has been found
 *  NULL otherwise
 ***********************************************************/
ReturnedColumn* buildAggFrmTempField(Item* item, gp_walk_info& gwi)
{
  ReturnedColumn* result = NULL;
  Item_field* ifip = NULL;
  Item_ref* irip;
  Item_func_or_sum* isfp;

  switch (item->type())
  {
    case Item::FIELD_ITEM: ifip = reinterpret_cast<Item_field*>(item); break;
    default:
      irip = reinterpret_cast<Item_ref*>(item);
      if (irip)
        ifip = reinterpret_cast<Item_field*>(irip->ref[0]);
      break;
  }

  if (ifip && ifip->field)
  {
    std::vector<Item*>::iterator iter = gwi.extSelAggColsItems.begin();
    for (; iter != gwi.extSelAggColsItems.end(); iter++)
    {
      isfp = reinterpret_cast<Item_func_or_sum*>(*iter);

      if (isfp->type() == Item::SUM_FUNC_ITEM && isfp->result_field == ifip->field)
      {
        ReturnedColumn* rc = buildAggregateColumn(isfp, gwi);

        if (rc)
          result = rc;

        break;
      }
    }
  }

  return result;
}

/*@brief  isDuplicateSF - search for a duplicate SimpleFilter*/
/***********************************************************
 * DESCRIPTION:
 * As of 1.4 CS potentially has duplicate filter predicates
 * in both join->conds and join->cond_equal. This utility f()
 * searches for semantically equal SF in the list of already
 * applied equi predicates.
 * TODO
 *  We must move find_select_handler to either future or
 *  later execution phase.
 * PARAMETERS:
 *  gwi           main structure
 *  sf            SimpleFilter * to find
 * RETURNS
 *  true          if sf has been found in the applied SF list
 *  false         otherwise
 * USED
 *  buildPredicateItem()
 ***********************************************************/
bool isDuplicateSF(gp_walk_info* gwip, execplan::SimpleFilter* sfp)
{
  List_iterator<execplan::SimpleFilter> it(gwip->equiCondSFList);
  execplan::SimpleFilter* isfp;
  while ((isfp = it++))
  {
    if (sfp->semanticEq(*isfp))
    {
      return true;
    }
  }

  return false;
}

// DESCRIPTION:
// This f() checks if the arguments is a UDF Item
// PARAMETERS:
//    Item * Item to traverse
// RETURN:
//    bool
inline bool isUDFSumItem(const Item_sum* isp)
{
  return typeid(*isp) == typeid(Item_sum_udf_int) || typeid(*isp) == typeid(Item_sum_udf_float) ||
         typeid(*isp) == typeid(Item_sum_udf_decimal) || typeid(*isp) == typeid(Item_sum_udf_str);
}

string getViewName(TABLE_LIST* table_ptr)
{
  string viewName = "";

  if (!table_ptr)
    return viewName;

  TABLE_LIST* view = table_ptr->referencing_view;

  if (view)
  {
    if (!view->derived)
      viewName = view->alias.str;

    while ((view = view->referencing_view))
    {
      if (view->derived)
        continue;

      viewName = view->alias.str + string(".") + viewName;
    }
  }

  return viewName;
}

#ifdef DEBUG_WALK_COND
void debug_walk(const Item* item, void* arg)
{
  switch (item->type())
  {
    case Item::FIELD_ITEM:
    {
      Item_field* ifp = (Item_field*)item;
      cerr << "FIELD_ITEM: " << (ifp->db_name.str ? ifp->db_name.str : "") << '.' << bestTableName(ifp) << '.'
           << ifp->field_name.str << endl;
      break;
    }
    case Item::CONST_ITEM:
    {
      switch (item->cmp_type())
      {
        case INT_RESULT:
        {
          Item_int* iip = (Item_int*)item;
          cerr << "INT_ITEM: ";

          if (iip->name.length)
            cerr << iip->name.str << " (from name string)" << endl;
          else
            cerr << iip->val_int() << endl;

          break;
        }
        case STRING_RESULT:
        {
          Item_string* isp = (Item_string*)item;
          String val, *str = isp->val_str(&val);
          string valStr;
          valStr.assign(str->ptr(), str->length());
          cerr << "STRING_ITEM: >" << valStr << '<' << endl;
          break;
        }
        case REAL_RESULT:
        {
          cerr << "REAL_ITEM" << endl;
          break;
        }
        case DECIMAL_RESULT:
        {
          cerr << "DECIMAL_ITEM" << endl;
          break;
        }
        case TIME_RESULT:
        {
          String val, *str = NULL;
          Item_temporal_literal* itp = (Item_temporal_literal*)item;
          str = itp->val_str(&val);
          cerr << "DATE ITEM: ";

          if (str)
            cerr << ": (" << str->ptr() << ')' << endl;
          else
            cerr << ": <NULL>" << endl;

          break;
        }
        default:
        {
          cerr << ": Unknown cmp_type" << endl;
          break;
        }
      }
      break;
    }
    case Item::FUNC_ITEM:
    {
      Item_func* ifp = (Item_func*)item;
      Item_func_opt_neg* inp;
      cerr << "FUNC_ITEM: ";

      switch (ifp->functype())
      {
        case Item_func::UNKNOWN_FUNC:  // 0
          cerr << ifp->func_name() << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::GT_FUNC:  // 7
          cerr << '>' << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::EQ_FUNC:  // 1
          cerr << '=' << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::GE_FUNC:
          cerr << ">="
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::LE_FUNC:
          cerr << "<="
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::LT_FUNC: cerr << '<' << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::NE_FUNC:
          cerr << "<>"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NEG_FUNC:  // 45
          cerr << "unary minus"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::IN_FUNC:  // 16
          inp = (Item_func_opt_neg*)ifp;

          if (inp->negated)
            cerr << "not ";

          cerr << "in"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::BETWEEN:
          inp = (Item_func_opt_neg*)ifp;

          if (inp->negated)
            cerr << "not ";

          cerr << "between"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNULL_FUNC:  // 10
          cerr << "is null"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNOTNULL_FUNC:  // 11
          cerr << "is not null"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NOT_ALL_FUNC:
          cerr << "not_all"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NOT_FUNC:
          cerr << "not_func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::TRIG_COND_FUNC:
          cerr << "trig_cond_func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNOTNULLTEST_FUNC:
          cerr << "isnotnulltest_func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::MULT_EQUAL_FUNC:
        {
          cerr << "mult_equal_func:"
               << " (" << ifp->functype() << ")" << endl;
          Item_equal* item_eq = (Item_equal*)ifp;
          Item_equal_fields_iterator it(*item_eq);
          Item* item;

          while ((item = it++))
          {
            Field* equal_field = it.get_curr_field();
            cerr << equal_field->field_name.str << endl;
          }

          break;
        }

        case Item_func::EQUAL_FUNC:
          cerr << "equal func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::FT_FUNC:
          cerr << "ft func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::LIKE_FUNC:
          cerr << "like func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::COND_AND_FUNC:
          cerr << "cond and func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::COND_OR_FUNC:
          cerr << "cond or func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::XOR_FUNC:
          cerr << "xor func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::INTERVAL_FUNC:
          cerr << "interval func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_EQUALS_FUNC:
          cerr << "sp equals func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_DISJOINT_FUNC:
          cerr << "sp disjoint func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_INTERSECTS_FUNC:
          cerr << "sp intersects func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_TOUCHES_FUNC:
          cerr << "sp touches func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_CROSSES_FUNC:
          cerr << "sp crosses func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_WITHIN_FUNC:
          cerr << "sp within func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_CONTAINS_FUNC:
          cerr << "sp contains func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_OVERLAPS_FUNC:
          cerr << "sp overlaps func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_STARTPOINT:
          cerr << "sp startpoint func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_ENDPOINT:
          cerr << "sp endpoint func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_EXTERIORRING:
          cerr << "sp exteriorring func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_POINTN:
          cerr << "sp pointn func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_GEOMETRYN:
          cerr << "sp geometryn func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_INTERIORRINGN:
          cerr << "sp exteriorringn func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_RELATE_FUNC:
          cerr << "sp relate func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NOW_FUNC:
          cerr << "now func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SUSERVAR_FUNC:
          cerr << "suservar func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::GUSERVAR_FUNC:
          cerr << "guservar func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::COLLATE_FUNC:
          cerr << "collate func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::EXTRACT_FUNC:
          cerr << "extract func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::CHAR_TYPECAST_FUNC:
          cerr << "char typecast func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::FUNC_SP:
          cerr << "func sp func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::UDF_FUNC:
          cerr << "udf func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::GSYSVAR_FUNC:
          cerr << "gsysvar func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::DYNCOL_FUNC:
          cerr << "dyncol func"
               << " (" << ifp->functype() << ")" << endl;
          break;

        default: cerr << "type=" << ifp->functype() << endl; break;
      }

      break;
    }

    case Item::COND_ITEM:
    {
      Item_cond* icp = (Item_cond*)item;
      cerr << "COND_ITEM: " << icp->func_name() << endl;
      break;
    }

    case Item::SUM_FUNC_ITEM:
    {
      Item_sum* isp = (Item_sum*)item;
      char* item_name = const_cast<char*>(item->name.str);

      // MCOL-1052 This is an extended SELECT list item
      if (!item_name && isp->get_arg_count() && isp->get_arg(0)->name.length)
      {
        item_name = const_cast<char*>(isp->get_arg(0)->name.str);
      }
      else if (!item_name && isp->get_arg_count() && isp->get_arg(0)->type() == Item::CONST_ITEM &&
               isp->get_arg(0)->cmp_type() == INT_RESULT)
      {
        item_name = (char*)"INT||*";
      }
      else if (!item_name)
      {
        item_name = (char*)"<NULL>";
      }

      switch (isp->sum_func())
      {
        case Item_sum::SUM_FUNC: cerr << "SUM_FUNC: " << item_name << endl; break;

        case Item_sum::SUM_DISTINCT_FUNC: cerr << "SUM_DISTINCT_FUNC: " << item_name << endl; break;

        case Item_sum::AVG_FUNC: cerr << "AVG_FUNC: " << item_name << endl; break;

        case Item_sum::COUNT_FUNC: cerr << "COUNT_FUNC: " << item_name << endl; break;

        case Item_sum::COUNT_DISTINCT_FUNC: cerr << "COUNT_DISTINCT_FUNC: " << item_name << endl; break;

        case Item_sum::MIN_FUNC: cerr << "MIN_FUNC: " << item_name << endl; break;

        case Item_sum::MAX_FUNC: cerr << "MAX_FUNC: " << item_name << endl; break;

        case Item_sum::UDF_SUM_FUNC: cerr << "UDAF_FUNC: " << item_name << endl; break;

        default: cerr << "SUM_FUNC_ITEM type=" << isp->sum_func() << endl; break;
      }

      break;
    }

    case Item::SUBSELECT_ITEM:
    {
      Item_subselect* sub = (Item_subselect*)item;
      cerr << "SUBSELECT Item: ";

      switch (sub->substype())
      {
        case Item_subselect::EXISTS_SUBS: cerr << "EXISTS"; break;

        case Item_subselect::IN_SUBS: cerr << "IN"; break;

        default: cerr << sub->substype(); break;
      }

      cerr << endl;
      JOIN* join = sub->get_select_lex()->join;

      if (join)
      {
        Item_cond* cond = reinterpret_cast<Item_cond*>(join->conds);

        if (cond)
          cond->traverse_cond(debug_walk, arg, Item::POSTFIX);
      }

      cerr << "Finish subselect item traversing" << endl;
      break;
    }

    case Item::REF_ITEM:
    {
      Item_ref* ref = (Item_ref*)item;

      if (ref->real_item()->type() == Item::CACHE_ITEM)
      {
        Item* field = ((Item_cache*)ref->real_item())->get_example();

        if (field->type() == Item::FIELD_ITEM)
        {
          Item_field* ifp = (Item_field*)field;
          // ifp->cached_table->select_lex->select_number gives the select level.
          // could be used on alias.
          // could also be used to tell correlated join (equal level).
          cerr << "CACHED REF FIELD_ITEM: " << ifp->db_name.str << '.' << bestTableName(ifp) << '.'
               << ifp->field_name.str << endl;
          break;
        }
        else if (field->type() == Item::FUNC_ITEM)
        {
          Item_func* ifp = (Item_func*)field;
          cerr << "CACHED REF FUNC_ITEM " << ifp->func_name() << endl;
        }
        else if (field->type() == Item::REF_ITEM)
        {
          Item_ref* ifr = (Item_ref*)field;
          string refType;
          string realType;

          switch (ifr->ref_type())
          {
            case Item_ref::REF: refType = "REF"; break;

            case Item_ref::DIRECT_REF: refType = "DIRECT_REF"; break;

            case Item_ref::VIEW_REF: refType = "VIEW_REF"; break;

            case Item_ref::OUTER_REF: refType = "OUTER_REF"; break;

            case Item_ref::AGGREGATE_REF: refType = "AGGREGATE_REF"; break;

            default: refType = "UNKNOWN"; break;
          }

          switch (ifr->real_type())
          {
            case Item::FIELD_ITEM:
            {
              Item_field* ifp = (Item_field*)(*(ifr->ref));
              realType = "FIELD_ITEM ";
              realType += ifp->db_name.str;
              realType += '.';
              realType += bestTableName(ifp);
              realType += '.';
              realType += ifp->field_name.str;
              break;
            }

            case Item::SUM_FUNC_ITEM:
            {
              Item_sum* isp = (Item_sum*)(*(ifr->ref));

              if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
                realType = "GROUP_CONCAT_FUNC";
              else
                realType = "SUM_FUNC_ITEM";

              break;
            }

            case Item::REF_ITEM:
              // Need recursion here
              realType = "REF_ITEM";
              break;

            case Item::FUNC_ITEM:
            {
              Item_func* ifp = (Item_func*)(*(ifr->ref));
              realType = "FUNC_ITEM ";
              realType += ifp->func_name();
              break;
            }

            default:
            {
              realType = "UNKNOWN";
            }
          }

          cerr << "CACHED REF_ITEM: ref type " << refType.c_str() << " real type " << realType.c_str()
               << endl;
          break;
        }
        else
        {
          cerr << "REF_ITEM with CACHE_ITEM type unknown " << field->type() << endl;
        }
      }
      else if (ref->real_item()->type() == Item::FIELD_ITEM)
      {
        Item_field* ifp = (Item_field*)ref->real_item();

        // MCOL-1052 The field referenced presumable came from
        // extended SELECT list.
        if (!ifp->field_name.str)
        {
          cerr << "REF extra FIELD_ITEM: " << ifp->name.str << endl;
        }
        else
        {
          cerr << "REF FIELD_ITEM: " << ifp->db_name.str << '.' << bestTableName(ifp) << '.'
               << ifp->field_name.str << endl;
        }

        break;
      }
      else if (ref->real_item()->type() == Item::FUNC_ITEM)
      {
        Item_func* ifp = (Item_func*)ref->real_item();
        cerr << "REF FUNC_ITEM " << ifp->func_name() << endl;
      }
      else if (ref->real_item()->type() == Item::WINDOW_FUNC_ITEM)
      {
        Item_window_func* ifp = (Item_window_func*)ref->real_item();
        cerr << "REF WINDOW_FUNC_ITEM " << ifp->window_func()->func_name() << endl;
      }
      else
      {
        cerr << "UNKNOWN REF ITEM type " << ref->real_item()->type() << endl;
      }

      break;
    }

    case Item::ROW_ITEM:
    {
      Item_row* row = (Item_row*)item;
      cerr << "ROW_ITEM: " << endl;

      for (uint32_t i = 0; i < row->cols(); i++)
        debug_walk(row->element_index(i), 0);

      break;
    }

    case Item::EXPR_CACHE_ITEM:
    {
      cerr << "Expr Cache Item" << endl;
      ((Item_cache_wrapper*)item)->get_orig_item()->traverse_cond(debug_walk, arg, Item::POSTFIX);
      break;
    }

    case Item::CACHE_ITEM:
    {
      Item_cache* isp = (Item_cache*)item;
      // MCOL-46 isp->val_str() can cause a call to execute a subquery. We're not set up
      // to execute yet.
#if 0

            switch (item->result_type())
            {
                case STRING_RESULT:
                    cerr << "CACHE_STRING_ITEM" << endl;
                    break;

                case REAL_RESULT:
                    cerr << "CACHE_REAL_ITEM " << isp->val_real() << endl;
                    break;

                case INT_RESULT:
                    cerr << "CACHE_INT_ITEM " << isp->val_int() << endl;
                    break;

                case ROW_RESULT:
                    cerr << "CACHE_ROW_ITEM" << endl;
                    break;

                case DECIMAL_RESULT:
                    cerr << "CACHE_DECIMAL_ITEM " << isp->val_decimal() << endl;
                    break;

                default:
                    cerr << "CACHE_UNKNOWN_ITEM" << endl;
                    break;
            }

#endif
      Item* field = isp->get_example();

      if (field->type() == Item::FIELD_ITEM)
      {
        Item_field* ifp = (Item_field*)field;
        // ifp->cached_table->select_lex->select_number gives the select level.
        // could be used on alias.
        // could also be used to tell correlated join (equal level).
        cerr << "CACHED FIELD_ITEM: " << ifp->db_name.str << '.' << bestTableName(ifp) << '.'
             << ifp->field_name.str << endl;
        break;
      }
      else if (field->type() == Item::REF_ITEM)
      {
        Item_ref* ifr = (Item_ref*)field;
        string refType;
        string realType;

        switch (ifr->ref_type())
        {
          case Item_ref::REF: refType = "REF"; break;

          case Item_ref::DIRECT_REF: refType = "DIRECT_REF"; break;

          case Item_ref::VIEW_REF: refType = "VIEW_REF"; break;

          case Item_ref::OUTER_REF: refType = "OUTER_REF"; break;

          case Item_ref::AGGREGATE_REF: refType = "AGGREGATE_REF"; break;

          default: refType = "UNKNOWN"; break;
        }

        switch (ifr->real_type())
        {
          case Item::FIELD_ITEM:
          {
            Item_field* ifp = (Item_field*)(*(ifr->ref));
            realType = "FIELD_ITEM ";
            realType += ifp->db_name.str;
            realType += '.';
            realType += bestTableName(ifp);
            realType += '.';
            realType += ifp->field_name.str;
            break;
          }

          case Item::SUM_FUNC_ITEM:
          {
            Item_sum* isp = (Item_sum*)(*(ifr->ref));

            if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
              realType = "GROUP_CONCAT_FUNC";
            else
              realType = "SUM_FUNC_ITEM";

            break;
          }

          case Item::REF_ITEM:
            // Need recursion here
            realType = "REF_ITEM";
            break;

          case Item::FUNC_ITEM:
          {
            Item_func* ifp = (Item_func*)(*(ifr->ref));
            realType = "FUNC_ITEM ";
            realType += ifp->func_name();
            break;
          }

          default:
          {
            realType = "UNKNOWN";
          }
        }

        cerr << "CACHE_ITEM ref type " << refType.c_str() << " real type " << realType.c_str() << endl;
        break;
      }
      else if (field->type() == Item::FUNC_ITEM)
      {
        Item_func* ifp = (Item_func*)field;
        cerr << "CACHE_ITEM FUNC_ITEM " << ifp->func_name() << endl;
        break;
      }
      else
      {
        cerr << "CACHE_ITEM type unknown " << field->type() << endl;
      }

      break;
    }

    case Item::WINDOW_FUNC_ITEM:
    {
      Item_window_func* ifp = (Item_window_func*)item;
      cerr << "Window Function Item " << ifp->window_func()->func_name() << endl;
      break;
    }

    case Item::NULL_ITEM:
    {
      cerr << "NULL item" << endl;
      break;
    }

    case Item::TYPE_HOLDER:
    {
      cerr << "TYPE_HOLDER item with cmp_type " << item->cmp_type() << endl;
      break;
    }

    default:
    {
      cerr << "UNKNOWN_ITEM type " << item->type() << endl;
      break;
    }
  }
}
#endif

void buildNestedJoinLeafTables(List<TABLE_LIST>& join_list,
                               std::set<execplan::CalpontSystemCatalog::TableAliasName>& leafTables)
{
  TABLE_LIST* table;
  List_iterator<TABLE_LIST> li(join_list);

  while ((table = li++))
  {
    if (table->nested_join)
      buildNestedJoinLeafTables(table->nested_join->join_list, leafTables);
    else
    {
      CalpontSystemCatalog::TableAliasName tan = makeTableAliasName(table);
      leafTables.insert(tan);
    }
  }
}

uint32_t buildJoin(gp_walk_info& gwi, List<TABLE_LIST>& join_list,
                   std::stack<execplan::ParseTree*>& outerJoinStack)
{
  TABLE_LIST* table;
  List_iterator<TABLE_LIST> li(join_list);

  while ((table = li++))
  {
    // Make sure we don't process the derived table nests again,
    // they were already handled by FromSubQuery::transform()
    if (table->nested_join && !table->derived)
      buildJoin(gwi, table->nested_join->join_list, outerJoinStack);

    std::vector<COND*> tableOnExprList;

    auto iter = gwi.tableOnExprList.find(table);

    // Check if this table's on_expr is available in the hash map
    // built/updated during convertOuterJoinToInnerJoin().
    if ((iter != gwi.tableOnExprList.end()) && !iter->second.empty())
    {
      tableOnExprList = iter->second;
    }
    // This table's on_expr has not been seen/processed before.
    else if ((iter == gwi.tableOnExprList.end()) && table->on_expr)
    {
      tableOnExprList.push_back(table->on_expr);
    }

    if (!tableOnExprList.empty())
    {
      if (table->outer_join & (JOIN_TYPE_LEFT | JOIN_TYPE_RIGHT))
      {
        // inner tables block
        gp_walk_info gwi_outer = gwi;
        gwi_outer.subQuery = NULL;
        gwi_outer.hasSubSelect = false;
        gwi_outer.innerTables.clear();
        clearStacks(gwi_outer);

        // recursively build the leaf tables for this nested join node
        if (table->nested_join)
          buildNestedJoinLeafTables(table->nested_join->join_list, gwi_outer.innerTables);
        else  // this is a leaf table
        {
          CalpontSystemCatalog::TableAliasName tan = makeTableAliasName(table);
          gwi_outer.innerTables.insert(tan);
        }

#ifdef DEBUG_WALK_COND
        cerr << "inner tables: " << endl;
        set<CalpontSystemCatalog::TableAliasName>::const_iterator it;
        for (it = gwi_outer.innerTables.begin(); it != gwi_outer.innerTables.end(); ++it)
          cerr << (*it) << " ";
        cerr << endl;

        cerr << " outer table expression: " << endl;

        for (Item* expr : tableOnExprList)
          expr->traverse_cond(debug_walk, &gwi_outer, Item::POSTFIX);
#endif

        for (Item* expr : tableOnExprList)
        {
          expr->traverse_cond(gp_walk, &gwi_outer, Item::POSTFIX);

          // Error out subquery in outer join on filter for now
          if (gwi_outer.hasSubSelect)
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_OUTER_JOIN_SUBSELECT);
            setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText);
            return -1;
          }
        }

        // build outerjoinon filter
        ParseTree *filters = NULL, *ptp = NULL, *rhs = NULL;

        while (!gwi_outer.ptWorkStack.empty())
        {
          filters = gwi_outer.ptWorkStack.top();
          gwi_outer.ptWorkStack.pop();

          if (gwi_outer.ptWorkStack.empty())
            break;

          ptp = new ParseTree(new LogicOperator("and"));
          ptp->left(filters);
          rhs = gwi_outer.ptWorkStack.top();
          gwi_outer.ptWorkStack.pop();
          ptp->right(rhs);
          gwi_outer.ptWorkStack.push(ptp);
        }

        // should have only 1 pt left in stack.
        if (filters)
        {
          SPTP on_sp(filters);
          OuterJoinOnFilter* onFilter = new OuterJoinOnFilter(on_sp);
          ParseTree* pt = new ParseTree(onFilter);
          outerJoinStack.push(pt);
        }
      }
      else  // inner join
      {
        for (Item* expr : tableOnExprList)
        {
          expr->traverse_cond(gp_walk, &gwi, Item::POSTFIX);
        }

#ifdef DEBUG_WALK_COND
        cerr << " inner join expression: " << endl;
        for (Item* expr : tableOnExprList)
          expr->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
#endif
      }
    }
  }

  return 0;
}

ParseTree* buildRowPredicate(gp_walk_info* gwip, RowColumn* lhs, RowColumn* rhs, string predicateOp)
{
  PredicateOperator* po = new PredicateOperator(predicateOp);
  boost::shared_ptr<Operator> sop(po);
  LogicOperator* lo = NULL;

  if (predicateOp == "=")
    lo = new LogicOperator("and");
  else
    lo = new LogicOperator("or");

  ParseTree* pt = new ParseTree(lo);
  sop->setOpType(lhs->columnVec()[0]->resultType(), rhs->columnVec()[0]->resultType());
  SimpleFilter* sf = new SimpleFilter(sop, lhs->columnVec()[0].get(), rhs->columnVec()[0].get());
  sf->timeZone(gwip->timeZone);
  pt->left(new ParseTree(sf));

  for (uint32_t i = 1; i < lhs->columnVec().size(); i++)
  {
    sop.reset(po->clone());
    sop->setOpType(lhs->columnVec()[i]->resultType(), rhs->columnVec()[i]->resultType());
    SimpleFilter* sf = new SimpleFilter(sop, lhs->columnVec()[i].get(), rhs->columnVec()[i].get());
    sf->timeZone(gwip->timeZone);
    pt->right(new ParseTree(sf));

    if (i + 1 < lhs->columnVec().size())
    {
      ParseTree* lpt = pt;
      pt = new ParseTree(lo->clone());
      pt->left(lpt);
    }
  }

  return pt;
}

bool buildRowColumnFilter(gp_walk_info* gwip, RowColumn* rhs, RowColumn* lhs, Item_func* ifp)
{
  if (ifp->functype() == Item_func::EQ_FUNC || ifp->functype() == Item_func::NE_FUNC)
  {
    // (c1,c2,..) = (v1,v2,...) transform to: c1=v1 and c2=v2 and ...
    assert(!lhs->columnVec().empty() && lhs->columnVec().size() == rhs->columnVec().size());
    gwip->ptWorkStack.push(buildRowPredicate(gwip, rhs, lhs, ifp->func_name()));
  }
  else if (ifp->functype() == Item_func::IN_FUNC)
  {
    // (c1,c2,...) in ((v11,v12,...),(v21,v22,...)...) transform to:
    // ((c1 = v11 and c2 = v12 and ...) or (c1 = v21 and c2 = v22 and ...) or ...)
    // and c1 in (v11,v21,...) and c2 in (v12,v22,...) => to favor CP
    Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
    string predicateOp, logicOp;

    if (inp->negated)
    {
      predicateOp = "<>";
      logicOp = "and";
    }
    else
    {
      predicateOp = "=";
      logicOp = "or";
    }

    boost::scoped_ptr<LogicOperator> lo(new LogicOperator(logicOp));

    // 1st round. build the equivalent filters
    // two entries have been popped from the stack already: lhs and rhs
    stack<ReturnedColumn*> tmpStack;
    vector<RowColumn*> valVec;
    tmpStack.push(rhs);
    tmpStack.push(lhs);
    assert(gwip->rcWorkStack.size() >= ifp->argument_count() - 2);

    for (uint32_t i = 2; i < ifp->argument_count(); i++)
    {
      tmpStack.push(gwip->rcWorkStack.top());

      if (!gwip->rcWorkStack.empty())
        gwip->rcWorkStack.pop();
    }

    RowColumn* columns = dynamic_cast<RowColumn*>(tmpStack.top());
    tmpStack.pop();
    RowColumn* vals = dynamic_cast<RowColumn*>(tmpStack.top());
    valVec.push_back(vals);
    tmpStack.pop();
    ParseTree* pt = buildRowPredicate(gwip, columns, vals, predicateOp);

    while (!tmpStack.empty())
    {
      ParseTree* pt1 = new ParseTree(lo->clone());
      pt1->left(pt);
      vals = dynamic_cast<RowColumn*>(tmpStack.top());
      valVec.push_back(vals);
      tmpStack.pop();
      pt1->right(buildRowPredicate(gwip, columns->clone(), vals, predicateOp));
      pt = pt1;
    }

    gwip->ptWorkStack.push(pt);

    // done for NOTIN clause
    if (predicateOp == "<>")
      return true;

    // 2nd round. add the filter to favor casual partition for IN clause
    boost::shared_ptr<Operator> sop;
    boost::shared_ptr<SimpleColumn> ssc;
    stack<ParseTree*> tmpPtStack;

    for (uint32_t i = 0; i < columns->columnVec().size(); i++)
    {
      ConstantFilter* cf = new ConstantFilter();

      sop.reset(lo->clone());
      cf->op(sop);
      SimpleColumn* sc = dynamic_cast<SimpleColumn*>(columns->columnVec()[i].get());

      // no optimization for non-simple column because CP won't apply
      if (!sc)
        continue;

      ssc.reset(sc->clone());
      cf->col(ssc);

      uint32_t j = 0;

      for (; j < valVec.size(); j++)
      {
        sop.reset(new PredicateOperator(predicateOp));
        ConstantColumn* cc = dynamic_cast<ConstantColumn*>(valVec[j]->columnVec()[i].get());

        // no optimization for non-constant value because CP won't apply
        if (!cc)
          break;

        sop->setOpType(sc->resultType(), valVec[j]->columnVec()[i]->resultType());
        cf->pushFilter(
            new SimpleFilter(sop, sc->clone(), valVec[j]->columnVec()[i]->clone(), gwip->timeZone));
      }

      if (j < valVec.size())
        continue;

      tmpPtStack.push(new ParseTree(cf));
    }

    // "and" all the filters together
    ParseTree* ptp = NULL;
    pt = NULL;

    while (!tmpPtStack.empty())
    {
      pt = tmpPtStack.top();
      tmpPtStack.pop();

      if (tmpPtStack.empty())
        break;

      ptp = new ParseTree(new LogicOperator("and"));
      ptp->left(pt);
      pt = tmpPtStack.top();
      tmpPtStack.pop();
      ptp->right(pt);
      tmpPtStack.push(ptp);
    }

    if (pt)
    {
      tmpPtStack.push(pt);

      // AND with the pt built from the first round
      if (!gwip->ptWorkStack.empty() && !tmpPtStack.empty())
      {
        pt = new ParseTree(new LogicOperator("and"));
        pt->left(gwip->ptWorkStack.top());
        gwip->ptWorkStack.pop();
        pt->right(tmpPtStack.top());
        tmpPtStack.pop();
      }

      // Push the final pt tree for this IN clause
      gwip->ptWorkStack.push(pt);
    }
  }
  else
  {
    gwip->fatalParseError = true;
    gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FUNC_MULTI_COL);
    return false;
  }

  return true;
}

void checkOuterTableColumn(gp_walk_info* gwip, const CalpontSystemCatalog::TableAliasName& tan,
                           execplan::ReturnedColumn* col)
{
  set<CalpontSystemCatalog::TableAliasName>::const_iterator it;

  bool notInner = true;

  for (it = gwip->innerTables.begin(); it != gwip->innerTables.end(); ++it)
  {
    if (tan.alias == it->alias && tan.view == it->view)
      notInner = false;
  }

  if (notInner)
  {
    col->returnAll(true);
    IDEBUG(cerr << "setting returnAll on " << tan << endl);
  }
}

bool buildEqualityPredicate(execplan::ReturnedColumn* lhs, execplan::ReturnedColumn* rhs, gp_walk_info* gwip,
                            boost::shared_ptr<Operator>& sop, const Item_func::Functype& funcType,
                            const vector<Item*>& itemList, bool isInSubs)
{
  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  // push the column that is associated with the correlated column to the returned
  // column list, so the materialized view have the complete projection list.
  // e.g. tout.c1 in (select tin.c1 from tin where tin.c2=tout.c2);
  // the projetion list of subquery will have tin.c1, tin.c2.
  ReturnedColumn* correlatedCol = nullptr;
  ReturnedColumn* localCol = nullptr;

  if (rhs->joinInfo() & JOIN_CORRELATED)
  {
    correlatedCol = rhs;
    localCol = lhs;
  }
  else if (lhs->joinInfo() & JOIN_CORRELATED)
  {
    correlatedCol = lhs;
    localCol = rhs;
  }

  if (correlatedCol && localCol)
  {
    ConstantColumn* cc = dynamic_cast<ConstantColumn*>(localCol);

    if ((!cc || (cc && funcType == Item_func::EQ_FUNC)) && !(localCol->joinInfo() & JOIN_CORRELATED))
    {
      if (isInSubs)
        localCol->sequence(0);
      else
        localCol->sequence(gwip->returnedCols.size());

      localCol->expressionId(ci->expressionId++);
      ReturnedColumn* rc = localCol->clone();
      rc->colSource(rc->colSource() | CORRELATED_JOIN);
      gwip->additionalRetCols.push_back(SRCP(rc));
      gwip->localCols.push_back(localCol);

      if (rc->hasWindowFunc() && !isInSubs)
        gwip->windowFuncList.push_back(rc);
    }

    // push the correlated join partner to the group by list only when there's aggregate
    // and we don't push aggregate column to the group by
    // @bug4756. mysql does not always give correct information about whether there is
    // aggregate on the SELECT list. Need to figure that by ourselves and then decide
    // to add the group by or not.
    if (gwip->subQuery)
    {
      if (!localCol->hasAggregate() && !localCol->hasWindowFunc())
        gwip->subGroupByCols.push_back(SRCP(localCol->clone()));
    }

    if (sop->op() == OP_EQ)
    {
      if (gwip->subSelectType == CalpontSelectExecutionPlan::IN_SUBS ||
          gwip->subSelectType == CalpontSelectExecutionPlan::EXISTS_SUBS)
        correlatedCol->joinInfo(correlatedCol->joinInfo() | JOIN_SEMI);
      else if (gwip->subSelectType == CalpontSelectExecutionPlan::NOT_IN_SUBS ||
               gwip->subSelectType == CalpontSelectExecutionPlan::NOT_EXISTS_SUBS)
        correlatedCol->joinInfo(correlatedCol->joinInfo() | JOIN_ANTI);
    }
  }

  SimpleFilter* sf = new SimpleFilter();
  sf->timeZone(gwip->timeZone);

  //@bug 2101 for when there are only constants in a delete or update where clause (eg "where 5 < 6").
  // There will be no field column and it will get here only if the comparison is true.
  if (gwip->columnMap.empty() && ((current_thd->lex->sql_command == SQLCOM_UPDATE) ||
                                  (current_thd->lex->sql_command == SQLCOM_UPDATE_MULTI) ||
                                  (current_thd->lex->sql_command == SQLCOM_DELETE) ||
                                  (current_thd->lex->sql_command == SQLCOM_DELETE_MULTI)))
  {
    IDEBUG(cerr << "deleted func with 2 const columns" << endl);
    delete rhs;
    delete lhs;
    return false;
  }

  // handle noop (only for table mode)
  if (rhs->data() == "noop" || lhs->data() == "noop")
  {
    sop.reset(new Operator("noop"));
  }
  else
  {
    for (uint32_t i = 0; i < itemList.size(); i++)
    {
      if (isPredicateFunction(itemList[i], gwip))
      {
        gwip->fatalParseError = true;
        gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SUB_EXPRESSION);
      }
    }
  }

  sf->op(sop);
  sf->lhs(lhs);
  sf->rhs(rhs);
  sop->setOpType(lhs->resultType(), rhs->resultType());
  sop->resultType(sop->operationType());

  if (sop->op() == OP_EQ)
  {
    CalpontSystemCatalog::TableAliasName tan_lhs;
    CalpontSystemCatalog::TableAliasName tan_rhs;
    bool outerjoin = (rhs->singleTable(tan_rhs) && lhs->singleTable(tan_lhs));

    // @bug 1632. Alias should be taken account to the identity of tables for selfjoin to work
    if (outerjoin && tan_lhs != tan_rhs)  // join
    {
      if (!gwip->condPush)  // vtable
      {
        if (!gwip->innerTables.empty())
        {
          checkOuterTableColumn(gwip, tan_lhs, lhs);
          checkOuterTableColumn(gwip, tan_rhs, rhs);
        }

        if (funcType == Item_func::EQ_FUNC)
        {
          gwip->equiCondSFList.push_back(sf);
        }

        ParseTree* ptp = new ParseTree(sf);
        gwip->ptWorkStack.push(ptp);
      }
    }
    else
    {
      ParseTree* ptp = new ParseTree(sf);
      gwip->ptWorkStack.push(ptp);
    }
  }
  else
  {
    ParseTree* ptp = new ParseTree(sf);
    gwip->ptWorkStack.push(ptp);
  }

  return true;
}

bool buildPredicateItem(Item_func* ifp, gp_walk_info* gwip)
{
  boost::shared_ptr<Operator> sop(new PredicateOperator(ifp->func_name()));

  if (ifp->functype() == Item_func::LIKE_FUNC)
  {
    // Starting with MariaDB 10.2, LIKE uses a negated flag instead of FUNC_NOT
    // Further processing is done below as before for LIKE
    if (((Item_func_like*)ifp)->get_negated())
    {
      sop->reverseOp();
    }
  }

  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  if (ifp->functype() == Item_func::BETWEEN)
  {
    idbassert(gwip->rcWorkStack.size() >= 3);
    ReturnedColumn* rhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    ReturnedColumn* lhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    ReturnedColumn* filterCol = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();  // pop gwip->scsp;
    Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;
    SimpleFilter* sfr = 0;
    SimpleFilter* sfl = 0;

    if (inp->negated)
    {
      sop.reset(new PredicateOperator(">"));
      sop->setOpType(filterCol->resultType(), rhs->resultType());
      sfr = new SimpleFilter(sop, filterCol, rhs);
      sfr->timeZone(gwip->timeZone);
      sop.reset(new PredicateOperator("<"));
      sop->setOpType(filterCol->resultType(), lhs->resultType());
      sfl = new SimpleFilter(sop, filterCol->clone(), lhs);
      sfl->timeZone(gwip->timeZone);
      ParseTree* ptp = new ParseTree(new LogicOperator("or"));
      ptp->left(sfr);
      ptp->right(sfl);
      gwip->ptWorkStack.push(ptp);
    }
    else
    {
      sop.reset(new PredicateOperator("<="));
      sop->setOpType(filterCol->resultType(), rhs->resultType());
      sfr = new SimpleFilter(sop, filterCol, rhs);
      sfr->timeZone(gwip->timeZone);
      sop.reset(new PredicateOperator(">="));
      sop->setOpType(filterCol->resultType(), lhs->resultType());
      sfl = new SimpleFilter(sop, filterCol->clone(), lhs);
      sfl->timeZone(gwip->timeZone);
      ParseTree* ptp = new ParseTree(new LogicOperator("and"));
      ptp->left(sfr);
      ptp->right(sfl);
      gwip->ptWorkStack.push(ptp);
    }

    return true;
  }
  else if (ifp->functype() == Item_func::IN_FUNC)
  {
    idbassert(gwip->rcWorkStack.size() >= 2);
    ReturnedColumn* rhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    ReturnedColumn* lhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();

    // @bug3038
    RowColumn* rrhs = dynamic_cast<RowColumn*>(rhs);
    RowColumn* rlhs = dynamic_cast<RowColumn*>(lhs);

    if (rrhs && rlhs)
    {
      return buildRowColumnFilter(gwip, rrhs, rlhs, ifp);
    }

    ConstantColumn* crhs = dynamic_cast<ConstantColumn*>(rhs);
    ConstantColumn* clhs = dynamic_cast<ConstantColumn*>(lhs);

    if (!crhs || !clhs)
    {
      gwip->fatalParseError = true;
      gwip->parseErrorText = "non constant value in IN clause";
      return false;
    }

    string eqop;
    string cmbop;
    Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;

    if (inp->negated)
    {
      eqop = "<>";
      cmbop = "and";
    }
    else
    {
      eqop = "=";
      cmbop = "or";
    }

    sop.reset(new PredicateOperator(eqop));
    sop->setOpType(gwip->scsp->resultType(), rhs->resultType());
    ConstantFilter* cf = 0;

    cf = new ConstantFilter(sop, gwip->scsp->clone(), rhs);
    sop.reset(new LogicOperator(cmbop));
    cf->op(sop);
    sop.reset(new PredicateOperator(eqop));
    sop->setOpType(gwip->scsp->resultType(), lhs->resultType());
    cf->pushFilter(new SimpleFilter(sop, gwip->scsp->clone(), lhs, gwip->timeZone));

    while (!gwip->rcWorkStack.empty())
    {
      lhs = gwip->rcWorkStack.top();

      if (dynamic_cast<ConstantColumn*>(lhs) == 0)
        break;

      gwip->rcWorkStack.pop();
      sop.reset(new PredicateOperator(eqop));
      sop->setOpType(gwip->scsp->resultType(), lhs->resultType());
      cf->pushFilter(new SimpleFilter(sop, gwip->scsp->clone(), lhs, gwip->timeZone));
    }

    if (!gwip->rcWorkStack.empty())
      gwip->rcWorkStack.pop();  // pop gwip->scsp

    if (cf->filterList().size() < inp->argument_count() - 1)
    {
      gwip->fatalParseError = true;
      gwip->parseErrorText = "non constant value in IN clause";
      delete cf;
      return false;
    }

    cf->functionName(gwip->funcName);
    String str;
    // @bug5811. This filter string is for cross engine to use.
    // Use real table name.
    ifp->print(&str, QT_ORDINARY);
    IDEBUG(cerr << str.ptr() << endl);

    if (str.ptr())
      cf->data(str.c_ptr());

    ParseTree* ptp = new ParseTree(cf);
    gwip->ptWorkStack.push(ptp);
  }

  else if (ifp->functype() == Item_func::ISNULL_FUNC || ifp->functype() == Item_func::ISNOTNULL_FUNC)
  {
    ReturnedColumn* rhs = NULL;
    if (!gwip->rcWorkStack.empty() && !gwip->inCaseStmt)
    {
      rhs = gwip->rcWorkStack.top();
      gwip->rcWorkStack.pop();
    }
    else
    {
      rhs = buildReturnedColumn(ifp->arguments()[0], *gwip, gwip->fatalParseError);
    }

    if (rhs && !gwip->fatalParseError)
      buildConstPredicate(ifp, rhs, gwip);
    else if (!rhs)  // @bug 3802
    {
      gwip->fatalParseError = true;
      gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
      return false;
    }
  }

  else if (ifp->functype() == Item_func::GUSERVAR_FUNC)
  {
    Item_func_get_user_var* udf = reinterpret_cast<Item_func_get_user_var*>(ifp);
    String buf;

    if (udf->result_type() == INT_RESULT)
    {
      if (udf->unsigned_flag)
      {
        gwip->rcWorkStack.push(new ConstantColumn((uint64_t)udf->val_uint()));
      }
      else
      {
        gwip->rcWorkStack.push(new ConstantColumn((int64_t)udf->val_int()));
      }
      (dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
    }
    else
    {
      // const String* str = udf->val_str(&buf);
      udf->val_str(&buf);

      if (!buf.ptr())
      {
        ostringstream oss;
        oss << "Unknown user variable: " << udf->name.str;
        gwip->parseErrorText = oss.str();
        gwip->fatalParseError = true;
        return false;
      }

      if (udf->result_type() == STRING_RESULT)
        gwip->rcWorkStack.push(new ConstantColumn(buf.ptr()));
      else
      {
        gwip->rcWorkStack.push(new ConstantColumn(buf.ptr(), ConstantColumn::NUM));
      }
      (dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);

      return false;
    }
  }

#if 0
    else if (ifp->functype() == Item_func::NEG_FUNC)
    {
        //peek at the (hopefully) ConstantColumn on the top of stack, negate it in place
        ConstantColumn* ccp = dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top());

        if (!ccp)
        {
            ostringstream oss;
            oss << "Attempt to negate a non-constant column";
            gwip->parseErrorText = oss.str();
            gwip->fatalParseError = true;
            return false;
        }

        string cval = ccp->constval();
        string newval;

        if (cval[0] == '-')
            newval.assign(cval, 1, string::npos);
        else
            newval = "-" + cval;

        ccp->constval(newval);
    }

#endif
  else if (ifp->functype() == Item_func::NOT_FUNC)
  {
    if (gwip->condPush && ifp->next->type() == Item::SUBSELECT_ITEM)
      return false;

    if (ifp->next && ifp->next->type() == Item::SUBSELECT_ITEM && gwip->lastSub)
    {
      gwip->lastSub->handleNot();
      return false;
    }

    idbassert(ifp->argument_count() == 1);
    ParseTree* ptp = 0;
    if (((Item_func*)(ifp->arguments()[0]))->functype() == Item_func::EQUAL_FUNC)
    {
      // negate it in place
      // Note that an EQUAL_FUNC ( a <=> b) was converted to
      // ( a = b OR ( a is null AND b is null) )
      // NOT of the above expression is: ( a != b AND (a is not null OR b is not null )

      if (!gwip->ptWorkStack.empty())
        ptp = gwip->ptWorkStack.top();

      if (ptp)
      {
        ParseTree* or_ptp = ptp;
        ParseTree* and_ptp = or_ptp->right();
        ParseTree* equal_ptp = or_ptp->left();
        ParseTree* nullck_left_ptp = and_ptp->left();
        ParseTree* nullck_right_ptp = and_ptp->right();
        SimpleFilter* sf_left_nullck = dynamic_cast<SimpleFilter*>(nullck_left_ptp->data());
        SimpleFilter* sf_right_nullck = dynamic_cast<SimpleFilter*>(nullck_right_ptp->data());
        SimpleFilter* sf_equal = dynamic_cast<SimpleFilter*>(equal_ptp->data());

        if (sf_left_nullck && sf_right_nullck && sf_equal)
        {
          // Negate the null checks
          sf_left_nullck->op()->reverseOp();
          sf_right_nullck->op()->reverseOp();
          sf_equal->op()->reverseOp();
          // Rehook the nodes
          ptp = and_ptp;
          ptp->left(equal_ptp);
          ptp->right(or_ptp);
          or_ptp->left(nullck_left_ptp);
          or_ptp->right(nullck_right_ptp);
          gwip->ptWorkStack.pop();
          gwip->ptWorkStack.push(ptp);
        }
        else
        {
          gwip->fatalParseError = true;
          gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_ASSERTION_FAILURE);
          return false;
        }
      }
    }
    else if (isPredicateFunction(ifp->arguments()[0], gwip) || ifp->arguments()[0]->type() == Item::COND_ITEM)
    {
      // negate it in place
      if (!gwip->ptWorkStack.empty())
        ptp = gwip->ptWorkStack.top();

      SimpleFilter* sf = 0;

      if (ptp)
      {
        sf = dynamic_cast<SimpleFilter*>(ptp->data());

        if (sf)
          sf->op()->reverseOp();
      }
    }
    else
    {
      // transfrom the not item to item = 0
      ReturnedColumn* rhs = 0;

      if (!gwip->rcWorkStack.empty())
      {
        rhs = gwip->rcWorkStack.top();
        gwip->rcWorkStack.pop();
      }
      else
      {
        rhs = buildReturnedColumn(ifp->arguments()[0], *gwip, gwip->fatalParseError);
      }

      if (rhs && !gwip->fatalParseError)
        return buildConstPredicate(ifp, rhs, gwip);
      else if (!rhs)  // @bug3802
      {
        gwip->fatalParseError = true;
        gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
        return false;
      }
    }
  }
  /*else if (ifp->functype() == Item_func::MULT_EQUAL_FUNC)
  {
      Item_equal *cur_item_eq = (Item_equal*)ifp;
      Item *lhs_item, *rhs_item;
      // There must be at least two items
      Item_equal_fields_iterator lhs_it(*cur_item_eq);
      Item_equal_fields_iterator rhs_it(*cur_item_eq); rhs_it++;
      while ((lhs_item = lhs_it++) && (rhs_item = rhs_it++))
      {
          ReturnedColumn* lhs = buildReturnedColumn(lhs_item, *gwip, gwip->fatalParseError);
          ReturnedColumn* rhs = buildReturnedColumn(rhs_item, *gwip, gwip->fatalParseError);
          if (!rhs || !lhs)
          {
              gwip->fatalParseError = true;
              gwip->parseErrorText = "Unsupport elements in MULT_EQUAL item";
              delete rhs;
              delete lhs;
              return false;
          }
          PredicateOperator* po = new PredicateOperator("=");
          boost::shared_ptr<Operator> sop(po);
          sop->setOpType(lhs->resultType(), rhs->resultType());
          SimpleFilter* sf = new SimpleFilter(sop, lhs, rhs);
          // Search in ptWorkStack for duplicates of the SF
          // before we push the SF
          if (!isDuplicateSF(gwip, sf))
          {
              ParseTree* pt = new ParseTree(sf);
              gwip->ptWorkStack.push(pt);
          }
      }
  }*/
  else if (ifp->functype() == Item_func::EQUAL_FUNC)
  {
    // Convert "a <=> b" to (a = b OR (a IS NULL AND b IS NULL))"
    idbassert(gwip->rcWorkStack.size() >= 2);
    ReturnedColumn* rhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    ReturnedColumn* lhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    SimpleFilter* sfn1 = 0;
    SimpleFilter* sfn2 = 0;
    SimpleFilter* sfo = 0;
    // b IS NULL
    ConstantColumn* nlhs1 = new ConstantColumn("", ConstantColumn::NULLDATA);
    nlhs1->timeZone(gwip->timeZone);
    sop.reset(new PredicateOperator("isnull"));
    sop->setOpType(lhs->resultType(), rhs->resultType());
    sfn1 = new SimpleFilter(sop, rhs, nlhs1);
    sfn1->timeZone(gwip->timeZone);
    ParseTree* ptpl = new ParseTree(sfn1);
    // a IS NULL
    ConstantColumn* nlhs2 = new ConstantColumn("", ConstantColumn::NULLDATA);
    nlhs2->timeZone(gwip->timeZone);
    sop.reset(new PredicateOperator("isnull"));
    sop->setOpType(lhs->resultType(), rhs->resultType());
    sfn2 = new SimpleFilter(sop, lhs, nlhs2);
    sfn2->timeZone(gwip->timeZone);
    ParseTree* ptpr = new ParseTree(sfn2);
    // AND them both
    ParseTree* ptpn = new ParseTree(new LogicOperator("and"));
    ptpn->left(ptpl);
    ptpn->right(ptpr);
    // a = b
    sop.reset(new PredicateOperator("="));
    sop->setOpType(lhs->resultType(), rhs->resultType());
    sfo = new SimpleFilter(sop, lhs->clone(), rhs->clone());
    sfo->timeZone(gwip->timeZone);
    // OR with the NULL comparison tree
    ParseTree* ptp = new ParseTree(new LogicOperator("or"));
    ptp->left(sfo);
    ptp->right(ptpn);
    gwip->ptWorkStack.push(ptp);
    return true;
  }
  else  // std rel ops (incl "like")
  {
    if (gwip->rcWorkStack.size() < 2)
    {
      idbassert(ifp->argument_count() == 2);

      if (isPredicateFunction(ifp->arguments()[0], gwip) || ifp->arguments()[0]->type() == Item::COND_ITEM ||
          isPredicateFunction(ifp->arguments()[1], gwip) || ifp->arguments()[1]->type() == Item::COND_ITEM)
      {
        gwip->fatalParseError = true;
        gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
      }

      return false;
    }

    ReturnedColumn* rhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();
    ReturnedColumn* lhs = gwip->rcWorkStack.top();
    gwip->rcWorkStack.pop();

    // @bug3038. rowcolumn filter
    RowColumn* rrhs = dynamic_cast<RowColumn*>(rhs);
    RowColumn* rlhs = dynamic_cast<RowColumn*>(lhs);

    if (rrhs && rlhs)
      return buildRowColumnFilter(gwip, rrhs, rlhs, ifp);

    vector<Item*> itemList;

    for (uint32_t i = 0; i < ifp->argument_count(); i++)
      itemList.push_back(ifp->arguments()[i]);

    return buildEqualityPredicate(lhs, rhs, gwip, sop, ifp->functype(), itemList);
  }

  return true;
}

bool buildConstPredicate(Item_func* ifp, ReturnedColumn* rhs, gp_walk_info* gwip)
{
  SimpleFilter* sf = new SimpleFilter();
  sf->timeZone(gwip->timeZone);
  boost::shared_ptr<Operator> sop(new PredicateOperator(ifp->func_name()));
  ConstantColumn* lhs = 0;

  if (ifp->functype() == Item_func::ISNULL_FUNC)
  {
    lhs = new ConstantColumn("", ConstantColumn::NULLDATA);
    sop.reset(new PredicateOperator("isnull"));
  }
  else if (ifp->functype() == Item_func::ISNOTNULL_FUNC)
  {
    lhs = new ConstantColumn("", ConstantColumn::NULLDATA);
    sop.reset(new PredicateOperator("isnotnull"));
  }
  else  // if (ifp->functype() == Item_func::NOT_FUNC)
  {
    lhs = new ConstantColumn((int64_t)0, ConstantColumn::NUM);
    sop.reset(new PredicateOperator("="));
  }
  lhs->timeZone(gwip->timeZone);

  CalpontSystemCatalog::ColType opType = rhs->resultType();

  if ((opType.colDataType == CalpontSystemCatalog::CHAR && opType.colWidth <= 8) ||
      (opType.colDataType == CalpontSystemCatalog::VARCHAR && opType.colWidth < 8) ||
      (opType.colDataType == CalpontSystemCatalog::VARBINARY && opType.colWidth < 8))
  {
    opType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
    opType.colWidth = 8;
  }

  sop->operationType(opType);
  sf->op(sop);

  // yes, these are backwards
  assert(lhs);
  sf->lhs(rhs);
  sf->rhs(lhs);
  ParseTree* ptp = new ParseTree(sf);
  gwip->ptWorkStack.push(ptp);
  return true;
}

SimpleColumn* buildSimpleColFromDerivedTable(gp_walk_info& gwi, Item_field* ifp)
{
  SimpleColumn* sc = NULL;

  // view name
  string viewName = getViewName(ifp->cached_table);

  // Check from the end because local scope resolve is preferred
  for (int32_t i = gwi.tbList.size() - 1; i >= 0; i--)
  {
    if (sc)
      break;

    if (!gwi.tbList[i].schema.empty() && !gwi.tbList[i].table.empty() &&
        (!ifp->table_name.str || strcasecmp(ifp->table_name.str, gwi.tbList[i].alias.c_str()) == 0))
    {
      CalpontSystemCatalog::TableName tn(gwi.tbList[i].schema, gwi.tbList[i].table);
      CalpontSystemCatalog::RIDList oidlist = gwi.csc->columnRIDs(tn, true);

      for (unsigned int j = 0; j < oidlist.size(); j++)
      {
        CalpontSystemCatalog::TableColName tcn = gwi.csc->colName(oidlist[j].objnum);
        CalpontSystemCatalog::ColType ct = gwi.csc->colType(oidlist[j].objnum);

        if (strcasecmp(ifp->field_name.str, tcn.column.c_str()) == 0)
        {
          // @bug4827. Remove the checking because outside tables could be the same
          // name as inner tables. This function is to identify column from a table,
          // as long as it matches the next step in predicate parsing will tell the scope
          // of the column.
          /*if (sc)
          {
              gwi.fatalParseError = true;
              Message::Args args;
              args.add(ifp->name);
              gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_AMBIGUOUS_COL, args);
              return NULL;
          }*/

          sc = new SimpleColumn();
          sc->columnName(tcn.column);
          sc->tableName(tcn.table);
          sc->schemaName(tcn.schema);
          sc->oid(oidlist[j].objnum);

          // @bug 3003. Keep column alias if it has.
          sc->alias(!ifp->is_explicit_name() ? tcn.column : ifp->name.str);

          sc->tableAlias(gwi.tbList[i].alias);
          sc->viewName(viewName, lower_case_table_names);
          sc->resultType(ct);
          sc->timeZone(gwi.timeZone);
          break;
        }
      }
    }
  }

  if (sc)
    return sc;

  // Check from the end because local scope resolve is preferred
  for (int32_t i = gwi.derivedTbList.size() - 1; i >= 0; i--)
  {
    if (sc)
      break;

    CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(gwi.derivedTbList[i].get());
    string derivedName = csep->derivedTbAlias();

    if (!ifp->table_name.str || strcasecmp(ifp->table_name.str, derivedName.c_str()) == 0)
    {
      CalpontSelectExecutionPlan::ReturnedColumnList cols = csep->returnedCols();

      for (uint32_t j = 0; j < cols.size(); j++)
      {
        // @bug 3167 duplicate column alias is full name
        SimpleColumn* col = dynamic_cast<SimpleColumn*>(cols[j].get());
        string alias = cols[j]->alias();

        if (strcasecmp(ifp->field_name.str, alias.c_str()) == 0 ||
            (col && alias.find(".") != string::npos &&
             (strcasecmp(ifp->field_name.str, col->columnName().c_str()) == 0 ||
              strcasecmp(ifp->field_name.str, (alias.substr(alias.find_last_of(".") + 1)).c_str()) ==
                  0)))  //@bug6066
        {
          // @bug4827. Remove the checking because outside tables could be the same
          // name as inner tables. This function is to identify column from a table,
          // as long as it matches the next step in predicate parsing will tell the scope
          // of the column.
          /*if (sc)
          {
              gwi.fatalParseError = true;
              Message::Args args;
              args.add(ifp->name);
              gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_AMBIGUOUS_COL, args);
              return NULL;
          }*/
          sc = new SimpleColumn();

          if (!col)
            sc->columnName(cols[j]->alias());
          else
            sc->columnName(col->columnName());

          // @bug 3003. Keep column alias if it has.
          sc->alias(!ifp->is_explicit_name() ? cols[j]->alias() : ifp->name.str);
          sc->tableName(csep->derivedTbAlias());
          sc->colPosition(j);
          sc->tableAlias(csep->derivedTbAlias());
          sc->timeZone(gwi.timeZone);
          if (!viewName.empty())
          {
            sc->viewName(viewName, lower_case_table_names);
          }
          else
          {
            sc->viewName(csep->derivedTbView());
          }
          sc->resultType(cols[j]->resultType());
          sc->hasAggregate(cols[j]->hasAggregate());

          if (col)
            sc->isColumnStore(col->isColumnStore());

          // @bug5634, @bug5635. mark used derived col on derived table.
          // outer join inner table filter can not be moved in
          // MariaDB 10.1: cached_table is never available for derived tables.
          // Find the uncached object in table_list
          TABLE_LIST* tblList = ifp->context ? ifp->context->table_list : NULL;

          while (tblList)
          {
            if (strcasecmp(tblList->alias.str, ifp->table_name.str) == 0)
            {
              if (!tblList->outer_join)
              {
                sc->derivedTable(derivedName);
                sc->derivedRefCol(cols[j].get());
              }

              break;
            }

            tblList = tblList->next_local;
          }

          cols[j]->incRefCount();
          break;
        }
      }
    }
  }

  if (!sc)
  {
    gwi.fatalParseError = true;
    Message::Args args;
    string name;

    if (ifp->db_name.str)
      name += string(ifp->db_name.str) + ".";

    if (ifp->table_name.str)
      name += string(ifp->table_name.str) + ".";

    if (ifp->name.length)
      name += ifp->name.str;
    args.add(name);
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL, args);
  }

  if (ifp->depended_from)
  {
    sc->joinInfo(sc->joinInfo() | JOIN_CORRELATED);

    if (gwi.subQuery)
      gwi.subQuery->correlated(true);

    // for error out non-support select filter case (comparison outside semi join tables)
    gwi.correlatedTbNameVec.push_back(make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias()));

    // imply semi for scalar for now.
    if (gwi.subSelectType == CalpontSelectExecutionPlan::SINGLEROW_SUBS)
      sc->joinInfo(sc->joinInfo() | JOIN_SCALAR | JOIN_SEMI);

    if (gwi.subSelectType == CalpontSelectExecutionPlan::SELECT_SUBS)
      sc->joinInfo(sc->joinInfo() | JOIN_SCALAR | JOIN_OUTER_SELECT);
  }

  return sc;
}

// for FROM clause subquery. get all the columns from real tables and derived tables.
void collectAllCols(gp_walk_info& gwi, Item_field* ifp)
{
  // view name
  string viewName = getViewName(ifp->cached_table);

  if (gwi.derivedTbList.empty())
  {
    gwi.fatalParseError = true;
    Message::Args args;
    args.add("*");
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL, args);
  }

  string tableName = (ifp->table_name.str ? string(ifp->table_name.str) : "");
  CalpontSelectExecutionPlan::SelectList::const_iterator it = gwi.derivedTbList.begin();

  for (uint32_t i = 0; i < gwi.tbList.size(); i++)
  {
    SRCP srcp;

    // derived table
    if (gwi.tbList[i].schema.empty())
    {
      CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>((*it).get());
      ++it;

      if (!tableName.empty() && strcasecmp(tableName.c_str(), csep->derivedTbAlias().c_str()) != 0)
        continue;

      CalpontSelectExecutionPlan::ReturnedColumnList cols = csep->returnedCols();

      for (uint32_t j = 0; j < cols.size(); j++)
      {
        SimpleColumn* sc = new SimpleColumn();
        sc->columnName(cols[j]->alias());
        sc->colPosition(j);
        sc->tableAlias(csep->derivedTbAlias());
        sc->viewName(gwi.tbList[i].view);
        sc->resultType(cols[j]->resultType());
        sc->timeZone(gwi.timeZone);

        // @bug5634 derived table optimization
        cols[j]->incRefCount();
        sc->derivedTable(sc->tableAlias());
        sc->derivedRefCol(cols[j].get());
        srcp.reset(sc);
        gwi.returnedCols.push_back(srcp);
        gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
      }
    }
    // real tables
    else
    {
      CalpontSystemCatalog::TableName tn(gwi.tbList[i].schema, gwi.tbList[i].table);

      if (lower_case_table_names)
      {
        if (!tableName.empty() && (strcasecmp(tableName.c_str(), tn.table.c_str()) != 0 &&
                                   strcasecmp(tableName.c_str(), gwi.tbList[i].alias.c_str()) != 0))
          continue;
      }
      else
      {
        if (!tableName.empty() && (strcmp(tableName.c_str(), tn.table.c_str()) != 0 &&
                                   strcmp(tableName.c_str(), gwi.tbList[i].alias.c_str()) != 0))
          continue;
      }
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(tn.schema);
        boost::algorithm::to_lower(tn.table);
      }
      CalpontSystemCatalog::RIDList oidlist = gwi.csc->columnRIDs(tn, true);

      for (unsigned int j = 0; j < oidlist.size(); j++)
      {
        SimpleColumn* sc = new SimpleColumn();
        CalpontSystemCatalog::TableColName tcn = gwi.csc->colName(oidlist[j].objnum);
        CalpontSystemCatalog::ColType ct = gwi.csc->colType(oidlist[j].objnum);
        sc->columnName(tcn.column);
        sc->tableName(tcn.table, lower_case_table_names);
        sc->schemaName(tcn.schema, lower_case_table_names);
        sc->oid(oidlist[j].objnum);
        sc->alias(tcn.column);
        sc->resultType(ct);
        sc->tableAlias(gwi.tbList[i].alias, lower_case_table_names);
        sc->viewName(viewName, lower_case_table_names);
        sc->timeZone(gwi.timeZone);
        srcp.reset(sc);
        gwi.returnedCols.push_back(srcp);
        gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
      }
    }
  }
}

void buildSubselectFunc(Item_func* ifp, gp_walk_info* gwip)
{
  // @bug 3035
  if (!isPredicateFunction(ifp, gwip))
  {
    gwip->fatalParseError = true;
    gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_FUNC_SUB);
    return;
  }

  WhereSubQuery* subquery = NULL;

  for (uint32_t i = 0; i < ifp->argument_count(); i++)
  {
#if MYSQL_VERSION_ID >= 50172

    // changes comply with mysql 5.1.72
    if (ifp->arguments()[i]->type() == Item::FUNC_ITEM &&
        string(((Item_func*)ifp->arguments()[i])->func_name()) == "<in_optimizer>")
    {
      if (ifp->functype() == Item_func::NOT_FUNC)
      {
        if (gwip->lastSub)
          gwip->lastSub->handleNot();
      }
    }

#endif

    if (ifp->arguments()[i]->type() == Item::SUBSELECT_ITEM)
    {
      Item_subselect* sub = (Item_subselect*)ifp->arguments()[i];

      switch (sub->substype())
      {
        case Item_subselect::SINGLEROW_SUBS: subquery = new ScalarSub(*gwip, ifp); break;

        case Item_subselect::IN_SUBS: subquery = new InSub(*gwip, ifp); break;

        case Item_subselect::EXISTS_SUBS:

          // exists sub has been handled earlier. here is for not function
          if (ifp->functype() == Item_func::NOT_FUNC)
          {
            if (gwip->lastSub)
              gwip->lastSub->handleNot();
          }

          break;

        default:
          Message::Args args;
          gwip->fatalParseError = true;
          gwip->parseErrorText = "non supported subquery";
          return;
      }
    }
  }

  if (subquery)
  {
    gwip->hasSubSelect = true;
    SubQuery* orig = gwip->subQuery;
    gwip->subQuery = subquery;
    // no need to check NULL for now. error will be handled in gp_walk
    gwip->ptWorkStack.push(subquery->transform());
    // recover original sub. Save current sub for Not handling.
    gwip->lastSub = subquery;
    gwip->subQuery = orig;
  }

  return;
}

bool isPredicateFunction(Item* item, gp_walk_info* gwip)
{
  if (item->type() == Item::COND_ITEM)
    return true;

  if (item->type() != Item::FUNC_ITEM)
    return false;

  Item_func* ifp = (Item_func*)item;
  return ( ifp->functype() == Item_func::EQ_FUNC ||
             ifp->functype() == Item_func::NE_FUNC ||
             ifp->functype() == Item_func::LT_FUNC ||
             ifp->functype() == Item_func::LE_FUNC ||
             ifp->functype() == Item_func::GE_FUNC ||
             ifp->functype() == Item_func::GT_FUNC ||
             ifp->functype() == Item_func::LIKE_FUNC ||
             ifp->functype() == Item_func::BETWEEN ||
             ifp->functype() == Item_func::IN_FUNC ||
             (ifp->functype() == Item_func::ISNULL_FUNC &&
              (gwip->clauseType == WHERE || gwip->clauseType == HAVING)) ||
             (ifp->functype() == Item_func::ISNOTNULL_FUNC &&
              (gwip->clauseType == WHERE || gwip->clauseType == HAVING)) ||
             ifp->functype() == Item_func::NOT_FUNC ||
             ifp->functype() == Item_func::ISNOTNULLTEST_FUNC ||
             ifp->functype() == Item_func::TRIG_COND_FUNC ||
             string(ifp->func_name()) == "<in_optimizer>"/* ||
		string(ifp->func_name()) == "xor"*/);
}

void setError(THD* thd, uint32_t errcode, string errmsg)
{
  thd->get_stmt_da()->set_overwrite_status(true);

  if (errmsg.empty())
    errmsg = "Unknown error";

  if (errcode < ER_ERROR_FIRST || errcode > ER_ERROR_LAST)
  {
    errcode = ER_UNKNOWN_ERROR;
  }

  thd->raise_error_printf(errcode, errmsg.c_str());

  // reset expressionID
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  ci->expressionId = 0;
}

void setError(THD* thd, uint32_t errcode, string errmsg, gp_walk_info& gwi)
{
  setError(thd, errcode, errmsg);
  clearStacks(gwi);
}

int setErrorAndReturn(gp_walk_info& gwi)
{
  // if this is dervied table process phase, mysql may have not developed the plan
  // completely. Do not error and eventually mysql will call JOIN::exec() again.
  // related to bug 2922. Need to find a way to skip calling rnd_init for derived table
  // processing.
  if (gwi.thd->derived_tables_processing)
  {
    gwi.cs_vtable_is_update_with_derive = true;
    return -1;
  }

  setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
  return ER_INTERNAL_ERROR;
}

const string bestTableName(const Item_field* ifp)
{
  idbassert(ifp);

  if (!ifp->table_name.str)
    return "";

  if (!ifp->field)
    return ifp->table_name.str;

  string table_name(ifp->table_name.str);
  string field_table_table_name;

  if (ifp->cached_table)
    field_table_table_name = ifp->cached_table->table_name.str;
  else if (ifp->field->table && ifp->field->table->s && ifp->field->table->s->table_name.str)
    field_table_table_name = ifp->field->table->s->table_name.str;

  string tn;

  if (!field_table_table_name.empty())
    tn = field_table_table_name;
  else
    tn = table_name;

  return tn;
}

uint32_t setAggOp(AggregateColumn* ac, Item_sum* isp)
{
  Item_sum::Sumfunctype agg_type = isp->sum_func();
  uint32_t rc = 0;

  switch (agg_type)
  {
    case Item_sum::COUNT_FUNC: ac->aggOp(AggregateColumn::COUNT); return rc;

    case Item_sum::SUM_FUNC: ac->aggOp(AggregateColumn::SUM); return rc;

    case Item_sum::AVG_FUNC: ac->aggOp(AggregateColumn::AVG); return rc;

    case Item_sum::MIN_FUNC: ac->aggOp(AggregateColumn::MIN); return rc;

    case Item_sum::MAX_FUNC: ac->aggOp(AggregateColumn::MAX); return rc;

    case Item_sum::COUNT_DISTINCT_FUNC:
      ac->aggOp(AggregateColumn::DISTINCT_COUNT);
      ac->distinct(true);
      return rc;

    case Item_sum::SUM_DISTINCT_FUNC:
      ac->aggOp(AggregateColumn::DISTINCT_SUM);
      ac->distinct(true);
      return rc;

    case Item_sum::AVG_DISTINCT_FUNC:
      ac->aggOp(AggregateColumn::DISTINCT_AVG);
      ac->distinct(true);
      return rc;

    case Item_sum::STD_FUNC:
    {
      Item_sum_variance* var = (Item_sum_variance*)isp;

      if (var->sample)
        ac->aggOp(AggregateColumn::STDDEV_SAMP);
      else
        ac->aggOp(AggregateColumn::STDDEV_POP);

      return rc;
    }

    case Item_sum::VARIANCE_FUNC:
    {
      Item_sum_variance* var = (Item_sum_variance*)isp;

      if (var->sample)
        ac->aggOp(AggregateColumn::VAR_SAMP);
      else
        ac->aggOp(AggregateColumn::VAR_POP);

      return rc;
    }

    case Item_sum::GROUP_CONCAT_FUNC:
    {
      Item_func_group_concat* gc = (Item_func_group_concat*)isp;
      ac->aggOp(AggregateColumn::GROUP_CONCAT);
      ac->distinct(gc->get_distinct());
      return rc;
    }

    case Item_sum::SUM_BIT_FUNC:
    {
      string funcName = isp->func_name();

      if (funcName.compare("bit_and(") == 0)
        ac->aggOp(AggregateColumn::BIT_AND);
      else if (funcName.compare("bit_or(") == 0)
        ac->aggOp(AggregateColumn::BIT_OR);
      else if (funcName.compare("bit_xor(") == 0)
        ac->aggOp(AggregateColumn::BIT_XOR);
      else
        return ER_CHECK_NOT_IMPLEMENTED;

      return rc;
    }

    case Item_sum::UDF_SUM_FUNC: ac->aggOp(AggregateColumn::UDAF); return rc;

    default: return ER_CHECK_NOT_IMPLEMENTED;
  }
}

/* get the smallest column of a table. Used for filling columnmap */
SimpleColumn* getSmallestColumn(boost::shared_ptr<CalpontSystemCatalog> csc,
                                CalpontSystemCatalog::TableName& tn,
                                CalpontSystemCatalog::TableAliasName& tan, TABLE* table, gp_walk_info& gwi)
{
  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(tn.schema);
    boost::algorithm::to_lower(tn.table);
    boost::algorithm::to_lower(tan.schema);
    boost::algorithm::to_lower(tan.table);
    boost::algorithm::to_lower(tan.alias);
    boost::algorithm::to_lower(tan.view);
  }
  // derived table
  if (tan.schema.empty())
  {
    for (uint32_t i = 0; i < gwi.derivedTbList.size(); i++)
    {
      CalpontSelectExecutionPlan* csep =
          dynamic_cast<CalpontSelectExecutionPlan*>(gwi.derivedTbList[i].get());

      if (tan.alias == csep->derivedTbAlias())
      {
        const CalpontSelectExecutionPlan::ReturnedColumnList& cols = csep->returnedCols();

        CalpontSelectExecutionPlan::ReturnedColumnList::const_iterator iter;

        ReturnedColumn* rc;

        for (iter = cols.begin(); iter != cols.end(); iter++)
        {
          if ((*iter)->refCount() != 0)
          {
            rc = dynamic_cast<ReturnedColumn*>(iter->get());
            break;
          }
        }

        if (iter == cols.end())
        {
          assert(!cols.empty());

          // We take cols[0] here due to the optimization happening in
          // derivedTableOptimization. All cols with refCount 0 from
          // the end of the cols list are optimized out, until the
          // first column with non-zero refCount is encountered. So
          // here, if instead of cols[0], we take cols[1] (based on
          // some logic) and increment it's refCount, then cols[0] is
          // not optimized out in derivedTableOptimization and is
          // added as a ConstantColumn to the derived table's returned
          // column list. This later causes an ineffective row group
          // with row of the form (1, cols[1]_value1) to be created in ExeMgr.
          rc = dynamic_cast<ReturnedColumn*>(cols[0].get());

          // @bug5634 derived table optimization.
          rc->incRefCount();
        }

        SimpleColumn* sc = new SimpleColumn();
        sc->columnName(rc->alias());
        sc->sequence(0);
        sc->tableAlias(tan.alias);
        sc->timeZone(gwi.timeZone);
        sc->derivedTable(csep->derivedTbAlias());
        sc->derivedRefCol(rc);
        return sc;
      }
    }

    throw runtime_error("getSmallestColumn: Internal error.");
  }

  // check engine type
  if (!tan.fisColumnStore)
  {
    // get the first column to project. @todo optimization to get the smallest one for foreign engine.
    Field* field = *(table->field);
    SimpleColumn* sc = new SimpleColumn(table->s->db.str, table->s->table_name.str, field->field_name.str,
                                        tan.fisColumnStore, gwi.sessionid, lower_case_table_names);
    sc->tableAlias(table->alias.ptr(), lower_case_table_names);
    sc->isColumnStore(false);
    sc->timeZone(gwi.timeZone);
    sc->resultType(fieldType_MysqlToIDB(field));
    sc->oid(field->field_index + 1);
    return sc;
  }

  CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(tn, true);
  idbassert(oidlist.size() == table->s->fields);
  CalpontSystemCatalog::TableColName tcn;
  int minColWidth = -1;
  int minWidthColOffset = 0;

  for (unsigned int j = 0; j < oidlist.size(); j++)
  {
    CalpontSystemCatalog::ColType ct = csc->colType(oidlist[j].objnum);

    if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
      continue;

    if (minColWidth == -1 || ct.colWidth < minColWidth)
    {
      minColWidth = ct.colWidth;
      minWidthColOffset = j;
    }
  }

  tcn = csc->colName(oidlist[minWidthColOffset].objnum);
  SimpleColumn* sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, csc->sessionID());
  sc->tableAlias(tan.alias);
  sc->viewName(tan.view);
  sc->timeZone(gwi.timeZone);
  sc->resultType(csc->colType(oidlist[minWidthColOffset].objnum));
  sc->charsetNumber(table->field[minWidthColOffset]->charset()->number);
  return sc;
}

CalpontSystemCatalog::ColType fieldType_MysqlToIDB(const Field* field)
{
  CalpontSystemCatalog::ColType ct;
  ct.precision = 4;

  switch (field->result_type())
  {
    case INT_RESULT:
      ct.colDataType = CalpontSystemCatalog::BIGINT;
      ct.colWidth = 8;
      break;

    case STRING_RESULT:
      ct.colDataType = CalpontSystemCatalog::VARCHAR;
      ct.colWidth = field->field_length;
      break;

    case DECIMAL_RESULT:
    {
      Field_decimal* idp = (Field_decimal*)field;
      ct.colDataType = CalpontSystemCatalog::DECIMAL;
      ct.colWidth = 8;
      ct.scale = idp->dec;

      if (ct.scale == 0)
        ct.precision = idp->field_length - 1;
      else
        ct.precision = idp->field_length - idp->dec;

      break;
    }

    case REAL_RESULT:
      ct.colDataType = CalpontSystemCatalog::DOUBLE;
      ct.colWidth = 8;
      break;

    default:
      IDEBUG(cerr << "fieldType_MysqlToIDB:: Unknown result type of MySQL " << field->result_type() << endl);
      break;
  }

  return ct;
}

CalpontSystemCatalog::ColType colType_MysqlToIDB(const Item* item)
{
  CalpontSystemCatalog::ColType ct;
  ct.precision = 4;

  switch (item->result_type())
  {
    case INT_RESULT:
      if (item->unsigned_flag)
      {
        ct.colDataType = CalpontSystemCatalog::UBIGINT;
      }
      else
      {
        ct.colDataType = CalpontSystemCatalog::BIGINT;
      }

      ct.colWidth = 8;
      break;

    case STRING_RESULT:
      ct.colDataType = CalpontSystemCatalog::VARCHAR;

      // MCOL-4758 the longest TEXT we deal with is 16777215 so
      // limit to that.
      if (item->max_length < 16777215)
        ct.colWidth = item->max_length;
      else
        ct.colWidth = 16777215;

      // force token
      if (item->type() == Item::FUNC_ITEM)
      {
        if (ct.colWidth < 20)
          ct.colWidth = 20;  // for infinidb date length
      }

      // @bug5083. MySQL gives string type for date/datetime column.
      // need to adjust here.
      if (item->type() == Item::FIELD_ITEM)
      {
        if (item->field_type() == MYSQL_TYPE_DATE)
        {
          ct.colDataType = CalpontSystemCatalog::DATE;
          ct.colWidth = 4;
        }
        else if (item->field_type() == MYSQL_TYPE_DATETIME || item->field_type() == MYSQL_TYPE_DATETIME2)
        {
          ct.colDataType = CalpontSystemCatalog::DATETIME;
          ct.colWidth = 8;
        }
        else if (item->field_type() == MYSQL_TYPE_TIMESTAMP || item->field_type() == MYSQL_TYPE_TIMESTAMP2)
        {
          ct.colDataType = CalpontSystemCatalog::TIMESTAMP;
          ct.colWidth = 8;
        }
        else if (item->field_type() == MYSQL_TYPE_TIME)
        {
          ct.colDataType = CalpontSystemCatalog::TIME;
          ct.colWidth = 8;
        }

        if (item->field_type() == MYSQL_TYPE_BLOB)
        {
          ct.colDataType = CalpontSystemCatalog::BLOB;
        }
      }

      break;

    /* FIXME:
                    case xxxBINARY_RESULT:
                            ct.colDataType = CalpontSystemCatalog::VARBINARY;
                            ct.colWidth = item->max_length;
                            // force token
                            if (item->type() == Item::FUNC_ITEM)
                            {
                                    if (ct.colWidth < 20)
                                            ct.colWidth = 20; // for infinidb date length
                            }
                            break;
    */
    case DECIMAL_RESULT:
    {
      Item_decimal* idp = (Item_decimal*)item;

      ct.colDataType = CalpontSystemCatalog::DECIMAL;

      unsigned int precision = idp->decimal_precision();
      unsigned int scale = idp->decimal_scale();

      ct.setDecimalScalePrecision(precision, scale);

      break;
    }

    case REAL_RESULT:
      ct.colDataType = CalpontSystemCatalog::DOUBLE;
      ct.colWidth = 8;
      break;

    default:
      IDEBUG(cerr << "colType_MysqlToIDB:: Unknown result type of MySQL " << item->result_type() << endl);
      break;
  }
  ct.charsetNumber = item->collation.collation->number;
  return ct;
}

ReturnedColumn* buildReturnedColumnNull(gp_walk_info& gwi)
{
  if (gwi.condPush)
    return new SimpleColumn("noop");
  ConstantColumn* rc = new ConstantColumnNull();
  if (rc)
    rc->timeZone(gwi.timeZone);
  return rc;
}

class ValStrStdString : public string
{
  bool mIsNull;

 public:
  ValStrStdString(Item* item)
  {
    String val, *str = item->val_str(&val);
    mIsNull = (str == nullptr);
    DBUG_ASSERT(mIsNull == item->null_value);
    if (!mIsNull)
      assign(str->ptr(), str->length());
  }
  bool isNull() const
  {
    return mIsNull;
  }
};

/*
  Create a ConstantColumn according to cmp_type().
  But do not set the time zone yet.

  Handles NOT NULL values.

  Three ways of value extraction are used depending on the data type:
  1. Using a native val_xxx().
  2. Using val_str() with further convertion to the native representation.
  3. Using both val_str() and a native val_xxx().

  We should eventually get rid of N2 and N3 and use N1 for all data types:
  - N2 contains a redundant code for str->native conversion.
    It should be replaced to an existing code (a Type_handler method call?).
  - N3 performs double evalation of the value, which may cause
    various negative effects (double side effects or double warnings).
*/
static ConstantColumn* newConstantColumnNotNullUsingValNativeNoTz(Item* item, gp_walk_info& gwi)
{
  DBUG_ASSERT(item->const_item());

  switch (item->cmp_type())
  {
    case INT_RESULT:
    {
      if (item->unsigned_flag)
        return new ConstantColumnUInt((uint64_t)item->val_uint(), (int8_t)item->decimal_scale(),
                                      (uint8_t)item->decimal_precision());
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new ConstantColumnSInt(colType_MysqlToIDB(item), str, (int64_t)item->val_int());
    }
    case STRING_RESULT:
    {
      // Special handling for 0xHHHH literals
      if (item->type_handler() == &type_handler_hex_hybrid)
        return new ConstantColumn((int64_t)item->val_int(), ConstantColumn::NUM);
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new ConstantColumnString(str);
    }
    case REAL_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new ConstantColumnReal(colType_MysqlToIDB(item), str, item->val_real());
    }
    case DECIMAL_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return buildDecimalColumn(item, str, gwi);
    }
    case TIME_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new ConstantColumnTemporal(colType_MysqlToIDB(item), str);
    }
    default:
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = "Unknown item type";
      break;
    }
  }

  return nullptr;
}

/*
  Create a ConstantColumn according to cmp_type().
  But do not set the time zone yet.

  Handles NULL and NOT NULL values.

  Uses a simplified logic regarding to data types:
    always extracts the value through val_str().

  Should probably be joined with the previous function, to have
  a single function which can at the same time:
  a. handle both NULL and NOT NULL values
  b. extract values using a native val_xxx() method,
     to avoid possible negative effects mentioned in the comments
     to newConstantColumnNotNullUsingValNativeNoTz().
*/
static ConstantColumn* newConstantColumnMaybeNullFromValStrNoTz(const Item* item,
                                                                const ValStrStdString& valStr,
                                                                gp_walk_info& gwi)
{
  if (valStr.isNull())
    return new ConstantColumnNull();

  switch (item->result_type())
  {
    case STRING_RESULT: return new ConstantColumnString(valStr);
    case DECIMAL_RESULT: return buildDecimalColumn(item, valStr, gwi);
    case TIME_RESULT:
    case INT_RESULT:
    case REAL_RESULT:
    case ROW_RESULT: return new ConstantColumnNum(colType_MysqlToIDB(item), valStr);
  }
  return nullptr;
}

// Create a ConstantColumn from a previously evaluated val_str() result,
// Supports both NULL and NOT NULL values.
// Sets the time zone according to gwi.

static ConstantColumn* buildConstantColumnMaybeNullFromValStr(const Item* item, const ValStrStdString& valStr,
                                                              gp_walk_info& gwi)
{
  ConstantColumn* rc = newConstantColumnMaybeNullFromValStrNoTz(item, valStr, gwi);
  if (rc)
    rc->timeZone(gwi.timeZone);
  return rc;
}

// Create a ConstantColumn by calling val_str().
// Supports both NULL and NOT NULL values.
// Sets the time zone according to gwi.

static ConstantColumn* buildConstantColumnMaybeNullUsingValStr(Item* item, gp_walk_info& gwi)
{
  return buildConstantColumnMaybeNullFromValStr(item, ValStrStdString(item), gwi);
}

// Create a ConstantColumn for a NOT NULL expression.
// Sets the time zone according to gwi.
static ConstantColumn* buildConstantColumnNotNullUsingValNative(Item* item, gp_walk_info& gwi)
{
  ConstantColumn* rc = newConstantColumnNotNullUsingValNativeNoTz(item, gwi);
  if (rc)
    rc->timeZone(gwi.timeZone);
  return rc;
}

ReturnedColumn* buildReturnedColumn(Item* item, gp_walk_info& gwi, bool& nonSupport, bool isRefItem)
{
  ReturnedColumn* rc = NULL;

  if (gwi.thd)
  {
    // if ( ((gwi.thd->lex)->sql_command == SQLCOM_UPDATE ) || ((gwi.thd->lex)->sql_command ==
    // SQLCOM_UPDATE_MULTI ))
    {
      if (!item->fixed())
      {
        item->fix_fields(gwi.thd, (Item**)&item);
      }
    }
  }

  switch (item->type())
  {
    case Item::FIELD_ITEM:
    {
      Item_field* ifp = (Item_field*)item;

      if (isRefItem && gwi.isGroupByHandler && !gwi.extSelAggColsItems.empty())
      {
        return buildAggFrmTempField(ifp, gwi);
      }

      return buildSimpleColumn(ifp, gwi);
    }
    case Item::NULL_ITEM: return buildReturnedColumnNull(gwi);
    case Item::CONST_ITEM:
    {
      rc = buildConstantColumnNotNullUsingValNative(item, gwi);
      break;
    }
    case Item::FUNC_ITEM:
    {
      if (item->const_item())
      {
        rc = buildConstantColumnMaybeNullUsingValStr(item, gwi);
        break;
      }
      Item_func* ifp = (Item_func*)item;
      string func_name = ifp->func_name();

      // try to evaluate const F&E. only for select clause
      vector<Item_field*> tmpVec;
      // bool hasAggColumn = false;
      uint16_t parseInfo = 0;
      parse_item(ifp, tmpVec, gwi.fatalParseError, parseInfo, &gwi);

      if (parseInfo & SUB_BIT)
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        break;
      }

      if (!gwi.fatalParseError && !nonConstFunc(ifp) && !(parseInfo & AF_BIT) && (tmpVec.size() == 0))
      {
        rc = buildConstantColumnMaybeNullUsingValStr(item, gwi);
        break;
      }

      if (func_name == "+" || func_name == "-" || func_name == "*" || func_name == "/")
        return buildArithmeticColumn(ifp, gwi, nonSupport);
      else
        return buildFunctionColumn(ifp, gwi, nonSupport);
    }

    case Item::SUM_FUNC_ITEM:
    {
      return buildAggregateColumn(item, gwi);
    }

    case Item::REF_ITEM:
    {
      Item_ref* ref = (Item_ref*)item;

      switch ((*(ref->ref))->type())
      {
        case Item::SUM_FUNC_ITEM: return buildAggregateColumn(*(ref->ref), gwi);

        case Item::FIELD_ITEM: return buildReturnedColumn(*(ref->ref), gwi, nonSupport);

        case Item::REF_ITEM: return buildReturnedColumn(*(((Item_ref*)(*(ref->ref)))->ref), gwi, nonSupport);

        case Item::FUNC_ITEM: return buildFunctionColumn((Item_func*)(*(ref->ref)), gwi, nonSupport);

        case Item::WINDOW_FUNC_ITEM: return buildWindowFunctionColumn(*(ref->ref), gwi, nonSupport);

        case Item::SUBSELECT_ITEM:
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
          break;

        default:
          gwi.fatalParseError = true;
          gwi.parseErrorText = "Unknown REF item";
          break;
      }
      return buildReturnedColumnNull(gwi);
    }
    case Item::CACHE_ITEM:
    {
      Item* col = ((Item_cache*)item)->get_example();
      rc = buildReturnedColumn(col, gwi, nonSupport);

      if (rc)
      {
        ConstantColumn* cc = dynamic_cast<ConstantColumn*>(rc);

        if (!cc)
        {
          rc->joinInfo(rc->joinInfo() | JOIN_CORRELATED);

          if (gwi.subQuery)
            gwi.subQuery->correlated(true);
        }
      }

      break;
    }

    case Item::EXPR_CACHE_ITEM:
    {
      // TODO: item is a Item_cache_wrapper
      printf("EXPR_CACHE_ITEM in buildReturnedColumn\n");
      cerr << "EXPR_CACHE_ITEM in buildReturnedColumn" << endl;
      break;
    }

    case Item::WINDOW_FUNC_ITEM:
    {
      return buildWindowFunctionColumn(item, gwi, nonSupport);
    }

#if INTERVAL_ITEM

    case Item::INTERVAL_ITEM:
    {
      Item_interval* interval = (Item_interval*)item;
      SRCP srcp;
      srcp.reset(buildReturnedColumn(interval->item, gwi, nonSupport));

      if (!srcp)
        return NULL;

      rc = new IntervalColumn(srcp, (int)interval->interval);
      rc->resultType(srcp->resultType());
      break;
    }

#endif

    case Item::SUBSELECT_ITEM:
    {
      gwi.hasSubSelect = true;
      break;
    }

    case Item::COND_ITEM:
    {
      // MCOL-1196: Allow COND_ITEM thru. They will be picked up
      // by further logic. It may become desirable to add code here.
      break;
    }

    default:
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = "Unknown item type";
      break;
    }
  }

  if (rc && item->name.length)
    rc->alias(item->name.str);

  if (rc)
    rc->charsetNumber(item->collation.collation->number);

  return rc;
}

ArithmeticColumn* buildArithmeticColumn(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  ArithmeticColumn* ac = new ArithmeticColumn();
  Item** sfitempp = item->arguments();
  ArithmeticOperator* aop = new ArithmeticOperator(item->func_name());
  aop->timeZone(gwi.timeZone);
  aop->setOverflowCheck(get_decimal_overflow_check(gwi.thd));
  ParseTree* pt = new ParseTree(aop);
  // ReturnedColumn *lhs = 0, *rhs = 0;
  ParseTree *lhs = 0, *rhs = 0;
  SRCP srcp;

  if (item->name.length)
    ac->alias(item->name.str);

  // argument_count() should generally be 2, except negate expression
  if (item->argument_count() == 2)
  {
    if (gwi.clauseType == SELECT || /*gwi.clauseType == HAVING || */ gwi.clauseType == GROUP_BY ||
        gwi.clauseType == FROM)  // select list
    {
      lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));

      if (!lhs->data() && (sfitempp[0]->type() == Item::FUNC_ITEM))
      {
        delete lhs;
        Item_func* ifp = (Item_func*)sfitempp[0];
        lhs = buildParseTree(ifp, gwi, nonSupport);
      }
      else if (!lhs->data() && (sfitempp[0]->type() == Item::REF_ITEM))
      {
        // There must be an aggregation column in extended SELECT
        // list so find the corresponding column.
        // Could have it set if there are aggregation funcs as this function arguments.
        gwi.fatalParseError = false;

        ReturnedColumn* rc = buildAggFrmTempField(sfitempp[0], gwi);
        if (rc)
          lhs = new ParseTree(rc);
      }
      rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));

      if (!rhs->data() && (sfitempp[1]->type() == Item::FUNC_ITEM))
      {
        delete rhs;
        Item_func* ifp = (Item_func*)sfitempp[1];
        rhs = buildParseTree(ifp, gwi, nonSupport);
      }
      else if (!rhs->data() && (sfitempp[1]->type() == Item::REF_ITEM))
      {
        // There must be an aggregation column in extended SELECT
        // list so find the corresponding column.
        // Could have it set if there are aggregation funcs as this function arguments.
        gwi.fatalParseError = false;

        ReturnedColumn* rc = buildAggFrmTempField(sfitempp[1], gwi);
        if (rc)
          rhs = new ParseTree(rc);
      }
    }
    else  // where clause
    {
      if (isPredicateFunction(sfitempp[1], &gwi))
      {
        if (gwi.ptWorkStack.empty())
        {
          rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));
        }
        else
        {
          rhs = gwi.ptWorkStack.top();
          gwi.ptWorkStack.pop();
        }
      }
      else
      {
        if (gwi.rcWorkStack.empty())
        {
          rhs = new ParseTree(buildReturnedColumn(sfitempp[1], gwi, nonSupport));
        }
        else
        {
          rhs = new ParseTree(gwi.rcWorkStack.top());
          gwi.rcWorkStack.pop();
        }
      }

      if (isPredicateFunction(sfitempp[0], &gwi))
      {
        if (gwi.ptWorkStack.empty())
        {
          lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
        }
        else
        {
          lhs = gwi.ptWorkStack.top();
          gwi.ptWorkStack.pop();
        }
      }
      else
      {
        if (gwi.rcWorkStack.empty())
        {
          lhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
        }
        else
        {
          lhs = new ParseTree(gwi.rcWorkStack.top());
          gwi.rcWorkStack.pop();
        }
      }
    }

    if (nonSupport || !lhs->data() || !rhs->data())
    {
      gwi.fatalParseError = true;

      if (gwi.parseErrorText.empty())
        gwi.parseErrorText = "Un-recognized Arithmetic Operand";

      delete lhs;
      delete rhs;
      return NULL;
    }

    // aop->operationType(lhs->resultType(), rhs->resultType());
    pt->left(lhs);
    pt->right(rhs);
  }
  else
  {
    ConstantColumn* cc = new ConstantColumn(string("0"), (int64_t)0);
    cc->timeZone(gwi.timeZone);

    if (gwi.clauseType == SELECT || gwi.clauseType == HAVING || gwi.clauseType == GROUP_BY)  // select clause
    {
      rhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
    }
    else
    {
      if (gwi.rcWorkStack.empty())
      {
        rhs = new ParseTree(buildReturnedColumn(sfitempp[0], gwi, nonSupport));
      }
      else
      {
        rhs = new ParseTree(gwi.rcWorkStack.top());
        gwi.rcWorkStack.pop();
      }
    }

    if (nonSupport || !rhs->data())
    {
      gwi.fatalParseError = true;

      if (gwi.parseErrorText.empty())
        gwi.parseErrorText = "Un-recognized Arithmetic Operand";

      delete rhs;
      return NULL;
    }

    pt->left(cc);
    pt->right(rhs);
  }

  // @bug5715. Use InfiniDB adjusted coltype for result type.
  // decimal arithmetic operation gives double result when the session variable is set.
  CalpontSystemCatalog::ColType mysqlType = colType_MysqlToIDB(item);

  const CalpontSystemCatalog::ColType& leftColType = pt->left()->data()->resultType();
  const CalpontSystemCatalog::ColType& rightColType = pt->right()->data()->resultType();

  if (datatypes::isDecimal(leftColType.colDataType) || datatypes::isDecimal(rightColType.colDataType))
  {
    int32_t leftColWidth = leftColType.colWidth;
    int32_t rightColWidth = rightColType.colWidth;

    if (leftColWidth == datatypes::MAXDECIMALWIDTH || rightColWidth == datatypes::MAXDECIMALWIDTH)
    {
      mysqlType.colWidth = datatypes::MAXDECIMALWIDTH;

      string funcName = item->func_name();

      int32_t scale1 = leftColType.scale;
      int32_t scale2 = rightColType.scale;

      mysqlType.precision = datatypes::INT128MAXPRECISION;

      if (funcName == "/" && (mysqlType.scale - (scale1 - scale2)) > datatypes::INT128MAXPRECISION)
      {
        Item_decimal* idp = (Item_decimal*)item;

        unsigned int precision = idp->decimal_precision();
        unsigned int scale = idp->decimal_scale();

        mysqlType.setDecimalScalePrecisionHeuristic(precision, scale);

        if (mysqlType.scale < scale1)
          mysqlType.scale = scale1;

        if (mysqlType.precision < mysqlType.scale)
          mysqlType.precision = mysqlType.scale;
      }
    }
  }

  if (get_double_for_decimal_math(current_thd) == true)
    aop->adjustResultType(mysqlType);
  else
    aop->resultType(mysqlType);

  // adjust decimal result type according to internalDecimalScale
  if (gwi.internalDecimalScale >= 0 && aop->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
  {
    CalpontSystemCatalog::ColType ct = aop->resultType();
    ct.scale = gwi.internalDecimalScale;
    aop->resultType(ct);
  }

  aop->operationType(aop->resultType());
  ac->expression(pt);
  ac->resultType(aop->resultType());
  ac->operationType(aop->operationType());
  ac->expressionId(ci->expressionId++);

  // @3391. optimization. try to associate expression ID to the expression on the select list
  if (gwi.clauseType != SELECT)
  {
    for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
    {
      if ((!ac->alias().empty()) &&
          strcasecmp(ac->alias().c_str(), gwi.returnedCols[i]->alias().c_str()) == 0)
      {
        ac->expressionId(gwi.returnedCols[i]->expressionId());
        break;
      }
    }
  }

  // For function join. If any argument has non-zero joininfo, set it to the function.
  ac->setSimpleColumnList();
  std::vector<SimpleColumn*> simpleColList = ac->simpleColumnList();

  for (uint i = 0; i < simpleColList.size(); i++)
  {
    if (simpleColList[i]->joinInfo() != 0)
    {
      ac->joinInfo(simpleColList[i]->joinInfo());
      break;
    }
  }

  return ac;
}

ReturnedColumn* buildFunctionColumn(Item_func* ifp, gp_walk_info& gwi, bool& nonSupport, bool selectBetweenIn)
{
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  string funcName = ifp->func_name();
  FuncExp* funcExp = FuncExp::instance();
  Func* functor;
  FunctionColumn* fc = NULL;

  // Pseudocolumn
  uint32_t pseudoType = PSEUDO_UNKNOWN;

  if (ifp->functype() == Item_func::UDF_FUNC)
    pseudoType = PseudoColumn::pseudoNameToType(funcName);

  if (pseudoType != PSEUDO_UNKNOWN)
  {
    ReturnedColumn* rc = buildPseudoColumn(ifp, gwi, gwi.fatalParseError, pseudoType);

    if (!rc || gwi.fatalParseError)
    {
      if (gwi.parseErrorText.empty())
        gwi.parseErrorText = "Unsupported function.";

      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
      return NULL;
    }

    return rc;
  }

  // Item_equal is supported by buildPredicateItem()
  if (funcName == "multiple equal")
    return NULL;

  // Arithmetic exp
  if (funcName == "+" || funcName == "-" || funcName == "*" || funcName == "/")
  {
    ArithmeticColumn* ac = buildArithmeticColumn(ifp, gwi, nonSupport);
    return ac;
  }

  else if (funcName == "case")
  {
    fc = buildCaseFunction(ifp, gwi, nonSupport);
  }

  else if ((funcName == "charset" || funcName == "collation") && ifp->argument_count() == 1 &&
           ifp->arguments()[0]->type() == Item::FIELD_ITEM)
  {
    Item_field* item = reinterpret_cast<Item_field*>(ifp->arguments()[0]);
    CHARSET_INFO* info = item->charset_for_protocol();
    ReturnedColumn* rc;
    string val;

    if (funcName == "charset")
    {
      val = info->cs_name.str;
    }
    else  // collation
    {
      val = info->coll_name.str;
    }

    rc = new ConstantColumn(val, ConstantColumn::LITERAL);

    return rc;
  }

  else if ((functor = funcExp->getFunctor(funcName)))
  {
    // where clause isnull still treated as predicate operator
    // MCOL-686: between also a predicate operator so that extent elimination can happen
    if ((funcName == "isnull" || funcName == "isnotnull" || funcName == "between") &&
        (gwi.clauseType == WHERE || gwi.clauseType == HAVING))
      return NULL;

    if (funcName == "in" || funcName == " IN " || funcName == "between")
    {
      // if F&E involved, build function. otherwise, fall to the old path.
      // @todo need more checks here
      if (ifp->arguments()[0]->type() == Item::ROW_ITEM)
      {
        return NULL;
      }

      if (!selectBetweenIn && (ifp->arguments()[0]->type() == Item::FIELD_ITEM ||
                               (ifp->arguments()[0]->type() == Item::REF_ITEM &&
                                (*(((Item_ref*)ifp->arguments()[0])->ref))->type() == Item::FIELD_ITEM)))
      {
        bool fe = false;

        for (uint32_t i = 1; i < ifp->argument_count(); i++)
        {
          if (!(ifp->arguments()[i]->type() == Item::NULL_ITEM ||
                (ifp->arguments()[i]->type() == Item::CONST_ITEM &&
                 (ifp->arguments()[i]->cmp_type() == INT_RESULT ||
                  ifp->arguments()[i]->cmp_type() == STRING_RESULT ||
                  ifp->arguments()[i]->cmp_type() == REAL_RESULT ||
                  ifp->arguments()[i]->cmp_type() == DECIMAL_RESULT))))
          {
            if (ifp->arguments()[i]->type() == Item::FUNC_ITEM)
            {
              // try to identify const F&E. fall to primitive if parms are constant F&E.
              vector<Item_field*> tmpVec;
              uint16_t parseInfo = 0;
              parse_item(ifp->arguments()[i], tmpVec, gwi.fatalParseError, parseInfo, &gwi);

              if (!gwi.fatalParseError && !(parseInfo & AF_BIT) && tmpVec.size() == 0)
                continue;
            }

            fe = true;
            break;
          }
        }

        if (!fe)
          return NULL;
      }

      Item_func_opt_neg* inp = (Item_func_opt_neg*)ifp;

      if (inp->negated)
        funcName = "not" + funcName;
    }

    // @todo non-support function as argument. need to do post process. Assume all support for now
    fc = new FunctionColumn();
    fc->data(funcName);
    FunctionParm funcParms;
    SPTP sptp;
    ClauseType clauseType = gwi.clauseType;

    if (gwi.clauseType == SELECT ||
        /*gwi.clauseType == HAVING || */ gwi.clauseType == GROUP_BY)  // select clause
    {
      for (uint32_t i = 0; i < ifp->argument_count(); i++)
      {
        // group by clause try to see if the arguments are alias
        if (gwi.clauseType == GROUP_BY && ifp->arguments()[i]->name.length)
        {
          uint32_t j = 0;

          for (; j < gwi.returnedCols.size(); j++)
          {
            if (string(ifp->arguments()[i]->name.str) == gwi.returnedCols[j]->alias())
            {
              ReturnedColumn* rc = gwi.returnedCols[j]->clone();
              rc->orderPos(j);
              sptp.reset(new ParseTree(rc));
              funcParms.push_back(sptp);
              break;
            }
          }

          if (j != gwi.returnedCols.size())
            continue;
        }

        // special handling for function that takes a filter arguments, like if().
        // @todo. merge this logic to buildParseTree().
        if ((funcName == "if" && i == 0) || funcName == "xor")
        {
          // make sure the rcWorkStack is cleaned.
          gwi.clauseType = WHERE;
          sptp.reset(buildParseTree((Item_func*)(ifp->arguments()[i]), gwi, nonSupport));
          gwi.clauseType = clauseType;

          if (!sptp)
          {
            nonSupport = true;
            return NULL;
          }

          funcParms.push_back(sptp);
          continue;
        }

        // @bug 3039
        // if (isPredicateFunction(ifp->arguments()[i], &gwi) || ifp->arguments()[i]->with_subquery())
        if (ifp->arguments()[i]->with_subquery())
        {
          nonSupport = true;
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SUB_EXPRESSION);
          return NULL;
        }

        ReturnedColumn* rc = buildReturnedColumn(ifp->arguments()[i], gwi, nonSupport);

        // MCOL-1510 It must be a temp table field, so find the corresponding column.
        if (!rc && ifp->arguments()[i]->type() == Item::REF_ITEM)
        {
          gwi.fatalParseError = false;
          rc = buildAggFrmTempField(ifp->arguments()[i], gwi);
        }

        if (!rc || nonSupport)
        {
          nonSupport = true;
          return NULL;
        }

        sptp.reset(new ParseTree(rc));
        funcParms.push_back(sptp);
      }
    }
    else  // where clause
    {
      stack<SPTP> tmpPtStack;

      for (int32_t i = ifp->argument_count() - 1; i >= 0; i--)
      {
        if (isPredicateFunction((ifp->arguments()[i]), &gwi) && !gwi.ptWorkStack.empty())
        {
          sptp.reset(gwi.ptWorkStack.top());
          tmpPtStack.push(sptp);
          gwi.ptWorkStack.pop();
        }
        else if (!isPredicateFunction((ifp->arguments()[i]), &gwi) && !gwi.rcWorkStack.empty())
        {
          sptp.reset(new ParseTree(gwi.rcWorkStack.top()));
          tmpPtStack.push(sptp);
          gwi.rcWorkStack.pop();
        }
        else
        {
          nonSupport = true;
          return NULL;
        }
      }

      while (!tmpPtStack.empty())
      {
        funcParms.push_back(tmpPtStack.top());
        tmpPtStack.pop();
      }
    }

    // the followings are special treatment of some functions
    if (funcName == "week" && funcParms.size() == 1)
    {
      THD* thd = current_thd;
      sptp.reset(
          new ParseTree(new ConstantColumn(static_cast<uint64_t>(thd->variables.default_week_format))));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
    }

    // add the keyword unit argument for interval function
    if (funcName == "date_add_interval" || funcName == "extract" || funcName == "timestampdiff")
    {
      addIntervalArgs(&gwi, ifp, funcParms);
    }

    // check for unsupported arguments add the keyword unit argument for extract functions
    if (funcName == "extract")
    {
      Item_date_add_interval* idai = (Item_date_add_interval*)ifp;

      switch (idai->int_type)
      {
        case INTERVAL_DAY_MICROSECOND:
        {
          nonSupport = true;
          gwi.fatalParseError = true;
          Message::Args args;
          string info = funcName + " with DAY_MICROSECOND parameter";
          args.add(info);
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
          return NULL;
        }

        case INTERVAL_HOUR_MICROSECOND:
        {
          nonSupport = true;
          gwi.fatalParseError = true;
          Message::Args args;
          string info = funcName + " with HOUR_MICROSECOND parameter";
          args.add(info);
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
          return NULL;
        }

        case INTERVAL_MINUTE_MICROSECOND:
        {
          nonSupport = true;
          gwi.fatalParseError = true;
          Message::Args args;
          string info = funcName + " with MINUTE_MICROSECOND parameter";
          args.add(info);
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
          return NULL;
        }

        default: break;
      }
    }

    // add the keyword unit argument and char length for cast functions
    if (funcName == "cast_as_char")
    {
      castCharArgs(&gwi, ifp, funcParms);
    }

    // add the length and scale arguments
    if (funcName == "decimal_typecast")
    {
      castDecimalArgs(&gwi, ifp, funcParms);
    }

    // add the type argument
    if (funcName == "get_format")
    {
      castTypeArgs(&gwi, ifp, funcParms);
    }

    // add my_time_zone
    if (funcName == "unix_timestamp")
    {
#ifndef _MSC_VER
      time_t tmp_t = 1;
      struct tm tmp;
      localtime_r(&tmp_t, &tmp);
      sptp.reset(new ParseTree(new ConstantColumn(static_cast<int64_t>(tmp.tm_gmtoff), ConstantColumn::NUM)));
#else
      // FIXME: Get GMT offset (in seconds east of GMT) in Windows...
      sptp.reset(new ParseTree(new ConstantColumn(static_cast<int64_t>(0), ConstantColumn::NUM)));
#endif
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
    }

    // add the default seed to rand function without arguments
    if (funcName == "rand")
    {
      if (funcParms.size() == 0)
      {
        sptp.reset(new ParseTree(new ConstantColumn((int64_t)gwi.thd->rand.seed1, ConstantColumn::NUM)));
        (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
        funcParms.push_back(sptp);
        sptp.reset(new ParseTree(new ConstantColumn((int64_t)gwi.thd->rand.seed2, ConstantColumn::NUM)));
        (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
        funcParms.push_back(sptp);
        gwi.no_parm_func_list.push_back(fc);
      }
      else
      {
        ConstantColumn* cc = dynamic_cast<ConstantColumn*>(funcParms[0]->data());

        if (cc)
          gwi.no_parm_func_list.push_back(fc);
      }
    }

    // Take the value of to/from TZ data and check if its a description or offset via
    // my_tzinfo_find. Offset value will leave the corresponding tzinfo funcParms empty.
    // while descriptions will lookup the time_zone_info structure and serialize for use
    // in primproc func_convert_tz
    if (funcName == "convert_tz")
    {
      messageqcpp::ByteStream bs;
      string tzinfo;
      SimpleColumn* scCheck;
      // MCOL-XXXX There is no way currently to perform this lookup when the timezone description
      // comes from another table of timezone descriptions.
      // 1. Move proc code into plugin where it will have access to this table data
      // 2. Create a library that primproc can use to access the time zone data tables.
      // for now throw a message that this is not supported
      scCheck = dynamic_cast<SimpleColumn*>(funcParms[1]->data());
      if (scCheck)
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = "convert_tz with parameter column_name in a Columnstore table";
        setError(gwi.thd, ER_NOT_SUPPORTED_YET, gwi.parseErrorText, gwi);
        return NULL;
      }
      scCheck = dynamic_cast<SimpleColumn*>(funcParms[2]->data());
      if (scCheck)
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = "convert_tz with parameter column_name in a Columnstore table";
        setError(gwi.thd, ER_NOT_SUPPORTED_YET, gwi.parseErrorText, gwi);
        return NULL;
      }
      dataconvert::TIME_ZONE_INFO* from_tzinfo = my_tzinfo_find(gwi.thd, ifp->arguments()[1]->val_str());
      dataconvert::TIME_ZONE_INFO* to_tzinfo = my_tzinfo_find(gwi.thd, ifp->arguments()[2]->val_str());
      if (from_tzinfo)
      {
        serializeTimezoneInfo(bs, from_tzinfo);
        uint32_t length = bs.length();
        uint8_t* buf = new uint8_t[length];
        bs >> buf;
        tzinfo = string((char*)buf, length);
      }
      sptp.reset(new ParseTree(new ConstantColumn(tzinfo)));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
      tzinfo.clear();
      if (to_tzinfo)
      {
        serializeTimezoneInfo(bs, to_tzinfo);
        uint32_t length = bs.length();
        uint8_t* buf = new uint8_t[length];
        bs >> buf;
        tzinfo = string((char*)buf, length);
      }
      sptp.reset(new ParseTree(new ConstantColumn(tzinfo)));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
      tzinfo.clear();
    }
    if (funcName == "sysdate")
    {
      gwi.no_parm_func_list.push_back(fc);
    }

    // func name is addtime/subtime in 10.3.9
    // note: this means get_time() can now go away in our server fork
    if ((funcName == "addtime") || (funcName == "subtime"))
    {
      int64_t sign = 1;
      if (funcName == "subtime")
      {
        sign = -1;
      }
      sptp.reset(new ParseTree(new ConstantColumn(sign)));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
    }

    fc->functionName(funcName);
    fc->functionParms(funcParms);
    fc->resultType(colType_MysqlToIDB(ifp));

    // if the result type is DECIMAL and any function parameter is a wide decimal
    // column, set the result colwidth to wide
    if (fc->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
    {
      for (size_t i = 0; i < funcParms.size(); i++)
      {
        if (funcParms[i]->data()->resultType().isWideDecimalType())
        {
          fc->resultType().colWidth = datatypes::MAXDECIMALWIDTH;
          break;
        }
      }
    }

    // MySQL give string result type for date function, but has the flag set.
    // we should set the result type to be datetime for comparision.
    if (ifp->field_type() == MYSQL_TYPE_DATETIME || ifp->field_type() == MYSQL_TYPE_DATETIME2)
    {
      CalpontSystemCatalog::ColType ct;
      ct.colDataType = CalpontSystemCatalog::DATETIME;
      ct.colWidth = 8;
      fc->resultType(ct);
    }
    if (ifp->field_type() == MYSQL_TYPE_TIMESTAMP || ifp->field_type() == MYSQL_TYPE_TIMESTAMP2)
    {
      CalpontSystemCatalog::ColType ct;
      ct.colDataType = CalpontSystemCatalog::TIMESTAMP;
      ct.colWidth = 8;
      fc->resultType(ct);
    }
    else if (ifp->field_type() == MYSQL_TYPE_DATE)
    {
      CalpontSystemCatalog::ColType ct;
      ct.colDataType = CalpontSystemCatalog::DATE;
      ct.colWidth = 4;
      fc->resultType(ct);
    }
    else if (ifp->field_type() == MYSQL_TYPE_TIME)
    {
      CalpontSystemCatalog::ColType ct;
      ct.colDataType = CalpontSystemCatalog::TIME;
      ct.colWidth = 8;
      fc->resultType(ct);
    }

#if 0

        if (is_temporal_type_with_date(ifp->field_type()))
        {
            CalpontSystemCatalog::ColType ct;
            ct.colDataType = CalpontSystemCatalog::DATETIME;
            ct.colWidth = 8;
            fc->resultType(ct);
        }

        if (funcName == "cast_as_date")
        {
            CalpontSystemCatalog::ColType ct;
            ct.colDataType = CalpontSystemCatalog::DATE;
            ct.colWidth = 4;
            fc->resultType(ct);
        }

#endif

    execplan::CalpontSystemCatalog::ColType& resultType = fc->resultType();
    resultType.setTimeZone(gwi.timeZone);
    fc->operationType(functor->operationType(funcParms, resultType));

    // For floor/ceiling/truncate/round functions applied on TIMESTAMP columns, set the
    // function result type to TIMESTAMP
    if ((funcName == "floor" || funcName == "ceiling" || funcName == "truncate" || funcName == "round") &&
        fc->operationType().colDataType == CalpontSystemCatalog::TIMESTAMP)
    {
      CalpontSystemCatalog::ColType ct = fc->resultType();
      ct.colDataType = CalpontSystemCatalog::TIMESTAMP;
      ct.colWidth = 8;
      fc->resultType(ct);
    }

    fc->expressionId(ci->expressionId++);

    // A few functions use a different collation than that found in
    // the base ifp class
    if (funcName == "locate" || funcName == "find_in_set" || funcName == "strcmp")
    {
      DTCollation dt;
      ifp->Type_std_attributes::agg_arg_charsets_for_comparison(dt, ifp->func_name_cstring(),
                                                                ifp->arguments(), 1, 1);
      fc->charsetNumber(dt.collation->number);
    }
    else
    {
      fc->charsetNumber(ifp->collation.collation->number);
    }
  }
  else if (ifp->type() == Item::COND_ITEM || ifp->functype() == Item_func::EQ_FUNC ||
           ifp->functype() == Item_func::NE_FUNC || ifp->functype() == Item_func::LT_FUNC ||
           ifp->functype() == Item_func::LE_FUNC || ifp->functype() == Item_func::GE_FUNC ||
           ifp->functype() == Item_func::GT_FUNC || ifp->functype() == Item_func::LIKE_FUNC ||
           ifp->functype() == Item_func::BETWEEN || ifp->functype() == Item_func::IN_FUNC ||
           ifp->functype() == Item_func::ISNULL_FUNC || ifp->functype() == Item_func::ISNOTNULL_FUNC ||
           ifp->functype() == Item_func::NOT_FUNC || ifp->functype() == Item_func::EQUAL_FUNC)
  {
    return NULL;
  }
  else
  {
    nonSupport = true;
    gwi.fatalParseError = true;
    Message::Args args;
    args.add(funcName);
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
    return NULL;
  }

  // adjust decimal result type according to internalDecimalScale
  if (!fc)
    return NULL;

  if (gwi.internalDecimalScale >= 0 && fc->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
  {
    CalpontSystemCatalog::ColType ct = fc->resultType();
    ct.scale = gwi.internalDecimalScale;
    fc->resultType(ct);
  }

  if (ifp->name.length)
    fc->alias(ifp->name.str);

  // @3391. optimization. try to associate expression ID to the expression on the select list
  if (gwi.clauseType != SELECT)
  {
    for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
    {
      if ((!fc->alias().empty()) &&
          strcasecmp(fc->alias().c_str(), gwi.returnedCols[i]->alias().c_str()) == 0)
        fc->expressionId(gwi.returnedCols[i]->expressionId());
    }
  }

  // For function join. If any argument has non-zero joininfo, set it to the function.
  fc->setSimpleColumnList();
  std::vector<SimpleColumn*> simpleColList = fc->simpleColumnList();

  for (uint i = 0; i < simpleColList.size(); i++)
  {
    if (simpleColList[i]->joinInfo() != 0)
    {
      fc->joinInfo(simpleColList[i]->joinInfo());
      break;
    }
  }

  fc->timeZone(gwi.timeZone);

  return fc;
}

FunctionColumn* buildCaseFunction(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  FunctionColumn* fc = new FunctionColumn();
  FunctionParm funcParms;
  SPTP sptp;
  stack<SPTP> tmpPtStack;
  FuncExp* funcexp = FuncExp::instance();
  string funcName = "case_simple";

  if (item->functype() == Item_func::CASE_SEARCHED_FUNC)
  {
    funcName = "case_searched";
  }
  /*    if (dynamic_cast<Item_func_case_searched*>(item))
      {
          funcName = "case_searched";
      }*/

  funcParms.reserve(item->argument_count());
  // so buildXXXcolumn function will not pop stack.
  ClauseType realClauseType = gwi.clauseType;
  gwi.clauseType = SELECT;

  // We ought to be able to just build from the stack, and would
  // be able to if there were any way to know which stack had the
  // next case item. Unfortunately, parameters may have been pushed
  // onto the ptWorkStack or rcWorkStack or neither, depending on type
  // and position. We can't tell which at this point, so we
  // rebuild the item from the arguments directly and then try to
  // figure what to pop, if anything, in order to sync the stacks.
  //
  // MCOL-1341 - With MariaDB 10.2.14 onwards CASE is now in the order:
  // [case,]when1,when2,...,then1,then2,...[,else]
  // See server commit bf1ca14ff3f3faa9f7a018097b25aa0f66d068cd for more
  // information.
  int32_t arg_offset = 0;

  if ((item->argument_count() - 1) % 2)
  {
    arg_offset = (item->argument_count() - 1) / 2;
  }
  else
  {
    arg_offset = item->argument_count() / 2;
  }

  for (int32_t i = item->argument_count() - 1; i >= 0; i--)
  {
    // For case_searched, we know the items for the WHEN clause will
    // not be ReturnedColumns. We do this separately just to save
    // some cpu cycles trying to build a ReturnedColumn as below.
    // Every even numbered arg is a WHEN. In between are the THEN.
    // An odd number of args indicates an ELSE residing in the last spot.
    if ((item->functype() == Item_func::CASE_SEARCHED_FUNC) && (i < arg_offset))
    {
      // MCOL-1472 Nested CASE with an ISNULL predicate. We don't want the predicate
      // to pull off of rcWorkStack, so we set this inCaseStmt flag to tell it
      // not to.
      gwi.inCaseStmt = true;
      sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));
      gwi.inCaseStmt = false;
      if (!gwi.ptWorkStack.empty() && *gwi.ptWorkStack.top() == *sptp.get())
      {
        gwi.ptWorkStack.pop();
      }
    }
    else
    {
      // First try building a ReturnedColumn. It may or may not succeed
      // depending on the types involved. There's also little correlation
      // between buildReturnedColumn and the existance of the item on
      // rwWorkStack or ptWorkStack.
      // For example, simple predicates, such as 1=1 or 1=0, land in the
      // ptWorkStack but other stuff might land in the rwWorkStack
      ReturnedColumn* parm = buildReturnedColumn(item->arguments()[i], gwi, nonSupport);

      if (parm)
      {
        sptp.reset(new ParseTree(parm));

        // We need to pop whichever stack is holding it, if any.
        if ((!gwi.rcWorkStack.empty()) && *gwi.rcWorkStack.top() == parm)
        {
          gwi.rcWorkStack.pop();
        }
        else if (!gwi.ptWorkStack.empty())
        {
          ReturnedColumn* ptrc = dynamic_cast<ReturnedColumn*>(gwi.ptWorkStack.top()->data());

          if (ptrc && *ptrc == *parm)
            gwi.ptWorkStack.pop();
        }
      }
      else
      {
        sptp.reset(buildParseTree((Item_func*)(item->arguments()[i]), gwi, nonSupport));

        // We need to pop whichever stack is holding it, if any.
        if ((!gwi.ptWorkStack.empty()) && *gwi.ptWorkStack.top()->data() == sptp->data())
        {
          gwi.ptWorkStack.pop();
        }
        else if (!gwi.rcWorkStack.empty())
        {
          // Probably won't happen, but it might have been on the
          // rcWorkStack all along.
          ReturnedColumn* ptrc = dynamic_cast<ReturnedColumn*>(sptp->data());

          if (ptrc && *ptrc == *gwi.rcWorkStack.top())
          {
            gwi.rcWorkStack.pop();
          }
        }
      }
    }

    funcParms.insert(funcParms.begin(), sptp);
  }

  // recover clause type
  gwi.clauseType = realClauseType;

  if (gwi.fatalParseError)
  {
    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
    return NULL;
  }

  Func* functor = funcexp->getFunctor(funcName);
  fc->resultType(colType_MysqlToIDB(item));
  execplan::CalpontSystemCatalog::ColType& resultType = fc->resultType();
  resultType.setTimeZone(gwi.timeZone);
  fc->operationType(functor->operationType(funcParms, resultType));
  fc->functionName(funcName);
  fc->functionParms(funcParms);
  fc->expressionId(ci->expressionId++);
  fc->timeZone(gwi.timeZone);

  // For function join. If any argument has non-zero joininfo, set it to the function.
  fc->setSimpleColumnList();
  std::vector<SimpleColumn*> simpleColList = fc->simpleColumnList();

  for (uint i = 0; i < simpleColList.size(); i++)
  {
    if (simpleColList[i]->joinInfo() != 0)
    {
      fc->joinInfo(simpleColList[i]->joinInfo());
      break;
    }
  }

  return fc;
}

ConstantColumn* buildDecimalColumn(const Item* idp, const std::string& valStr, gp_walk_info& gwi)
{
  IDB_Decimal columnstore_decimal;
  ostringstream columnstore_decimal_val;
  uint32_t i = 0;

  if (valStr[0] == '+' || valStr[0] == '-')
  {
    columnstore_decimal_val << valStr[0];
    i = 1;
  }

  bool specialPrecision = false;

  // handle the case when the constant value is 0.12345678901234567890123456789012345678
  // In this case idp->decimal_precision() = 39, but we can
  if (((i + 1) < valStr.length()) && valStr[i] == '0' && valStr[i + 1] == '.')
    specialPrecision = true;

  for (; i < valStr.length(); i++)
  {
    if (valStr[i] == '.')
      continue;

    columnstore_decimal_val << valStr[i];
  }

  if (idp->decimal_precision() <= datatypes::INT64MAXPRECISION)
    columnstore_decimal.value = strtoll(columnstore_decimal_val.str().c_str(), 0, 10);
  else if (idp->decimal_precision() <= datatypes::INT128MAXPRECISION ||
           (idp->decimal_precision() <= datatypes::INT128MAXPRECISION + 1 && specialPrecision))
  {
    bool dummy = false;
    columnstore_decimal.s128Value = dataconvert::strtoll128(columnstore_decimal_val.str().c_str(), dummy, 0);
  }

  // TODO MCOL-641 Add support here
  if (gwi.internalDecimalScale >= 0 && idp->decimals > (uint)gwi.internalDecimalScale)
  {
    columnstore_decimal.scale = gwi.internalDecimalScale;
    uint32_t diff = (uint32_t)(idp->decimals - gwi.internalDecimalScale);
    columnstore_decimal.value = columnstore_decimal.TDecimal64::toSInt64Round(diff);
  }
  else
    columnstore_decimal.scale = idp->decimal_scale();

  columnstore_decimal.precision = (idp->decimal_precision() > datatypes::INT128MAXPRECISION)
                                      ? datatypes::INT128MAXPRECISION
                                      : idp->decimal_precision();
  ConstantColumn* cc = new ConstantColumn(valStr, columnstore_decimal);
  cc->charsetNumber(idp->collation.collation->number);
  return cc;
}

SimpleColumn* buildSimpleColumn(Item_field* ifp, gp_walk_info& gwi)
{
  if (!gwi.csc)
  {
    gwi.csc = CalpontSystemCatalog::makeCalpontSystemCatalog(gwi.sessionid);
    gwi.csc->identity(CalpontSystemCatalog::FE);
  }

  bool isInformationSchema = false;

  // @bug5523
  if (ifp->cached_table && ifp->cached_table->db.length > 0 &&
      strcmp(ifp->cached_table->db.str, "information_schema") == 0)
    isInformationSchema = true;

  // support FRPM subquery. columns from the derived table has no definition
  if ((!ifp->field || !ifp->db_name.str || strlen(ifp->db_name.str) == 0) && !isInformationSchema)
    return buildSimpleColFromDerivedTable(gwi, ifp);

  CalpontSystemCatalog::ColType ct;
  datatypes::SimpleColumnParam prm(gwi.sessionid, true);

  try
  {
    // check foreign engine
    if (ifp->cached_table && ifp->cached_table->table)
      prm.columnStore(isMCSTable(ifp->cached_table->table));
    // @bug4509. ifp->cached_table could be null for myisam sometimes
    else if (ifp->field && ifp->field->table)
      prm.columnStore(isMCSTable(ifp->field->table));

    if (prm.columnStore())
    {
      ct = gwi.csc->colType(
          gwi.csc->lookupOID(make_tcn(ifp->db_name.str, bestTableName(ifp), ifp->field_name.str)));
    }
    else
    {
      ct = colType_MysqlToIDB(ifp);
    }
  }
  catch (std::exception& ex)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = ex.what();
    return NULL;
  }

  const datatypes::DatabaseQualifiedColumnName name(ifp->db_name.str, bestTableName(ifp),
                                                    ifp->field_name.str);
  const datatypes::TypeHandler* h = ct.typeHandler();
  SimpleColumn* sc = h->newSimpleColumn(name, ct, prm);

  sc->resultType(ct);
  sc->charsetNumber(ifp->collation.collation->number);
  string tbname(ifp->table_name.str);

  if (isInformationSchema)
  {
    sc->schemaName("information_schema");
    sc->tableName(tbname, lower_case_table_names);
  }

  sc->tableAlias(tbname, lower_case_table_names);

  // view name
  sc->viewName(getViewName(ifp->cached_table), lower_case_table_names);
  sc->alias(ifp->name.str);

  sc->isColumnStore(prm.columnStore());
  sc->timeZone(gwi.timeZone);

  if (!prm.columnStore() && ifp->field)
    sc->oid(ifp->field->field_index + 1);  // ExeMgr requires offset started from 1

  if (ifp->depended_from)
  {
    sc->joinInfo(sc->joinInfo() | JOIN_CORRELATED);

    if (gwi.subQuery)
      gwi.subQuery->correlated(true);

    // for error out non-support select filter case (comparison outside semi join tables)
    gwi.correlatedTbNameVec.push_back(make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias()));

    // imply semi for scalar for now.
    if (gwi.subSelectType == CalpontSelectExecutionPlan::SINGLEROW_SUBS)
      sc->joinInfo(sc->joinInfo() | JOIN_SCALAR | JOIN_SEMI);

    if (gwi.subSelectType == CalpontSelectExecutionPlan::SELECT_SUBS)
      sc->joinInfo(sc->joinInfo() | JOIN_SCALAR | JOIN_OUTER_SELECT);
  }

  return sc;
}

ParseTree* buildParseTree(Item_func* item, gp_walk_info& gwi, bool& nonSupport)
{
  ParseTree* pt = 0;
  Item_cond* icp = (Item_cond*)item;
#ifdef DEBUG_WALK_COND
  // debug
  cerr << "Build Parsetree: " << endl;
  icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
#endif
  //@bug5044. PPSTFIX walking should always be treated as WHERE clause filter
  ClauseType clauseType = gwi.clauseType;
  gwi.clauseType = WHERE;
  icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);
  gwi.clauseType = clauseType;

  if (gwi.fatalParseError)
    return NULL;

  // bug 2840. if the filter/function is constant, result is in rcWorkStack
  if (!gwi.ptWorkStack.empty())
  {
    pt = gwi.ptWorkStack.top();
    gwi.ptWorkStack.pop();
  }
  else if (!gwi.rcWorkStack.empty())
  {
    pt = new ParseTree(gwi.rcWorkStack.top());
    gwi.rcWorkStack.pop();
  }

  return pt;
}

class ConstArgParam
{
 public:
  unsigned int precision;
  unsigned int scale;
  bool bIsConst;
  bool hasDecimalConst;
  ConstArgParam() : precision(0), scale(0), bIsConst(false), hasDecimalConst(false)
  {
  }
};

static bool isSupportedAggregateWithOneConstArg(const Item_sum* item, Item** orig_args)
{
  if (item->argument_count() != 1 || !orig_args[0]->const_item())
    return false;
  switch (orig_args[0]->cmp_type())
  {
    case INT_RESULT:
    case STRING_RESULT:
    case REAL_RESULT:
    case DECIMAL_RESULT: return true;
    default: break;
  }
  return false;
}

static void processAggregateColumnConstArg(gp_walk_info& gwi, SRCP& parm, AggregateColumn* ac, Item* sfitemp,
                                           ConstArgParam& constParam)
{
  DBUG_ASSERT(sfitemp->const_item());
  switch (sfitemp->cmp_type())
  {
    case INT_RESULT:
    case STRING_RESULT:
    case REAL_RESULT:
    case DECIMAL_RESULT:
    {
      ReturnedColumn* rt = buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError);
      if (!rt)
      {
        gwi.fatalParseError = true;
        return;
      }
      ConstantColumn* cc;
      if ((cc = dynamic_cast<ConstantColumn*>(rt)) && cc->type() == ConstantColumn::NULLDATA)
      {
        // Explicit NULL or a const function that evaluated to NULL
        cc = new ConstantColumnNull();
        cc->timeZone(gwi.timeZone);
        parm.reset(cc);
        ac->constCol(SRCP(rt));
        return;
      }

      // treat as count(*)
      if (ac->aggOp() == AggregateColumn::COUNT)
        ac->aggOp(AggregateColumn::COUNT_ASTERISK);

      parm.reset(rt);
      ac->constCol(parm);
      constParam.bIsConst = true;
      if (sfitemp->cmp_type() == DECIMAL_RESULT)
      {
        constParam.hasDecimalConst = true;
        constParam.precision = sfitemp->decimal_precision();
        constParam.scale = sfitemp->decimal_scale();
      }
      break;
    }
    case TIME_RESULT:
      // QQ: why temporal constants are not handled?
    case ROW_RESULT:
    {
      gwi.fatalParseError = true;
    }
  }
}

ReturnedColumn* buildAggregateColumn(Item* item, gp_walk_info& gwi)
{
  // MCOL-1201 For UDAnF multiple parameters
  vector<SRCP> selCols;
  vector<SRCP> orderCols;
  ConstArgParam constArgParam;

  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  Item_sum* isp = reinterpret_cast<Item_sum*>(item);
  Item** sfitempp = isp->get_orig_args();
  SRCP parm;

  // @bug4756
  if (gwi.clauseType == SELECT)
    gwi.aggOnSelect = true;

  // Argument_count() is the # of formal parms to the agg fcn. Columnstore
  // only supports 1 argument except UDAnF, COUNT(DISTINC) and GROUP_CONCAT
  if (isp->argument_count() != 1 && isp->sum_func() != Item_sum::COUNT_DISTINCT_FUNC &&
      isp->sum_func() != Item_sum::GROUP_CONCAT_FUNC && isp->sum_func() != Item_sum::UDF_SUM_FUNC)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_MUL_ARG_AGG);
    return NULL;
  }

  AggregateColumn* ac = NULL;

  if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
  {
    ac = new GroupConcatColumn(gwi.sessionid);
  }
  else if (isp->sum_func() == Item_sum::UDF_SUM_FUNC)
  {
    ac = new UDAFColumn(gwi.sessionid);
  }
  else
  {
    ac = new AggregateColumn(gwi.sessionid);
  }

  ac->timeZone(gwi.timeZone);

  if (isp->name.length)
    ac->alias(isp->name.str);

  if ((setAggOp(ac, isp)))
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = "Non supported aggregate type on the select clause";

    if (ac)
      delete ac;

    return NULL;
  }

  try
  {
    // special parsing for group_concat
    if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
    {
      Item_func_group_concat* gc = (Item_func_group_concat*)isp;
      vector<SRCP> orderCols;
      RowColumn* rowCol = new RowColumn();
      vector<SRCP> selCols;

      uint32_t select_ctn = gc->get_count_field();
      ReturnedColumn* rc = NULL;

      for (uint32_t i = 0; i < select_ctn; i++)
      {
        rc = buildReturnedColumn(sfitempp[i], gwi, gwi.fatalParseError);

        if (!rc || gwi.fatalParseError)
        {
          if (ac)
            delete ac;

          return NULL;
        }

        selCols.push_back(SRCP(rc));
      }

      ORDER **order_item, **end;

      for (order_item = gc->get_order(), end = order_item + gc->get_order_field(); order_item < end;
           order_item++)
      {
        Item* ord_col = *(*order_item)->item;

        if (ord_col->type() == Item::CONST_ITEM && ord_col->cmp_type() == INT_RESULT)
        {
          Item_int* id = (Item_int*)ord_col;

          if (id->val_int() > (int)selCols.size())
          {
            gwi.fatalParseError = true;

            if (ac)
              delete ac;

            return NULL;
          }

          rc = selCols[id->val_int() - 1]->clone();
          rc->orderPos(id->val_int() - 1);
        }
        else
        {
          rc = buildReturnedColumn(ord_col, gwi, gwi.fatalParseError);

          if (!rc || gwi.fatalParseError)
          {
            if (ac)
              delete ac;

            return NULL;
          }
        }

        // 10.2 TODO: direction is now a tri-state flag
        rc->asc((*order_item)->direction == ORDER::ORDER_ASC ? true : false);
        orderCols.push_back(SRCP(rc));
      }

      rowCol->columnVec(selCols);
      (dynamic_cast<GroupConcatColumn*>(ac))->orderCols(orderCols);
      parm.reset(rowCol);
      ac->aggParms().push_back(parm);

      if (gc->get_separator())
      {
        string separator;
        separator.assign(gc->get_separator()->ptr(), gc->get_separator()->length());
        (dynamic_cast<GroupConcatColumn*>(ac))->separator(separator);
      }
    }
    else if (isSupportedAggregateWithOneConstArg(isp, sfitempp))
    {
      processAggregateColumnConstArg(gwi, parm, ac, sfitempp[0], constArgParam);
    }
    else
    {
      for (uint32_t i = 0; i < isp->argument_count(); i++)
      {
        Item* sfitemp = sfitempp[i];
        Item::Type sfitype = sfitemp->type();

        switch (sfitype)
        {
          case Item::FIELD_ITEM:
          {
            Item_field* ifp = reinterpret_cast<Item_field*>(sfitemp);
            SimpleColumn* sc = buildSimpleColumn(ifp, gwi);

            if (!sc)
            {
              gwi.fatalParseError = true;
              break;
            }

            parm.reset(sc);
            gwi.columnMap.insert(
                CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), parm));
            TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
            gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(),
                                         sc->isColumnStore())] = make_pair(1, tmp);
            break;
          }

          case Item::CONST_ITEM:
          case Item::NULL_ITEM:
          {
            processAggregateColumnConstArg(gwi, parm, ac, sfitemp, constArgParam);
            break;
          }

          case Item::FUNC_ITEM:
          {
            Item_func* ifp = (Item_func*)sfitemp;
            ReturnedColumn* rc = 0;

            // check count(1+1) case
            vector<Item_field*> tmpVec;
            uint16_t parseInfo = 0;
            parse_item(ifp, tmpVec, gwi.fatalParseError, parseInfo, &gwi);

            if (parseInfo & SUB_BIT)
            {
              gwi.fatalParseError = true;
              break;
            }
            else if (!gwi.fatalParseError && !(parseInfo & AGG_BIT) && !(parseInfo & AF_BIT) &&
                     tmpVec.size() == 0)
            {
              rc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);
              FunctionColumn* fc = dynamic_cast<FunctionColumn*>(rc);

              if ((fc && fc->functionParms().empty()) || !fc)
              {
                ReturnedColumn* rc = buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError);

                if (dynamic_cast<ConstantColumn*>(rc))
                {
                  //@bug5229. handle constant function on aggregate argument
                  ac->constCol(SRCP(rc));
                  break;
                }
              }
            }

            // MySQL carelessly allows correlated aggregate function on the WHERE clause.
            // Here is the work around to deal with that inconsistence.
            // e.g., SELECT (SELECT t.c FROM t1 AS t WHERE t.b=MAX(t1.b + 0)) FROM t1;
            ClauseType clauseType = gwi.clauseType;

            if (gwi.clauseType == WHERE)
              gwi.clauseType = HAVING;

            // @bug 3603. for cases like max(rand()). try to build function first.
            if (!rc)
              rc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);

            parm.reset(rc);
            gwi.clauseType = clauseType;

            if (gwi.fatalParseError)
              break;

            break;
          }

          case Item::REF_ITEM:
          {
            ReturnedColumn* rc = buildReturnedColumn(sfitemp, gwi, gwi.fatalParseError);

            if (rc)
            {
              parm.reset(rc);
              break;
            }
          }
            /* fall through */

          default:
          {
            gwi.fatalParseError = true;
          }
        }

        if (gwi.fatalParseError)
        {
          if (gwi.parseErrorText.empty())
          {
            Message::Args args;

            if (item->name.length)
              args.add(item->name.str);
            else
              args.add("");

            gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_AGG_ARGS, args);
          }

          if (ac)
            delete ac;

          return NULL;
        }

        if (parm)
        {
          // MCOL-1201 multi-argument aggregate
          ac->aggParms().push_back(parm);
        }
      }
    }

    bool isAvg = (isp->sum_func() == Item_sum::AVG_FUNC || isp->sum_func() == Item_sum::AVG_DISTINCT_FUNC);

    // Get result type
    // Modified for MCOL-1201 multi-argument aggregate
    if (!constArgParam.bIsConst && ac->aggParms().size() > 0)
    {
      // These are all one parm functions, so we can safely
      // use the first parm for result type.
      parm = ac->aggParms()[0];

      if (isAvg || isp->sum_func() == Item_sum::SUM_FUNC || isp->sum_func() == Item_sum::SUM_DISTINCT_FUNC)
      {
        CalpontSystemCatalog::ColType ct = parm->resultType();
        if (ct.isWideDecimalType())
        {
          uint32_t precision = ct.precision;
          uint32_t scale = ct.scale;
          if (isAvg)
          {
            datatypes::Decimal::setScalePrecision4Avg(precision, scale);
          }
          ct.precision = precision;
          ct.scale = scale;
        }
        else if (datatypes::hasUnderlyingWideDecimalForSumAndAvg(ct.colDataType))
        {
          uint32_t precision = datatypes::INT128MAXPRECISION;
          uint32_t scale = ct.scale;
          ct.colDataType = CalpontSystemCatalog::DECIMAL;
          ct.colWidth = datatypes::MAXDECIMALWIDTH;
          if (isAvg)
          {
            datatypes::Decimal::setScalePrecision4Avg(precision, scale);
          }
          ct.scale = scale;
          ct.precision = precision;
        }
        else
        {
          ct.colDataType = CalpontSystemCatalog::LONGDOUBLE;
          ct.colWidth = sizeof(long double);
          if (isAvg)
          {
            ct.scale += datatypes::MAXSCALEINC4AVG;
          }
          ct.precision = datatypes::INT64MAXPRECISION;
        }
        ac->resultType(ct);
      }
      else if (isp->sum_func() == Item_sum::COUNT_FUNC || isp->sum_func() == Item_sum::COUNT_DISTINCT_FUNC)
      {
        CalpontSystemCatalog::ColType ct;
        ct.colDataType = CalpontSystemCatalog::BIGINT;
        ct.colWidth = 8;
        ct.scale = 0;
        ac->resultType(ct);
      }
      else if (isp->sum_func() == Item_sum::STD_FUNC || isp->sum_func() == Item_sum::VARIANCE_FUNC)
      {
        CalpontSystemCatalog::ColType ct;
        ct.colDataType = CalpontSystemCatalog::DOUBLE;
        ct.colWidth = 8;
        ct.scale = 0;
        ac->resultType(ct);
      }
      else if (isp->sum_func() == Item_sum::SUM_BIT_FUNC)
      {
        CalpontSystemCatalog::ColType ct;
        ct.colDataType = CalpontSystemCatalog::BIGINT;
        ct.colWidth = 8;
        ct.scale = 0;
        ct.precision = -16;  // borrowed to indicate skip null value check on connector
        ac->resultType(ct);
      }
      else if (isp->sum_func() == Item_sum::GROUP_CONCAT_FUNC)
      {
        // Item_func_group_concat* gc = (Item_func_group_concat*)isp;
        CalpontSystemCatalog::ColType ct;
        ct.colDataType = CalpontSystemCatalog::VARCHAR;
        ct.colWidth = isp->max_length;
        ct.precision = 0;
        ac->resultType(ct);
      }
      // Setting the ColType in the resulting RowGroup
      // according with what MDB expects.
      // Internal processing UDAF result type will be set below.
      else if (isUDFSumItem(isp))
      {
        ac->resultType(colType_MysqlToIDB(isp));
      }
      // Using the first param to deduce ac data type
      else if (ac->aggParms().size() == 1)
      {
        ac->resultType(parm->resultType());
      }
      else
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText =
            "Can not deduce Aggregate Column resulting type \
because it has multiple arguments.";

        if (ac)
          delete ac;

        return nullptr;
      }
    }
    else if (constArgParam.bIsConst && constArgParam.hasDecimalConst && isAvg)
    {
      CalpontSystemCatalog::ColType ct = parm->resultType();
      if (datatypes::Decimal::isWideDecimalTypeByPrecision(constArgParam.precision))
      {
        ct.precision = constArgParam.precision;
        ct.scale = constArgParam.scale;
        ct.colWidth = datatypes::MAXDECIMALWIDTH;
      }
      ac->resultType(ct);
    }
    else
    {
      ac->resultType(colType_MysqlToIDB(isp));
    }

    // adjust decimal result type according to internalDecimalScale
    bool isWideDecimal = ac->resultType().isWideDecimalType();
    if (!isWideDecimal && gwi.internalDecimalScale >= 0 &&
        (ac->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
         ac->resultType().colDataType == CalpontSystemCatalog::UDECIMAL))
    {
      CalpontSystemCatalog::ColType ct = ac->resultType();
      ct.scale = gwi.internalDecimalScale;
      ac->resultType(ct);
    }

    // check for same aggregate on the select list
    ac->expressionId(ci->expressionId++);

    if (gwi.clauseType != SELECT)
    {
      for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
      {
        if (*ac == gwi.returnedCols[i].get() && ac->alias() == gwi.returnedCols[i].get()->alias())
          ac->expressionId(gwi.returnedCols[i]->expressionId());
      }
    }

    // @bug5977 @note Temporary fix to avoid mysqld crash. The permanent fix will
    // be applied in ExeMgr. When the ExeMgr fix is available, this checking
    // will be taken out.
    if (isp->sum_func() != Item_sum::UDF_SUM_FUNC)
    {
      if (ac->constCol() && gwi.tbList.empty() && gwi.derivedTbList.empty())
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = "No project column found for aggregate function";

        if (ac)
          delete ac;

        return NULL;
      }
      else if (ac->constCol())
      {
        gwi.count_asterisk_list.push_back(ac);
      }
    }

    // For UDAF, populate the context and call the UDAF init() function.
    // The return type is (should be) set in context by init().
    if (isp->sum_func() == Item_sum::UDF_SUM_FUNC)
    {
      UDAFColumn* udafc = dynamic_cast<UDAFColumn*>(ac);

      if (udafc)
      {
        mcsv1sdk::mcsv1Context& context = udafc->getContext();
        context.setName(isp->func_name());

        // Get the return type as defined by CREATE AGGREGATE FUNCTION
        // Most functions don't care, but some may.
        context.setMariaDBReturnType((mcsv1sdk::enum_mariadb_return_type)isp->field_type());

        // Set up the return type defaults for the call to init()
        context.setResultType(udafc->resultType().colDataType);
        context.setColWidth(udafc->resultType().colWidth);
        context.setScale(udafc->resultType().scale);
        context.setPrecision(udafc->resultType().precision);

        context.setParamCount(udafc->aggParms().size());
        utils::VLArray<mcsv1sdk::ColumnDatum> colTypes(udafc->aggParms().size());

        // Build the column type vector.
        // Modified for MCOL-1201 multi-argument aggregate
        for (uint32_t i = 0; i < udafc->aggParms().size(); ++i)
        {
          const execplan::CalpontSystemCatalog::ColType& resultType = udafc->aggParms()[i]->resultType();
          mcsv1sdk::ColumnDatum& colType = colTypes[i];
          colType.dataType = resultType.colDataType;
          colType.precision = resultType.precision;
          colType.scale = resultType.scale;
          colType.charsetNumber = resultType.charsetNumber;
        }

        // Call the user supplied init()
        mcsv1sdk::mcsv1_UDAF* udaf = context.getFunction();

        if (!udaf)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText =
              "Aggregate Function " + context.getName() + " doesn't exist in the ColumnStore engine";

          if (ac)
            delete ac;

          return NULL;
        }

        if (udaf->init(&context, colTypes) == mcsv1sdk::mcsv1_UDAF::ERROR)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = udafc->getContext().getErrorMessage();

          if (ac)
            delete ac;

          return NULL;
        }

        // UDAF_OVER_REQUIRED means that this function is for Window
        // Function only. Reject it here in aggregate land.
        if (udafc->getContext().getRunFlag(mcsv1sdk::UDAF_OVER_REQUIRED))
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText =
              logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WINDOW_FUNC_ONLY, context.getName());

          if (ac)
            delete ac;

          return NULL;
        }

        // Set the return type as set in init()
        CalpontSystemCatalog::ColType ct;
        ct.colDataType = context.getResultType();
        ct.colWidth = context.getColWidth();
        ct.scale = context.getScale();
        ct.precision = context.getPrecision();
        udafc->resultType(ct);
      }
    }
  }
  catch (std::logic_error& e)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = "error building Aggregate Function: ";
    gwi.parseErrorText += e.what();

    if (ac)
      delete ac;

    return NULL;
  }
  catch (...)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = "error building Aggregate Function: Unspecified exception";

    if (ac)
      delete ac;

    return NULL;
  }

  ac->charsetNumber(item->collation.collation->number);
  return ac;
}

void addIntervalArgs(gp_walk_info* gwip, Item_func* ifp, FunctionParm& functionParms)
{
  string funcName = ifp->func_name();
  int interval_type = -1;

  if (funcName == "date_add_interval")
    interval_type = ((Item_date_add_interval*)ifp)->int_type;
  else if (funcName == "timestampdiff")
    interval_type = ((Item_func_timestamp_diff*)ifp)->get_int_type();
  else if (funcName == "extract")
    interval_type = ((Item_extract*)ifp)->int_type;

  functionParms.push_back(getIntervalType(gwip, interval_type));
  SPTP sptp;

  if (funcName == "date_add_interval")
  {
    if (((Item_date_add_interval*)ifp)->date_sub_interval)
    {
      sptp.reset(new ParseTree(new ConstantColumn((int64_t)OP_SUB)));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);
      functionParms.push_back(sptp);
    }
    else
    {
      sptp.reset(new ParseTree(new ConstantColumn((int64_t)OP_ADD)));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);
      functionParms.push_back(sptp);
    }
  }
}

SPTP getIntervalType(gp_walk_info* gwip, int interval_type)
{
  SPTP sptp;
  sptp.reset(new ParseTree(new ConstantColumn((int64_t)interval_type)));
  (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);
  return sptp;
}

void castCharArgs(gp_walk_info* gwip, Item_func* ifp, FunctionParm& functionParms)
{
  Item_char_typecast* idai = (Item_char_typecast*)ifp;

  SPTP sptp;
  sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->get_cast_length())));
  (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);
  functionParms.push_back(sptp);
}

void castDecimalArgs(gp_walk_info* gwip, Item_func* ifp, FunctionParm& functionParms)
{
  Item_decimal_typecast* idai = (Item_decimal_typecast*)ifp;
  SPTP sptp;
  sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->decimals)));
  (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);
  functionParms.push_back(sptp);

  // max length including sign and/or decimal points
  if (idai->decimals == 0)
    sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->max_length - 1)));
  else
    sptp.reset(new ParseTree(new ConstantColumn((int64_t)idai->max_length - 2)));
  (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);

  functionParms.push_back(sptp);
}

void castTypeArgs(gp_walk_info* gwip, Item_func* ifp, FunctionParm& functionParms)
{
  Item_func_get_format* get_format = (Item_func_get_format*)ifp;
  SPTP sptp;

  if (get_format->type == MYSQL_TIMESTAMP_DATE)
    sptp.reset(new ParseTree(new ConstantColumn("DATE")));
  else
    sptp.reset(new ParseTree(new ConstantColumn("DATETIME")));
  (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwip->timeZone);

  functionParms.push_back(sptp);
}

bool isSecondArgumentConstItem(Item_func* ifp)
{
  return (ifp->argument_count() == 2 && ifp->arguments()[1]->type() == Item::CONST_ITEM);
}

// SELECT ... WHERE <col> NOT IN (SELECT <const_item>);
bool isNotFuncAndConstScalarSubSelect(Item_func* ifp, const std::string& funcName)
{
  return (ifp->with_subquery() && funcName == "not" && ifp->argument_count() == 1 &&
          ifp->arguments()[0]->type() == Item::FUNC_ITEM &&
          std::string(((Item_func*)ifp->arguments()[0])->func_name()) == "=" &&
          isSecondArgumentConstItem((Item_func*)ifp->arguments()[0]));
}

void gp_walk(const Item* item, void* arg)
{
  gp_walk_info* gwip = reinterpret_cast<gp_walk_info*>(arg);
  idbassert(gwip);

  // Bailout...
  if (gwip->fatalParseError)
    return;

  RecursionCounter r(gwip);  // Increments and auto-decrements upon exit.

  Item::Type itype = item->type();

  // Allow to process XOR(which is Item_func) like other logical operators (which are Item_cond)
  if (itype == Item::FUNC_ITEM && ((Item_func*)item)->functype() == Item_func::XOR_FUNC)
    itype = Item::COND_ITEM;

  switch (itype)
  {
    case Item::CACHE_ITEM:
    {
      // The item or condition is cached as per MariaDB server view but
      // for InfiniDB it need to be parsed and executed.
      // MCOL-1188 and MCOL-1029
      Item* orig_item = ((Item_cache*)item)->get_example();
      orig_item->traverse_cond(gp_walk, gwip, Item::POSTFIX);
      break;
    }
    case Item::FIELD_ITEM:
    {
      Item_field* ifp = (Item_field*)item;

      if (ifp)
      {
        SimpleColumn* scp = buildSimpleColumn(ifp, *gwip);

        if (!scp)
          break;

        string aliasTableName(scp->tableAlias());
        scp->tableAlias(aliasTableName);
        gwip->rcWorkStack.push(scp->clone());
        boost::shared_ptr<SimpleColumn> scsp(scp);
        gwip->scsp = scsp;

        gwip->funcName.clear();
        gwip->columnMap.insert(
            CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), scsp));

        //@bug4636 take where clause column as dummy projection column, but only on local column.
        // varbinary aggregate is not supported yet, so rule it out
        if (!((scp->joinInfo() & JOIN_CORRELATED) ||
              scp->colType().colDataType == CalpontSystemCatalog::VARBINARY))
        {
          TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
          gwip->tableMap[make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias(),
                                         scp->isColumnStore())] = make_pair(1, tmp);
        }
      }

      break;
    }

    case Item::CONST_ITEM:
    {
      switch (item->cmp_type())
      {
        case INT_RESULT:
        {
          Item_int* iip = (Item_int*)item;
          gwip->rcWorkStack.push(buildReturnedColumn(iip, *gwip, gwip->fatalParseError));
          break;
        }

        case STRING_RESULT:
        {
          // Special handling for 0xHHHH literals
          if (item->type_handler() == &type_handler_hex_hybrid)
          {
            Item_hex_hybrid* hip = reinterpret_cast<Item_hex_hybrid*>(const_cast<Item*>(item));
            gwip->rcWorkStack.push(new ConstantColumn((int64_t)hip->val_int(), ConstantColumn::NUM));
            ConstantColumn* cc = dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top());
            cc->timeZone(gwip->timeZone);
            break;
          }

          Item_string* isp = (Item_string*)item;

          if (isp)
          {
            if (isp->result_type() == STRING_RESULT)
            {
              String val, *str = isp->val_str(&val);
              string cval;

              if (str->ptr())
              {
                cval.assign(str->ptr(), str->length());
              }

              gwip->rcWorkStack.push(new ConstantColumn(cval));
              (dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
              break;
            }

            gwip->rcWorkStack.push(buildReturnedColumn(isp, *gwip, gwip->fatalParseError));
          }
          break;
        }

        case REAL_RESULT:
        {
          Item_float* ifp = (Item_float*)item;
          gwip->rcWorkStack.push(buildReturnedColumn(ifp, *gwip, gwip->fatalParseError));
          break;
        }

        case DECIMAL_RESULT:
        {
          Item_decimal* idp = (Item_decimal*)item;
          gwip->rcWorkStack.push(buildReturnedColumn(idp, *gwip, gwip->fatalParseError));
          break;
        }

        case TIME_RESULT:
        {
          Item_temporal_literal* itp = (Item_temporal_literal*)item;
          gwip->rcWorkStack.push(buildReturnedColumn(itp, *gwip, gwip->fatalParseError));
          break;
        }
        default:
        {
          if (gwip->condPush)
          {
            // push noop for unhandled item
            SimpleColumn* rc = new SimpleColumn("noop");
            rc->timeZone(gwip->timeZone);
            gwip->rcWorkStack.push(rc);
            break;
          }

          ostringstream oss;
          oss << "Unhandled Item type: " << item->type();
          gwip->parseErrorText = oss.str();
          gwip->fatalParseError = true;
          break;
        }
      }
      break;
    }
    case Item::NULL_ITEM:
    {
      if (gwip->condPush)
      {
        // push noop for unhandled item
        SimpleColumn* rc = new SimpleColumn("noop");
        rc->timeZone(gwip->timeZone);
        gwip->rcWorkStack.push(rc);
        break;
      }

      gwip->rcWorkStack.push(new ConstantColumn("", ConstantColumn::NULLDATA));
      (dynamic_cast<ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
      break;
    }

    case Item::FUNC_ITEM:
    {
      Item_func* ifp = (Item_func*)item;
      string funcName = ifp->func_name();

      if (!gwip->condPush)
      {
        if (!ifp->fixed())
        {
          ifp->fix_fields(gwip->thd, reinterpret_cast<Item**>(&ifp));
        }

        // Special handling for queries of the form:
        // SELECT ... WHERE col1 NOT IN (SELECT <const_item>);
        if (isNotFuncAndConstScalarSubSelect(ifp, funcName))
        {
          idbassert(!gwip->ptWorkStack.empty());
          ParseTree* pt = gwip->ptWorkStack.top();
          SimpleFilter* sf = dynamic_cast<SimpleFilter*>(pt->data());

          if (sf)
          {
            boost::shared_ptr<Operator> sop(new PredicateOperator("<>"));
            sf->op(sop);
            return;
          }
        }

        // Do not call buildSubselectFunc() if the subquery is a const scalar
        // subselect of the form:
        // (SELECT <const_item>)
        // As an example: SELECT col1 FROM t1 WHERE col2 = (SELECT 2);
        if ((ifp->with_subquery() && !isSecondArgumentConstItem(ifp)) || funcName == "<in_optimizer>")
        {
          buildSubselectFunc(ifp, gwip);
          return;
        }

        if (ifp->argument_count() > 0 && ifp->arguments())
        {
          for (uint32_t i = 0; i < ifp->argument_count(); i++)
          {
            if (ifp->arguments()[i]->type() == Item::SUBSELECT_ITEM)
            {
              // This is probably NOT IN subquery with derived table in it.
              // for some reason, MySQL has not fully optimized the plan at this point.
              // noop here, and eventually MySQL will continue its optimization and get
              // to rnd_init again.
              if (ifp->functype() == Item_func::NOT_FUNC)
                return;

              buildSubselectFunc(ifp, gwip);
              return;
            }
          }
        }

        if (ifp->functype() == Item_func::TRIG_COND_FUNC && gwip->subQuery)
        {
          gwip->subQuery->handleFunc(gwip, ifp);
          break;
        }

        // having clause null function added by MySQL
        if (ifp->functype() == Item_func::ISNOTNULLTEST_FUNC)
        {
          // @bug 4215. remove the argument in rcWorkStack.
          if (!gwip->rcWorkStack.empty())
            gwip->rcWorkStack.pop();

          break;
        }
      }

      // try to evaluate const F&E
      vector<Item_field*> tmpVec;
      uint16_t parseInfo = 0;
      parse_item(ifp, tmpVec, gwip->fatalParseError, parseInfo, gwip);

      // table mode takes only one table filter
      if (gwip->condPush)
      {
        set<string> tableSet;

        for (uint32_t i = 0; i < tmpVec.size(); i++)
        {
          if (tmpVec[i]->table_name.str)
            tableSet.insert(tmpVec[i]->table_name.str);
        }

        if (tableSet.size() > 1)
          break;
      }

      if (!gwip->fatalParseError && !(parseInfo & AGG_BIT) && !(parseInfo & SUB_BIT) && !nonConstFunc(ifp) &&
          !(parseInfo & AF_BIT) && tmpVec.size() == 0 && ifp->functype() != Item_func::MULT_EQUAL_FUNC)
      {
        ValStrStdString valStr(ifp);

        ConstantColumn* cc = buildConstantColumnMaybeNullFromValStr(ifp, valStr, *gwip);

        for (uint32_t i = 0; i < ifp->argument_count() && !gwip->rcWorkStack.empty(); i++)
        {
          gwip->rcWorkStack.pop();
        }

        // bug 3137. If filter constant like 1=0, put it to ptWorkStack
        // MariaDB bug 750. Breaks if compare is an argument to a function.
        //				if ((int32_t)gwip->rcWorkStack.size() <=  (gwip->rcBookMarkStack.empty() ? 0
        //: gwip->rcBookMarkStack.top())
        //				&& isPredicateFunction(ifp, gwip))
        if (isPredicateFunction(ifp, gwip))
          gwip->ptWorkStack.push(new ParseTree(cc));
        else
          gwip->rcWorkStack.push(cc);

        if (!valStr.isNull())
          IDEBUG(cerr << "Const F&E " << item->full_name() << " evaluate: " << valStr << endl);

        break;
      }

      ReturnedColumn* rc = NULL;

      // @bug4488. Process function for table mode also, not just vtable mode.
      rc = buildFunctionColumn(ifp, *gwip, gwip->fatalParseError);

      if (gwip->fatalParseError)
      {
        if (gwip->clauseType == SELECT)
          return;

        // @bug 2585
        if (gwip->parseErrorText.empty())
        {
          Message::Args args;
          args.add(funcName);
          gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
        }

        return;
      }

      // predicate operators fall in the old path
      if (rc)
      {
        // @bug 2383. For some reason func_name() for "in" gives " IN " always
        if (funcName == "between" || funcName == "in" || funcName == " IN ")
          gwip->ptWorkStack.push(new ParseTree(rc));
        else
          gwip->rcWorkStack.push(rc);
      }
      else
      {
        // push to pt or rc stack is handled inside the function
        buildPredicateItem(ifp, gwip);
      }

      break;
    }

    case Item::SUM_FUNC_ITEM:
    {
      Item_sum* isp = (Item_sum*)item;
      ReturnedColumn* rc = buildAggregateColumn(isp, *gwip);

      if (rc)
        gwip->rcWorkStack.push(rc);

      break;
    }

    case Item::COND_ITEM:
    {
      // All logical functions are handled here,  most of them are Item_cond,
      // but XOR (it is Item_func_boolean2)
      Item_func* func = (Item_func*)item;

      enum Item_func::Functype ftype = func->functype();
      bool isOr = (ftype == Item_func::COND_OR_FUNC);
      bool isXor = (ftype == Item_func::XOR_FUNC);

      List<Item>* argumentList;
      List<Item> xorArgumentList;

      if (isXor)
      {
        for (unsigned i = 0; i < func->argument_count(); i++)
        {
          xorArgumentList.push_back(func->arguments()[i]);
        }

        argumentList = &xorArgumentList;
      }
      else
      {
        argumentList = ((Item_cond*)item)->argument_list();
      }

      // @bug2932. if ptWorkStack contains less items than the condition's arguments,
      // the missing one should be in the rcWorkStack, unless the it's subselect.
      // @todo need to figure out a way to combine these two stacks while walking.
      // if (gwip->ptWorkStack.size() < icp->argument_list()->elements)
      {
        List_iterator_fast<Item> li(*argumentList);

        while (Item* it = li++)
        {
          //@bug3495, @bug5865 error out non-supported OR with correlated subquery
          if (isOr)
          {
            vector<Item_field*> fieldVec;
            uint16_t parseInfo = 0;
            parse_item(it, fieldVec, gwip->fatalParseError, parseInfo, gwip);

            if (parseInfo & CORRELATED)
            {
              gwip->fatalParseError = true;
              gwip->parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_CORRELATED_SUB_OR);
              return;
            }
          }

          if ((it->type() == Item::FIELD_ITEM ||
               (it->type() == Item::CONST_ITEM &&
                (it->cmp_type() == INT_RESULT || it->cmp_type() == DECIMAL_RESULT ||
                 it->cmp_type() == STRING_RESULT || it->cmp_type() == REAL_RESULT)) ||
               it->type() == Item::NULL_ITEM ||
               (it->type() == Item::FUNC_ITEM && !isPredicateFunction(it, gwip))) &&
              !gwip->rcWorkStack.empty())
          {
            gwip->ptWorkStack.push(new ParseTree(gwip->rcWorkStack.top()));
            gwip->rcWorkStack.pop();
          }
        }
      }

      // @bug1603. MySQL's filter tree is a multi-tree grouped by operator. So more than
      // two filters saved on the stack so far might belong to this operator.
      uint32_t leftInStack = gwip->ptWorkStack.size() - argumentList->elements + 1;

      while (true)
      {
        if (gwip->ptWorkStack.size() < 2)
          break;

        ParseTree* lhs = gwip->ptWorkStack.top();
        gwip->ptWorkStack.pop();
        SimpleFilter* lsf = dynamic_cast<SimpleFilter*>(lhs->data());

        if (lsf && lsf->op()->data() == "noop")
        {
          if (isOr)
          {
            gwip->parseErrorText = "Unhandled item in WHERE or HAVING clause";
            gwip->fatalParseError = true;
            break;
          }
          else
            continue;
        }

        ParseTree* rhs = gwip->ptWorkStack.top();
        gwip->ptWorkStack.pop();
        SimpleFilter* rsf = dynamic_cast<SimpleFilter*>(rhs->data());

        if (rsf && rsf->op()->data() == "noop")
        {
          if (isOr)
          {
            gwip->parseErrorText = "Unhandled item in WHERE or HAVING clause";
            gwip->fatalParseError = true;
            break;
          }
          else
          {
            gwip->ptWorkStack.push(lhs);
            continue;
          }
        }

        Operator* op = new LogicOperator(func->func_name());
        ParseTree* ptp = new ParseTree(op);
        ptp->left(lhs);
        ptp->right(rhs);
        gwip->ptWorkStack.push(ptp);

        if (gwip->ptWorkStack.size() == leftInStack)
          break;
      }

      // special handling for subquery with aggregate. MySQL adds isnull function to the selected
      // column. InfiniDB will remove it and set nullmatch flag if it's NOT_IN sub.
      // @todo need more checking here to make sure it's not a user input OR operator
      if (isOr && gwip->subQuery)
        gwip->subQuery->handleFunc(gwip, func);

      break;
    }

    case Item::REF_ITEM:
    {
      Item* col = *(((Item_ref*)item)->ref);
      ReturnedColumn* rc = NULL;
      // ref item is not pre-walked. force clause type to SELECT
      ClauseType clauseType = gwip->clauseType;
      gwip->clauseType = SELECT;

      if (col->type() != Item::COND_ITEM)
      {
        rc = buildReturnedColumn(col, *gwip, gwip->fatalParseError, true);

        if (col->type() == Item::FIELD_ITEM)
          gwip->fatalParseError = false;
      }

      SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

      if (sc)
      {
        boost::shared_ptr<SimpleColumn> scsp(sc->clone());
        gwip->scsp = scsp;

        if (col->type() == Item::FIELD_ITEM)
        {
          const auto& field_name = string(((Item_field*)item)->field_name.str);
          auto colMap = CalpontSelectExecutionPlan::ColumnMap::value_type(field_name, scsp);
          gwip->columnMap.insert(colMap);
        }
      }

      bool cando = true;
      gwip->clauseType = clauseType;

      if (rc)
      {
        if (((Item_ref*)item)->depended_from)
        {
          rc->joinInfo(rc->joinInfo() | JOIN_CORRELATED);

          if (gwip->subQuery)
            gwip->subQuery->correlated(true);

          SimpleColumn* scp = dynamic_cast<SimpleColumn*>(rc);

          if (scp)
            gwip->correlatedTbNameVec.push_back(
                make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias()));

          if (gwip->subSelectType == CalpontSelectExecutionPlan::SINGLEROW_SUBS)
            rc->joinInfo(rc->joinInfo() | JOIN_SCALAR | JOIN_SEMI);

          if (gwip->subSelectType == CalpontSelectExecutionPlan::SELECT_SUBS)
            rc->joinInfo(rc->joinInfo() | JOIN_SCALAR | JOIN_OUTER_SELECT);
        }

        gwip->rcWorkStack.push(rc);
      }
      else if (col->type() == Item::FUNC_ITEM)
      {
        // sometimes mysql treat having filter items inconsistently. In such cases,
        // which are always predicate operator, the function (gp_key>3) comes in as
        // one item.
        Item_func* ifp = (Item_func*)col;

        for (uint32_t i = 0; i < ifp->argument_count(); i++)
        {
          ReturnedColumn* operand = NULL;

          if (ifp->arguments()[i]->type() == Item::REF_ITEM)
          {
            Item* op = *(((Item_ref*)ifp->arguments()[i])->ref);
            operand = buildReturnedColumn(op, *gwip, gwip->fatalParseError);
          }
          else
            operand = buildReturnedColumn(ifp->arguments()[i], *gwip, gwip->fatalParseError);

          if (operand)
            gwip->rcWorkStack.push(operand);
          else
          {
            cando = false;
            break;
          }
        }

        if (cando)
          buildPredicateItem(ifp, gwip);
      }
      else if (col->type() == Item::COND_ITEM)
      {
        Item_func* ifp = (Item_func*)col;
        gwip->ptWorkStack.push(buildParseTree(ifp, *gwip, gwip->fatalParseError));
      }
      else if (col->type() == Item::FIELD_ITEM && gwip->clauseType == HAVING)
      {
        ReturnedColumn* rc = buildAggFrmTempField(const_cast<Item*>(item), *gwip);
        if (rc)
          gwip->rcWorkStack.push(rc);

        break;
      }
      else
        cando = false;

      if (!cando)
      {
        ostringstream oss;
        oss << "Unhandled Item type: " << item->type();
        gwip->parseErrorText = oss.str();
        gwip->fatalParseError = true;
      }

      break;
    }

    case Item::SUBSELECT_ITEM:
    {
      if (gwip->condPush)  // table mode
        break;

      Item_subselect* sub = (Item_subselect*)item;

      if (sub->substype() == Item_subselect::EXISTS_SUBS)
      {
        SubQuery* orig = gwip->subQuery;
        ExistsSub* existsSub = new ExistsSub(*gwip, sub);
        gwip->hasSubSelect = true;
        gwip->subQuery = existsSub;
        gwip->ptWorkStack.push(existsSub->transform());
        // MCOL-2178 isUnion member only assigned, never used
        // MIGR::infinidb_vtable.isUnion = true; // only temp. bypass the 2nd phase.
        // recover original
        gwip->subQuery = orig;
        gwip->lastSub = existsSub;
      }
      else if (sub->substype() == Item_subselect::IN_SUBS)
      {
        if (!((Item_in_subselect*)sub)->optimizer && gwip->thd->derived_tables_processing)
        {
          ostringstream oss;
          oss << "Invalid In_optimizer: " << item->type();
          gwip->parseErrorText = oss.str();
          gwip->fatalParseError = true;
          break;
        }
      }

      // store a dummy subselect object. the transform is handled in item_func.
      SubSelect* subselect = new SubSelect();
      gwip->rcWorkStack.push(subselect);
      break;
    }

    case Item::ROW_ITEM:
    {
      Item_row* row = (Item_row*)item;
      RowColumn* rowCol = new RowColumn();
      vector<SRCP> cols;
      // temp change clause type because the elements of row column are not walked yet
      gwip->clauseType = SELECT;

      for (uint32_t i = 0; i < row->cols(); i++)
        cols.push_back(SRCP(buildReturnedColumn(row->element_index(i), *gwip, gwip->fatalParseError)));

      gwip->clauseType = WHERE;
      rowCol->columnVec(cols);
      gwip->rcWorkStack.push(rowCol);
      break;
    }

    case Item::EXPR_CACHE_ITEM:
    {
      ((Item_cache_wrapper*)item)->get_orig_item()->traverse_cond(gp_walk, arg, Item::POSTFIX);
      break;
    }

    case Item::WINDOW_FUNC_ITEM:
    {
      gwip->hasWindowFunc = true;
      Item_window_func* ifa = (Item_window_func*)item;
      ReturnedColumn* af = buildWindowFunctionColumn(ifa, *gwip, gwip->fatalParseError);

      if (af)
        gwip->rcWorkStack.push(af);

      break;
    }

    case Item::COPY_STR_ITEM: printf("********** received COPY_STR_ITEM *********\n"); break;

    case Item::FIELD_AVG_ITEM: printf("********** received FIELD_AVG_ITEM *********\n"); break;

    case Item::DEFAULT_VALUE_ITEM: printf("********** received DEFAULT_VALUE_ITEM *********\n"); break;

    case Item::PROC_ITEM: printf("********** received PROC_ITEM *********\n"); break;

    case Item::FIELD_STD_ITEM: printf("********** received FIELD_STD_ITEM *********\n"); break;

    case Item::FIELD_VARIANCE_ITEM: printf("********** received FIELD_VARIANCE_ITEM *********\n"); break;

    case Item::INSERT_VALUE_ITEM: printf("********** received INSERT_VALUE_ITEM *********\n"); break;

    case Item::PARAM_ITEM: printf("********** received PARAM_ITEM *********\n"); break;

    case Item::TRIGGER_FIELD_ITEM: printf("********** received TRIGGER_FIELD_ITEM *********\n"); break;

    case Item::TYPE_HOLDER: std::cerr << "********** received TYPE_HOLDER *********" << std::endl; break;
    default:
    {
      if (gwip->condPush)
      {
        // push noop for unhandled item
        SimpleColumn* rc = new SimpleColumn("noop");
        rc->timeZone(gwip->timeZone);
        gwip->rcWorkStack.push(rc);
        break;
      }

      ostringstream oss;
      oss << "Unhandled Item type: " << item->type();
      gwip->parseErrorText = oss.str();
      gwip->fatalParseError = true;
      break;
    }
  }

  return;
}

/** @info this function recursivly walks an item's arguments and push all
 *  the involved item_fields to the passed in vector. It's used in parsing
 *  functions or arithmetic expressions for vtable post process.
 */
void parse_item(Item* item, vector<Item_field*>& field_vec, bool& hasNonSupportItem, uint16_t& parseInfo,
                gp_walk_info* gwi)
{
  Item::Type itype = item->type();

  switch (itype)
  {
    case Item::FIELD_ITEM:
    {
      Item_field* ifp = reinterpret_cast<Item_field*>(item);
      field_vec.push_back(ifp);
      return;
    }

    case Item::SUM_FUNC_ITEM:
    {
      // hasAggColumn = true;
      parseInfo |= AGG_BIT;
      Item_sum* isp = reinterpret_cast<Item_sum*>(item);
      Item** sfitempp = isp->arguments();

      for (uint32_t i = 0; i < isp->argument_count(); i++)
        parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::FUNC_ITEM:
    {
      Item_func* isp = reinterpret_cast<Item_func*>(item);

      if (string(isp->func_name()) == "<in_optimizer>")
      {
        parseInfo |= SUB_BIT;
        parseInfo |= CORRELATED;
        break;
      }

      for (uint32_t i = 0; i < isp->argument_count(); i++)
        parse_item(isp->arguments()[i], field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::COND_ITEM:
    {
      Item_cond* icp = reinterpret_cast<Item_cond*>(item);
      List_iterator_fast<Item> it(*(icp->argument_list()));
      Item* cond_item;

      while ((cond_item = it++))
        parse_item(cond_item, field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::REF_ITEM:
    {
      while (true)
      {
        Item_ref* ref = (Item_ref*)item;

        if ((*(ref->ref))->type() == Item::SUM_FUNC_ITEM)
        {
          parseInfo |= AGG_BIT;
          Item_sum* isp = reinterpret_cast<Item_sum*>(*(ref->ref));
          Item** sfitempp = isp->arguments();

          // special handling for count(*). This should not be treated as constant.
          if (isSupportedAggregateWithOneConstArg(isp, sfitempp))
          {
            field_vec.push_back((Item_field*)item);  // dummy
          }

          for (uint32_t i = 0; i < isp->argument_count(); i++)
            parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo, gwi);

          break;
        }
        else if ((*(ref->ref))->type() == Item::FIELD_ITEM)
        {
          // MCOL-1510. This could be a non-supported function
          // argument in form of a temp_table_field, so check
          // and set hasNonSupportItem if it is so.
          ReturnedColumn* rc = NULL;
          if (gwi)
            rc = buildAggFrmTempField(ref, *gwi);

          if (!rc)
          {
            Item_field* ifp = reinterpret_cast<Item_field*>(*(ref->ref));
            field_vec.push_back(ifp);
          }
          break;
        }
        else if ((*(ref->ref))->type() == Item::FUNC_ITEM)
        {
          Item_func* isp = reinterpret_cast<Item_func*>(*(ref->ref));
          Item** sfitempp = isp->arguments();

          for (uint32_t i = 0; i < isp->argument_count(); i++)
            parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo, gwi);

          break;
        }
        else if ((*(ref->ref))->type() == Item::CACHE_ITEM)
        {
          Item_cache* isp = reinterpret_cast<Item_cache*>(*(ref->ref));
          parse_item(isp->get_example(), field_vec, hasNonSupportItem, parseInfo, gwi);
          break;
        }
        else if ((*(ref->ref))->type() == Item::REF_ITEM)
        {
          item = (*(ref->ref));
          continue;
        }
        else if ((*(ref->ref))->type() == Item::WINDOW_FUNC_ITEM)
        {
          parseInfo |= AF_BIT;
          break;
        }
        else
        {
          cerr << "UNKNOWN REF Item" << endl;
          break;
        }
      }

      break;
    }

    case Item::SUBSELECT_ITEM:
    {
      parseInfo |= SUB_BIT;
      Item_subselect* sub = (Item_subselect*)item;

      if (sub->is_correlated)
        parseInfo |= CORRELATED;

      break;
    }

    case Item::ROW_ITEM:
    {
      Item_row* row = (Item_row*)item;

      for (uint32_t i = 0; i < row->cols(); i++)
        parse_item(row->element_index(i), field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::EXPR_CACHE_ITEM:
    {
      // item is a Item_cache_wrapper. Shouldn't get here.
      // DRRTUY TODO Why
      IDEBUG(std::cerr << "EXPR_CACHE_ITEM in parse_item\n" << std::endl);
      gwi->fatalParseError = true;
      // DRRTUY The questionable error text. I've seen
      // ERR_CORRELATED_SUB_OR
      string parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SUB_QUERY_TYPE);
      setError(gwi->thd, ER_CHECK_NOT_IMPLEMENTED, parseErrorText);
      break;
    }

    case Item::WINDOW_FUNC_ITEM: parseInfo |= AF_BIT; break;

    default: break;
  }
}

bool isMCSTable(TABLE* table_ptr)
{
#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)

  if (!(table_ptr->s && (*table_ptr->s->db_plugin)->name.str))
#else
  if (!(table_ptr->s && (table_ptr->s->db_plugin)->name.str))
#endif
    return true;

#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
  string engineName = (*table_ptr->s->db_plugin)->name.str;
#else
  string engineName = table_ptr->s->db_plugin->name.str;
#endif

  if (engineName == "Columnstore" || engineName == "Columnstore_cache")
    return true;
  else
    return false;
}

bool isForeignTableUpdate(THD* thd)
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

bool isMCSTableUpdate(THD* thd)
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

bool isMCSTableDelete(THD* thd)
{
  LEX* lex = thd->lex;

  if (!isDeleteStatement(lex->sql_command))
    return false;

  TABLE_LIST* table_ptr = lex->first_select_lex()->get_table_list();

  if (table_ptr && table_ptr->table && isMCSTable(table_ptr->table))
    return true;

  return false;
}

// This function is different from isForeignTableUpdate()
// above as it only checks if any of the tables involved
// in the multi-table update statement is a foreign table,
// irrespective of whether the update is performed on the
// foreign table or not, as in isForeignTableUpdate().
bool isUpdateHasForeignTable(THD* thd)
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

/*@brief  set some runtime params to run the query         */
/***********************************************************
 * DESCRIPTION:
 * This function just sets a number of runtime params that
 * limits resource consumed.
 ***********************************************************/
void setExecutionParams(gp_walk_info& gwi, SCSEP& csep)
{
  gwi.internalDecimalScale = (get_use_decimal_scale(gwi.thd) ? get_decimal_scale(gwi.thd) : -1);
  // @bug 2123. Override large table estimate if infinidb_ordered hint was used.
  // @bug 2404. Always override if the infinidb_ordered_only variable is turned on.
  if (get_ordered_only(gwi.thd))
    csep->overrideLargeSideEstimate(true);

  // @bug 5741. Set a flag when in Local PM only query mode
  csep->localQuery(get_local_query(gwi.thd));

  // @bug 3321. Set max number of blocks in a dictionary file to be scanned for filtering
  csep->stringScanThreshold(get_string_scan_threshold(gwi.thd));

  csep->stringTableThreshold(get_stringtable_threshold(gwi.thd));

  csep->djsSmallSideLimit(get_diskjoin_smallsidelimit(gwi.thd) * 1024ULL * 1024);
  csep->djsLargeSideLimit(get_diskjoin_largesidelimit(gwi.thd) * 1024ULL * 1024);
  csep->djsPartitionSize(get_diskjoin_bucketsize(gwi.thd) * 1024ULL * 1024);

  if (get_um_mem_limit(gwi.thd) == 0)
    csep->umMemLimit(numeric_limits<int64_t>::max());
  else
    csep->umMemLimit(get_um_mem_limit(gwi.thd) * 1024ULL * 1024);
}

/*@brief  Process FROM part of the query or sub-query      */
/***********************************************************
 * DESCRIPTION:
 *  This function processes elements of List<TABLE_LIST> in
 *  FROM part of the query.
 *  isUnion tells that CS processes FROM taken from UNION UNIT.
 *  The notion is described in MDB code.
 * RETURNS
 *  error id as an int
 ***********************************************************/
int processFrom(bool& isUnion, SELECT_LEX& select_lex, gp_walk_info& gwi, SCSEP& csep)
{
  // populate table map and trigger syscolumn cache for all the tables (@bug 1637).
  // all tables on FROM list must have at least one col in colmap
  TABLE_LIST* table_ptr = select_lex.get_table_list();

#ifdef DEBUG_WALK_COND
  List_iterator<TABLE_LIST> sj_list_it(select_lex.sj_nests);
  TABLE_LIST* sj_nest;

  while ((sj_nest = sj_list_it++))
  {
    cerr << sj_nest->db.str << "." << sj_nest->table_name.str << endl;
  }
#endif

  try
  {
    for (; table_ptr; table_ptr = table_ptr->next_local)
    {
      // Until we handle recursive cte:
      // Checking here ensures we catch all with clauses in the query.
      if (table_ptr->is_recursive_with_table())
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = "Recursive CTE";
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      string viewName = getViewName(table_ptr);
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(viewName);
      }

      // @todo process from subquery
      if (table_ptr->derived)
      {
        SELECT_LEX* select_cursor = table_ptr->derived->first_select();
        FromSubQuery fromSub(gwi, select_cursor);
        string alias(table_ptr->alias.str);
        if (lower_case_table_names)
        {
          boost::algorithm::to_lower(alias);
        }
        fromSub.alias(alias);

        CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, viewName);
        // @bug 3852. check return execplan
        SCSEP plan = fromSub.transform();

        if (!plan)
        {
          setError(gwi.thd, ER_INTERNAL_ERROR, fromSub.gwip().parseErrorText, gwi);
          CalpontSystemCatalog::removeCalpontSystemCatalog(gwi.sessionid);
          return ER_INTERNAL_ERROR;
        }

        gwi.derivedTbList.push_back(plan);
        gwi.tbList.push_back(tn);
        CalpontSystemCatalog::TableAliasName tan = make_aliastable("", alias, alias);
        gwi.tableMap[tan] = make_pair(0, table_ptr);
        // MCOL-2178 isUnion member only assigned, never used
        // MIGR::infinidb_vtable.isUnion = true; //by-pass the 2nd pass of rnd_init
      }
      else if (table_ptr->view)
      {
        View* view = new View(*table_ptr->view->first_select_lex(), &gwi);
        CalpontSystemCatalog::TableAliasName tn = make_aliastable(
            table_ptr->db.str, table_ptr->table_name.str, table_ptr->alias.str, true, lower_case_table_names);
        view->viewName(tn);
        gwi.viewList.push_back(view);
        view->transform();
      }
      else
      {
        // check foreign engine tables
        bool columnStore = (table_ptr->table ? isMCSTable(table_ptr->table) : true);

        // trigger system catalog cache
        if (columnStore)
          gwi.csc->columnRIDs(
              make_table(table_ptr->db.str, table_ptr->table_name.str, lower_case_table_names), true);

        string table_name = table_ptr->table_name.str;

        // @bug5523
        if (table_ptr->db.length && strcmp(table_ptr->db.str, "information_schema") == 0)
          table_name =
              (table_ptr->schema_table_name.length ? table_ptr->schema_table_name.str : table_ptr->alias.str);

        CalpontSystemCatalog::TableAliasName tn =
            make_aliasview(table_ptr->db.str, table_name, table_ptr->alias.str, viewName, columnStore,
                           lower_case_table_names);
        gwi.tbList.push_back(tn);
        CalpontSystemCatalog::TableAliasName tan = make_aliastable(
            table_ptr->db.str, table_name, table_ptr->alias.str, columnStore, lower_case_table_names);
        gwi.tableMap[tan] = make_pair(0, table_ptr);
#ifdef DEBUG_WALK_COND
        cerr << tn << endl;
#endif
      }
    }

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }
  }
  catch (IDBExcept& ie)
  {
    setError(gwi.thd, ER_INTERNAL_ERROR, ie.what(), gwi);
    CalpontSystemCatalog::removeCalpontSystemCatalog(gwi.sessionid);
    // @bug 3852. set error status for gwi.
    gwi.fatalParseError = true;
    gwi.parseErrorText = ie.what();
    return ER_INTERNAL_ERROR;
  }
  catch (...)
  {
    string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
    // @bug3852 set error status for gwi.
    gwi.fatalParseError = true;
    gwi.parseErrorText = emsg;
    setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
    CalpontSystemCatalog::removeCalpontSystemCatalog(gwi.sessionid);
    return ER_INTERNAL_ERROR;
  }

  csep->tableList(gwi.tbList);

  // Send this recursively to getSelectPlan
  bool unionSel = false;
  // UNION master unit check
  // Existed pushdown handlers won't get in this scope
  // except UNION pushdown that is to come.
  // is_unit_op() give a segv for derived_handler's SELECT_LEX
  if (!isUnion && select_lex.master_unit()->is_unit_op())
  {
    // MCOL-2178 isUnion member only assigned, never used
    // MIGR::infinidb_vtable.isUnion = true;
    CalpontSelectExecutionPlan::SelectList unionVec;
    SELECT_LEX* select_cursor = select_lex.master_unit()->first_select();
    unionSel = true;
    uint8_t distUnionNum = 0;

    for (SELECT_LEX* sl = select_cursor; sl; sl = sl->next_select())
    {
      SCSEP plan(new CalpontSelectExecutionPlan());
      plan->txnID(csep->txnID());
      plan->verID(csep->verID());
      plan->sessionID(csep->sessionID());
      plan->traceFlags(csep->traceFlags());
      plan->data(csep->data());

      // gwi for the union unit
      gp_walk_info union_gwi(gwi.timeZone);
      union_gwi.thd = gwi.thd;
      uint32_t err = 0;

      if ((err = getSelectPlan(union_gwi, *sl, plan, unionSel)) != 0)
        return err;

      unionVec.push_back(SCEP(plan));

      // distinct union num
      if (sl == select_lex.master_unit()->union_distinct)
        distUnionNum = unionVec.size();
    }

    csep->unionVec(unionVec);
    csep->distinctUnionNum(distUnionNum);
  }

  return 0;
}

/*@brief  Process WHERE part of the query or sub-query      */
/***********************************************************
 * DESCRIPTION:
 *  This function processes conditions from either JOIN->conds
 *  or SELECT_LEX->where|prep_where
 * RETURNS
 *  error id as an int
 ***********************************************************/
int processWhere(SELECT_LEX& select_lex, gp_walk_info& gwi, SCSEP& csep, const std::vector<COND*>& condStack)
{
  JOIN* join = select_lex.join;
  Item* icp = 0;
  bool isUpdateDelete = false;

  // Flag to indicate if this is a prepared statement
  bool isPS = gwi.thd->stmt_arena && gwi.thd->stmt_arena->is_stmt_execute();

  if (join != 0 && !isPS)
    icp = join->conds;
  else if (isPS && select_lex.prep_where)
    icp = select_lex.prep_where;

  // if icp is null, try to find the where clause other where
  if (!join && gwi.thd->lex->derived_tables)
  {
    if (select_lex.prep_where)
      icp = select_lex.prep_where;
    else if (select_lex.where)
      icp = select_lex.where;
  }
  else if (!join && isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
  {
    isUpdateDelete = true;
  }

  if (icp)
  {
    // MariaDB bug 624 - without the fix_fields call, delete with join may error with "No query step".
    //@bug 3039. fix fields for constants
    if (!icp->fixed())
    {
      icp->fix_fields(gwi.thd, (Item**)&icp);
    }

    gwi.fatalParseError = false;
#ifdef DEBUG_WALK_COND
    std::cerr << "------------------ WHERE -----------------------" << std::endl;
    icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
    std::cerr << "------------------------------------------------\n" << std::endl;
#endif

    icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

    if (gwi.fatalParseError)
    {
      return setErrorAndReturn(gwi);
    }
  }
  else if (isUpdateDelete)
  {
    // MCOL-4023 For updates/deletes, we iterate over the pushed down condStack
    if (!condStack.empty())
    {
      std::vector<COND*>::const_iterator condStackIter = condStack.begin();

      while (condStackIter != condStack.end())
      {
        COND* cond = *condStackIter++;

        cond->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

        if (gwi.fatalParseError)
        {
          return setErrorAndReturn(gwi);
        }
      }
    }
    // if condStack is empty(), check the select_lex for where conditions
    // as a last resort
    else if ((icp = select_lex.where) != 0)
    {
      icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

      if (gwi.fatalParseError)
      {
        return setErrorAndReturn(gwi);
      }
    }
  }
  else if (join && join->zero_result_cause)
  {
    gwi.rcWorkStack.push(new ConstantColumn((int64_t)0, ConstantColumn::NUM));
    (dynamic_cast<ConstantColumn*>(gwi.rcWorkStack.top()))->timeZone(gwi.timeZone);
  }

  for (Item* item : gwi.condList)
  {
    if (item && (item != icp))
    {
      item->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

      if (gwi.fatalParseError)
      {
        return setErrorAndReturn(gwi);
      }
    }
  }

  // ZZ - the followinig debug shows the structure of nested outer join. should
  // use a recursive function.
#ifdef OUTER_JOIN_DEBUG
  List<TABLE_LIST>* tables = &(select_lex.top_join_list);
  List_iterator_fast<TABLE_LIST> ti(*tables);

  TABLE_LIST** table = (TABLE_LIST**)gwi.thd->alloc(sizeof(TABLE_LIST*) * tables->elements);
  for (TABLE_LIST** t = table + (tables->elements - 1); t >= table; t--)
    *t = ti++;

  DBUG_ASSERT(tables->elements >= 1);

  TABLE_LIST** end = table + tables->elements;
  for (TABLE_LIST** tbl = table; tbl < end; tbl++)
  {
    TABLE_LIST* curr;

    while ((curr = ti++))
    {
      TABLE_LIST* curr = *tbl;

      if (curr->table_name.length)
        cerr << curr->table_name.str << " ";
      else
        cerr << curr->alias.str << endl;

      if (curr->outer_join)
        cerr << " is inner table" << endl;
      else if (curr->straight)
        cerr << "straight_join" << endl;
      else
        cerr << "join" << endl;

      if (curr->nested_join)
      {
        List<TABLE_LIST>* inners = &(curr->nested_join->join_list);
        List_iterator_fast<TABLE_LIST> li(*inners);
        TABLE_LIST** inner = (TABLE_LIST**)gwi.thd->alloc(sizeof(TABLE_LIST*) * inners->elements);

        for (TABLE_LIST** t = inner + (inners->elements - 1); t >= inner; t--)
          *t = li++;

        TABLE_LIST** end1 = inner + inners->elements;

        for (TABLE_LIST** tb = inner; tb < end1; tb++)
        {
          TABLE_LIST* curr1 = *tb;
          cerr << curr1->alias.str << endl;

          if (curr1->sj_on_expr)
          {
            curr1->sj_on_expr->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
          }
        }
      }

      if (curr->sj_on_expr)
      {
        curr->sj_on_expr->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
      }
    }
  }
#endif

  uint32_t failed = 0;

  // InfiniDB bug5764 requires outer joins to be appended to the
  // end of the filter list. This causes outer join filters to
  // have a higher join id than inner join filters.
  // TODO MCOL-4680 Figure out why this is the case, and possibly
  // eliminate this requirement.
  std::stack<execplan::ParseTree*> outerJoinStack;

  if ((failed = buildJoin(gwi, select_lex.top_join_list, outerJoinStack)))
    return failed;

  if (gwi.subQuery)
  {
    for (uint i = 0; i < gwi.viewList.size(); i++)
    {
      if ((failed = gwi.viewList[i]->processJoin(gwi, outerJoinStack)))
        return failed;
    }
  }

  ParseTree* filters = NULL;
  ParseTree* outerJoinFilters = NULL;
  ParseTree* ptp = NULL;
  ParseTree* rhs = NULL;

  // @bug 2932. for "select * from region where r_name" case. if icp not null and
  // ptWorkStack empty, the item is in rcWorkStack.
  // MySQL 5.6 (MariaDB?). when icp is null and zero_result_cause is set, a constant 0
  // is pushed to rcWorkStack.
  if (gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
  {
    filters = new ParseTree(gwi.rcWorkStack.top());
    gwi.rcWorkStack.pop();
  }

  while (!gwi.ptWorkStack.empty())
  {
    filters = gwi.ptWorkStack.top();
    gwi.ptWorkStack.pop();

    if (gwi.ptWorkStack.empty())
      break;

    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(filters);
    rhs = gwi.ptWorkStack.top();
    gwi.ptWorkStack.pop();
    ptp->right(rhs);
    gwi.ptWorkStack.push(ptp);
  }

  while (!outerJoinStack.empty())
  {
    outerJoinFilters = outerJoinStack.top();
    outerJoinStack.pop();

    if (outerJoinStack.empty())
      break;

    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(outerJoinFilters);
    rhs = outerJoinStack.top();
    outerJoinStack.pop();
    ptp->right(rhs);
    outerJoinStack.push(ptp);
  }

  // Append outer join filters at the end of inner join filters.
  // JLF_ExecPlanToJobList::walkTree processes ParseTree::left
  // before ParseTree::right which is what we intend to do in the
  // below.
  if (filters && outerJoinFilters)
  {
    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(filters);
    ptp->right(outerJoinFilters);
    filters = ptp;
  }
  else if (outerJoinFilters)
  {
    filters = outerJoinFilters;
  }

  if (filters)
  {
    csep->filters(filters);
    std::string aTmpDir(startup::StartUp::tmpDir());
    aTmpDir = aTmpDir + "/filter1.dot";
    filters->drawTree(aTmpDir);
  }

  return 0;
}

/*@brief  Process LIMIT part of a query or sub-query      */
/***********************************************************
 * DESCRIPTION:
 *  Processes LIMIT and OFFSET parts
 * RETURNS
 *  error id as an int
 ***********************************************************/
int processLimitAndOffset(SELECT_LEX& select_lex, gp_walk_info& gwi, SCSEP& csep, bool unionSel, bool isUnion,
                          bool isSelectHandlerTop)
{
  // LIMIT processing part
  uint64_t limitNum = std::numeric_limits<uint64_t>::max();

  // non-MAIN union branch
  if (unionSel || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
  {
    /* Consider the following query:
       "select a from t1 where exists (select b from t2 where a=b);"
       CS first builds a hash table for t2, then pushes down the hash to
       PrimProc for a distributed hash join execution, with t1 being the
       large-side table. However, the server applies an optimization in
       Item_exists_subselect::fix_length_and_dec in sql/item_subselect.cc
       (see server commit ae476868a5394041a00e75a29c7d45917e8dfae8)
       where it sets explicit_limit to true, which causes csep->limitNum set to 1.
       This causes the hash table for t2 to only contain a single record for the
       hash join, giving less number of rows in the output result set than expected.
       We therefore do not allow limit set to 1 here for such queries.
    */
    if (gwi.subSelectType != CalpontSelectExecutionPlan::IN_SUBS &&
        gwi.subSelectType != CalpontSelectExecutionPlan::EXISTS_SUBS &&
        select_lex.master_unit()->global_parameters()->limit_params.explicit_limit)
    {
      if (select_lex.master_unit()->global_parameters()->limit_params.offset_limit)
      {
        Item_int* offset =
            (Item_int*)select_lex.master_unit()->global_parameters()->limit_params.offset_limit;
        csep->limitStart(offset->val_int());
      }

      if (select_lex.master_unit()->global_parameters()->limit_params.select_limit)
      {
        Item_int* select =
            (Item_int*)select_lex.master_unit()->global_parameters()->limit_params.select_limit;
        csep->limitNum(select->val_int());
        // MCOL-894 Activate parallel ORDER BY
        csep->orderByThreads(get_orderby_threads(gwi.thd));
      }
    }
  }
  // union with explicit select at the top level
  else if (isUnion && select_lex.limit_params.explicit_limit)
  {
    if (select_lex.braces)
    {
      if (select_lex.limit_params.offset_limit)
        csep->limitStart(((Item_int*)select_lex.limit_params.offset_limit)->val_int());

      if (select_lex.limit_params.select_limit)
        csep->limitNum(((Item_int*)select_lex.limit_params.select_limit)->val_int());
    }
  }
  // other types of queries that have explicit LIMIT
  else if (select_lex.limit_params.explicit_limit)
  {
    uint32_t limitOffset = 0;

    if (select_lex.join)
    {
      JOIN* join = select_lex.join;

      // @bug5729. After upgrade, join->unit sometimes is uninitialized pointer
      // (not null though) and will cause seg fault. Prefer checking
      // select_lex->limit_params.offset_limit if not null.
      if (join->select_lex && join->select_lex->limit_params.offset_limit &&
          join->select_lex->limit_params.offset_limit->fixed() &&
          join->select_lex->limit_params.select_limit && join->select_lex->limit_params.select_limit->fixed())
      {
        limitOffset = join->select_lex->limit_params.offset_limit->val_int();
        limitNum = join->select_lex->limit_params.select_limit->val_int();
      }
      else if (join->unit)
      {
        limitOffset = join->unit->lim.get_offset_limit();
        limitNum = join->unit->lim.get_select_limit() - limitOffset;
      }
    }
    else
    {
      if (select_lex.master_unit()->global_parameters()->limit_params.offset_limit)
      {
        Item_int* offset =
            (Item_int*)select_lex.master_unit()->global_parameters()->limit_params.offset_limit;
        limitOffset = offset->val_int();
      }

      if (select_lex.master_unit()->global_parameters()->limit_params.select_limit)
      {
        Item_int* select =
            (Item_int*)select_lex.master_unit()->global_parameters()->limit_params.select_limit;
        limitNum = select->val_int();
      }
    }

    csep->limitStart(limitOffset);
    csep->limitNum(limitNum);
  }
  // If an explicit limit is not specified, use the system variable value
  else
  {
    csep->limitNum(gwi.thd->variables.select_limit);
  }

  // We don't currently support limit with correlated subquery
  if (csep->limitNum() != (uint64_t)-1 && gwi.subQuery && !gwi.correlatedTbNameVec.empty())
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_LIMIT_SUB);
    setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  return 0;
}

/*@brief Create in-to-exists predicate for an IN subquery   */
/***********************************************************
 * DESCRIPTION:
 * This function processes the lhs and rhs of an IN predicate
 * for a query such as:
 * select col1 from t1 where col2 in (select col2' from t2);
 * here, lhs is col2 and rhs is the in subquery "select col2' from t2".
 * It creates a new predicate of the form "col2=col2'" which then later
 * gets injected into the execution plan of the subquery.
 * If lhs is of type Item::ROW_ITEM instead, such as:
 * select col1 from t1 where (col2,col3) in (select col2',col3' from t2);
 * the function builds an "and" filter of the form "col2=col2' and col3=col3'".
 * RETURNS
 *  none
 ***********************************************************/
void buildInToExistsFilter(gp_walk_info& gwi, SELECT_LEX& select_lex)
{
  RowColumn* rlhs = dynamic_cast<RowColumn*>(gwi.inSubQueryLHS);

  size_t additionalRetColsBefore = gwi.additionalRetCols.size();

  if (rlhs)
  {
    idbassert(gwi.inSubQueryLHSItem->type() == Item::ROW_ITEM);

    Item_row* row = (Item_row*)gwi.inSubQueryLHSItem;

    idbassert(!rlhs->columnVec().empty() && (rlhs->columnVec().size() == gwi.returnedCols.size()) &&
              row->cols() && (row->cols() == select_lex.item_list.elements) &&
              (row->cols() == gwi.returnedCols.size()));

    List_iterator_fast<Item> it(select_lex.item_list);
    Item* item;

    int i = 0;

    ParseTree* rowFilter = nullptr;

    while ((item = it++))
    {
      boost::shared_ptr<Operator> sop(new PredicateOperator("="));
      vector<Item*> itemList = {row->element_index(i), item};
      ReturnedColumn* rhs = gwi.returnedCols[i]->clone();

      buildEqualityPredicate(rlhs->columnVec()[i]->clone(), rhs, &gwi, sop, Item_func::EQ_FUNC, itemList,
                             true);

      if (gwi.fatalParseError)
      {
        delete rlhs;
        return;
      }

      ParseTree* tmpFilter = nullptr;

      if (!gwi.ptWorkStack.empty())
      {
        tmpFilter = gwi.ptWorkStack.top();
        gwi.ptWorkStack.pop();
      }

      if (i == 0 && tmpFilter)
      {
        rowFilter = tmpFilter;
      }
      else if (i != 0 && tmpFilter && rowFilter)
      {
        ParseTree* ptp = new ParseTree(new LogicOperator("and"));
        ptp->left(rowFilter);
        ptp->right(tmpFilter);
        rowFilter = ptp;
      }

      i++;
    }

    delete rlhs;

    if (rowFilter)
      gwi.ptWorkStack.push(rowFilter);
  }
  else
  {
    idbassert((gwi.returnedCols.size() == 1) && (select_lex.item_list.elements == 1));

    boost::shared_ptr<Operator> sop(new PredicateOperator("="));
    vector<Item*> itemList = {gwi.inSubQueryLHSItem, select_lex.item_list.head()};
    ReturnedColumn* rhs = gwi.returnedCols[0]->clone();
    buildEqualityPredicate(gwi.inSubQueryLHS, rhs, &gwi, sop, Item_func::EQ_FUNC, itemList, true);

    if (gwi.fatalParseError)
      return;
  }

  size_t additionalRetColsAdded = gwi.additionalRetCols.size() - additionalRetColsBefore;

  if (gwi.returnedCols.size() && (gwi.returnedCols.size() == additionalRetColsAdded))
  {
    for (size_t i = 0; i < gwi.returnedCols.size(); i++)
    {
      gwi.returnedCols[i]->expressionId(gwi.additionalRetCols[additionalRetColsBefore + i]->expressionId());
      gwi.returnedCols[i]->colSource(gwi.additionalRetCols[additionalRetColsBefore + i]->colSource());
    }

    // Delete the duplicate copy of the returned cols
    auto iter = gwi.additionalRetCols.begin();
    std::advance(iter, additionalRetColsBefore);
    gwi.additionalRetCols.erase(iter, gwi.additionalRetCols.end());
  }
}

/*@brief  Translates SELECT_LEX into CSEP                  */
/***********************************************************
 * DESCRIPTION:
 *  This function takes SELECT_LEX and tries to produce
 *  a corresponding CSEP out of it. It is made of parts that
 *  process parts of the query, e.g. FROM, WHERE, SELECT,
 *  HAVING, GROUP BY, ORDER BY. FROM, WHERE, LIMIT are processed
 *  by corresponding methods. CS calls getSelectPlan()
 *  recursively to process subqueries.
 * ARGS
 *  isUnion if true CS processes UNION unit now
 *  isSelectHandlerTop removes offset at the top of SH query.
 * RETURNS
 *  error id as an int
 ***********************************************************/
int getSelectPlan(gp_walk_info& gwi, SELECT_LEX& select_lex, SCSEP& csep, bool isUnion,
                  bool isSelectHandlerTop, const std::vector<COND*>& condStack)
{
#ifdef DEBUG_WALK_COND
  cerr << "getSelectPlan()" << endl;
#endif
  int rc = 0;
  // rollup is currently not supported
  if (select_lex.olap == ROLLUP_TYPE)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_ROLLUP_NOT_SUPPORT);
    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  setExecutionParams(gwi, csep);

  gwi.subSelectType = csep->subType();
  uint32_t sessionID = csep->sessionID();
  gwi.sessionid = sessionID;
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);
  csep->timeZone(gwi.timeZone);
  gwi.csc = csc;

  CalpontSelectExecutionPlan::SelectList derivedTbList;
  // @bug 1796. Remember table order on the FROM list.
  gwi.clauseType = FROM;
  if ((rc = processFrom(isUnion, select_lex, gwi, csep)))
  {
    return rc;
  }

  bool unionSel = (!isUnion && select_lex.master_unit()->is_unit_op()) ? true : false;

  gwi.clauseType = WHERE;
  if ((rc = processWhere(select_lex, gwi, csep, condStack)))
  {
    return rc;
  }

  gwi.clauseType = SELECT;
#ifdef DEBUG_WALK_COND
  {
    cerr << "------------------- SELECT --------------------" << endl;
    List_iterator_fast<Item> it(select_lex.item_list);
    Item* item;

    while ((item = it++))
    {
      debug_walk(item, 0);
    }

    cerr << "-----------------------------------------------\n" << endl;
  }
#endif

  // populate returnedcolumnlist and columnmap
  List_iterator_fast<Item> it(select_lex.item_list);
  Item* item;
  vector<Item_field*> funcFieldVec;

  // empty rcWorkStack and ptWorkStack. They should all be empty by now.
  clearStacks(gwi);

  // indicate the starting pos of scalar returned column, because some join column
  // has been inserted to the returned column list.
  if (gwi.subQuery)
  {
    ScalarSub* scalar = dynamic_cast<ScalarSub*>(gwi.subQuery);

    if (scalar)
      scalar->returnedColPos(gwi.additionalRetCols.size());
  }

  CalpontSelectExecutionPlan::SelectList selectSubList;

  while ((item = it++))
  {
    string itemAlias = (item->name.length ? item->name.str : "<NULL>");

    // @bug 5916. Need to keep checking until getting concret item in case
    // of nested view.
    while (item->type() == Item::REF_ITEM)
    {
      Item_ref* ref = (Item_ref*)item;
      item = (*(ref->ref));
    }

    Item::Type itype = item->type();

    switch (itype)
    {
      case Item::FIELD_ITEM:
      {
        Item_field* ifp = (Item_field*)item;
        SimpleColumn* sc = NULL;

        if (ifp->field_name.length && string(ifp->field_name.str) == "*")
        {
          collectAllCols(gwi, ifp);
          break;
        }

        sc = buildSimpleColumn(ifp, gwi);

        if (sc)
        {
          boost::shared_ptr<SimpleColumn> spsc(sc);

          string fullname;
          String str;
          ifp->print(&str, QT_ORDINARY);
          fullname = str.c_ptr();

          if (!ifp->is_explicit_name())  // no alias
          {
            sc->alias(fullname);
          }
          else  // alias
          {
            if (!itemAlias.empty())
              sc->alias(itemAlias);
          }

          gwi.returnedCols.push_back(spsc);

          gwi.columnMap.insert(
              CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), spsc));
          TABLE_LIST* tmp = 0;

          if (ifp->cached_table)
            tmp = ifp->cached_table;

          gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(),
                                       sc->isColumnStore())] = make_pair(1, tmp);
        }
        else
        {
          setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
          delete sc;
          return ER_INTERNAL_ERROR;
        }

        break;
      }

      // aggregate column
      case Item::SUM_FUNC_ITEM:
      {
        ReturnedColumn* ac = buildAggregateColumn(item, gwi);

        if (gwi.fatalParseError)
        {
          // e.g., non-support ref column
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          delete ac;
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        // add this agg col to returnedColumnList
        boost::shared_ptr<ReturnedColumn> spac(ac);
        gwi.returnedCols.push_back(spac);
        break;
      }

      case Item::FUNC_ITEM:
      {
        Item_func* ifp = reinterpret_cast<Item_func*>(item);

        // @bug4383. error out non-support stored function
        if (ifp->functype() == Item_func::FUNC_SP)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SP_FUNCTION_NOT_SUPPORT);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        if (string(ifp->func_name()) == "xor")
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        uint16_t parseInfo = 0;
        vector<Item_field*> tmpVec;
        bool hasNonSupportItem = false;
        parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo, &gwi);

        if (ifp->with_subquery() || string(ifp->func_name()) == string("<in_optimizer>") ||
            ifp->functype() == Item_func::NOT_ALL_FUNC || parseInfo & SUB_BIT)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        // if "IN" or "BETWEEN" are in the SELECT clause, build function column
        string funcName = ifp->func_name();
        ReturnedColumn* rc;
        if (funcName == "in" || funcName == " IN " || funcName == "between")
        {
          rc = buildFunctionColumn(ifp, gwi, hasNonSupportItem, true);
        }
        else
        {
          rc = buildFunctionColumn(ifp, gwi, hasNonSupportItem);
        }

        SRCP srcp(rc);

        if (rc)
        {
          // MCOL-2178 CS has to process determenistic functions with constant arguments.
          if (!hasNonSupportItem && ifp->const_item() && !(parseInfo & AF_BIT) && tmpVec.size() == 0)
          {
            srcp.reset(buildReturnedColumn(item, gwi, gwi.fatalParseError));
            gwi.returnedCols.push_back(srcp);

            if (ifp->name.length)
              srcp->alias(ifp->name.str);

            continue;
          }

          gwi.returnedCols.push_back(srcp);
        }
        else  // This was a vtable post-process block
        {
          hasNonSupportItem = false;
          uint32_t before_size = funcFieldVec.size();
          parse_item(ifp, funcFieldVec, hasNonSupportItem, parseInfo, &gwi);
          uint32_t after_size = funcFieldVec.size();

          // pushdown handler projection functions
          // @bug3881. set_user_var can not be treated as constant function
          // @bug5716. Try to avoid post process function for union query.
          if (!hasNonSupportItem && (after_size - before_size) == 0 && !(parseInfo & AGG_BIT) &&
              !(parseInfo & SUB_BIT))
          {
            ConstantColumn* cc = buildConstantColumnMaybeNullUsingValStr(ifp, gwi);

            SRCP srcp(cc);

            if (ifp->name.length)
              cc->alias(ifp->name.str);

            gwi.returnedCols.push_back(srcp);

            // clear the error set by buildFunctionColumn
            gwi.fatalParseError = false;
            gwi.parseErrorText = "";
            break;
          }
          else if (hasNonSupportItem || parseInfo & AGG_BIT || parseInfo & SUB_BIT ||
                   (gwi.fatalParseError && gwi.subQuery))
          {
            if (gwi.parseErrorText.empty())
            {
              Message::Args args;
              args.add(ifp->func_name());
              gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
            }

            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }
          else if (gwi.subQuery && (isPredicateFunction(ifp, &gwi) || ifp->type() == Item::COND_ITEM))
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }

          //@Bug 3030 Add error check for dml statement
          if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
          {
            if (after_size - before_size != 0)
            {
              gwi.parseErrorText = ifp->func_name();
              return -1;
            }
          }
          else
          {
            Message::Args args;
            args.add(ifp->func_name());
            gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }
        }

        break;
      }  // End of FUNC_ITEM

      // DRRTUY Replace the whole section with typeid() checks or use
      // reinterpret_cast here
      case Item::CONST_ITEM:
      {
        switch (item->cmp_type())
        {
          case INT_RESULT:
          case STRING_RESULT:
          case DECIMAL_RESULT:
          case REAL_RESULT:
          case TIME_RESULT:
          {
            if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
            {
            }
            else
            {
              // do not push the dummy column (mysql added) to returnedCol
              if (item->name.length && string(item->name.str) == "Not_used")
                continue;

              // @bug3509. Constant column is sent to ExeMgr now.
              SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));

              if (item->name.length)
                srcp->alias(item->name.str);

              gwi.returnedCols.push_back(srcp);
            }

            break;
          }
          // MCOL-2178 This switch doesn't handl
          // ROW_
          default:
          {
            IDEBUG(cerr << "Warning unsupported cmp_type() in projection" << endl);
            // noop
          }
        }
        break;
      }  // CONST_ITEM ends here

      case Item::NULL_ITEM:
      {
        if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
        {
        }
        else
        {
          SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
          gwi.returnedCols.push_back(srcp);

          if (item->name.length)
            srcp->alias(item->name.str);
        }

        break;
      }

      case Item::SUBSELECT_ITEM:
      {
        Item_subselect* sub = (Item_subselect*)item;

        if (sub->substype() != Item_subselect::SINGLEROW_SUBS)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

#ifdef DEBUG_WALK_COND
        cerr << "SELECT clause SUBSELECT Item: " << sub->substype() << endl;
        JOIN* join = sub->get_select_lex()->join;

        if (join)
        {
          Item_cond* cond = reinterpret_cast<Item_cond*>(join->conds);

          if (cond)
            cond->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
        }

        cerr << "Finish SELECT clause subselect item traversing" << endl;
#endif
        SelectSubQuery* selectSub = new SelectSubQuery(gwi, sub);
        // selectSub->gwip(&gwi);
        SCSEP ssub = selectSub->transform();

        if (!ssub || gwi.fatalParseError)
        {
          if (gwi.parseErrorText.empty())
            gwi.parseErrorText = "Unsupported Item in SELECT subquery.";

          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        selectSubList.push_back(ssub);
        SimpleColumn* rc = new SimpleColumn();
        rc->colSource(rc->colSource() | SELECT_SUB);
        rc->timeZone(gwi.timeZone);

        if (sub->get_select_lex()->get_table_list())
        {
          rc->viewName(getViewName(sub->get_select_lex()->get_table_list()), lower_case_table_names);
        }
        if (sub->name.length)
          rc->alias(sub->name.str);

        gwi.returnedCols.push_back(SRCP(rc));

        break;
      }

      case Item::COND_ITEM:
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      case Item::EXPR_CACHE_ITEM:
      {
        printf("EXPR_CACHE_ITEM in getSelectPlan\n");
        gwi.fatalParseError = true;
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL);
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      case Item::WINDOW_FUNC_ITEM:
      {
        SRCP srcp(buildWindowFunctionColumn(item, gwi, gwi.fatalParseError));

        if (!srcp || gwi.fatalParseError)
        {
          if (gwi.parseErrorText.empty())
            gwi.parseErrorText = "Unsupported Item in SELECT subquery.";

          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        gwi.returnedCols.push_back(srcp);
        break;
      }
      case Item::TYPE_HOLDER:
      {
        if (!gwi.tbList.size())
        {
          gwi.parseErrorText = "subquery with VALUES";
          gwi.fatalParseError = true;
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }
        else
        {
          std::cerr << "********** received TYPE_HOLDER *********" << std::endl;
        }
        break;
      }

      default: 
      {
        break;
      }
    }
  }

  // @bug4388 normalize the project coltypes for union main select list
  if (!csep->unionVec().empty())
  {
    for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
    {
      vector<CalpontSystemCatalog::ColType> coltypes;

      for (uint32_t j = 0; j < csep->unionVec().size(); j++)
      {
        coltypes.push_back(dynamic_cast<CalpontSelectExecutionPlan*>(csep->unionVec()[j].get())
                               ->returnedCols()[i]
                               ->resultType());

        // @bug5976. set hasAggregate true for the main column if
        // one corresponding union column has aggregate
        if (dynamic_cast<CalpontSelectExecutionPlan*>(csep->unionVec()[j].get())
                ->returnedCols()[i]
                ->hasAggregate())
          gwi.returnedCols[i]->hasAggregate(true);
      }

      gwi.returnedCols[i]->resultType(CalpontSystemCatalog::ColType::convertUnionColType(coltypes));
    }
  }

  // Having clause handling
  gwi.clauseType = HAVING;
  clearStacks(gwi);
  ParseTree* havingFilter = 0;
  // clear fatalParseError that may be left from post process functions
  gwi.fatalParseError = false;
  gwi.parseErrorText = "";

  if (select_lex.having != 0)
  {
    Item_cond* having = reinterpret_cast<Item_cond*>(select_lex.having);
#ifdef DEBUG_WALK_COND
    cerr << "------------------- HAVING ---------------------" << endl;
    having->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
    cerr << "------------------------------------------------\n" << endl;
#endif
    having->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }

    ParseTree* ptp = 0;
    ParseTree* rhs = 0;

    // @bug 4215. some function filter will be in the rcWorkStack.
    if (gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
    {
      havingFilter = new ParseTree(gwi.rcWorkStack.top());
      gwi.rcWorkStack.pop();
    }

    while (!gwi.ptWorkStack.empty())
    {
      havingFilter = gwi.ptWorkStack.top();
      gwi.ptWorkStack.pop();

      if (gwi.ptWorkStack.empty())
        break;

      ptp = new ParseTree(new LogicOperator("and"));
      ptp->left(havingFilter);
      rhs = gwi.ptWorkStack.top();
      gwi.ptWorkStack.pop();
      ptp->right(rhs);
      gwi.ptWorkStack.push(ptp);
    }
  }

  // MCOL-4617 If this is an IN subquery, then create the in-to-exists
  // predicate and inject it into the csep
  if (gwi.subQuery && gwi.subSelectType == CalpontSelectExecutionPlan::IN_SUBS && gwi.inSubQueryLHS &&
      gwi.inSubQueryLHSItem)
  {
    // create the predicate
    buildInToExistsFilter(gwi, select_lex);

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }

    // now inject the created predicate
    if (!gwi.ptWorkStack.empty())
    {
      ParseTree* inToExistsFilter = gwi.ptWorkStack.top();
      gwi.ptWorkStack.pop();

      if (havingFilter)
      {
        ParseTree* ptp = new ParseTree(new LogicOperator("and"));
        ptp->left(havingFilter);
        ptp->right(inToExistsFilter);
        havingFilter = ptp;
      }
      else
      {
        if (csep->filters())
        {
          ParseTree* ptp = new ParseTree(new LogicOperator("and"));
          ptp->left(csep->filters());
          ptp->right(inToExistsFilter);
          csep->filters(ptp);
        }
        else
        {
          csep->filters(inToExistsFilter);
        }
      }
    }
  }

  // for post process expressions on the select list
  // error out post process for union and sub select unit
  if (isUnion || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
  {
    if (funcFieldVec.size() != 0 && !gwi.fatalParseError)
    {
      string emsg("Fatal parse error in vtable mode: Unsupported Items in union or sub select unit");
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
      return ER_CHECK_NOT_IMPLEMENTED;
    }
  }

  for (uint32_t i = 0; i < funcFieldVec.size(); i++)
  {
    SimpleColumn* sc = buildSimpleColumn(funcFieldVec[i], gwi);

    if (!sc || gwi.fatalParseError)
    {
      string emsg;

      if (gwi.parseErrorText.empty())
      {
        emsg = "un-recognized column";

        if (funcFieldVec[i]->name.length)
          emsg += string(funcFieldVec[i]->name.str);
      }
      else
      {
        emsg = gwi.parseErrorText;
      }

      setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
      return ER_INTERNAL_ERROR;
    }

    String str;
    funcFieldVec[i]->print(&str, QT_ORDINARY);
    sc->alias(string(str.c_ptr()));
    sc->tableAlias(sc->tableAlias());
    SRCP srcp(sc);
    uint32_t j = 0;

    for (; j < gwi.returnedCols.size(); j++)
    {
      if (sc->sameColumn(gwi.returnedCols[j].get()))
      {
        SimpleColumn* field = dynamic_cast<SimpleColumn*>(gwi.returnedCols[j].get());

        if (field && field->alias() == sc->alias())
          break;
      }
    }

    if (j == gwi.returnedCols.size())
    {
      gwi.returnedCols.push_back(srcp);
      gwi.columnMap.insert(
          CalpontSelectExecutionPlan::ColumnMap::value_type(string(funcFieldVec[i]->field_name.str), srcp));

      string fullname;
      fullname = str.c_ptr();
      TABLE_LIST* tmp = (funcFieldVec[i]->cached_table ? funcFieldVec[i]->cached_table : 0);
      gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(),
                                   sc->isColumnStore())] = make_pair(1, tmp);
    }
  }

  SRCP minSc;  // min width projected column. for count(*) use

  // Group by list. not valid for union main query
  if (!unionSel)
  {
    gwi.clauseType = GROUP_BY;
    Item* nonSupportItem = NULL;
    ORDER* groupcol = reinterpret_cast<ORDER*>(select_lex.group_list.first);

    // check if window functions are in order by. InfiniDB process order by list if
    // window functions are involved, either in order by or projection.
    bool hasWindowFunc = gwi.hasWindowFunc;
    gwi.hasWindowFunc = false;

    for (; groupcol; groupcol = groupcol->next)
    {
      if ((*(groupcol->item))->type() == Item::WINDOW_FUNC_ITEM)
        gwi.hasWindowFunc = true;
    }

    if (gwi.hasWindowFunc)
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_WF_NOT_ALLOWED, "GROUP BY clause");
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

    gwi.hasWindowFunc = hasWindowFunc;
    groupcol = reinterpret_cast<ORDER*>(select_lex.group_list.first);

    for (; groupcol; groupcol = groupcol->next)
    {
      Item* groupItem = *(groupcol->item);

      // @bug5993. Could be nested ref.
      while (groupItem->type() == Item::REF_ITEM)
        groupItem = (*((Item_ref*)groupItem)->ref);

      if (groupItem->type() == Item::FUNC_ITEM)
      {
        Item_func* ifp = (Item_func*)groupItem;

        // call buildFunctionColumn here mostly for finding out
        // non-support column on GB list. Should be simplified.
        ReturnedColumn* fc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);

        if (!fc || gwi.fatalParseError)
        {
          nonSupportItem = ifp;
          break;
        }

        if (groupcol->in_field_list && groupcol->counter_used)
        {
          delete fc;
          fc = gwi.returnedCols[groupcol->counter - 1].get();
          SRCP srcp(fc->clone());

          // check if no column parm
          for (uint32_t i = 0; i < gwi.no_parm_func_list.size(); i++)
          {
            if (gwi.no_parm_func_list[i]->expressionId() == fc->expressionId())
            {
              gwi.no_parm_func_list.push_back(dynamic_cast<FunctionColumn*>(srcp.get()));
              break;
            }
          }

          srcp->orderPos(groupcol->counter - 1);
          gwi.groupByCols.push_back(srcp);
          continue;
        }
        else if (groupItem->is_explicit_name())  // alias
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (string(groupItem->name.str) == gwi.returnedCols[i]->alias())
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              delete fc;
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            nonSupportItem = groupItem;
            break;
          }
        }
        else
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (fc->operator==(gwi.returnedCols[i].get()))
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              delete fc;
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            gwi.groupByCols.push_back(SRCP(fc));
            break;
          }
        }
      }
      else if (groupItem->type() == Item::FIELD_ITEM)
      {
        Item_field* ifp = (Item_field*)groupItem;
        // this GB col could be an alias of F&E on the SELECT clause, not necessarily a field.
        ReturnedColumn* rc = buildSimpleColumn(ifp, gwi);
        SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

        for (uint32_t j = 0; j < gwi.returnedCols.size(); j++)
        {
          if (sc)
          {
            if (sc->sameColumn(gwi.returnedCols[j].get()))
            {
              sc->orderPos(j);
              break;
            }
            else if (strcasecmp(sc->alias().c_str(), gwi.returnedCols[j]->alias().c_str()) == 0)
            {
              rc = gwi.returnedCols[j].get()->clone();
              rc->orderPos(j);
              break;
            }
          }
          else
          {
            if (ifp->name.length && string(ifp->name.str) == gwi.returnedCols[j].get()->alias())
            {
              rc = gwi.returnedCols[j].get()->clone();
              rc->orderPos(j);
              break;
            }
          }
        }

        if (!rc)
        {
          nonSupportItem = ifp;
          break;
        }

        SRCP srcp(rc);

        // bug 3151
        AggregateColumn* ac = dynamic_cast<AggregateColumn*>(rc);

        if (ac)
        {
          nonSupportItem = ifp;
          break;
        }

        gwi.groupByCols.push_back(srcp);
        gwi.columnMap.insert(
            CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), srcp));
      }
      // @bug5638. The group by column is constant but not counter, alias has to match a column
      // on the select list
      else if (!groupcol->counter_used &&
               (groupItem->type() == Item::CONST_ITEM &&
                (groupItem->cmp_type() == INT_RESULT || groupItem->cmp_type() == STRING_RESULT ||
                 groupItem->cmp_type() == REAL_RESULT || groupItem->cmp_type() == DECIMAL_RESULT)))
      {
        ReturnedColumn* rc = 0;

        for (uint32_t j = 0; j < gwi.returnedCols.size(); j++)
        {
          if (groupItem->name.length && string(groupItem->name.str) == gwi.returnedCols[j].get()->alias())
          {
            rc = gwi.returnedCols[j].get()->clone();
            rc->orderPos(j);
            break;
          }
        }

        if (!rc)
        {
          nonSupportItem = groupItem;
          break;
        }

        gwi.groupByCols.push_back(SRCP(rc));
      }
      else if ((*(groupcol->item))->type() == Item::SUBSELECT_ITEM)
      {
        if (!groupcol->in_field_list || !groupItem->name.length)
        {
          nonSupportItem = groupItem;
        }
        else
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (string(groupItem->name.str) == gwi.returnedCols[i]->alias())
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            nonSupportItem = groupItem;
          }
        }
      }
      // @bug 3761.
      else if (groupcol->counter_used)
      {
        if (gwi.returnedCols.size() <= (uint32_t)(groupcol->counter - 1))
        {
          nonSupportItem = groupItem;
        }
        else
        {
          gwi.groupByCols.push_back(SRCP(gwi.returnedCols[groupcol->counter - 1]->clone()));
        }
      }
      else
      {
        nonSupportItem = groupItem;
      }
    }

    // @bug 4756. Add internal groupby column for correlated join to the groupby list
    if (gwi.aggOnSelect && !gwi.subGroupByCols.empty())
      gwi.groupByCols.insert(gwi.groupByCols.end(), gwi.subGroupByCols.begin(), gwi.subGroupByCols.end());

    // this is window func on SELECT becuase ORDER BY has not been processed
    if (!gwi.windowFuncList.empty() && !gwi.subGroupByCols.empty())
    {
      for (uint32_t i = 0; i < gwi.windowFuncList.size(); i++)
      {
        if (gwi.windowFuncList[i]->hasWindowFunc())
        {
          vector<WindowFunctionColumn*> windowFunctions = gwi.windowFuncList[i]->windowfunctionColumnList();

          for (uint32_t j = 0; j < windowFunctions.size(); j++)
            windowFunctions[j]->addToPartition(gwi.subGroupByCols);
        }
      }
    }

    if (nonSupportItem)
    {
      if (gwi.parseErrorText.length() == 0)
      {
        Message::Args args;
        if (nonSupportItem->name.length)
          args.add("'" + string(nonSupportItem->name.str) + "'");
        else
          args.add("");
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_GROUP_BY, args);
      }
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }
  }

  // ORDER BY processing
  {
    SQL_I_List<ORDER> order_list = select_lex.order_list;
    ORDER* ordercol = reinterpret_cast<ORDER*>(order_list.first);

    // check if window functions are in order by. InfiniDB process order by list if
    // window functions are involved, either in order by or projection.
    for (; ordercol; ordercol = ordercol->next)
    {
      if ((*(ordercol->item))->type() == Item::WINDOW_FUNC_ITEM)
        gwi.hasWindowFunc = true;
      // MCOL-2166 Looking for this sorting item in GROUP_BY items list.
      // Shouldn't look into this if query doesn't have GROUP BY or
      // aggregations
      if (select_lex.agg_func_used() && select_lex.group_list.first &&
          !sortItemIsInGrouping(*ordercol->item, select_lex.group_list.first))
      {
        std::ostringstream ostream;
        std::ostringstream& osr = ostream;
        getColNameFromItem(osr, *ordercol->item);
        Message::Args args;
        args.add(ostream.str());
        string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NOT_GROUPBY_EXPRESSION, args);
        gwi.parseErrorText = emsg;
        setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
        return ERR_NOT_GROUPBY_EXPRESSION;
      }
    }

    // re-visit the first of ordercol list
    ordercol = reinterpret_cast<ORDER*>(order_list.first);

    {
      for (; ordercol; ordercol = ordercol->next)
      {
        ReturnedColumn* rc = NULL;

        if (ordercol->in_field_list && ordercol->counter_used)
        {
          rc = gwi.returnedCols[ordercol->counter - 1]->clone();
          rc->orderPos(ordercol->counter - 1);
          // can not be optimized off if used in order by with counter.
          // set with self derived table alias if it's derived table
          gwi.returnedCols[ordercol->counter - 1]->incRefCount();
        }
        else
        {
          Item* ord_item = *(ordercol->item);

          // ignore not_used column on order by.
          if ((ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT) &&
              ord_item->full_name() && !strcmp(ord_item->full_name(), "Not_used"))
          {
            continue;
          }
          else if (ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT)
          {
            // DRRTUY This section looks useless b/c there is no
            // way to put constant INT into an ORDER BY list
            rc = gwi.returnedCols[((Item_int*)ord_item)->val_int() - 1]->clone();
          }
          else if (ord_item->type() == Item::SUBSELECT_ITEM)
          {
            gwi.fatalParseError = true;
          }
          else if ((ord_item->type() == Item::FUNC_ITEM) &&
                   (((Item_func*)ord_item)->functype() == Item_func::COLLATE_FUNC))
          {
            push_warning(gwi.thd, Sql_condition::WARN_LEVEL_NOTE, WARN_OPTION_IGNORED,
                         "COLLATE is ignored in ColumnStore");
            continue;
          }
          else
          {
            rc = buildReturnedColumn(ord_item, gwi, gwi.fatalParseError);
          }
          // @bug5501 try item_ptr if item can not be fixed. For some
          // weird dml statement state, item can not be fixed but the
          // infomation is available in item_ptr.
          if (!rc || gwi.fatalParseError)
          {
            Item* item_ptr = ordercol->item_ptr;

            while (item_ptr->type() == Item::REF_ITEM)
              item_ptr = *(((Item_ref*)item_ptr)->ref);

            rc = buildReturnedColumn(item_ptr, gwi, gwi.fatalParseError);
          }

          if (!rc)
          {
            string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
            gwi.parseErrorText = emsg;
            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, emsg, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }
        }

        if (ordercol->direction == ORDER::ORDER_ASC)
          rc->asc(true);
        else
          rc->asc(false);

        gwi.orderByCols.push_back(SRCP(rc));
      }
    }
    // make sure columnmap, returnedcols and count(*) arg_list are not empty
    TableMap::iterator tb_iter = gwi.tableMap.begin();

    try
    {
      for (; tb_iter != gwi.tableMap.end(); tb_iter++)
      {
        if ((*tb_iter).second.first == 1)
          continue;

        CalpontSystemCatalog::TableAliasName tan = (*tb_iter).first;
        CalpontSystemCatalog::TableName tn = make_table((*tb_iter).first.schema, (*tb_iter).first.table);
        SimpleColumn* sc = getSmallestColumn(csc, tn, tan, (*tb_iter).second.second->table, gwi);
        SRCP srcp(sc);
        gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
        (*tb_iter).second.first = 1;
      }
    }
    catch (runtime_error& e)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, e.what(), gwi);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      return ER_INTERNAL_ERROR;
    }
    catch (...)
    {
      string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
      setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      return ER_INTERNAL_ERROR;
    }

    if (!gwi.count_asterisk_list.empty() || !gwi.no_parm_func_list.empty() || gwi.returnedCols.empty())
    {
      // get the smallest column from colmap
      CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;
      int minColWidth = 0;
      CalpontSystemCatalog::ColType ct;

      try
      {
        for (iter = gwi.columnMap.begin(); iter != gwi.columnMap.end(); ++iter)
        {
          // should always not null
          SimpleColumn* sc = dynamic_cast<SimpleColumn*>(iter->second.get());

          if (sc && !(sc->joinInfo() & JOIN_CORRELATED))
          {
            ct = csc->colType(sc->oid());

            if (minColWidth == 0)
            {
              minColWidth = ct.colWidth;
              minSc = iter->second;
            }
            else if (ct.colWidth < minColWidth)
            {
              minColWidth = ct.colWidth;
              minSc = iter->second;
            }
          }
        }
      }
      catch (...)
      {
        string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
        setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
        CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
        return ER_INTERNAL_ERROR;
      }

      if (gwi.returnedCols.empty() && gwi.additionalRetCols.empty() && minSc)
        gwi.returnedCols.push_back(minSc);
    }

    // ORDER BY translation part
    if (!isUnion && !gwi.hasWindowFunc && gwi.subSelectType == CalpontSelectExecutionPlan::MAIN_SELECT)
    {
      {
        if (unionSel)
          order_list = select_lex.master_unit()->global_parameters()->order_list;

        ordercol = reinterpret_cast<ORDER*>(order_list.first);

        for (; ordercol; ordercol = ordercol->next)
        {
          Item* ord_item = *(ordercol->item);

          if (ord_item->name.length)
          {
            // for union order by 1 case. For unknown reason, it doesn't show in_field_list
            if (ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT)
            {
            }
            else if (ord_item->type() == Item::SUBSELECT_ITEM)
            {
            }
            else
            {
            }
          }
          else if (ord_item->type() == Item::FUNC_ITEM)
          {
            // @bug5636. check if this order by column is on the select list
            ReturnedColumn* rc = buildFunctionColumn((Item_func*)(ord_item), gwi, gwi.fatalParseError);

            for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
            {
              if (rc && rc->operator==(gwi.returnedCols[i].get()))
              {
                ostringstream oss;
                oss << i + 1;
                break;
              }
            }
          }
        }
      }

      if (gwi.orderByCols.size())  // has order by
      {
        csep->hasOrderBy(true);
        // To activate LimitedOrderBy
        csep->orderByThreads(get_orderby_threads(gwi.thd));
        csep->specHandlerProcessed(true);
      }
    }

    // We don't currently support limit with correlated subquery
    if ((rc = processLimitAndOffset(select_lex, gwi, csep, unionSel, isUnion, isSelectHandlerTop)))
    {
      return rc;
    }
  }  // ORDER BY end

  if (select_lex.options & SELECT_DISTINCT)
    csep->distinct(true);

  // add the smallest column to count(*) parm.
  // select constant in subquery case
  std::vector<AggregateColumn*>::iterator coliter;

  if (!minSc)
  {
    if (!gwi.returnedCols.empty())
      minSc = gwi.returnedCols[0];
    else if (!gwi.additionalRetCols.empty())
      minSc = gwi.additionalRetCols[0];
  }

  // @bug3523, count(*) on subquery always pick column[0].
  SimpleColumn* sc = dynamic_cast<SimpleColumn*>(minSc.get());

  if (sc && sc->schemaName().empty())
  {
    if (gwi.derivedTbList.size() >= 1)
    {
      SimpleColumn* sc1 = new SimpleColumn();
      sc1->columnName(sc->columnName());
      sc1->tableName(sc->tableName());
      sc1->tableAlias(sc->tableAlias());
      sc1->viewName(sc->viewName());
      sc1->colPosition(0);
      sc1->timeZone(gwi.timeZone);
      minSc.reset(sc1);
    }
  }

  for (coliter = gwi.count_asterisk_list.begin(); coliter != gwi.count_asterisk_list.end(); ++coliter)
  {
    // @bug5977 @note should never throw this, but checking just in case.
    // When ExeMgr fix is ready, this should not error out...
    if (dynamic_cast<AggregateColumn*>(minSc.get()))
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = "No project column found for aggregate function";
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

    // Replace the last (presumably constant) object with minSc
    if ((*coliter)->aggParms().empty())
    {
      (*coliter)->aggParms().push_back(minSc);
    }
    else
    {
      (*coliter)->aggParms()[0] = minSc;
    }
  }

  std::vector<FunctionColumn*>::iterator funciter;

  SPTP sptp(new ParseTree(minSc.get()->clone()));

  for (funciter = gwi.no_parm_func_list.begin(); funciter != gwi.no_parm_func_list.end(); ++funciter)
  {
    FunctionParm funcParms = (*funciter)->functionParms();
    funcParms.push_back(sptp);
    (*funciter)->functionParms(funcParms);
  }

  // set sequence# for subquery localCols
  for (uint32_t i = 0; i < gwi.localCols.size(); i++)
    gwi.localCols[i]->sequence(i);

  // append additionalRetCols to returnedCols
  gwi.returnedCols.insert(gwi.returnedCols.begin(), gwi.additionalRetCols.begin(),
                          gwi.additionalRetCols.end());

  csep->groupByCols(gwi.groupByCols);
  csep->orderByCols(gwi.orderByCols);
  csep->returnedCols(gwi.returnedCols);
  csep->columnMap(gwi.columnMap);
  csep->having(havingFilter);
  csep->derivedTableList(gwi.derivedTbList);
  csep->selectSubList(selectSubList);
  csep->subSelectList(gwi.subselectList);
  clearStacks(gwi);
  return 0;
}

int cp_get_table_plan(THD* thd, SCSEP& csep, cal_table_info& ti, long timeZone)
{
  gp_walk_info* gwi = ti.condInfo;

  if (!gwi)
    gwi = new gp_walk_info(timeZone);

  gwi->thd = thd;
  LEX* lex = thd->lex;
  idbassert(lex != 0);
  uint32_t sessionID = csep->sessionID();
  gwi->sessionid = sessionID;
  TABLE* table = ti.msTablePtr;
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);

  // get all columns that mysql needs to fetch
  MY_BITMAP* read_set = table->read_set;
  Field **f_ptr, *field;
  gwi->columnMap.clear();

  for (f_ptr = table->field; (field = *f_ptr); f_ptr++)
  {
    if (bitmap_is_set(read_set, field->field_index))
    {
      SimpleColumn* sc =
          new SimpleColumn(table->s->db.str, table->s->table_name.str, field->field_name.str, sessionID);
      string alias(table->alias.c_ptr());
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(alias);
      }
      sc->tableAlias(alias);
      sc->timeZone(gwi->timeZone);
      assert(sc);
      boost::shared_ptr<SimpleColumn> spsc(sc);
      gwi->returnedCols.push_back(spsc);
      gwi->columnMap.insert(
          CalpontSelectExecutionPlan::ColumnMap::value_type(string(field->field_name.str), spsc));
    }
  }

  if (gwi->columnMap.empty())
  {
    CalpontSystemCatalog::TableName tn = make_table(table->s->db.str, table->s->table_name.str);
    CalpontSystemCatalog::TableAliasName tan =
        make_aliastable(table->s->db.str, table->s->table_name.str, table->alias.c_ptr());
    SimpleColumn* sc = getSmallestColumn(csc, tn, tan, table, *gwi);
    SRCP srcp(sc);
    gwi->columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
    gwi->returnedCols.push_back(srcp);
  }

  // get filter
  if (ti.condInfo)
  {
    gp_walk_info* gwi = ti.condInfo;
    ParseTree* filters = 0;
    ParseTree* ptp = 0;
    ParseTree* rhs = 0;

    while (!gwi->ptWorkStack.empty())
    {
      filters = gwi->ptWorkStack.top();
      gwi->ptWorkStack.pop();
      SimpleFilter* sf = dynamic_cast<SimpleFilter*>(filters->data());

      if (sf && sf->op()->data() == "noop")
      {
        delete filters;
        filters = 0;

        if (gwi->ptWorkStack.empty())
          break;

        continue;
      }

      if (gwi->ptWorkStack.empty())
        break;

      ptp = new ParseTree(new LogicOperator("and"));
      ptp->left(filters);
      rhs = gwi->ptWorkStack.top();
      gwi->ptWorkStack.pop();
      ptp->right(rhs);
      gwi->ptWorkStack.push(ptp);
    }

    csep->filters(filters);
  }

  csep->returnedCols(gwi->returnedCols);
  csep->columnMap(gwi->columnMap);
  CalpontSelectExecutionPlan::TableList tblist;
  tblist.push_back(make_aliastable(table->s->db.str, table->s->table_name.str, table->alias.c_ptr(), true,
                                   lower_case_table_names));
  csep->tableList(tblist);

  // @bug 3321. Set max number of blocks in a dictionary file to be scanned for filtering
  csep->stringScanThreshold(get_string_scan_threshold(gwi->thd));

  return 0;
}

int cp_get_group_plan(THD* thd, SCSEP& csep, cal_impl_if::cal_group_info& gi)
{
  SELECT_LEX* select_lex = gi.groupByTables->select_lex;
  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
  gp_walk_info gwi(timeZoneOffset);
  gwi.thd = thd;
  gwi.isGroupByHandler = true;
  int status = getGroupPlan(gwi, *select_lex, csep, gi);

#ifdef DEBUG_WALK_COND
  cerr << "---------------- cp_get_group_plan EXECUTION PLAN ----------------" << endl;
  cerr << *csep << endl;
  cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
#endif

  if (status > 0)
    return ER_INTERNAL_ERROR;
  else if (status < 0)
    return status;
  // Derived table projection and filter optimization.
  derivedTableOptimization(&gwi, csep);

  return 0;
}

int cs_get_derived_plan(ha_columnstore_derived_handler* handler, THD* thd, SCSEP& csep, gp_walk_info& gwi)
{
  SELECT_LEX& select_lex = *handler->select;
  int status = getSelectPlan(gwi, select_lex, csep, false);

  if (status > 0)
    return ER_INTERNAL_ERROR;
  else if (status < 0)
    return status;

#ifdef DEBUG_WALK_COND
  cerr << "---------------- cs_get_derived_plan EXECUTION PLAN ----------------" << endl;
  cerr << *csep << endl;
  cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
#endif
  // Derived table projection and filter optimization.
  derivedTableOptimization(&gwi, csep);
  return 0;
}

int cs_get_select_plan(ha_columnstore_select_handler* handler, THD* thd, SCSEP& csep, gp_walk_info& gwi)
{
  SELECT_LEX& select_lex = *handler->select;

  if (select_lex.where)
  {
    gwi.condList.push_back(select_lex.where);
  }

  buildTableOnExprList(&select_lex.top_join_list, gwi.tableOnExprList);

  convertOuterJoinToInnerJoin(&select_lex.top_join_list, gwi.tableOnExprList, gwi.condList,
                              handler->tableOuterJoinMap);

  int status = getSelectPlan(gwi, select_lex, csep, false, true);

  if (status > 0)
    return ER_INTERNAL_ERROR;
  else if (status < 0)
    return status;

#ifdef DEBUG_WALK_COND
  cerr << "---------------- cs_get_select_plan EXECUTION PLAN ----------------" << endl;
  cerr << *csep << endl;
  cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
#endif
  // Derived table projection and filter optimization.
  derivedTableOptimization(&gwi, csep);

  return 0;
}

/*@brief  buildConstColFromFilter- change SimpleColumn into ConstColumn*/
/***********************************************************
 * DESCRIPTION:
 * Server could optimize out fields from GROUP BY list, when certain
 * filter predicate is used, e.g.
 * field = 'AIR', field IN ('AIR'). This utility function tries to
 * replace such fields with ConstantColumns using cond_pushed filters.
 * TBD Take into account that originalSC SimpleColumn could be:
 * SimpleColumn, ArithmeticColumn, FunctionColumn.
 * PARAMETERS:
 *    originalSC    SimpleColumn* removed field
 *    gwi           main strucutre
 *    gi            auxilary group_by handler structure
 * RETURNS
 *  ConstantColumn* if originalSC equals with cond_pushed columns.
 *  NULL otherwise
 ***********************************************************/
ConstantColumn* buildConstColFromFilter(SimpleColumn* originalSC, gp_walk_info& gwi, cal_group_info& gi)
{
  execplan::SimpleColumn* simpleCol;
  execplan::ConstantColumn* constCol;
  execplan::SOP op;
  execplan::SimpleFilter* simpFilter;
  execplan::ConstantColumn* result = NULL;
  std::vector<ParseTree*>::iterator ptIt = gi.pushedPts.begin();

  for (; ptIt != gi.pushedPts.end(); ptIt++)
  {
    simpFilter = dynamic_cast<execplan::SimpleFilter*>((*ptIt)->data());

    if (simpFilter == NULL)
      continue;

    simpleCol = dynamic_cast<execplan::SimpleColumn*>(simpFilter->lhs());
    constCol = dynamic_cast<execplan::ConstantColumn*>(simpFilter->rhs());

    if (simpleCol == NULL || constCol == NULL)
      continue;

    op = simpFilter->op();
    execplan::ReturnedColumn* rc = dynamic_cast<execplan::ReturnedColumn*>(simpleCol);

    // The filter could have any kind of op
    if (originalSC->sameColumn(rc))
    {
#ifdef DEBUG_WALK_COND
      cerr << "buildConstColFromFilter() replaced " << endl;
      cerr << simpleCol->toString() << endl;
      cerr << " with " << endl;
      cerr << constCol << endl;
#endif
      result = constCol;
    }
  }

  return result;
}

int getGroupPlan(gp_walk_info& gwi, SELECT_LEX& select_lex, SCSEP& csep, cal_group_info& gi, bool isUnion)
{
#ifdef DEBUG_WALK_COND
  cerr << "getGroupPlan()" << endl;
#endif

  // rollup is currently not supported
  if (select_lex.olap == ROLLUP_TYPE)
  {
    gwi.fatalParseError = true;
    gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_ROLLUP_NOT_SUPPORT);
    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  gwi.internalDecimalScale = (get_use_decimal_scale(gwi.thd) ? get_decimal_scale(gwi.thd) : -1);
  gwi.subSelectType = csep->subType();

  JOIN* join = select_lex.join;
  Item_cond* icp = 0;

  if (gi.groupByWhere)
    icp = reinterpret_cast<Item_cond*>(gi.groupByWhere);

  uint32_t sessionID = csep->sessionID();
  gwi.sessionid = sessionID;
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);
  gwi.csc = csc;

  // @bug 2123. Override large table estimate if infinidb_ordered hint was used.
  // @bug 2404. Always override if the infinidb_ordered_only variable is turned on.
  if (get_ordered_only(gwi.thd))
    csep->overrideLargeSideEstimate(true);

  // @bug 5741. Set a flag when in Local PM only query mode
  csep->localQuery(get_local_query(gwi.thd));

  // @bug 3321. Set max number of blocks in a dictionary file to be scanned for filtering
  csep->stringScanThreshold(get_string_scan_threshold(gwi.thd));

  csep->stringTableThreshold(get_stringtable_threshold(gwi.thd));

  csep->djsSmallSideLimit(get_diskjoin_smallsidelimit(gwi.thd) * 1024ULL * 1024);
  csep->djsLargeSideLimit(get_diskjoin_largesidelimit(gwi.thd) * 1024ULL * 1024);
  csep->djsPartitionSize(get_diskjoin_bucketsize(gwi.thd) * 1024ULL * 1024);

  if (get_um_mem_limit(gwi.thd) == 0)
    csep->umMemLimit(numeric_limits<int64_t>::max());
  else
    csep->umMemLimit(get_um_mem_limit(gwi.thd) * 1024ULL * 1024);

  // populate table map and trigger syscolumn cache for all the tables (@bug 1637).
  // all tables on FROM list must have at least one col in colmap
  TABLE_LIST* table_ptr = gi.groupByTables;
  CalpontSelectExecutionPlan::SelectList derivedTbList;

// DEBUG
#ifdef DEBUG_WALK_COND
  List_iterator<TABLE_LIST> sj_list_it(select_lex.sj_nests);
  TABLE_LIST* sj_nest;

  while ((sj_nest = sj_list_it++))
  {
    cerr << sj_nest->db.str << "." << sj_nest->table_name.str << endl;
  }

#endif

  // @bug 1796. Remember table order on the FROM list.
  gwi.clauseType = FROM;

  try
  {
    for (; table_ptr; table_ptr = table_ptr->next_local)
    {
      // mysql put vtable here for from sub. we ignore it
      // if (string(table_ptr->table_name).find("$vtable") != string::npos)
      //    continue;

      // Until we handle recursive cte:
      // Checking here ensures we catch all with clauses in the query.
      if (table_ptr->is_recursive_with_table())
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = "Recursive CTE";
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      string viewName = getViewName(table_ptr);
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(viewName);
      }

      // @todo process from subquery
      if (table_ptr->derived)
      {
        String str;
        (table_ptr->derived->first_select())->print(gwi.thd, &str, QT_ORDINARY);

        SELECT_LEX* select_cursor = table_ptr->derived->first_select();
        // Use Pushdown handler for subquery processing
        FromSubQuery fromSub(gwi, select_cursor);
        string alias(table_ptr->alias.str);
        if (lower_case_table_names)
        {
          boost::algorithm::to_lower(alias);
        }
        fromSub.alias(alias);

        CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, viewName);
        // @bug 3852. check return execplan
        SCSEP plan = fromSub.transform();

        if (!plan)
        {
          setError(gwi.thd, ER_INTERNAL_ERROR, fromSub.gwip().parseErrorText, gwi);
          CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
          return ER_INTERNAL_ERROR;
        }

        gwi.derivedTbList.push_back(plan);
        gwi.tbList.push_back(tn);
        CalpontSystemCatalog::TableAliasName tan = make_aliastable("", alias, alias);
        gwi.tableMap[tan] = make_pair(0, table_ptr);
        // MCOL-2178 isUnion member only assigned, never used
        // MIGR::infinidb_vtable.isUnion = true; //by-pass the 2nd pass of rnd_init
      }
      else if (table_ptr->view)
      {
        View* view = new View(*table_ptr->view->first_select_lex(), &gwi);
        CalpontSystemCatalog::TableAliasName tn = make_aliastable(
            table_ptr->db.str, table_ptr->table_name.str, table_ptr->alias.str, true, lower_case_table_names);
        view->viewName(tn);
        gwi.viewList.push_back(view);
        view->transform();
      }
      else
      {
        // check foreign engine tables
        bool columnStore = (table_ptr->table ? isMCSTable(table_ptr->table) : true);

        // trigger system catalog cache
        if (columnStore)
          csc->columnRIDs(make_table(table_ptr->db.str, table_ptr->table_name.str, lower_case_table_names),
                          true);

        string table_name = table_ptr->table_name.str;

        // @bug5523
        if (table_ptr->db.length && strcmp(table_ptr->db.str, "information_schema") == 0)
          table_name =
              (table_ptr->schema_table_name.length ? table_ptr->schema_table_name.str : table_ptr->alias.str);

        CalpontSystemCatalog::TableAliasName tn =
            make_aliasview(table_ptr->db.str, table_name, table_ptr->alias.str, viewName, columnStore,
                           lower_case_table_names);
        gwi.tbList.push_back(tn);
        CalpontSystemCatalog::TableAliasName tan = make_aliastable(
            table_ptr->db.str, table_name, table_ptr->alias.str, columnStore, lower_case_table_names);
        gwi.tableMap[tan] = make_pair(0, table_ptr);
#ifdef DEBUG_WALK_COND
        cerr << tn << endl;
#endif
      }
    }

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }
  }
  catch (IDBExcept& ie)
  {
    setError(gwi.thd, ER_INTERNAL_ERROR, ie.what(), gwi);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    // @bug 3852. set error status for gwi.
    gwi.fatalParseError = true;
    gwi.parseErrorText = ie.what();
    return ER_INTERNAL_ERROR;
  }
  catch (...)
  {
    string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
    // @bug3852 set error status for gwi.
    gwi.fatalParseError = true;
    gwi.parseErrorText = emsg;
    setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return ER_INTERNAL_ERROR;
  }

  csep->tableList(gwi.tbList);

  bool unionSel = false;

  gwi.clauseType = WHERE;

  if (icp)
  {
    // MCOL-1052 The condition could be useless.
    // MariaDB bug 624 - without the fix_fields call, delete with join may error with "No query step".
    //#if MYSQL_VERSION_ID < 50172
    //@bug 3039. fix fields for constants
    if (!icp->fixed())
    {
      icp->fix_fields(gwi.thd, (Item**)&icp);
    }

    //#endif
    gwi.fatalParseError = false;
#ifdef DEBUG_WALK_COND
    cerr << "------------------ WHERE -----------------------" << endl;
    icp->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
    cerr << "------------------------------------------------\n" << endl;
#endif

    icp->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

    if (gwi.fatalParseError)
    {
      // if this is dervied table process phase, mysql may have not developed the plan
      // completely. Do not error and eventually mysql will call JOIN::exec() again.
      // related to bug 2922. Need to find a way to skip calling rnd_init for derived table
      // processing.
      if (gwi.thd->derived_tables_processing)
      {
        // MCOL-2178 isUnion member only assigned, never used
        // MIGR::infinidb_vtable.isUnion = false;
        return -1;
      }

      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }
  }
  else if (join && join->zero_result_cause)
  {
    gwi.rcWorkStack.push(new ConstantColumn((int64_t)0, ConstantColumn::NUM));
    (dynamic_cast<ConstantColumn*>(gwi.rcWorkStack.top()))->timeZone(gwi.timeZone);
  }

  SELECT_LEX tmp_select_lex;
  tmp_select_lex.table_list.first = gi.groupByTables;

  // InfiniDB bug5764 requires outer joins to be appended to the
  // end of the filter list. This causes outer join filters to
  // have a higher join id than inner join filters.
  // TODO MCOL-4680 Figure out why this is the case, and possibly
  // eliminate this requirement.
  std::stack<execplan::ParseTree*> outerJoinStack;

  uint32_t failed = buildJoin(gwi, tmp_select_lex.top_join_list, outerJoinStack);

  if (failed)
    return failed;

  if (gwi.subQuery)
  {
    for (uint i = 0; i < gwi.viewList.size(); i++)
    {
      failed = gwi.viewList[i]->processJoin(gwi, outerJoinStack);

      if (failed)
        break;
    }
  }

  if (failed != 0)
    return failed;

  ParseTree* filters = NULL;
  ParseTree* outerJoinFilters = NULL;
  ParseTree* ptp = NULL;
  ParseTree* rhs = NULL;

  // @bug 2932. for "select * from region where r_name" case. if icp not null and
  // ptWorkStack empty, the item is in rcWorkStack.
  // MySQL 5.6 (MariaDB?). when icp is null and zero_result_cause is set, a constant 0
  // is pushed to rcWorkStack.
  if (/*icp && */ gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
  {
    filters = new ParseTree(gwi.rcWorkStack.top());
    gwi.rcWorkStack.pop();
  }

  while (!gwi.ptWorkStack.empty())
  {
    filters = gwi.ptWorkStack.top();
    gwi.ptWorkStack.pop();

    if (gwi.ptWorkStack.empty())
      break;

    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(filters);
    rhs = gwi.ptWorkStack.top();
    gwi.ptWorkStack.pop();
    ptp->right(rhs);
    gwi.ptWorkStack.push(ptp);
  }

  while (!outerJoinStack.empty())
  {
    outerJoinFilters = outerJoinStack.top();
    outerJoinStack.pop();

    if (outerJoinStack.empty())
      break;

    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(outerJoinFilters);
    rhs = outerJoinStack.top();
    outerJoinStack.pop();
    ptp->right(rhs);
    outerJoinStack.push(ptp);
  }

  // Append outer join filters at the end of inner join filters.
  // JLF_ExecPlanToJobList::walkTree processes ParseTree::left
  // before ParseTree::right which is what we intend to do in the
  // below.
  if (filters && outerJoinFilters)
  {
    ptp = new ParseTree(new LogicOperator("and"));
    ptp->left(filters);
    ptp->right(outerJoinFilters);
    filters = ptp;
  }
  else if (outerJoinFilters)
  {
    filters = outerJoinFilters;
  }

  if (filters)
  {
    csep->filters(filters);
#ifdef DEBUG_WALK_COND
    std::string aTmpDir(startup::StartUp::tmpDir());
    aTmpDir = aTmpDir + "/filter1.dot";
    filters->drawTree(aTmpDir);
#endif
  }

  gwi.clauseType = SELECT;
#ifdef DEBUG_WALK_COND
  {
    cerr << "------------------- SELECT --------------------" << endl;
    List_iterator_fast<Item> it(*gi.groupByFields);
    Item* item;

    while ((item = it++))
    {
      debug_walk(item, 0);
    }

    cerr << "-----------------------------------------------\n" << endl;
  }
#endif

  // populate returnedcolumnlist and columnmap
  List_iterator_fast<Item> it(*gi.groupByFields);
  Item* item;
  vector<Item_field*> funcFieldVec;
  bool redo = false;

  // empty rcWorkStack and ptWorkStack. They should all be empty by now.
  clearStacks(gwi);

  // indicate the starting pos of scalar returned column, because some join column
  // has been inserted to the returned column list.
  if (gwi.subQuery)
  {
    ScalarSub* scalar = dynamic_cast<ScalarSub*>(gwi.subQuery);

    if (scalar)
      scalar->returnedColPos(gwi.additionalRetCols.size());
  }

  CalpontSelectExecutionPlan::SelectList selectSubList;

  while ((item = it++))
  {
    string itemAlias;
    if (item->name.length)
      itemAlias = (item->name.str);
    else
    {
      itemAlias = "<NULL>";
    }

    // @bug 5916. Need to keep checking until getting concret item in case
    // of nested view.
    while (item->type() == Item::REF_ITEM)
    {
      Item_ref* ref = (Item_ref*)item;
      item = (*(ref->ref));
    }

    Item::Type itype = item->type();

    switch (itype)
    {
      case Item::FIELD_ITEM:
      {
        Item_field* ifp = (Item_field*)item;
        SimpleColumn* sc = NULL;
        ConstantColumn* constCol = NULL;

        if (ifp->field_name.length && string(ifp->field_name.str) == "*")
        {
          collectAllCols(gwi, ifp);
          break;
        }

        sc = buildSimpleColumn(ifp, gwi);

        if (sc)
        {
          constCol = buildConstColFromFilter(sc, gwi, gi);
          boost::shared_ptr<ConstantColumn> spcc(constCol);
          boost::shared_ptr<SimpleColumn> spsc(sc);

          string fullname;
          String str;
          ifp->print(&str, QT_ORDINARY);
          fullname = str.c_ptr();

          if (!ifp->is_explicit_name())  // no alias
          {
            sc->alias(fullname);
          }
          else  // alias
          {
            if (!itemAlias.empty())
              sc->alias(itemAlias);
          }

          // MCOL-1052 Replace SimpleColumn with ConstantColumn,
          // since it must have a single value only.
          if (constCol)
          {
            gwi.returnedCols.push_back(spcc);
            gwi.columnMap.insert(
                CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), spcc));
          }
          else
          {
            gwi.returnedCols.push_back(spsc);
            gwi.columnMap.insert(
                CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), spsc));
          }

          TABLE_LIST* tmp = 0;

          if (ifp->cached_table)
            tmp = ifp->cached_table;

          gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(),
                                       sc->isColumnStore())] = make_pair(1, tmp);
        }
        else
        {
          setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
          delete sc;
          return ER_INTERNAL_ERROR;
        }

        break;
      }

      // aggregate column
      case Item::SUM_FUNC_ITEM:
      {
        ReturnedColumn* ac = buildAggregateColumn(item, gwi);

        if (gwi.fatalParseError)
        {
          // e.g., non-support ref column
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          delete ac;
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        // add this agg col to returnedColumnList
        boost::shared_ptr<ReturnedColumn> spac(ac);
        gwi.returnedCols.push_back(spac);
        // This item could be used in projection or HAVING later.
        gwi.extSelAggColsItems.push_back(item);

        break;
      }

      case Item::FUNC_ITEM:
      {
        Item_func* ifp = reinterpret_cast<Item_func*>(item);

        // @bug4383. error out non-support stored function
        if (ifp->functype() == Item_func::FUNC_SP)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_SP_FUNCTION_NOT_SUPPORT);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        if (string(ifp->func_name()) == "xor")
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        uint16_t parseInfo = 0;
        vector<Item_field*> tmpVec;
        bool hasNonSupportItem = false;
        parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo, &gwi);

        if (ifp->with_subquery() || string(ifp->func_name()) == string("<in_optimizer>") ||
            ifp->functype() == Item_func::NOT_ALL_FUNC || parseInfo & SUB_BIT)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        ReturnedColumn* rc = buildFunctionColumn(ifp, gwi, hasNonSupportItem, true);
        SRCP srcp(rc);

        if (rc)
        {
          if (!hasNonSupportItem && !nonConstFunc(ifp) && !(parseInfo & AF_BIT) && tmpVec.size() == 0)
          {
            if (isUnion || unionSel || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT ||
                parseInfo & SUB_BIT)  //|| select_lex.group_list.elements != 0)
            {
              srcp.reset(buildReturnedColumn(item, gwi, gwi.fatalParseError));
              gwi.returnedCols.push_back(srcp);

              if (ifp->name.length)
                srcp->alias(ifp->name.str);

              continue;
            }

            break;
          }

          gwi.returnedCols.push_back(srcp);
        }
        else  // InfiniDB Non support functions still go through post process for now
        {
          hasNonSupportItem = false;
          uint32_t before_size = funcFieldVec.size();
          // MCOL-1510 Use gwi pointer here to catch funcs with
          // not supported aggregate args in projections,
          // e.g. NOT(SUM(i)).
          parse_item(ifp, funcFieldVec, hasNonSupportItem, parseInfo, &gwi);
          uint32_t after_size = funcFieldVec.size();

          // group by func and func in subquery can not be post processed
          // @bug3881. set_user_var can not be treated as constant function
          // @bug5716. Try to avoid post process function for union query.
          if ((gwi.subQuery /*|| select_lex.group_list.elements != 0 */ || !csep->unionVec().empty() ||
               isUnion) &&
              !hasNonSupportItem && (after_size - before_size) == 0 && !(parseInfo & AGG_BIT) &&
              !(parseInfo & SUB_BIT))
          {
            ConstantColumn* cc = buildConstantColumnMaybeNullUsingValStr(ifp, gwi);

            SRCP srcp(cc);

            if (ifp->name.length)
              cc->alias(ifp->name.str);

            gwi.returnedCols.push_back(srcp);

            // clear the error set by buildFunctionColumn
            gwi.fatalParseError = false;
            gwi.parseErrorText = "";
            break;
          }
          else if (hasNonSupportItem || parseInfo & AGG_BIT || parseInfo & SUB_BIT ||
                   (gwi.fatalParseError && gwi.subQuery))
          {
            if (gwi.parseErrorText.empty())
            {
              Message::Args args;
              args.add(ifp->func_name());
              gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORTED_FUNCTION, args);
            }

            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }
          else if (gwi.subQuery && (isPredicateFunction(ifp, &gwi) || ifp->type() == Item::COND_ITEM))
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
            setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
            return ER_CHECK_NOT_IMPLEMENTED;
          }

          //@Bug 3030 Add error check for dml statement
          if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
          {
            if (after_size - before_size != 0)
            {
              gwi.parseErrorText = ifp->func_name();
              return -1;
            }
          }
          else
          {
            // clear the error set by buildFunctionColumn
            gwi.fatalParseError = false;
            gwi.parseErrorText = "";
          }
        }

        break;
      }

      // DRRTUY Replace the whole section with typeid() checks or use
      // reinterpret_cast here
      case Item::CONST_ITEM:
      {
        switch (item->cmp_type())
        {
          case INT_RESULT:
          case STRING_RESULT:
          case DECIMAL_RESULT:
          case REAL_RESULT:
          case TIME_RESULT:
          {
            if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
            {
            }
            else
            {
              // do not push the dummy column (mysql added) to returnedCol
              if (item->name.length && string(item->name.str) == "Not_used")
                continue;

              // @bug3509. Constant column is sent to ExeMgr now.
              SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));

              if (item->name.length)
                srcp->alias(item->name.str);

              gwi.returnedCols.push_back(srcp);
            }

            break;
          }

          // MCOL-2178 This switch doesn't handl
          // ROW_
          default:
          {
            IDEBUG(cerr << "Warning unsupported cmp_type() in projection" << endl);
          }
        }
        break;
      }  // CONST_ITEM ends here

      case Item::NULL_ITEM:
      {
        if (isUpdateOrDeleteStatement(gwi.thd->lex->sql_command))
        {
        }
        else
        {
          SRCP srcp(buildReturnedColumn(item, gwi, gwi.fatalParseError));
          gwi.returnedCols.push_back(srcp);

          if (item->name.length)
            srcp->alias(item->name.str);
        }

        break;
      }

      case Item::SUBSELECT_ITEM:
      {
        Item_subselect* sub = (Item_subselect*)item;

        if (sub->substype() != Item_subselect::SINGLEROW_SUBS)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SELECT_SUB);
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

#ifdef DEBUG_WALK_COND
        cerr << "SELECT clause SUBSELECT Item: " << sub->substype() << endl;
        JOIN* join = sub->get_select_lex()->join;

        if (join)
        {
          Item_cond* cond = reinterpret_cast<Item_cond*>(join->conds);

          if (cond)
            cond->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
        }

        cerr << "Finish SELECT clause subselect item traversing" << endl;
#endif
        SelectSubQuery* selectSub = new SelectSubQuery(gwi, sub);
        // selectSub->gwip(&gwi);
        SCSEP ssub = selectSub->transform();

        if (!ssub || gwi.fatalParseError)
        {
          if (gwi.parseErrorText.empty())
            gwi.parseErrorText = "Unsupported Item in SELECT subquery.";

          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        selectSubList.push_back(ssub);
        SimpleColumn* rc = new SimpleColumn();
        rc->colSource(rc->colSource() | SELECT_SUB);
        rc->timeZone(gwi.timeZone);

        if (sub->get_select_lex()->get_table_list())
        {
          rc->viewName(getViewName(sub->get_select_lex()->get_table_list()), lower_case_table_names);
        }
        if (sub->name.length)
          rc->alias(sub->name.str);

        gwi.returnedCols.push_back(SRCP(rc));

        break;
      }

      case Item::COND_ITEM:
      {
        gwi.fatalParseError = true;
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_FILTER_COND_EXP);
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      case Item::EXPR_CACHE_ITEM:
      {
        printf("EXPR_CACHE_ITEM in getSelectPlan\n");
        gwi.fatalParseError = true;
        gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL);
        setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
        return ER_CHECK_NOT_IMPLEMENTED;
      }

      case Item::WINDOW_FUNC_ITEM:
      {
        SRCP srcp(buildWindowFunctionColumn(item, gwi, gwi.fatalParseError));

        if (!srcp || gwi.fatalParseError)
        {
          if (gwi.parseErrorText.empty())
            gwi.parseErrorText = "Unsupported Item in SELECT subquery.";

          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }

        gwi.returnedCols.push_back(srcp);
        break;
      }

      default:
      {
        break;
      }
    }
  }

  // @bug4388 normalize the project coltypes for union main select list
  if (!csep->unionVec().empty())
  {
    for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
    {
      vector<CalpontSystemCatalog::ColType> coltypes;

      for (uint32_t j = 0; j < csep->unionVec().size(); j++)
      {
        coltypes.push_back(dynamic_cast<CalpontSelectExecutionPlan*>(csep->unionVec()[j].get())
                               ->returnedCols()[i]
                               ->resultType());

        // @bug5976. set hasAggregate true for the main column if
        // one corresponding union column has aggregate
        if (dynamic_cast<CalpontSelectExecutionPlan*>(csep->unionVec()[j].get())
                ->returnedCols()[i]
                ->hasAggregate())
          gwi.returnedCols[i]->hasAggregate(true);
      }

      gwi.returnedCols[i]->resultType(CalpontSystemCatalog::ColType::convertUnionColType(coltypes));
    }
  }

  // Having clause handling
  gwi.clauseType = HAVING;
  clearStacks(gwi);
  ParseTree* havingFilter = 0;
  // clear fatalParseError that may be left from post process functions
  gwi.fatalParseError = false;
  gwi.parseErrorText = "";

  if (gi.groupByHaving != 0)
  {
    Item_cond* having = reinterpret_cast<Item_cond*>(gi.groupByHaving);
#ifdef DEBUG_WALK_COND
    cerr << "------------------- HAVING ---------------------" << endl;
    having->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
    cerr << "------------------------------------------------\n" << endl;
#endif
    having->traverse_cond(gp_walk, &gwi, Item::POSTFIX);

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_INTERNAL_ERROR;
    }

    ParseTree* ptp = 0;
    ParseTree* rhs = 0;

    // @bug 4215. some function filter will be in the rcWorkStack.
    if (gwi.ptWorkStack.empty() && !gwi.rcWorkStack.empty())
    {
      havingFilter = new ParseTree(gwi.rcWorkStack.top());
      gwi.rcWorkStack.pop();
    }

    while (!gwi.ptWorkStack.empty())
    {
      havingFilter = gwi.ptWorkStack.top();
      gwi.ptWorkStack.pop();

      if (gwi.ptWorkStack.empty())
        break;

      ptp = new ParseTree(new LogicOperator("and"));
      ptp->left(havingFilter);
      rhs = gwi.ptWorkStack.top();
      gwi.ptWorkStack.pop();
      ptp->right(rhs);
      gwi.ptWorkStack.push(ptp);
    }
  }

  // for post process expressions on the select list
  // error out post process for union and sub select unit
  if (isUnion || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)
  {
    if (funcFieldVec.size() != 0 && !gwi.fatalParseError)
    {
      string emsg("Fatal parse error in vtable mode: Unsupported Items in union or sub select unit");
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
      return ER_CHECK_NOT_IMPLEMENTED;
    }
  }

  for (uint32_t i = 0; i < funcFieldVec.size(); i++)
  {
    SimpleColumn* sc = buildSimpleColumn(funcFieldVec[i], gwi);

    if (!sc || gwi.fatalParseError)
    {
      string emsg;

      if (gwi.parseErrorText.empty())
      {
        emsg = "un-recognized column";

        if (funcFieldVec[i]->name.length)
          emsg += string(funcFieldVec[i]->name.str);
      }
      else
      {
        emsg = gwi.parseErrorText;
      }

      setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
      return ER_INTERNAL_ERROR;
    }

    String str;
    funcFieldVec[i]->print(&str, QT_ORDINARY);
    sc->alias(string(str.c_ptr()));
    // sc->tableAlias(funcFieldVec[i]->table_name);
    sc->tableAlias(sc->alias());
    SRCP srcp(sc);
    uint32_t j = 0;

    for (; j < gwi.returnedCols.size(); j++)
    {
      if (sc->sameColumn(gwi.returnedCols[j].get()))
      {
        SimpleColumn* field = dynamic_cast<SimpleColumn*>(gwi.returnedCols[j].get());

        if (field && field->alias() == sc->alias())
          break;
      }
    }

    if (j == gwi.returnedCols.size())
    {
      gwi.returnedCols.push_back(srcp);
      gwi.columnMap.insert(
          CalpontSelectExecutionPlan::ColumnMap::value_type(string(funcFieldVec[i]->field_name.str), srcp));

      string fullname;
      fullname = str.c_ptr();
      TABLE_LIST* tmp = (funcFieldVec[i]->cached_table ? funcFieldVec[i]->cached_table : 0);
      gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(),
                                   sc->isColumnStore())] = make_pair(1, tmp);
    }
  }

  // post-process Order by list and expressions on select by redo phase1. only for vtable
  // ignore ORDER BY clause for union select unit
  string ord_cols = "";  // for normal select phase
  SRCP minSc;            // min width projected column. for count(*) use

  // Group by list. not valid for union main query
  if (!unionSel)
  {
    gwi.clauseType = GROUP_BY;
    Item* nonSupportItem = NULL;
    ORDER* groupcol = reinterpret_cast<ORDER*>(gi.groupByGroup);

    // check if window functions are in order by. InfiniDB process order by list if
    // window functions are involved, either in order by or projection.
    bool hasWindowFunc = gwi.hasWindowFunc;
    gwi.hasWindowFunc = false;

    for (; groupcol; groupcol = groupcol->next)
    {
      if ((*(groupcol->item))->type() == Item::WINDOW_FUNC_ITEM)
        gwi.hasWindowFunc = true;
    }

    if (gwi.hasWindowFunc)
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_WF_NOT_ALLOWED, "GROUP BY clause");
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

    gwi.hasWindowFunc = hasWindowFunc;
    groupcol = reinterpret_cast<ORDER*>(gi.groupByGroup);

    for (; groupcol; groupcol = groupcol->next)
    {
      Item* groupItem = *(groupcol->item);

      // @bug5993. Could be nested ref.
      while (groupItem->type() == Item::REF_ITEM)
        groupItem = (*((Item_ref*)groupItem)->ref);

      if (groupItem->type() == Item::FUNC_ITEM)
      {
        Item_func* ifp = (Item_func*)groupItem;

        // call buildFunctionColumn here mostly for finding out
        // non-support column on GB list. Should be simplified.
        ReturnedColumn* fc = buildFunctionColumn(ifp, gwi, gwi.fatalParseError);

        if (!fc || gwi.fatalParseError)
        {
          nonSupportItem = ifp;
          break;
        }

        if (groupcol->in_field_list && groupcol->counter_used)
        {
          delete fc;
          fc = gwi.returnedCols[groupcol->counter - 1].get();
          SRCP srcp(fc->clone());

          // check if no column parm
          for (uint32_t i = 0; i < gwi.no_parm_func_list.size(); i++)
          {
            if (gwi.no_parm_func_list[i]->expressionId() == fc->expressionId())
            {
              gwi.no_parm_func_list.push_back(dynamic_cast<FunctionColumn*>(srcp.get()));
              break;
            }
          }

          srcp->orderPos(groupcol->counter - 1);
          gwi.groupByCols.push_back(srcp);
          continue;
        }
        else if (groupItem->is_explicit_name())  // alias
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (string(groupItem->name.str) == gwi.returnedCols[i]->alias())
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              delete fc;
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            nonSupportItem = groupItem;
            break;
          }
        }
        else
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (fc->operator==(gwi.returnedCols[i].get()))
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              delete fc;
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            gwi.groupByCols.push_back(SRCP(fc));
            break;
          }
        }
      }
      else if (groupItem->type() == Item::FIELD_ITEM)
      {
        Item_field* ifp = (Item_field*)groupItem;
        // this GB col could be an alias of F&E on the SELECT clause, not necessarily a field.
        ReturnedColumn* rc = buildSimpleColumn(ifp, gwi);
        SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

        for (uint32_t j = 0; j < gwi.returnedCols.size(); j++)
        {
          if (sc)
          {
            if (sc->sameColumn(gwi.returnedCols[j].get()))
            {
              sc->orderPos(j);
              break;
            }
            else if (strcasecmp(sc->alias().c_str(), gwi.returnedCols[j]->alias().c_str()) == 0)
            {
              rc = gwi.returnedCols[j].get()->clone();
              rc->orderPos(j);
              break;
            }
          }
          else
          {
            if (ifp->name.length && string(ifp->name.str) == gwi.returnedCols[j].get()->alias())
            {
              rc = gwi.returnedCols[j].get()->clone();
              rc->orderPos(j);
              break;
            }
          }
        }

        if (!rc)
        {
          nonSupportItem = ifp;
          break;
        }

        SRCP srcp(rc);

        // bug 3151
        AggregateColumn* ac = dynamic_cast<AggregateColumn*>(rc);

        if (ac)
        {
          nonSupportItem = ifp;
          break;
        }

        gwi.groupByCols.push_back(srcp);
        gwi.columnMap.insert(
            CalpontSelectExecutionPlan::ColumnMap::value_type(string(ifp->field_name.str), srcp));
      }
      // @bug5638. The group by column is constant but not counter, alias has to match a column
      // on the select list
      else if (!groupcol->counter_used &&
               (groupItem->type() == Item::CONST_ITEM &&
                (groupItem->cmp_type() == INT_RESULT || groupItem->cmp_type() == STRING_RESULT ||
                 groupItem->cmp_type() == REAL_RESULT || groupItem->cmp_type() == DECIMAL_RESULT)))

      {
        ReturnedColumn* rc = 0;

        for (uint32_t j = 0; j < gwi.returnedCols.size(); j++)
        {
          if (groupItem->name.length && string(groupItem->name.str) == gwi.returnedCols[j].get()->alias())
          {
            rc = gwi.returnedCols[j].get()->clone();
            rc->orderPos(j);
            break;
          }
        }

        if (!rc)
        {
          nonSupportItem = groupItem;
          break;
        }

        gwi.groupByCols.push_back(SRCP(rc));
      }
      else if ((*(groupcol->item))->type() == Item::SUBSELECT_ITEM)
      {
        if (!groupcol->in_field_list || !groupItem->name.length)
        {
          nonSupportItem = groupItem;
        }
        else
        {
          uint32_t i = 0;

          for (; i < gwi.returnedCols.size(); i++)
          {
            if (string(groupItem->name.str) == gwi.returnedCols[i]->alias())
            {
              ReturnedColumn* rc = gwi.returnedCols[i]->clone();
              rc->orderPos(i);
              gwi.groupByCols.push_back(SRCP(rc));
              break;
            }
          }

          if (i == gwi.returnedCols.size())
          {
            nonSupportItem = groupItem;
          }
        }
      }
      // @bug 3761.
      else if (groupcol->counter_used)
      {
        if (gwi.returnedCols.size() <= (uint32_t)(groupcol->counter - 1))
        {
          nonSupportItem = groupItem;
        }
        else
        {
          gwi.groupByCols.push_back(SRCP(gwi.returnedCols[groupcol->counter - 1]->clone()));
        }
      }
      else
      {
        nonSupportItem = groupItem;
      }
    }

    // @bug 4756. Add internal groupby column for correlated join to the groupby list
    if (gwi.aggOnSelect && !gwi.subGroupByCols.empty())
      gwi.groupByCols.insert(gwi.groupByCols.end(), gwi.subGroupByCols.begin(), gwi.subGroupByCols.end());

    // this is window func on SELECT becuase ORDER BY has not been processed
    if (!gwi.windowFuncList.empty() && !gwi.subGroupByCols.empty())
    {
      for (uint32_t i = 0; i < gwi.windowFuncList.size(); i++)
      {
        if (gwi.windowFuncList[i]->hasWindowFunc())
        {
          vector<WindowFunctionColumn*> windowFunctions = gwi.windowFuncList[i]->windowfunctionColumnList();

          for (uint32_t j = 0; j < windowFunctions.size(); j++)
            windowFunctions[j]->addToPartition(gwi.subGroupByCols);
        }
      }
    }

    if (nonSupportItem)
    {
      Message::Args args;

      if (nonSupportItem->name.length)
        args.add("'" + string(nonSupportItem->name.str) + "'");
      else
        args.add("");

      gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_GROUP_BY, args);
      setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }
  }  // GROUP processing ends here

  // ORDER BY processing starts here
  {
    ORDER* ordercol = reinterpret_cast<ORDER*>(gi.groupByOrder);

    // check if window functions are in order by. InfiniDB process order by list if
    // window functions are involved, either in order by or projection.
    for (; ordercol; ordercol = ordercol->next)
    {
      if ((*(ordercol->item))->type() == Item::WINDOW_FUNC_ITEM)
        gwi.hasWindowFunc = true;
    }

    // re-visit the first of ordercol list
    ordercol = reinterpret_cast<ORDER*>(gi.groupByOrder);

    // for subquery, order+limit by will be supported in infinidb. build order by columns
    // @todo union order by and limit support
    // if (gwi.hasWindowFunc || gwi.subSelectType != CalpontSelectExecutionPlan::MAIN_SELECT)

    for (; ordercol; ordercol = ordercol->next)
    {
      ReturnedColumn* rc = NULL;

      if (ordercol->in_field_list && ordercol->counter_used)
      {
        rc = gwi.returnedCols[ordercol->counter - 1]->clone();
        rc->orderPos(ordercol->counter - 1);
        // can not be optimized off if used in order by with counter.
        // set with self derived table alias if it's derived table
        gwi.returnedCols[ordercol->counter - 1]->incRefCount();
      }
      else
      {
        Item* ord_item = *(ordercol->item);
        bool nonAggField = true;

        // ignore not_used column on order by.
        if ((ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT) &&
            ord_item->full_name() && !strcmp(ord_item->full_name(), "Not_used"))
        {
          continue;
        }
        else if (ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT)
        {
          rc = gwi.returnedCols[((Item_int*)ord_item)->val_int() - 1]->clone();
        }
        else if (ord_item->type() == Item::SUBSELECT_ITEM)
        {
          gwi.fatalParseError = true;
        }
        else if (ordercol->in_field_list && ord_item->type() == Item::FIELD_ITEM)
        {
          rc = buildReturnedColumn(ord_item, gwi, gwi.fatalParseError);
          Item_field* ifp = static_cast<Item_field*>(ord_item);

          // The item must be an alias for a projected column
          // and extended SELECT list must contain a proper rc
          // either aggregation or a field.
          if (!rc && ifp->name.length)
          {
            gwi.fatalParseError = false;
            execplan::CalpontSelectExecutionPlan::ReturnedColumnList::iterator iter =
                gwi.returnedCols.begin();

            for (; iter != gwi.returnedCols.end(); iter++)
            {
              if ((*iter).get()->alias() == ord_item->name.str)
              {
                rc = (*iter).get()->clone();
                nonAggField = rc->hasAggregate() ? false : true;
                break;
              }
            }
          }
        }
        else
          rc = buildReturnedColumn(ord_item, gwi, gwi.fatalParseError);

        // Looking for a match for this item in GROUP BY list.
        if (rc && ord_item->type() == Item::FIELD_ITEM && nonAggField)
        {
          execplan::CalpontSelectExecutionPlan::ReturnedColumnList::iterator iter = gwi.groupByCols.begin();

          for (; iter != gwi.groupByCols.end(); iter++)
          {
            if (rc->sameColumn((*iter).get()))
              break;
          }

          // MCOL-1052 Find and remove the optimized field
          // from ORDER using cond_pushed filters.
          if (buildConstColFromFilter(dynamic_cast<SimpleColumn*>(rc), gwi, gi))
          {
            break;
          }

          // MCOL-1052 GROUP BY items list doesn't contain
          // this ORDER BY item.
          if (iter == gwi.groupByCols.end())
          {
            std::ostringstream ostream;
            std::ostringstream& osr = ostream;
            getColNameFromItem(osr, *ordercol->item);
            Message::Args args;
            args.add(ostream.str());
            string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NOT_GROUPBY_EXPRESSION, args);
            gwi.parseErrorText = emsg;
            setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
            return ERR_NOT_GROUPBY_EXPRESSION;
          }
        }

        // @bug5501 try item_ptr if item can not be fixed. For some
        // weird dml statement state, item can not be fixed but the
        // infomation is available in item_ptr.
        if (!rc || gwi.fatalParseError)
        {
          gwi.fatalParseError = false;
          Item* item_ptr = ordercol->item_ptr;

          while (item_ptr->type() == Item::REF_ITEM)
            item_ptr = *(((Item_ref*)item_ptr)->ref);

          rc = buildReturnedColumn(item_ptr, gwi, gwi.fatalParseError);
        }

        // This ORDER BY item must be an agg function -
        // the ordercol->item_ptr and exteded SELECT list
        // must contain the corresponding item.
        if (!rc)
        {
          Item* item_ptr = ordercol->item_ptr;

          if (item_ptr)
            rc = buildReturnedColumn(item_ptr, gwi, gwi.fatalParseError);
        }

        if (!rc)
        {
          string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
          gwi.parseErrorText = emsg;
          setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, emsg, gwi);
          return ER_CHECK_NOT_IMPLEMENTED;
        }
      }

      if (ordercol->direction == ORDER::ORDER_ASC)
        rc->asc(true);
      else
        rc->asc(false);

      gwi.orderByCols.push_back(SRCP(rc));
    }

    // make sure columnmap, returnedcols and count(*) arg_list are not empty
    TableMap::iterator tb_iter = gwi.tableMap.begin();

    try
    {
      for (; tb_iter != gwi.tableMap.end(); tb_iter++)
      {
        if ((*tb_iter).second.first == 1)
          continue;

        CalpontSystemCatalog::TableAliasName tan = (*tb_iter).first;
        CalpontSystemCatalog::TableName tn = make_table((*tb_iter).first.schema, (*tb_iter).first.table);
        SimpleColumn* sc = getSmallestColumn(csc, tn, tan, (*tb_iter).second.second->table, gwi);
        SRCP srcp(sc);
        gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
        (*tb_iter).second.first = 1;
      }
    }
    catch (runtime_error& e)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, e.what(), gwi);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      return ER_INTERNAL_ERROR;
    }
    catch (...)
    {
      string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
      setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      return ER_INTERNAL_ERROR;
    }

    if (!gwi.count_asterisk_list.empty() || !gwi.no_parm_func_list.empty() || gwi.returnedCols.empty())
    {
      // get the smallest column from colmap
      CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;
      int minColWidth = 0;
      CalpontSystemCatalog::ColType ct;

      try
      {
        for (iter = gwi.columnMap.begin(); iter != gwi.columnMap.end(); ++iter)
        {
          // should always not null
          SimpleColumn* sc = dynamic_cast<SimpleColumn*>(iter->second.get());

          if (sc && !(sc->joinInfo() & JOIN_CORRELATED))
          {
            ct = csc->colType(sc->oid());

            if (minColWidth == 0)
            {
              minColWidth = ct.colWidth;
              minSc = iter->second;
            }
            else if (ct.colWidth < minColWidth)
            {
              minColWidth = ct.colWidth;
              minSc = iter->second;
            }
          }
        }
      }
      catch (...)
      {
        string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
        setError(gwi.thd, ER_INTERNAL_ERROR, emsg, gwi);
        CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
        return ER_INTERNAL_ERROR;
      }

      if (gwi.returnedCols.empty() && gwi.additionalRetCols.empty())
        gwi.returnedCols.push_back(minSc);
    }

    if (!isUnion && !gwi.hasWindowFunc && gwi.subSelectType == CalpontSelectExecutionPlan::MAIN_SELECT)
    {
      // re-construct the select query and redo phase 1
      if (redo)
      {
        TABLE_LIST* table_ptr = gi.groupByTables;

        // put all tables, derived tables and views on the list
        // TABLE_LIST* table_ptr = select_lex.get_table_list();
        set<string> aliasSet;  // to avoid duplicate table alias

        for (; table_ptr; table_ptr = table_ptr->next_local)
        {
          if (string(table_ptr->table_name.str).find("$vtable") != string::npos)
            continue;

          if (table_ptr->derived)
          {
            if (aliasSet.find(table_ptr->alias.str) != aliasSet.end())
              continue;

            aliasSet.insert(table_ptr->alias.str);
          }
          else if (table_ptr->view)
          {
            if (aliasSet.find(table_ptr->alias.str) != aliasSet.end())
              continue;

            aliasSet.insert(table_ptr->alias.str);
          }
          else
          {
            // table referenced by view is represented by viewAlias_tableAlias.
            // consistent with item.cc field print.
            if (table_ptr->referencing_view)
            {
              if (aliasSet.find(string(table_ptr->referencing_view->alias.str) + "_" +
                                string(table_ptr->alias.str)) != aliasSet.end())
                continue;

              aliasSet.insert(string(table_ptr->referencing_view->alias.str) + "_" +
                              string(table_ptr->alias.str));
            }
            else
            {
              if (aliasSet.find(table_ptr->alias.str) != aliasSet.end())
                continue;

              aliasSet.insert(table_ptr->alias.str);
            }
          }
        }
      }
      else
      {
        // remove order by clause in case this phase has been executed before.
        // need a better fix later, like skip all the other non-optimized phase.

        // MCOL-1052
        if (unionSel)
        {
          ordercol = reinterpret_cast<ORDER*>(gi.groupByOrder);
        }
        else
          ordercol = 0;

        for (; ordercol; ordercol = ordercol->next)
        {
          Item* ord_item = *(ordercol->item);

          if (ord_item->type() == Item::NULL_ITEM)
          {
            // MCOL-793 Do nothing for an ORDER BY NULL
          }
          else if (ord_item->type() == Item::SUM_FUNC_ITEM)
          {
            Item_sum* ifp = (Item_sum*)(*(ordercol->item));
            ReturnedColumn* fc = buildAggregateColumn(ifp, gwi);

            for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
            {
              if (fc->operator==(gwi.returnedCols[i].get()))
              {
                ostringstream oss;
                oss << i + 1;
                ord_cols += oss.str();
                break;
              }
            }

            // continue;
          }
          // @bug 3518. if order by clause = selected column, use position.
          else if (ord_item->name.length && ord_item->type() == Item::FIELD_ITEM)
          {
            Item_field* field = reinterpret_cast<Item_field*>(ord_item);
            string fullname;

            if (field->db_name.str)
              fullname += string(field->db_name.str) + ".";

            if (field->table_name.str)
              fullname += string(field->table_name.str) + ".";

            if (field->field_name.length)
              fullname += string(field->field_name.str);

            uint32_t i = 0;

            for (i = 0; i < gwi.returnedCols.size(); i++)
            {
              SimpleColumn* sc = dynamic_cast<SimpleColumn*>(gwi.returnedCols[i].get());

              if (sc && ((Item_field*)ord_item)->cached_table &&
                  (strcasecmp(getViewName(((Item_field*)ord_item)->cached_table).c_str(),
                              sc->viewName().c_str()) != 0))
                continue;

              if (strcasecmp(fullname.c_str(), gwi.returnedCols[i]->alias().c_str()) == 0 ||
                  strcasecmp(ord_item->name.str, gwi.returnedCols[i]->alias().c_str()) == 0)
              {
                ostringstream oss;
                oss << i + 1;
                ord_cols += oss.str();
                break;
              }
            }

            if (i == gwi.returnedCols.size())
              ord_cols += string(" `") + escapeBackTick(ord_item->name.str) + '`';
          }

          else if (ord_item->name.length)
          {
            // for union order by 1 case. For unknown reason, it doesn't show in_field_list
            if (ord_item->type() == Item::CONST_ITEM && ord_item->cmp_type() == INT_RESULT)
            {
              ord_cols += ord_item->name.str;
            }
            else if (ord_item->type() == Item::SUBSELECT_ITEM)
            {
              string emsg = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_ORDER_BY);
              setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, emsg, gwi);
              return ER_CHECK_NOT_IMPLEMENTED;
            }
            else
            {
              ord_cols += string(" `") + escapeBackTick(ord_item->name.str) + '`';
            }
          }
          else if (ord_item->type() == Item::FUNC_ITEM)
          {
            // @bug5636. check if this order by column is on the select list
            ReturnedColumn* rc = buildFunctionColumn((Item_func*)(ord_item), gwi, gwi.fatalParseError);

            for (uint32_t i = 0; i < gwi.returnedCols.size(); i++)
            {
              if (rc && rc->operator==(gwi.returnedCols[i].get()))
              {
                ostringstream oss;
                oss << i + 1;
                ord_cols += oss.str();
                break;
              }
            }
          }
          else
          {
            String str;
            ord_item->print(&str, QT_ORDINARY);
            ord_cols += string(str.c_ptr());
          }

          if (ordercol->direction != ORDER::ORDER_ASC)
            ord_cols += " desc";
        }
      }

      if (gwi.orderByCols.size())  // has order by
      {
        csep->hasOrderBy(true);
        csep->specHandlerProcessed(true);
        csep->orderByThreads(get_orderby_threads(gwi.thd));
      }
    }

    // LIMIT and OFFSET are extracted from TABLE_LIST elements.
    // All of JOIN-ed tables contain relevant limit and offset.
    uint64_t limit = (uint64_t)-1;
    if (gi.groupByTables->select_lex->limit_params.select_limit &&
        (limit =
             static_cast<Item_int*>(gi.groupByTables->select_lex->limit_params.select_limit)->val_int()) &&
        limit != (uint64_t)-1)
    {
      csep->limitNum(limit);
    }
    else if (csep->hasOrderBy())
    {
      // We use LimitedOrderBy so set the limit to
      // go through the check in addOrderByAndLimit
      csep->limitNum((uint64_t)-2);
    }

    if (gi.groupByTables->select_lex->limit_params.offset_limit)
    {
      csep->limitStart(((Item_int*)gi.groupByTables->select_lex->limit_params.offset_limit)->val_int());
    }

    // We don't currently support limit with correlated subquery
    if (csep->limitNum() != (uint64_t)-1 && gwi.subQuery && !gwi.correlatedTbNameVec.empty())
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_LIMIT_SUB);
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

  }  // ORDER BY processing ends here

  if (gi.groupByDistinct)
    csep->distinct(true);

  // add the smallest column to count(*) parm.
  // select constant in subquery case
  std::vector<AggregateColumn*>::iterator coliter;

  if (!minSc)
  {
    if (!gwi.returnedCols.empty())
      minSc = gwi.returnedCols[0];
    else if (!gwi.additionalRetCols.empty())
      minSc = gwi.additionalRetCols[0];
  }

  // @bug3523, count(*) on subquery always pick column[0].
  SimpleColumn* sc = dynamic_cast<SimpleColumn*>(minSc.get());

  if (sc && sc->schemaName().empty())
  {
    if (gwi.derivedTbList.size() >= 1)
    {
      SimpleColumn* sc1 = new SimpleColumn();
      sc1->columnName(sc->columnName());
      sc1->tableName(sc->tableName());
      sc1->tableAlias(sc->tableAlias());
      sc1->viewName(sc->viewName());
      sc1->timeZone(gwi.timeZone);
      sc1->colPosition(0);
      minSc.reset(sc1);
    }
  }

  for (coliter = gwi.count_asterisk_list.begin(); coliter != gwi.count_asterisk_list.end(); ++coliter)
  {
    // @bug5977 @note should never throw this, but checking just in case.
    // When ExeMgr fix is ready, this should not error out...
    if (dynamic_cast<AggregateColumn*>(minSc.get()))
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = "No project column found for aggregate function";
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText, gwi);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

    // Replace the last (presumably constant) object with minSc
    if ((*coliter)->aggParms().empty())
    {
      (*coliter)->aggParms().push_back(minSc);
    }
    else
    {
      (*coliter)->aggParms()[0] = minSc;
    }
  }

  std::vector<FunctionColumn*>::iterator funciter;

  SPTP sptp(new ParseTree(minSc.get()->clone()));

  for (funciter = gwi.no_parm_func_list.begin(); funciter != gwi.no_parm_func_list.end(); ++funciter)
  {
    FunctionParm funcParms = (*funciter)->functionParms();
    funcParms.push_back(sptp);
    (*funciter)->functionParms(funcParms);
  }

  // set sequence# for subquery localCols
  for (uint32_t i = 0; i < gwi.localCols.size(); i++)
    gwi.localCols[i]->sequence(i);

  // append additionalRetCols to returnedCols
  gwi.returnedCols.insert(gwi.returnedCols.begin(), gwi.additionalRetCols.begin(),
                          gwi.additionalRetCols.end());

  csep->groupByCols(gwi.groupByCols);
  csep->orderByCols(gwi.orderByCols);
  csep->returnedCols(gwi.returnedCols);
  csep->columnMap(gwi.columnMap);
  csep->having(havingFilter);
  csep->derivedTableList(gwi.derivedTbList);
  csep->selectSubList(selectSubList);
  csep->subSelectList(gwi.subselectList);
  clearStacks(gwi);
  return 0;
}

}  // namespace cal_impl_if
// vim:ts=4 sw=4:
