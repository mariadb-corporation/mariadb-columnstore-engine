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

/***********************************************************************
 *   $Id: ha_view.cpp 9642 2013-06-24 14:57:42Z rdempsey $
 *
 *
 ***********************************************************************/

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include "idb_mysql.h"

#include <string>
using namespace std;

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

#include "errorids.h"
using namespace logging;

#include "parsetree.h"
#include "simplefilter.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "ha_subquery.h"
#include "ha_view.h"

namespace cal_impl_if
{
extern uint32_t buildJoin(gp_walk_info& gwi, List<TABLE_LIST>& join_list,
                          std::stack<execplan::ParseTree*>& outerJoinStack);
extern string getViewName(TABLE_LIST* table_ptr);

CalpontSystemCatalog::TableAliasName& View::viewName()
{
  return fViewName;
}

void View::viewName(execplan::CalpontSystemCatalog::TableAliasName& viewName)
{
  fViewName = viewName;
}

void View::transform()
{
  CalpontSelectExecutionPlan* csep = new CalpontSelectExecutionPlan();
  csep->sessionID(fParentGwip->sessionid);

  // gwi for the sub query
  gp_walk_info gwi(fParentGwip->timeZone);
  gwi.thd = fParentGwip->thd;

  uint32_t sessionID = csep->sessionID();
  gwi.sessionid = sessionID;
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);
  gwi.csc = csc;

  // traverse the table list of the view
  TABLE_LIST* table_ptr = fSelect.get_table_list();
  CalpontSelectExecutionPlan::SelectList derivedTbList;

  // @bug 1796. Remember table order on the FROM list.
  gwi.clauseType = FROM;

  try
  {
    for (; table_ptr; table_ptr = table_ptr->next_local)
    {
      // mysql put vtable here for from sub. we ignore it
      if (string(table_ptr->table_name.str).find("$vtable") != string::npos)
        continue;

      string viewName = getViewName(table_ptr);
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(viewName);
      }

      if (table_ptr->derived)
      {
        SELECT_LEX* select_cursor = table_ptr->derived->first_select();
        FromSubQuery* fromSub = new FromSubQuery(gwi, select_cursor);
        string alias(table_ptr->alias.str);
        if (lower_case_table_names)
        {
          boost::algorithm::to_lower(alias);
        }
        gwi.viewName =
            make_aliasview("", alias, table_ptr->belong_to_view->alias.str, "", true, lower_case_table_names);
        fromSub->alias(alias);
        gwi.derivedTbList.push_back(SCSEP(fromSub->transform()));
        // set alias to both table name and alias name of the derived table
        CalpontSystemCatalog::TableAliasName tn = make_aliasview("", "", alias, viewName);
        gwi.tbList.push_back(tn);
        gwi.tableMap[tn] = make_pair(0, table_ptr);
        // TODO MCOL-2178 isUnion member only assigned, never used
        // MIGR::infinidb_vtable.isUnion = true; //by-pass the 2nd pass of rnd_init
      }
      else if (table_ptr->view)
      {
        // for nested view, the view name is vout.vin... format
        CalpontSystemCatalog::TableAliasName tn =
            make_aliasview(table_ptr->db.str, table_ptr->table_name.str, table_ptr->alias.str, viewName, true,
                           lower_case_table_names);
        gwi.viewName = make_aliastable(table_ptr->db.str, table_ptr->table_name.str, viewName, true,
                                       lower_case_table_names);
        View* view = new View(*table_ptr->view->first_select_lex(), &gwi);
        view->viewName(gwi.viewName);
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

        CalpontSystemCatalog::TableAliasName tn =
            make_aliasview(table_ptr->db.str, table_ptr->table_name.str, table_ptr->alias.str, viewName,
                           columnStore, lower_case_table_names);
        gwi.tbList.push_back(tn);
        gwi.tableMap[tn] = make_pair(0, table_ptr);
        fParentGwip->tableMap[tn] = make_pair(0, table_ptr);
      }
    }

    if (gwi.fatalParseError)
    {
      setError(gwi.thd, ER_INTERNAL_ERROR, gwi.parseErrorText);
      return;
    }
  }
  catch (IDBExcept& ie)
  {
    setError(gwi.thd, ER_INTERNAL_ERROR, ie.what());
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return;
  }
  catch (...)
  {
    string emsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
    setError(gwi.thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return;
  }

  // merge table list to parent select
  fParentGwip->tbList.insert(fParentGwip->tbList.end(), gwi.tbList.begin(), gwi.tbList.end());
  fParentGwip->derivedTbList.insert(fParentGwip->derivedTbList.end(), gwi.derivedTbList.begin(),
                                    gwi.derivedTbList.end());
  fParentGwip->correlatedTbNameVec.insert(fParentGwip->correlatedTbNameVec.end(),
                                          gwi.correlatedTbNameVec.begin(), gwi.correlatedTbNameVec.end());

  // merge view list to parent
  fParentGwip->viewList.insert(fParentGwip->viewList.end(), gwi.viewList.begin(), gwi.viewList.end());

  // merge non-collapsed outer join to parent select
  stack<ParseTree*> tmpstack;

  while (!gwi.ptWorkStack.empty())
  {
    tmpstack.push(gwi.ptWorkStack.top());
    gwi.ptWorkStack.pop();
  }

  while (!tmpstack.empty())
  {
    fParentGwip->ptWorkStack.push(tmpstack.top());
    tmpstack.pop();
  }
}

uint32_t View::processJoin(gp_walk_info& gwi, std::stack<execplan::ParseTree*>& outerJoinStack)
{
  return buildJoin(gwi, fSelect.top_join_list, outerJoinStack);
}

}  // namespace cal_impl_if
