/* Copyright (C) 2025 MariaDB Corporation

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

#include "ha_mcs_execplan_helpers.h"
#include "ha_mcs_execplan_parseinfo_bits.h"
#include "ha_mcs_execplan_walks.h"
#include "ha_mcs_impl_if.h"
#include "ha_subquery.h"

#include "constantcolumn.h"
#include "logicoperator.h"
#include "rowcolumn.h"
#include "simplefilter.h"

using namespace std;

namespace cal_impl_if
{
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
  RecursionCounter(cal_impl_if::gp_walk_info* gwip) : fgwip(gwip)
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

  cal_impl_if::gp_walk_info* fgwip;
};

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
  cal_impl_if::gp_walk_info* gwip = static_cast<cal_impl_if::gp_walk_info*>(arg);
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
        // XXX: this looks awfuly wrong.
        execplan::SimpleColumn* scp = buildSimpleColumn(ifp, *gwip);

        if (!scp)
          break;

        std::string aliasTableName(scp->tableAlias());
        scp->tableAlias(aliasTableName);
        gwip->rcWorkStack.push(scp->clone());
        boost::shared_ptr<execplan::SimpleColumn> scsp(scp);
        gwip->scsp = scsp;

        gwip->funcName.clear();
        gwip->columnMap.insert(execplan::CalpontSelectExecutionPlan::ColumnMap::value_type(
            std::string(ifp->field_name.str), scsp));

        //@bug4636 take where clause column as dummy projection column, but only on local column.
        // varbinary aggregate is not supported yet, so rule it out
        if (!((scp->joinInfo() & execplan::JOIN_CORRELATED) ||
              scp->colType().colDataType == execplan::CalpontSystemCatalog::VARBINARY))
        {
          TABLE_LIST* tmp = (ifp->cached_table ? ifp->cached_table : 0);
          gwip->tableMap[execplan::make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias(),
                                                   scp->isColumnStore())] = std::make_pair(1, tmp);
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
          Item* non_const_item = const_cast<Item*>(item);
          gwip->rcWorkStack.push(buildReturnedColumn(non_const_item, *gwip, gwip->fatalParseError));
          break;
        }

        case STRING_RESULT:
        {
          // Special handling for 0xHHHH literals
          if (item->type_handler() == &type_handler_hex_hybrid)
          {
            Item_hex_hybrid* hip = static_cast<Item_hex_hybrid*>(const_cast<Item*>(item));
            gwip->rcWorkStack.push(
                new execplan::ConstantColumn((int64_t)hip->val_int(), execplan::ConstantColumn::NUM));
            execplan::ConstantColumn* cc = dynamic_cast<execplan::ConstantColumn*>(gwip->rcWorkStack.top());
            cc->timeZone(gwip->timeZone);
            break;
          }

          if (item->result_type() == STRING_RESULT)
          {
            // dangerous cast here
            Item* isp = const_cast<Item*>(item);
            String val, *str = isp->val_str(&val);
            if (str)
            {
              std::string cval;

              if (str->ptr())
              {
                cval.assign(str->ptr(), str->length());
              }

              gwip->rcWorkStack.push(new execplan::ConstantColumn(cval));
              (dynamic_cast<execplan::ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
              break;
            }
            else
            {
              gwip->rcWorkStack.push(new execplan::ConstantColumn("", execplan::ConstantColumn::NULLDATA));
              (dynamic_cast<execplan::ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
              break;
            }

            gwip->rcWorkStack.push(buildReturnedColumn(isp, *gwip, gwip->fatalParseError));
          }
          break;
        }

        case REAL_RESULT:
        case DECIMAL_RESULT:
        case TIME_RESULT:
        {
          Item* nonConstItem = const_cast<Item*>(item);
          gwip->rcWorkStack.push(buildReturnedColumn(nonConstItem, *gwip, gwip->fatalParseError));
          break;
        }

        default:
        {
          if (gwip->condPush)
          {
            // push noop for unhandled item
            execplan::SimpleColumn* rc = new execplan::SimpleColumn("noop");
            rc->timeZone(gwip->timeZone);
            gwip->rcWorkStack.push(rc);
            break;
          }

          std::ostringstream oss;
          oss << "Unhandled Item type(): " << item->type();
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
        execplan::SimpleColumn* rc = new execplan::SimpleColumn("noop");
        rc->timeZone(gwip->timeZone);
        gwip->rcWorkStack.push(rc);
        break;
      }

      gwip->rcWorkStack.push(new execplan::ConstantColumn("", execplan::ConstantColumn::NULLDATA));
      (dynamic_cast<execplan::ConstantColumn*>(gwip->rcWorkStack.top()))->timeZone(gwip->timeZone);
      break;
    }

    case Item::FUNC_ITEM:
    {
      Item* ncitem = const_cast<Item*>(item);
      Item_func* ifp = static_cast<Item_func*>(ncitem);

      std::string funcName = ifp->func_name();

      if (!gwip->condPush)
      {
        if (!ifp->fixed())
        {
          ifp->fix_fields(gwip->thd, &ncitem);
        }

        // Special handling for queries of the form:
        // SELECT ... WHERE col1 NOT IN (SELECT <const_item>);
        if (isNotFuncAndConstScalarSubSelect(ifp, funcName))
        {
          idbassert(!gwip->ptWorkStack.empty());
          execplan::ParseTree* pt = gwip->ptWorkStack.top();
          execplan::SimpleFilter* sf = dynamic_cast<execplan::SimpleFilter*>(pt->data());

          if (sf)
          {
            boost::shared_ptr<execplan::Operator> sop(new execplan::PredicateOperator("<>"));
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
          {
            delete gwip->rcWorkStack.top();
            gwip->rcWorkStack.pop();
          }

          break;
        }
      }

      // try to evaluate const F&E
      std::vector<Item_field*> tmpVec;
      uint16_t parseInfo = 0;
      parse_item(ifp, tmpVec, gwip->fatalParseError, parseInfo, gwip);

      // table mode takes only one table filter
      if (gwip->condPush)
      {
        std::set<std::string> tableSet;

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

        execplan::ConstantColumn* cc = buildConstantColumnMaybeNullFromValStr(ifp, valStr, *gwip);

        for (uint32_t i = 0; i < ifp->argument_count() && !gwip->rcWorkStack.empty(); i++)
        {
          delete gwip->rcWorkStack.top();
          gwip->rcWorkStack.pop();
        }

        // bug 3137. If filter constant like 1=0, put it to ptWorkStack
        // MariaDB bug 750. Breaks if compare is an argument to a function.
        //				if ((int32_t)gwip->rcWorkStack.size() <=
        //(gwip->rcBookMarkStack.empty()
        //?
        // 0
        //: gwip->rcBookMarkStack.top())
        //				&& isPredicateFunction(ifp, gwip))
        if (isPredicateFunction(ifp, gwip))
          gwip->ptWorkStack.push(new execplan::ParseTree(cc));
        else
          gwip->rcWorkStack.push(cc);

        if (!valStr.isNull())
          IDEBUG(cerr << "Const F&E " << item->full_name() << " evaluate: " << valStr << endl);

        break;
      }

      execplan::ReturnedColumn* rc = NULL;

      // @bug4488. Process function for table mode also, not just vtable mode.
      rc = buildFunctionColumn(ifp, *gwip, gwip->fatalParseError);

      if (gwip->fatalParseError)
      {
        if (gwip->clauseType == SELECT)
          return;

        // @bug 2585
        if (gwip->parseErrorText.empty())
        {
          logging::Message::Args args;
          args.add(funcName);
          gwip->parseErrorText =
              logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NON_SUPPORTED_FUNCTION, args);
        }

        return;
      }

      // predicate operators fall in the old path
      if (rc)
      {
        // @bug 2383. For some reason func_name() for "in" gives " IN " always
        if (funcName == "between" || funcName == "in" || funcName == " IN ")
          gwip->ptWorkStack.push(new execplan::ParseTree(rc));
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
      execplan::ReturnedColumn* rc = buildAggregateColumn(isp, *gwip);

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
            std::vector<Item_field*> fieldVec;
            uint16_t parseInfo = 0;
            parse_item(it, fieldVec, gwip->fatalParseError, parseInfo, gwip);

            if (parseInfo & CORRELATED)
            {
              gwip->fatalParseError = true;
              gwip->parseErrorText =
                  logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_CORRELATED_SUB_OR);
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
            gwip->ptWorkStack.push(new execplan::ParseTree(gwip->rcWorkStack.top()));
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

        execplan::ParseTree* lhs = gwip->ptWorkStack.top();
        gwip->ptWorkStack.pop();
        execplan::SimpleFilter* lsf = dynamic_cast<execplan::SimpleFilter*>(lhs->data());

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

        execplan::ParseTree* rhs = gwip->ptWorkStack.top();
        gwip->ptWorkStack.pop();
        execplan::SimpleFilter* rsf = dynamic_cast<execplan::SimpleFilter*>(rhs->data());

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
            delete rhs;
            gwip->ptWorkStack.push(lhs);
            continue;
          }
        }

        execplan::Operator* op = new execplan::LogicOperator(func->func_name());
        execplan::ParseTree* ptp = new execplan::ParseTree(op);
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
      execplan::ReturnedColumn* rc = NULL;
      // ref item is not pre-walked. force clause type to SELECT
      ClauseType clauseType = gwip->clauseType;
      gwip->clauseType = SELECT;

      if (col->type() != Item::COND_ITEM)
      {
        rc = buildReturnedColumn(col, *gwip, gwip->fatalParseError, true);

        if (col->type() == Item::FIELD_ITEM)
          gwip->fatalParseError = false;
      }

      execplan::SimpleColumn* sc = clauseType == HAVING ? nullptr : dynamic_cast<execplan::SimpleColumn*>(rc);

      if (sc)
      {
        boost::shared_ptr<execplan::SimpleColumn> scsp(sc->clone());
        gwip->scsp = scsp;

        if (col->type() == Item::FIELD_ITEM)
        {
          const Item_ident* ident_field = dynamic_cast<const Item_ident*>(item);
          if (ident_field)
          {
            const auto& field_name = string(ident_field->field_name.str);
            auto colMap = execplan::CalpontSelectExecutionPlan::ColumnMap::value_type(field_name, scsp);
            gwip->columnMap.insert(colMap);
          }
        }
      }

      bool cando = true;
      gwip->clauseType = clauseType;

      if (rc)
      {
        if (((Item_ref*)item)->depended_from)
        {
          rc->joinInfo(rc->joinInfo() | execplan::JOIN_CORRELATED);

          if (gwip->subQuery)
            gwip->subQuery->correlated(true);

          execplan::SimpleColumn* scp = dynamic_cast<execplan::SimpleColumn*>(rc);

          if (scp)
            gwip->correlatedTbNameVec.push_back(
                execplan::make_aliastable(scp->schemaName(), scp->tableName(), scp->tableAlias()));

          if (gwip->subSelectType == execplan::CalpontSelectExecutionPlan::SINGLEROW_SUBS)
            rc->joinInfo(rc->joinInfo() | execplan::JOIN_SCALAR | execplan::JOIN_SEMI);

          if (gwip->subSelectType == execplan::CalpontSelectExecutionPlan::SELECT_SUBS)
            rc->joinInfo(rc->joinInfo() | execplan::JOIN_SCALAR | execplan::JOIN_OUTER_SELECT);
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
          execplan::ReturnedColumn* operand = NULL;

          if (ifp->arguments()[i]->type() == Item::REF_ITEM)
          {
            Item* op = *(((Item_ref*)ifp->arguments()[i])->ref);
            operand = buildReturnedColumn(op, *gwip, gwip->fatalParseError);
          }
          else
            operand = buildReturnedColumn(ifp->arguments()[i], *gwip, gwip->fatalParseError);

          if (operand)
          {
            gwip->rcWorkStack.push(operand);
            if (i == 0 && gwip->scsp == NULL)  // first item is the WHEN LHS
            {
              execplan::SimpleColumn* sc = dynamic_cast<execplan::SimpleColumn*>(operand);
              if (sc)
              {
                gwip->scsp.reset(sc->clone());  // We need to clone else sc gets double deleted. This code is
                                                // rarely executed so the cost is acceptable.
              }
            }
          }
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
        gwip->ptWorkStack.push(buildParseTree(col, *gwip, gwip->fatalParseError));
      }
      else if (col->type() == Item::FIELD_ITEM && gwip->clauseType == HAVING)
      {
        // ReturnedColumn* rc = buildAggFrmTempField(const_cast<Item*>(item), *gwip);
        execplan::ReturnedColumn* rc =
            buildReturnedColumn(const_cast<Item*>(item), *gwip, gwip->fatalParseError);
        if (rc)
          gwip->rcWorkStack.push(rc);

        break;
      }
      else
      {
        cando = false;
      }

      execplan::SimpleColumn* thisSC = dynamic_cast<execplan::SimpleColumn*>(rc);
      if (thisSC)
      {
        gwip->scsp.reset(thisSC->clone());
      }
      if (!rc && !cando)
      {
        ostringstream oss;
        oss << "Unhandled Item type(): " << item->type();
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
      execplan::SubSelect* subselect = new execplan::SubSelect();
      gwip->rcWorkStack.push(subselect);
      break;
    }

    case Item::ROW_ITEM:
    {
      Item_row* row = (Item_row*)item;
      execplan::RowColumn* rowCol = new execplan::RowColumn();
      vector<execplan::SRCP> cols;
      // temp change clause type because the elements of row column are not walked yet
      gwip->clauseType = SELECT;
      for (uint32_t i = 0; i < row->cols(); i++)
        cols.push_back(
            execplan::SRCP(buildReturnedColumn(row->element_index(i), *gwip, gwip->fatalParseError)));

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
      execplan::ReturnedColumn* af = buildWindowFunctionColumn(ifa, *gwip, gwip->fatalParseError);

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
        execplan::SimpleColumn* rc = new execplan::SimpleColumn("noop");
        rc->timeZone(gwip->timeZone);
        gwip->rcWorkStack.push(rc);
        break;
      }

      ostringstream oss;
      oss << "Unhandled Item type (2): " << item->type();
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
      Item_field* ifp = static_cast<Item_field*>(item);
      field_vec.push_back(ifp);
      return;
    }

    case Item::SUM_FUNC_ITEM:
    {
      // hasAggColumn = true;
      parseInfo |= AGG_BIT;
      Item_sum* isp = static_cast<Item_sum*>(item);
      Item** sfitempp = isp->arguments();

      for (uint32_t i = 0; i < isp->argument_count(); i++)
        parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::FUNC_ITEM:
    {
      Item_func* isp = static_cast<Item_func*>(item);

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
      Item_cond* icp = static_cast<Item_cond*>(item);
      List_iterator_fast<Item> it(*(icp->argument_list()));
      Item* cond_item;

      while ((cond_item = it++))
        parse_item(cond_item, field_vec, hasNonSupportItem, parseInfo, gwi);

      break;
    }

    case Item::REF_ITEM:
    {
      Item_ref* ref = (Item_ref*)item;
      if (ref->ref_type() == Item_ref::DIRECT_REF)
      {
        parse_item(ref->real_item(), field_vec, hasNonSupportItem, parseInfo, gwi);
        break;
      }
      while (true)
      {
        ref = (Item_ref*)item;
        if ((*(ref->ref))->type() == Item::SUM_FUNC_ITEM)
        {
          parseInfo |= AGG_BIT;
          Item_sum* isp = static_cast<Item_sum*>(*(ref->ref));
          Item** sfitempp = isp->arguments();

          // special handling for count(*). This should not be treated as constant.
          if (isSupportedAggregateWithOneConstArg(isp, sfitempp))
          {
            field_vec.push_back(nullptr);  // dummy
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
          // ReturnedColumn* rc = NULL;
          // if (gwi)
          //  rc = buildAggFrmTempField(ref, *gwi);

          // if (!rc)
          //{
          Item_field* ifp = static_cast<Item_field*>(*(ref->ref));
          field_vec.push_back(ifp);
          //}
          break;
        }
        else if ((*(ref->ref))->type() == Item::FUNC_ITEM)
        {
          Item_func* isp = static_cast<Item_func*>(*(ref->ref));
          Item** sfitempp = isp->arguments();

          for (uint32_t i = 0; i < isp->argument_count(); i++)
            parse_item(sfitempp[i], field_vec, hasNonSupportItem, parseInfo, gwi);

          break;
        }
        else if ((*(ref->ref))->type() == Item::CACHE_ITEM)
        {
          Item_cache* isp = static_cast<Item_cache*>(*(ref->ref));
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
      string parseErrorText =
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NON_SUPPORT_SUB_QUERY_TYPE);
      setError(gwi->thd, ER_CHECK_NOT_IMPLEMENTED, parseErrorText, *gwi);
      break;
    }

    case Item::WINDOW_FUNC_ITEM: parseInfo |= AF_BIT; break;

    default: break;
  }
}

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

        case Item_func::GE_FUNC: cerr << ">=" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::LE_FUNC: cerr << "<=" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::LT_FUNC: cerr << '<' << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::NE_FUNC: cerr << "<>" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::NEG_FUNC:  // 45
          cerr << "unary minus" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::IN_FUNC:  // 16
          inp = (Item_func_opt_neg*)ifp;

          if (inp->negated)
            cerr << "not ";

          cerr << "in" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::BETWEEN:
          inp = (Item_func_opt_neg*)ifp;

          if (inp->negated)
            cerr << "not ";

          cerr << "between" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNULL_FUNC:  // 10
          cerr << "is null" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNOTNULL_FUNC:  // 11
          cerr << "is not null" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NOT_ALL_FUNC: cerr << "not_all" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::NOT_FUNC: cerr << "not_func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::TRIG_COND_FUNC:
          cerr << "trig_cond_func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::ISNOTNULLTEST_FUNC:
          cerr << "isnotnulltest_func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::MULT_EQUAL_FUNC:
        {
          cerr << "mult_equal_func:" << " (" << ifp->functype() << ")" << endl;
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

        case Item_func::EQUAL_FUNC: cerr << "equal func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::FT_FUNC: cerr << "ft func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::LIKE_FUNC: cerr << "like func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::COND_AND_FUNC:
          cerr << "cond and func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::COND_OR_FUNC: cerr << "cond or func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::XOR_FUNC: cerr << "xor func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::INTERVAL_FUNC:
          cerr << "interval func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_EQUALS_FUNC:
          cerr << "sp equals func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_DISJOINT_FUNC:
          cerr << "sp disjoint func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_INTERSECTS_FUNC:
          cerr << "sp intersects func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_TOUCHES_FUNC:
          cerr << "sp touches func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_CROSSES_FUNC:
          cerr << "sp crosses func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_WITHIN_FUNC:
          cerr << "sp within func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_CONTAINS_FUNC:
          cerr << "sp contains func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_OVERLAPS_FUNC:
          cerr << "sp overlaps func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_STARTPOINT:
          cerr << "sp startpoint func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_ENDPOINT:
          cerr << "sp endpoint func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_EXTERIORRING:
          cerr << "sp exteriorring func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_POINTN: cerr << "sp pointn func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::SP_GEOMETRYN:
          cerr << "sp geometryn func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_INTERIORRINGN:
          cerr << "sp exteriorringn func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::SP_RELATE_FUNC:
          cerr << "sp relate func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::NOW_FUNC: cerr << "now func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::SUSERVAR_FUNC:
          cerr << "suservar func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::GUSERVAR_FUNC:
          cerr << "guservar func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::COLLATE_FUNC: cerr << "collate func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::EXTRACT_FUNC: cerr << "extract func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::CHAR_TYPECAST_FUNC:
          cerr << "char typecast func" << " (" << ifp->functype() << ")" << endl;
          break;

        case Item_func::FUNC_SP: cerr << "func sp func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::UDF_FUNC: cerr << "udf func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::GSYSVAR_FUNC: cerr << "gsysvar func" << " (" << ifp->functype() << ")" << endl; break;

        case Item_func::DYNCOL_FUNC: cerr << "dyncol func" << " (" << ifp->functype() << ")" << endl; break;

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
        Item_cond* cond = static_cast<Item_cond*>(join->conds);

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

}  // namespace cal_impl_if