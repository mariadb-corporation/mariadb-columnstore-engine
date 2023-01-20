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

/***********************************************************************
 *   $Id: ha_window_function.cpp 9586 2013-05-31 22:02:06Z zzhu $
 *
 *
 ***********************************************************************/
#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <iostream>
#include <string>
using namespace std;

#include "idb_mysql.h"
#include "ha_mcs_impl_if.h"
#include "ha_mcs_sysvars.h"

#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "intervalcolumn.h"
#include "windowfunctioncolumn.h"
using namespace execplan;

#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "functor.h"
#include "funcexp.h"
using namespace funcexp;

#include "mcsv1_udaf.h"
using namespace mcsv1sdk;

#include "vlarray.h"

namespace cal_impl_if
{
ReturnedColumn* nullOnError(gp_walk_info& gwi)
{
  if (gwi.hasSubSelect)
  {
    gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NON_SUPPORT_SELECT_SUB);
    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText);
  }

  if (gwi.parseErrorText.empty())
  {
    gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_NON_SUPPORT);
    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText);
  }

  return NULL;
}

WF_FRAME frame(Window_frame_bound::Bound_precedence_type bound, Item* offset)
{
  switch (bound)
  {
    case Window_frame_bound::PRECEDING:
      if (offset)
        return WF_PRECEDING;
      else
        return WF_UNBOUNDED_PRECEDING;

    case Window_frame_bound::FOLLOWING:
      if (offset)
        return WF_FOLLOWING;
      else
        return WF_UNBOUNDED_FOLLOWING;

    case Window_frame_bound::CURRENT:  // Offset is meaningless
      return WF_CURRENT_ROW;

    default: return WF_UNKNOWN;
  }
}
ReturnedColumn* buildBoundExp(WF_Boundary& bound, SRCP& order, gp_walk_info& gwi)
{
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  bool addOp = true;
  ReturnedColumn* rc = NULL;

  if (bound.fFrame == execplan::WF_PRECEDING)
  {
    if (order->asc())
      addOp = false;
  }
  else if (!order->asc())  // must be WF_FOLLOWING
    addOp = false;

  funcexp::FunctionParm funcParms;
  SPTP sptp;
  IntervalColumn* intervalCol = dynamic_cast<IntervalColumn*>(bound.fVal.get());

  // @todo error out non constant. only support literal interval for now.
  if (!intervalCol && order->resultType().colDataType == CalpontSystemCatalog::DATE)
  {
    intervalCol = new IntervalColumn(bound.fVal, (int)IntervalColumn::INTERVAL_DAY);
    bound.fVal.reset(intervalCol);
  }

  if (intervalCol)
  {
    // date_add
    rc = new FunctionColumn();
    string funcName = "date_add_interval";

    // @bug6061 . YEAR, QUARTER, MONTH, WEEK, DAY type
    CalpontSystemCatalog::ColType ct;

    if (order->resultType().colDataType == CalpontSystemCatalog::DATE &&
        intervalCol->intervalType() <= IntervalColumn::INTERVAL_DAY)
    {
      ct.colDataType = CalpontSystemCatalog::DATE;
      ct.colWidth = 4;
    }
    else
    {
      ct.colDataType = CalpontSystemCatalog::DATETIME;
      ct.colWidth = 8;
    }

    // put interval val column to bound
    (dynamic_cast<FunctionColumn*>(rc))->functionName(funcName);
    (dynamic_cast<FunctionColumn*>(rc))->timeZone(gwi.timeZone);
    sptp.reset(new ParseTree(order->clone()));
    funcParms.push_back(sptp);
    sptp.reset(new ParseTree(intervalCol->val()->clone()));
    funcParms.push_back(sptp);
    funcParms.push_back(getIntervalType(&gwi, intervalCol->intervalType()));
    SRCP srcp(intervalCol->val());
    bound.fVal = srcp;

    if (addOp)
    {
      sptp.reset(new ParseTree(new ConstantColumn("ADD")));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
    }
    else
    {
      sptp.reset(new ParseTree(new ConstantColumn("SUB")));
      (dynamic_cast<ConstantColumn*>(sptp->data()))->timeZone(gwi.timeZone);
      funcParms.push_back(sptp);
    }

    (dynamic_cast<FunctionColumn*>(rc))->functionParms(funcParms);

    rc->resultType(ct);
    // @bug6061. Use result type as operation type for WF bound expression
    rc->operationType(ct);
    rc->expressionId(ci->expressionId++);
    return rc;
  }

  // arithmetic
  rc = new ArithmeticColumn();
  ArithmeticOperator* aop;

  if (addOp)
    aop = new ArithmeticOperator("+");
  else
    aop = new ArithmeticOperator("-");

  aop->timeZone(gwi.timeZone);
  ParseTree* pt = new ParseTree(aop);
  ParseTree *lhs = 0, *rhs = 0;
  lhs = new ParseTree(order->clone());
  rhs = new ParseTree(bound.fVal->clone());
  pt->left(lhs);
  pt->right(rhs);
  aop->resultType(order->resultType());
  aop->operationType(aop->resultType());
  (dynamic_cast<ArithmeticColumn*>(rc))->expression(pt);
  rc->resultType(aop->resultType());
  rc->operationType(aop->operationType());
  rc->expressionId(ci->expressionId++);
  return rc;
}

// Since columnstore implemented Windows Functions before MariaDB, we need
// map from the enum MariaDB uses to the string that columnstore uses to
// identify the function type.
string ConvertFuncName(Item_sum* item)
{
  switch (item->sum_func())
  {
    case Item_sum::COUNT_FUNC:
      if (!item->arguments()[0]->name.str)
        return "COUNT(*)";

      return "COUNT";
      break;

    case Item_sum::COUNT_DISTINCT_FUNC: return "COUNT_DISTINCT"; break;

    case Item_sum::SUM_FUNC: return "SUM"; break;

    case Item_sum::SUM_DISTINCT_FUNC: return "SUM_DISTINCT"; break;

    case Item_sum::AVG_FUNC: return "AVG"; break;

    case Item_sum::AVG_DISTINCT_FUNC: return "AVG_DISTINCT"; break;

    case Item_sum::MIN_FUNC: return "MIN"; break;

    case Item_sum::MAX_FUNC: return "MAX"; break;

    case Item_sum::STD_FUNC:
      if (((Item_sum_variance*)item)->sample)
        return "STDDEV_SAMP";
      else
        return "STDDEV_POP";

      break;

    case Item_sum::VARIANCE_FUNC:
      if (((Item_sum_variance*)item)->sample)
        return "VAR_SAMP";
      else
        return "VAR_POP";

      break;

    case Item_sum::SUM_BIT_FUNC:
      if (strcmp(item->func_name(), "bit_or(") == 0)
        return "BIT_OR";

      if (strcmp(item->func_name(), "bit_and(") == 0)
        return "BIT_AND";

      if (strcmp(item->func_name(), "bit_xor(") == 0)
        return "BIT_XOR";

      break;

    case Item_sum::UDF_SUM_FUNC: return "UDAF_FUNC"; break;

    case Item_sum::GROUP_CONCAT_FUNC:
      return "GROUP_CONCAT";  // Not supported
      break;

    case Item_sum::ROW_NUMBER_FUNC: return "ROW_NUMBER"; break;

    case Item_sum::RANK_FUNC: return "RANK"; break;

    case Item_sum::DENSE_RANK_FUNC: return "DENSE_RANK"; break;

    case Item_sum::PERCENT_RANK_FUNC: return "PERCENT_RANK"; break;

    case Item_sum::PERCENTILE_CONT_FUNC: return "PERCENTILE_CONT"; break;

    case Item_sum::PERCENTILE_DISC_FUNC: return "PERCENTILE_DISC";

    case Item_sum::CUME_DIST_FUNC: return "CUME_DIST"; break;

    case Item_sum::NTILE_FUNC: return "NTILE"; break;

    case Item_sum::FIRST_VALUE_FUNC: return "FIRST_VALUE"; break;

    case Item_sum::LAST_VALUE_FUNC: return "LAST_VALUE"; break;

    case Item_sum::NTH_VALUE_FUNC: return "NTH_VALUE"; break;

    case Item_sum::LEAD_FUNC: return "LEAD"; break;

    case Item_sum::LAG_FUNC: return "LAG"; break;
    default:
      // We just don't handle it.
      break;
  };

  return "";
}

ReturnedColumn* buildWindowFunctionColumn(Item* item, gp_walk_info& gwi, bool& nonSupport)
{
  //@todo fix print for create view
  // String str;
  // item->print(&str, QT_INFINIDB_NO_QUOTE);
  // cout << str.c_ptr() << endl;
  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  gwi.hasWindowFunc = true;
  Item_window_func* wf = (Item_window_func*)item;
  Item_sum* item_sum = wf->window_func();
  string funcName = ConvertFuncName(item_sum);
  WindowFunctionColumn* ac = new WindowFunctionColumn(funcName);
  ac->timeZone(gwi.timeZone);
  ac->distinct(item_sum->has_with_distinct());
  Window_spec* win_spec = wf->window_spec;
  SRCP srcp;
  CalpontSystemCatalog::ColType ct;  // For return type
  // arguments
  vector<SRCP> funcParms;

  for (uint32_t i = 0; i < item_sum->argument_count(); i++)
  {
    srcp.reset(buildReturnedColumn((item_sum->arguments()[i]), gwi, nonSupport));

    if (!srcp)
      return nullOnError(gwi);

    funcParms.push_back(srcp);

    if (gwi.clauseType == WHERE && !gwi.rcWorkStack.empty())
      gwi.rcWorkStack.pop();
  }

  // Setup UDAnF functions
  if (item_sum->sum_func() == Item_sum::UDF_SUM_FUNC)
  {
    Item_udf_sum* udfsum = (Item_udf_sum*)item_sum;

    mcsv1sdk::mcsv1Context& context = ac->getUDAFContext();
    context.setName(udfsum->func_name());

    // Set up the return type defaults for the call to init()
    execplan::CalpontSystemCatalog::ColType& rt = ac->resultType();
    context.setResultType(rt.colDataType);
    context.setColWidth(rt.colWidth);
    context.setScale(rt.scale);
    context.setPrecision(rt.precision);
    context.setParamCount(funcParms.size());

    utils::VLArray<mcsv1sdk::ColumnDatum> colTypes(funcParms.size());

    // Turn on the Analytic flag so the function is aware it is being called
    // as a Window Function.
    context.setContextFlag(CONTEXT_IS_ANALYTIC);

    // Build the column type vector.
    // Modified for MCOL-1201 multi-argument aggregate
    for (size_t i = 0; i < funcParms.size(); ++i)
    {
      const execplan::CalpontSystemCatalog::ColType& resultType = funcParms[i]->resultType();
      mcsv1sdk::ColumnDatum& colType = colTypes[i];
      colType.dataType = resultType.colDataType;
      colType.precision = resultType.precision;
      colType.scale = resultType.scale;
      colType.charsetNumber = resultType.charsetNumber;
    }

    // Call the user supplied init()
    if (context.getFunction()->init(&context, colTypes) == mcsv1_UDAF::ERROR)
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = context.getErrorMessage();
      return NULL;
    }

    if (!context.getRunFlag(UDAF_OVER_REQUIRED) && !context.getRunFlag(UDAF_OVER_ALLOWED))
    {
      gwi.parseErrorText =
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_UDANF_NOT_ALLOWED, context.getName());
      return nullOnError(gwi);
    }

    // Set the return type as set in init()
    ct.colDataType = context.getResultType();
    ct.colWidth = context.getColWidth();
    ct.scale = context.getScale();
    ct.precision = context.getPrecision();
    ac->resultType(ct);
  }

  // Some functions, such as LEAD/LAG don't have all parameters implemented in the
  // front end. Add dummies here to make the backend use defaults.
  // Some of these will be temporary until they are implemented in the front end.
  // Others need to stay because the back end expects them, but the front end
  // no longer sends them.
  // This case is kept in enum order in hopes the compiler can optimize
  switch (item_sum->sum_func())
  {
    case Item_sum::UDF_SUM_FUNC:
    {
      unsigned long bRespectNulls = (ac->getUDAFContext().getRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS)) ? 0 : 1;
      char sRespectNulls[18];
      sprintf(sRespectNulls, "%lu", bRespectNulls);
      srcp.reset(new ConstantColumn(sRespectNulls, (uint64_t)bRespectNulls,
                                    ConstantColumn::NUM));  // IGNORE/RESPECT NULLS. 1 => RESPECT
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      break;
    }

    case Item_sum::FIRST_VALUE_FUNC:
      srcp.reset(new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // OFFSET (always one)
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // FROM_FIRST
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(
          new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // IGNORE/RESPECT NULLS. 1 => RESPECT
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      break;

    case Item_sum::LAST_VALUE_FUNC:
      srcp.reset(new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // OFFSET (always one)
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(new ConstantColumn("0", (uint64_t)0, ConstantColumn::NUM));  // FROM_LAST
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(
          new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // IGNORE/RESPECT NULLS. 1 => RESPECT
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      break;

    case Item_sum::NTH_VALUE_FUNC:
      // When the front end supports these paramters, this needs modification
      srcp.reset(new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // FROM FIRST/LAST 1 => FIRST
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(
          new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // IGNORE/RESPECT NULLS. 1 => RESPECT
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      break;

    case Item_sum::LEAD_FUNC:
    case Item_sum::LAG_FUNC:
      // When the front end supports these paramters, this needs modification
      srcp.reset(new ConstantColumn("", ConstantColumn::NULLDATA));  // Default to fill in for NULL values
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      srcp.reset(
          new ConstantColumn("1", (uint64_t)1, ConstantColumn::NUM));  // IGNORE/RESPECT NULLS. 1 => RESPECT
      (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
      funcParms.push_back(srcp);
      break;

    default: break;
  };

  ac->functionParms(funcParms);

  // Partition by
  if (win_spec)
  {
    vector<SRCP> partitions;

    for (ORDER* ord = win_spec->partition_list->first; ord; ord = ord->next)
    {
      srcp.reset(buildReturnedColumn(*ord->item, gwi, nonSupport));

      if (!srcp)
        return nullOnError(gwi);

      partitions.push_back(srcp);
    }

    ac->partitions(partitions);

    // Order by
    WF_OrderBy orderBy;

    // order columns
    if (win_spec->order_list)
    {
      // It is an error to have an order by clause if a UDAnF says it shouldn't
      if (item_sum->sum_func() == Item_sum::UDF_SUM_FUNC)
      {
        mcsv1sdk::mcsv1Context& context = ac->getUDAFContext();

        if (!context.getRunFlag(UDAF_ORDER_ALLOWED))
        {
          gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
              logging::ERR_WF_UDANF_ORDER_NOT_ALLOWED, context.getName());
          return nullOnError(gwi);
        }
      }

      vector<SRCP> orders;
      ORDER* orderCol = reinterpret_cast<ORDER*>(win_spec->order_list->first);

      for (; orderCol; orderCol = orderCol->next)
      {
        Item* orderItem = *(orderCol->item);
        srcp.reset(buildReturnedColumn(orderItem, gwi, nonSupport));

        // MCOL-1052 GROUP BY handler has all of query's agg Items
        // as field and correlates them with its extended SELECT Items.
        if (!srcp)
        {
          orderItem = orderCol->item_ptr;

          if (orderItem)
          {
            gwi.fatalParseError = false;
            srcp.reset(buildReturnedColumn(orderItem, gwi, nonSupport));
          }
        }

        if (!srcp)
          return nullOnError(gwi);

        srcp->asc(orderCol->direction == ORDER::ORDER_ASC ? true : false);
        //					srcp->nullsFirst(orderCol->nulls); // nulls 2-default, 1-nulls first,
        //0-nulls last
        srcp->nullsFirst(orderCol->direction == ORDER::ORDER_ASC
                             ? 1
                             : 0);  // WINDOWS TODO: implement NULLS FIRST/LAST in 10.2 front end
        orders.push_back(srcp);
      }

      orderBy.fOrders = orders;
    }
    else
    {
      if (item_sum->sum_func() == Item_sum::UDF_SUM_FUNC)
      {
        mcsv1sdk::mcsv1Context& context = ac->getUDAFContext();

        if (context.getRunFlag(UDAF_ORDER_REQUIRED))
        {
          gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_UDANF_NOT_ALLOWED,
                                                                           context.getName());
          return nullOnError(gwi);
        }
      }
    }

    // window frame
    WF_Frame frm;

    if (win_spec->window_frame)
    {
      // It is an error to have a frame clause if a UDAnF says it shouldn't
      if (item_sum->sum_func() == Item_sum::UDF_SUM_FUNC)
      {
        mcsv1sdk::mcsv1Context& context = ac->getUDAFContext();

        if (!context.getRunFlag(UDAF_WINDOWFRAME_ALLOWED))
        {
          gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
              logging::ERR_WF_UDANF_FRAME_NOT_ALLOWED, context.getName());
          return nullOnError(gwi);
        }
      }

      frm.fIsRange = win_spec->window_frame->units == Window_frame::UNITS_RANGE;

      // start
      if (win_spec->window_frame->top_bound)
      {
        frm.fStart.fFrame = frame(win_spec->window_frame->top_bound->precedence_type,
                                  win_spec->window_frame->top_bound->offset);  // offset NULL means UNBOUNDED

        if (win_spec->window_frame->top_bound->offset)
        {
          frm.fStart.fVal.reset(
              buildReturnedColumn(win_spec->window_frame->top_bound->offset, gwi, nonSupport));

          if (!frm.fStart.fVal)
            return nullOnError(gwi);

          // 1. check expr is numeric type (rows) or interval (range)
          bool boundTypeErr = false;

          switch (frm.fStart.fVal->resultType().colDataType)
          {
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::VARBINARY:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::CLOB: boundTypeErr = true; break;

            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
              if (!frm.fIsRange)
                boundTypeErr = true;
              else if (dynamic_cast<IntervalColumn*>(frm.fStart.fVal.get()) == NULL)
                boundTypeErr = true;

              break;

            default:  // okay
              break;
          }

          if (boundTypeErr)
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
                logging::ERR_WF_INVALID_BOUND_TYPE,
                colDataTypeToString(frm.fStart.fVal->resultType().colDataType));
            return nullOnError(gwi);
          }
        }
      }

      // end
      if (win_spec->window_frame->bottom_bound)
      {
        frm.fEnd.fFrame = frame(win_spec->window_frame->bottom_bound->precedence_type,
                                win_spec->window_frame->bottom_bound->offset);

        if (win_spec->window_frame->bottom_bound->offset)
        {
          frm.fEnd.fVal.reset(
              buildReturnedColumn(win_spec->window_frame->bottom_bound->offset, gwi, nonSupport));

          if (!frm.fEnd.fVal)
            return nullOnError(gwi);

          // check expr is numeric type (rows) or interval (range)
          bool boundTypeErr = false;

          switch (frm.fEnd.fVal->resultType().colDataType)
          {
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::VARBINARY:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::CLOB: boundTypeErr = true; break;

            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
              if (!frm.fIsRange)
                boundTypeErr = true;
              else if (dynamic_cast<IntervalColumn*>(frm.fEnd.fVal.get()) == NULL)
                boundTypeErr = true;

              break;

            default:  // okay
              break;
          }

          if (boundTypeErr)
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
                logging::ERR_WF_INVALID_BOUND_TYPE,
                colDataTypeToString(frm.fStart.fVal->resultType().colDataType));
            return nullOnError(gwi);
          }
        }
      }
      else  // no end specified. default end to current row
      {
        frm.fEnd.fFrame = WF_CURRENT_ROW;
      }

      if (frm.fStart.fVal || frm.fEnd.fVal)
      {
        // check order by key only 1 (should be error-ed out in parser. double check here)
        if (frm.fIsRange && orderBy.fOrders.size() > 1)
        {
          gwi.fatalParseError = true;
          gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_INVALID_ORDER_KEY);
          return nullOnError(gwi);
        }

        // check order by key type is numeric or date/datetime
        bool orderTypeErr = false;

        if (frm.fIsRange && orderBy.fOrders.size() == 1)
        {
          switch (orderBy.fOrders[0]->resultType().colDataType)
          {
            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::VARBINARY:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::CLOB: orderTypeErr = true; break;

            default:  // okay

              // interval bound has to have date/datetime order key
              if ((dynamic_cast<IntervalColumn*>(frm.fStart.fVal.get()) != NULL ||
                   dynamic_cast<IntervalColumn*>(frm.fEnd.fVal.get()) != NULL))
              {
                if (orderBy.fOrders[0]->resultType().colDataType != CalpontSystemCatalog::DATE &&
                    orderBy.fOrders[0]->resultType().colDataType != CalpontSystemCatalog::DATETIME)
                  orderTypeErr = true;
              }
              else
              {
                if (orderBy.fOrders[0]->resultType().colDataType == CalpontSystemCatalog::DATETIME)
                  orderTypeErr = true;
              }

              break;
          }

          if (orderTypeErr)
          {
            gwi.fatalParseError = true;
            gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
                logging::ERR_WF_INVALID_ORDER_TYPE,
                colDataTypeToString(orderBy.fOrders[0]->resultType().colDataType));
            return nullOnError(gwi);
          }
        }
      }

      // construct +,- or interval function for boundary
      if (frm.fIsRange && frm.fStart.fVal)
      {
        frm.fStart.fBound.reset(buildBoundExp(frm.fStart, orderBy.fOrders[0], gwi));

        if (!frm.fStart.fBound)
          return nullOnError(gwi);
      }

      if (frm.fIsRange && frm.fEnd.fVal)
      {
        frm.fEnd.fBound.reset(buildBoundExp(frm.fEnd, orderBy.fOrders[0], gwi));

        if (!frm.fEnd.fVal)
          return nullOnError(gwi);
      }
    }
    else
    {
      // Certain function types have different default boundaries
      // This case is kept in enum order in hopes the compiler can optimize
      switch (item_sum->sum_func())
      {
        case Item_sum::COUNT_FUNC:
        case Item_sum::COUNT_DISTINCT_FUNC:
        case Item_sum::SUM_FUNC:
        case Item_sum::SUM_DISTINCT_FUNC:
        case Item_sum::AVG_FUNC:
        case Item_sum::AVG_DISTINCT_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;

        case Item_sum::MIN_FUNC:
        case Item_sum::MAX_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          //					frm.fEnd.fFrame = WF_UNBOUNDED_FOLLOWING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;

        case Item_sum::STD_FUNC:
        case Item_sum::VARIANCE_FUNC:
        case Item_sum::SUM_BIT_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;

        case Item_sum::UDF_SUM_FUNC:
        {
          // UDAnF functions each have their own default set in context.
          mcsv1sdk::mcsv1Context& context = ac->getUDAFContext();

          if (context.getRunFlag(UDAF_WINDOWFRAME_REQUIRED))
          {
            gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(
                logging::ERR_WF_UDANF_FRAME_REQUIRED, context.getName());
            return nullOnError(gwi);
          }

          int32_t bound;
          context.getStartFrame(frm.fStart.fFrame, bound);

          if (frm.fStart.fFrame == execplan::WF_PRECEDING)
          {
            if (bound == 0)
              bound = 1;

            srcp.reset(new ConstantColumn((int64_t)bound));
            (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
            frm.fStart.fVal = srcp;
            frm.fStart.fBound.reset(buildBoundExp(frm.fStart, srcp, gwi));

            if (!frm.fStart.fBound)
              return nullOnError(gwi);
          }

          context.getEndFrame(frm.fEnd.fFrame, bound);

          if (frm.fEnd.fFrame == execplan::WF_FOLLOWING)
          {
            if (bound == 0)
              bound = 1;

            srcp.reset(new ConstantColumn((int64_t)bound));
            (dynamic_cast<ConstantColumn*>(srcp.get()))->timeZone(gwi.timeZone);
            frm.fEnd.fVal = srcp;
            frm.fEnd.fBound.reset(buildBoundExp(frm.fEnd, srcp, gwi));

            if (!frm.fEnd.fBound)
              return nullOnError(gwi);
          }

          break;
        }

        case Item_sum::GROUP_CONCAT_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;

        case Item_sum::ROW_NUMBER_FUNC:
        case Item_sum::RANK_FUNC:
        case Item_sum::DENSE_RANK_FUNC:
        case Item_sum::PERCENT_RANK_FUNC:
        case Item_sum::CUME_DIST_FUNC:
        case Item_sum::NTILE_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_UNBOUNDED_FOLLOWING;
          break;

        case Item_sum::FIRST_VALUE_FUNC:
        case Item_sum::LAST_VALUE_FUNC:
        case Item_sum::NTH_VALUE_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;

        case Item_sum::LEAD_FUNC:
        case Item_sum::LAG_FUNC:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_UNBOUNDED_FOLLOWING;
          break;

        default:
          frm.fStart.fFrame = WF_UNBOUNDED_PRECEDING;
          frm.fEnd.fFrame = WF_CURRENT_ROW;
          break;
      };
    }

    orderBy.fFrame = frm;
    ac->orderBy(orderBy);
  }

  if (gwi.fatalParseError || nonSupport)
  {
    if (gwi.parseErrorText.empty())
      gwi.parseErrorText = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_WF_NON_SUPPORT);

    setError(gwi.thd, ER_CHECK_NOT_IMPLEMENTED, gwi.parseErrorText);
    return NULL;
  }
#if 0
    if (item_sum->sum_func() != Item_sum::UDF_SUM_FUNC &&
        item_sum->sum_func() != Item_sum::SUM_FUNC && 
        item_sum->sum_func() != Item_sum::SUM_DISTINCT_FUNC &&
        item_sum->sum_func() != Item_sum::AVG_FUNC &&
        item_sum->sum_func() != Item_sum::AVG_DISTINCT_FUNC)
#endif
  if (item_sum->sum_func() != Item_sum::UDF_SUM_FUNC)
  {
    ac->resultType(colType_MysqlToIDB(item_sum));
    // bug5736. Make the result type double for some window functions when
    // plugin variable double_for_decimal_math is set.
    ac->adjustResultType();
  }

  ac->expressionId(ci->expressionId++);

  if (item->full_name())
    ac->alias(item->full_name());

  ac->charsetNumber(item->collation.collation->number);

  // put ac on windowFuncList
  gwi.windowFuncList.push_back(ac);
  return ac;
}

}  // namespace cal_impl_if
