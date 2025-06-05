#include "execplan/calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "execplan/calpontsystemcatalog.h"
#include "simplefilter.h"
#include "constantcolumn.h"

using namespace execplan;
const SOP opeq(new Operator("="));



int main()
{
  CalpontSelectExecutionPlan csep;

  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
  CalpontSelectExecutionPlan::ColumnMap colMap;

  string columnlength = CALPONT_SCHEMA + "." + SYSCOLUMN_TABLE + "." + COLUMNLEN_COL;

  SimpleColumn* col[1];
  col[0] = new SimpleColumn(columnlength, 22222222);
  
  SRCP srcp;
  srcp.reset(col[0]);
  colMap.insert({columnlength, srcp});
  csep.columnMapNonStatic(colMap);
  srcp.reset(col[0]->clone());
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  {
    SCSEP csepDerived(new CalpontSelectExecutionPlan());
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnListLocal;
    CalpontSelectExecutionPlan::ColumnMap colMapLocal;

    string columnlength = CALPONT_SCHEMA + "." + SYSCOLUMN_TABLE + "." + COLUMNLEN_COL;

    SimpleColumn* col[1];
    col[0] = new SimpleColumn(columnlength, 22222222);
  
    SRCP srcpLocal;
    srcpLocal.reset(col[0]);
    colMapLocal.insert({columnlength, srcpLocal});
    csepDerived->columnMapNonStatic(colMapLocal);

    srcp.reset(col[0]->clone());
    returnedColumnListLocal.push_back(srcpLocal);
    csepDerived->returnedCols(returnedColumnList);

    CalpontSelectExecutionPlan::SelectList derivedTables;
    derivedTables.push_back(csepDerived);
    csep.derivedTableList(derivedTables);
  }
  
  CalpontSelectExecutionPlan::TableList tableList = {execplan::CalpontSystemCatalog::TableAliasName("", "", "alias")};
  csep.tableList(tableList);

  CalpontSelectExecutionPlan::SelectList unionVec;

  for (size_t i = 0; i < 3; ++i)
  {
    SCSEP plan(new CalpontSelectExecutionPlan());
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnListLocal;
    CalpontSelectExecutionPlan::ColumnMap colMapLocal;

    SRCP srcpLocal;
    srcpLocal.reset(col[0]);
    colMapLocal.insert({columnlength, srcpLocal});
    plan->columnMapNonStatic(colMapLocal);
    srcpLocal.reset(col[0]->clone());
    returnedColumnListLocal.push_back(srcpLocal);
    plan->returnedCols(returnedColumnListLocal);

    plan->txnID(csep.txnID());
    plan->verID(csep.verID());
    plan->sessionID(csep.sessionID());
    plan->columnMapNonStatic(colMapLocal);
    plan->returnedCols(returnedColumnListLocal);
    unionVec.push_back(plan);

    // std::cout << plan->toString() << std::endl;
  }
  
  csep.unionVec(unionVec);
  std::cout << csep.toString() << std::endl;
}