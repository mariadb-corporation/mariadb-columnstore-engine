#include <random>
#include <format>
#include <memory>

#include "mock.h"
#include "calpontselectexecutionplan.h"
#include "clientrotator.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "sessionmanager.h"
#include "simplecolumn.h"
#include "returnedcolumn.h"
#include "simplefilter.h"
#include "errorids.h"

namespace execplan
{
namespace
{
using RowSize = uint32_t;

RowSize processQuery(const CalpontSelectExecutionPlan& csep)
{
  auto exeMgr = std::make_unique<execplan::ClientRotator>(csep.sessionID(), "ExeMgr");
  try
  {
    exeMgr->connect(5);
  }
  catch (...)
  {
    throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
  }

  messageqcpp::ByteStream msgCSEP;

  // send code to indicat tuple
  messageqcpp::ByteStream::quadbyte qb = 4;
  msgCSEP << qb;
  exeMgr->write(msgCSEP);
  msgCSEP.restart();

  // Send the CalpontSelectExecutionPlan to ExeMgr.
  csep.serialize(msgCSEP);
  exeMgr->write(msgCSEP);

  msgCSEP.restart();
  msgCSEP = exeMgr->read();

  messageqcpp::ByteStream msg;
  msg = exeMgr->read();
  if (msg.length() == 0)
  {
    throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
  }

  std::string errMsg;
  msg >> errMsg;

  bool hasErr = false;
  if (msgCSEP.length() == 4)
  {
    msgCSEP >> qb;

    if (qb != 0)
      hasErr = true;
  }
  else
  {
    hasErr = true;
  }

  if (hasErr)
  {
    throw runtime_error(errMsg);
  }

  std::unique_ptr<rowgroup::RowGroup> rowGroup;
  rowgroup::RGData rgData;
  RowSize result = 0;
  while (true)
  {
    msg.restart();
    msg = exeMgr->read();

    // ExeMgr connection lost
    if (msg.length() == 0)
    {
      throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
    }

    if (!rowGroup)
    {
      rowGroup.reset(new rowgroup::RowGroup());
      rowGroup->deserialize(msg);
      continue;
    }
    else
    {
      rgData.deserialize(msg, true);
      rowGroup->setData(&rgData);
    }

    if (uint32_t status = rowGroup->getStatus(); status != 0)
    {
      if (status >= 1000)  // new error system
      {
        msg >> errMsg;
        throw logging::IDBExcept(errMsg, rowGroup->getStatus());
      }
      else
      {
        throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
      }
    }

    if (uint32_t rowSize = rowGroup->getRowCount(); rowSize > 0)
    {
      rowgroup::Row row;
      rowGroup->initRow(&row);
      for (uint32_t i = 0; i < rowSize; i++)
      {
        rowGroup->getRow(i, &row);
        result = row.getUintField(0);
      }
    }
    else
      break;
  }

  msg.reset();
  qb = 0;
  msg << qb;
  exeMgr->write(msg);
  return result;
}

RowSize getAUXPartitionSize(BRM::LogicalPartition partitionNum,
                            execplan::CalpontSystemCatalog::TableName tableNameObj, int sessionID)
{
  CalpontSelectExecutionPlan csep;
  csep.sessionID(sessionID);

  auto csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(execplan::CalpontSystemCatalog::FE);
  CalpontSystemCatalog::RIDList oidList = csc->columnRIDs(tableNameObj);
  if (oidList.empty())
  {
    return 0;
  }

  SessionManager sm;
  BRM::TxnID txnID;
  txnID = sm.getTxnID(sessionID);

  if (!txnID.valid)
  {
    txnID.id = 0;
    txnID.valid = true;
  }

  BRM::QueryContext verID;
  verID = sm.verID();
  csep.txnID(txnID.id);
  csep.verID(verID);
  csep.sessionID(sessionID);

  std::string colName = csc->colName(oidList[0].objnum).column;

  // SQL Statement: select count(col) from target_table from idbpartition(col) = "*.*.*";
  std::string schemaTablePrefix = std::format("{}.{}.", tableNameObj.schema, tableNameObj.table);
  FunctionColumn* fc1 = new FunctionColumn("count", schemaTablePrefix + colName, sessionID);
  FunctionColumn* fc2 = new FunctionColumn("idbpartition", schemaTablePrefix + colName, sessionID);
  SimpleColumn* sc = new SimpleColumn(colName, sessionID);

  CalpontSelectExecutionPlan::ColumnMap colMap;
  SRCP srcp;
  srcp.reset(fc1);
  colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(fc1->data(), srcp));
  srcp.reset(fc2);
  colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(fc2->data(), srcp));
  srcp.reset(sc);
  colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(schemaTablePrefix + colName, srcp));
  csep.columnMapNonStatic(colMap);

  srcp.reset(fc1->clone());
  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  SOP opeq(new Operator("="));
  std::string partitionNumStr =
      std::format("{}.{}.{}", partitionNum.dbroot, partitionNum.pp, partitionNum.seg);
  SimpleFilter* sf =
      new SimpleFilter(opeq, fc2->clone(), new ConstantColumn(partitionNumStr, ConstantColumn::LITERAL));

  CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
  filterTokenList.push_back(sf);
  csep.filterTokenList(filterTokenList);

  CalpontSelectExecutionPlan::TableList tablelist;
  tablelist.push_back(make_aliastable(tableNameObj.schema, tableNameObj.table, ""));
  csep.tableList(tablelist);

  ostringstream oss;
  oss << std::format("select {} from {}.{} where {}='{}' --getTableAUXColSize/", fc1->data(),
                     tableNameObj.schema, tableNameObj.table, fc2->data(), partitionNumStr);
  csep.data(oss.str());

  return processQuery(csep);
}
}  // namespace

std::vector<bool> getPartitionDeletedBitmap(BRM::LogicalPartition partitionNum,
                                            execplan::CalpontSystemCatalog::TableName tableName,
                                            int sessionID)
{
  std::vector<bool> auxColMock;
  uint32_t rowSize = getAUXPartitionSize(partitionNum, tableName, sessionID);
  auxColMock.reserve(rowSize);

  std::random_device rd;
  std::mt19937 gen(rd());
  // 50% chance for true or false
  std::bernoulli_distribution d(0.5);

  for (size_t i = 0; i < rowSize; ++i)
  {
    auxColMock.push_back(d(gen));
  }

  return auxColMock;
}
}  // namespace execplan
