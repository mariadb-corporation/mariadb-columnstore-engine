#include <random>
#include <format>
#include <memory>

#include "mock.h"
#include "calpontselectexecutionplan.h"
#include "clientrotator.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "simplecolumn.h"
#include "returnedcolumn.h"
#include "simplefilter.h"
#include "errorids.h"

namespace execplan
{
namespace
{
using RowSize = uint32_t;

RowSize doProcessQuery(const CalpontExecutionPlan& csep, std::shared_ptr<execplan::ClientRotator> exeMgr)
{
  messageqcpp::ByteStream msg;

  // send code to indicat tuple
  messageqcpp::ByteStream::quadbyte qb = 4;
  msg << qb;
  exeMgr->write(msg);
  msg.restart();

  // Send the CalpontSelectExecutionPlan to ExeMgr.
  csep.serialize(msg);
  exeMgr->write(msg);

  // Get the table oid for the system table being queried.
  uint32_t tableOID = IDB_VTABLE_ID;
  uint16_t status = 0;

  // Send the request for the table.
  qb = static_cast<messageqcpp::ByteStream::quadbyte>(tableOID);
  messageqcpp::ByteStream bs;
  bs << qb;
  exeMgr->write(bs);
  std::unique_ptr<rowgroup::RowGroup> rowGroup;
  rowgroup::RGData rgData;

  msg.restart();
  bs.restart();
  msg = exeMgr->read();
  bs = exeMgr->read();

  if (bs.length() == 0)
  {
    throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
  }

  std::string emsgStr;
  bs >> emsgStr;
  bool err = false;

  if (msg.length() == 4)
  {
    msg >> qb;

    if (qb != 0)
      err = true;
  }
  else
  {
    err = true;
  }

  if (err)
  {
    throw runtime_error(emsgStr);
  }

  while (true)
  {
    bs.restart();
    bs = exeMgr->read();

    // @bug 1782. check ExeMgr connection lost
    if (bs.length() == 0)
      throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);

    if (!rowGroup)
    {
      rowGroup.reset(new rowgroup::RowGroup());
      rowGroup->deserialize(bs);
      continue;
    }
    else
    {
      rgData.deserialize(bs, true);
      rowGroup->setData(&rgData);
    }

    if ((status = rowGroup->getStatus()) != 0)
    {
      if (status >= 1000)  // new error system
      {
        // bs.advance(rowGroup->getDataSize());
        bs >> emsgStr;
        throw logging::IDBExcept(emsgStr, rowGroup->getStatus());
      }
      else
      {
        throw logging::IDBExcept(logging::ERR_SYSTEM_CATALOG);
      }
    }

    if (rowGroup->getRowCount() > 0)
      // rowGroup->addToSysDataList(sysDataList);
      rowGroup->getColumnCount();
    else
      break;
  }

  bs.reset();
  qb = 0;
  bs << qb;
  exeMgr->write(bs);
  return 0;
}

RowSize processQuery(const CalpontExecutionPlan& cesp)
{
  RowSize result = 0;
  auto exeMgr = std::make_shared<execplan::ClientRotator>(0, "ExeMgr");
  int maxTries = 0;
  while (maxTries < 5)
  {
    maxTries++;

    try
    {
      result = doProcessQuery(cesp, exeMgr);
      break;
    }
    // error already occurred. this is not a broken pipe
    catch (logging::IDBExcept&)
    {
      throw;
    }
    catch (...)
    {
      // may be a broken pipe. re-establish exeMgr and send the message
      exeMgr.reset(new ClientRotator(0, "ExeMgr"));
      try
      {
        exeMgr->connect(5);
      }
      catch (...)
      {
        throw logging::IDBExcept(logging::ERR_LOST_CONN_EXEMGR);
      }
    }
  }
  if (maxTries >= 5)
    throw logging::IDBExcept(logging::ERR_SYSTEM_CATALOG);

  return result;
}
RowSize getAUXPartitionSize(BRM::LogicalPartition partitionNum,
                            execplan::CalpontSystemCatalog::TableName tableNameObj)
{
  CalpontSelectExecutionPlan csep;

  CalpontSystemCatalog csc;
  CalpontSystemCatalog::RIDList oidList = csc.columnRIDs(tableNameObj);
  if (oidList.empty())
  {
    return 0;
  }

  std::string colName = csc.colName(oidList[0].objnum).column;

  // SQL Statement: select count(col) from target_table from idbpartition(col) = "*.*.*";
  std::string schemaTablePrefix = std::format("{}.{}.", tableNameObj.schema, tableNameObj.table);
  FunctionColumn* fc1 = new FunctionColumn("count", schemaTablePrefix + colName);
  FunctionColumn* fc2 = new FunctionColumn("idbpartition", schemaTablePrefix + colName);
  SimpleColumn* sc = new SimpleColumn(colName);

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
                                            execplan::CalpontSystemCatalog::TableName tableName)
{
  std::vector<bool> auxColMock;
  uint32_t rowSize = getAUXPartitionSize(partitionNum, tableName);
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
