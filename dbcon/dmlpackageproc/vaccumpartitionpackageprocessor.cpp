/* Copyright (C) 2024 MariaDB Corporation

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

#include <iostream>
#include <boost/scoped_ptr.hpp>

#include "vaccumpartitionpackageprocessor.h"

#include "messagelog.h"
#include "simplecolumn.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "rowgroup.h"
#include "bytestream.h"
#include "we_messages.h"
#include "oamcache.h"
#include "tablelockdata.h"
#include "bytestream.h"
#include "mockutils.h"

namespace dmlpackageprocessor
{
DMLPackageProcessor::DMLResult VaccumPartitionPackageProcessor::processPackageInternal(
    dmlpackage::CalpontDMLPackage& cpackage)
{
  SUMMARY_INFO("VaccumPartitionPackageProcessor::processPackageInternal");

  DMLResult result;
  result.result = NO_ERROR;

  BRM::TxnID txnID;
  txnID.id = cpackage.get_TxnID();
  txnID.valid = true;

  if (int rc = fDbrm->isReadWrite(); rc != 0)
  {
    logging::Message::Args args;
    logging::Message message(9);
    args.add("Unable to execute the statement due to DBRM is read only");
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    fSessionManager.rolledback(txnID);
    return result;
  }

  auto vaccumPartitionPkg = dynamic_cast<dmlpackage::VaccumPartitionDMLPackage*>(&cpackage);
  if (!vaccumPartitionPkg)
  {
    logging::Message::Args args;
    logging::Message message(9);
    args.add("VaccumPartitionDMLPackage wrong cast");
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    return result;
  }

  fSessionID = cpackage.get_SessionID();
  VERBOSE_INFO("VaccumPartitionPackageProcessor is processing CalpontDMLPackage ...");

  uint64_t uniqueId = 0;
  try
  {
    uniqueId = fDbrm->getUnique64();
  }
  catch (std::exception& ex)
  {
    logging::Message::Args args;
    logging::Message message(9);
    args.add(ex.what());
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    fSessionManager.rolledback(txnID);
    return result;
  }
  catch (...)
  {
    logging::Message::Args args;
    logging::Message message(9);
    args.add("Unknown error occurred while getting unique number.");
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    fSessionManager.rolledback(txnID);
    return result;
  }
  fWEClient->addQueue(uniqueId);

  TablelockData* tablelockData = TablelockData::makeTablelockData(fSessionID);

  auto systemCatalogPtr = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
  execplan::CalpontSystemCatalog::TableName tableName(cpackage.get_SchemaName(), cpackage.get_TableName());
  execplan::CalpontSystemCatalog::ROPair roPair;
  try
  {
    string stmt =
        cpackage.get_SQLStatement() + "|" + cpackage.get_SchemaName() + "|" + cpackage.get_TableName();
    logging::SQLLogger sqlLogger(stmt, DMLLoggingId, fSessionID, txnID.id);

    roPair = systemCatalogPtr->tableRID(tableName);
    // Check whether this table is locked already for this session
    uint64_t tableLockId = tablelockData->getTablelockId(roPair.objnum);
    if (!tableLockId)
    {
      uint32_t processID = ::getpid();
      int32_t txnId = txnID.id;
      std::string processName("DMLProc");
      int32_t sessionId = fSessionID;
      oam::OamCache* oamcache = oam::OamCache::makeOamCache();

      std::vector<int> pmList = oamcache->getModuleIds();
      std::vector<uint32_t> pms;
      for (unsigned i = 0; i < pmList.size(); i++)
      {
        pms.push_back((uint32_t)pmList[i]);
      }

      try
      {
        tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId,
                                          BRM::LOADING);
      }
      catch (std::exception&)
      {
        throw std::runtime_error(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_HARD_FAILURE));
      }

      // Retry 30 times when failed to get lock
      if (!tableLockId)
      {
        int waitPeriod = 10;
        int sleepTime = 100;  // sleep 100 milliseconds between checks
        int maxTries = 30;    // try 30 times (3 seconds)
        waitPeriod = WriteEngine::Config::getWaitPeriod();
        maxTries = waitPeriod * 10;
        struct timespec rm_ts;

        rm_ts.tv_sec = sleepTime / 1000;
        rm_ts.tv_nsec = sleepTime % 1000 * 1000000;

        int numTries = 0;
        for (; numTries < maxTries; numTries++)
        {
          struct timespec abs_ts;

          do
          {
            abs_ts.tv_sec = rm_ts.tv_sec;
            abs_ts.tv_nsec = rm_ts.tv_nsec;
          } while (nanosleep(&abs_ts, &rm_ts) < 0);

          try
          {
            processID = ::getpid();
            txnId = txnID.id;
            sessionId = fSessionID;
            processName = "DMLProc";
            tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId,
                                              &txnId, BRM::LOADING);
          }
          catch (std::exception&)
          {
            throw std::runtime_error(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_HARD_FAILURE));
          }

          if (tableLockId > 0)
            break;

          if (numTries >= maxTries)
          {
            result.result = VACCUM_ERROR;
            logging::Message::Args args;
            args.add("Vaccum");
            args.add(processName);
            args.add((uint64_t)processID);
            args.add(sessionId);
            throw std::runtime_error(
                logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_TABLE_LOCKED, args));
          }
        }
      }
    }

    tablelockData->setTablelock(roPair.objnum, tableLockId);
    execplan::CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName);
    execplan::CalpontSystemCatalog::ColType colType;

    for (const auto& [_, objnum] : ridList)
    {
      // If user hit ctrl+c in the mysql console, fRollbackPending will be true.
      if (fRollbackPending)
      {
        result.result = JOB_CANCELED;
        break;
      }

      colType = systemCatalogPtr->colType(objnum);

      if (colType.autoincrement)
      {
        try
        {
          uint64_t nextVal = systemCatalogPtr->nextAutoIncrValue(tableName);
          fDbrm->startAISequence(objnum, nextVal, colType.colWidth, colType.colDataType);
          break;
        }
        catch (std::exception& ex)
        {
          result.result = VACCUM_ERROR;
          throw std::runtime_error(ex.what());
        }
      }
    }

    uint32_t rowsProcessed = doVaccumRows(*vaccumPartitionPkg, result, uniqueId, roPair.objnum);

    if (result.result == JOB_CANCELED)
      throw std::runtime_error("Query execution was interrupted");

    if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
      throw std::runtime_error(result.message.msg());

    result.rowCount = rowsProcessed;

    logging::logDML(cpackage.get_SessionID(), txnID.id, cpackage.get_SQLStatement(),
                    cpackage.get_SchemaName());
  }
  catch (exception& ex)
  {
    if (checkPPLostConnection(ex))
    {
      result.result = PP_LOST_CONNECTION;
    }
    else
    {
      cerr << "VaccumPartitionPackageProcessor::processPackage: " << ex.what() << endl;

      if (result.result == 0)
      {
        result.result = VACCUM_ERROR;
      }

      result.message = logging::Message(ex.what());
    }
  }
  catch (...)
  {
    cerr << "VaccumPartitionPackageProcessor::processPackage: caught unknown exception!" << endl;
    logging::Message::Args args;
    logging::Message message(7);
    args.add("Delete Failed: ");
    args.add("encountered unknown exception");
    args.add(result.message.msg());
    args.add("");
    message.format(args);

    result.result = VACCUM_ERROR;
    result.message = message;
  }

  std::map<uint32_t, uint32_t> oids;
  int rc = 0;

  if (result.result == NO_ERROR)
  {
    rc = flushDataFiles(result.result, oids, uniqueId, txnID, roPair.objnum);

    if (rc != NO_ERROR)
    {
      cerr << "VaccumPartitionPackageProcessor::processPackage: write data to disk failed" << endl;

      if (!fRollbackPending)
      {
        logging::Message::Args args;
        logging::Message message(7);
        args.add("vaccum Failed: ");
        args.add("error when writing data to disk");
        args.add("");
        args.add("");
        message.format(args);

        result.result = VACCUM_ERROR;
        result.message = message;
      }

      result.rowCount = 0;
      rc = endTransaction(uniqueId, txnID, false);

      if ((rc != NO_ERROR) && (!fRollbackPending))
      {
        logging::Message::Args args;
        logging::Message message(7);
        args.add("Delete Failed: ");
        args.add("error when cleaning up data files");
        args.add("");
        args.add("");
        message.format(args);

        result.result = VACCUM_ERROR;
        result.message = message;
        result.rowCount = 0;
      }
    }
    else
    {
      if (fRollbackPending)
        rc = endTransaction(uniqueId, txnID, false);
      else
        rc = endTransaction(uniqueId, txnID, true);
    }
  }
  else
  {
    rc = flushDataFiles(result.result, oids, uniqueId, txnID, roPair.objnum);
    result.rowCount = 0;
    rc = endTransaction(uniqueId, txnID, false);
  }

  if (fRollbackPending)
  {
    result.result = JOB_CANCELED;
    logging::Message::Args args;
    args.add("Query execution was interrupted");
    result.message.format(args);
  }

  fWEClient->removeQueue(uniqueId);

  VERBOSE_INFO("Finished Processing Delete DML Package");
  return result;
}

uint64_t VaccumPartitionPackageProcessor::doVaccumRows(dmlpackage::VaccumPartitionDMLPackage& package,
                                                       DMLResult& result, const uint64_t uniqueId,
                                                       const uint32_t tableOid)
{
  messageqcpp::ByteStream bs, bsCpy, bsErrMsg;
  rowgroup::RGData rgData;
  uint32_t qb = 4;
  bs << qb;
  std::unique_ptr<rowgroup::RowGroup> rowGroup;
  uint64_t rowsProcessed = 0;
  uint32_t dbroot = 1;
  oam::OamCache* oamCache = oam::OamCache::makeOamCache();
  std::vector<int> pmIds = oamCache->getModuleIds();
  std::map<unsigned, bool> pmState;
  std::string errMsg;
  bool err = false;

  try
  {
    for (const int& pmId : pmIds)
    {
      pmState[pmId] = true;
    }

    fExeMgr->write(bs);
    // TODO select * from t where idbpartition(col) = "*.*.*"
    fExeMgr->write(*(package.get_ExecutionPlan()));

    bs.restart();
    bs = fExeMgr->read();
    if (bs.length() == 4)
    {
      bs >> qb;

      if (qb != 0)
        err = true;
    }
    else
    {
      qb = 999;
      err = true;
    }

    if (err)
    {
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ExeMgr Error");
      args.add((int)qb);
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
      return rowsProcessed;
    }

    bsErrMsg.restart();
    bsErrMsg = fExeMgr->read();
    if (bsErrMsg.empty())
    {
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ");
      args.add("Lost connection to ExeMgr");
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
      return rowsProcessed;
    }

    bsErrMsg >> errMsg;

    while (true)
    {
      if (fRollbackPending)
      {
        break;
      }

      bs.restart();
      bs = fExeMgr->read();
      if (bs.empty())
      {
        cerr << "VaccumPartitionPackageProcessor::processPackage::doVaccumRows" << endl;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add("Lost connection to ExeMgr");
        message.format(args);
        result.result = VACCUM_ERROR;
        result.message = message;
        break;
      }
      else
      {
        bsCpy.restart();
        bsCpy = bs;

        if (rowGroup.get() == NULL)
        {
          // This is mete data, need to send all PMs.
          processMetaRG(bsCpy, result, uniqueId, package, pmState, dbroot);
          rowGroup = std::make_unique<rowgroup::RowGroup>();
          rowGroup->deserialize(bs);
          bs.restart();
          qb = 100;
          bs << qb;
          fExeMgr->write(bs);
          continue;
        }

        rgData.deserialize(bs, true);
        rowGroup->setData(&rgData);

        if (bool err = (rowGroup->getStatus() != 0); err)
        {
          string errorMsg;
          bs >> errorMsg;
          logging::Message::Args args;
          logging::Message message(2);
          args.add("Update Failed: ");
          args.add(errorMsg);
          message.format(args);
          result.result = VACCUM_ERROR;
          result.message = message;
          DMLResult tmpResult;
          receiveAll(tmpResult, uniqueId, pmIds, pmState, tableOid);
          break;
        }

        if (rowGroup->getRGData() == NULL)
        {
          bs.restart();
        }
        // done fetching
        if (rowGroup->getRowCount() == 0)
        {
          err = receiveAll(result, uniqueId, pmIds, pmState, tableOid);
          break;
        }

        if (rowGroup->getBaseRid() == (uint64_t)(-1))
        {
          continue;
        }

        dbroot = rowGroup->getDBRoot();

        if (bool err = processRG(bsCpy, result, uniqueId, package, pmState, dbroot); err)
        {
          logging::LoggingID logid(DMLLoggingId, fSessionID, package.get_TxnID());
          logging::Message::Args args;
          logging::Message msg(1);
          args.add("SQL statement erroring out, need to receive all messages from WES");
          msg.format(args);
          logging::Logger logger(logid.fSubsysID);
          logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);

          DMLResult tmpResult;
          receiveAll(tmpResult, uniqueId, pmIds, pmState, tableOid);

          logging::Message::Args args2;
          logging::Message msg2(1);
          args2.add("SQL statement erroring out, received all messages from WES");
          msg2.format(args2);
          logger.logMessage(logging::LOG_TYPE_DEBUG, msg2, logid);
          break;
        }

        rowsProcessed += rowGroup->getRowCount();
      }
    }

    if (fRollbackPending)
    {
      err = true;
      cerr << "VaccumPartitionPackageProcessor::processPackage::doVaccumRows Rollback Pending" << endl;
      result.result = JOB_CANCELED;

      logging::LoggingID logid(DMLLoggingId, fSessionID, package.get_TxnID());
      logging::Message::Args args1;
      logging::Message msg1(1);
      args1.add("SQL statement canceled by user");
      msg1.format(args1);
      logging::Logger logger(logid.fSubsysID);
      logger.logMessage(logging::LOG_TYPE_DEBUG, msg1, logid);

      DMLResult tmpResult;
      receiveAll(tmpResult, uniqueId, pmIds, pmState, tableOid);
    }

    if (!err)
    {
      qb = 3;
      bs.restart();
      bs << qb;
      fExeMgr->write(bs);
      bs = fExeMgr->read();
      bs >> result.queryStats;
      bs >> result.extendedStats;
      bs >> result.miniStats;
      result.stats.unserialize(bs);
    }

    if (err)
    {
      bs.restart();
      bs << qb;
      fExeMgr->write(bs);
    }

    return rowsProcessed;
  }
  catch (runtime_error& ex)
  {
    cerr << "VaccumPartitionPackageProcessor::processPackage::doVaccumRows" << ex.what() << endl;
    logging::Message::Args args;
    logging::Message message(2);
    args.add("Update Failed: ");
    args.add(ex.what());
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    qb = 0;
    bs.restart();
    bs << qb;
    fExeMgr->write(bs);
    return rowsProcessed;
  }
  catch (...)
  {
    cerr << "VaccumPartitionPackageProcessor::processPackage::doVaccumRows" << endl;
    logging::Message::Args args;
    logging::Message message(2);
    args.add("Update Failed: ");
    args.add("Unknown error caught when communicating with ExeMgr");
    message.format(args);
    result.result = VACCUM_ERROR;
    result.message = message;
    qb = 0;
    bs.restart();
    bs << qb;
    fExeMgr->write(bs);
    return rowsProcessed;
  }

  return rowsProcessed;
}

bool VaccumPartitionPackageProcessor::processMetaRG(messageqcpp::ByteStream& bsRowGroup, DMLResult& result,
                                                    const uint64_t uniqueId,
                                                    dmlpackage::VaccumPartitionDMLPackage& package,
                                                    std::map<unsigned, bool>& pmState, uint32_t dbroot)
{
  bool err = false;
  const uint32_t pmNum = (*fDbRootPMMap)[dbroot];

  messageqcpp::ByteStream bsSend;
  bsSend << (uint8_t)WriteEngine::ServerMessages::WE_SVR_VACCUM_PARTITION;
  bsSend << uniqueId;
  bsSend << pmNum;
  bsSend << (uint32_t)package.get_TxnID();

  execplan::CalpontSystemCatalog::TableName tableName(package.get_SchemaName(), package.get_TableName());
  vector<bool> bitmap = mockutils::getPartitionDeletedBitmap(package.getPartition(), tableName);
  bsSend << (uint32_t)bitmap.size();
  for (const bool flag : bitmap)
  {
    bsSend << (uint8_t)flag;
  }

  bsSend += bsRowGroup;

  boost::shared_ptr<messageqcpp::ByteStream> bsRecv;
  bsRecv.reset(new messageqcpp::ByteStream());
  string errorMsg;

  uint32_t msgRecv = 0;
  package.write(bsSend);
  fWEClient->write_to_all(bsSend);

  while (1)
  {
    if (msgRecv == fWEClient->getPmCount())
      break;

    fWEClient->read(uniqueId, bsRecv);

    if (bsRecv->empty())
    {
      err = true;
      break;
    }

    messageqcpp::ByteStream::byte tmp8;
    *bsRecv >> tmp8;
    if (tmp8 > 0)
    {
      *bsRecv >> errorMsg;
      err = true;
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ");
      args.add(errorMsg);
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
      break;
    }

    msgRecv++;
  }

  return err;
}

bool VaccumPartitionPackageProcessor::processRG(messageqcpp::ByteStream& bsRowGroup, DMLResult& result,
                                                const uint64_t uniqueId,
                                                dmlpackage::VaccumPartitionDMLPackage& cpackage,
                                                std::map<unsigned, bool>& pmState, uint32_t dbroot)
{
  bool err = false;
  const uint32_t pmNum = (*fDbRootPMMap)[dbroot];

  messageqcpp::ByteStream bsSend;
  bsSend << (uint8_t)WriteEngine::ServerMessages::WE_SVR_VACCUM_PARTITION;
  bsSend << uniqueId;
  bsSend << pmNum;
  bsSend << (uint32_t)cpackage.get_TxnID();
  bsSend += bsRowGroup;

  boost::shared_ptr<messageqcpp::ByteStream> bsRecv;
  bsRecv.reset(new messageqcpp::ByteStream());
  string errorMsg;

  if (pmState[pmNum])
  {
    try
    {
      fWEClient->write(bsSend, (uint32_t)pmNum);
      pmState[pmNum] = false;
    }
    catch (runtime_error& ex)
    {
      err = true;
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ");
      args.add(ex.what());
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
    }
    catch (...)
    {
      err = true;
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ");
      args.add("Unknown error caught when communicating with WES");
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
    }
  }
  else
  {
    while (1)
    {
      bsRecv.reset(new messageqcpp::ByteStream());

      try
      {
        fWEClient->read(uniqueId, bsRecv);
        if (bsRecv->empty())
        {
          err = true;
          errorMsg = "Lost connection to Write Engine Server while updating";
          throw std::runtime_error(errorMsg);
        }

        messageqcpp::ByteStream::byte tmp8;
        *bsRecv >> tmp8;
        *bsRecv >> errorMsg;

        if (tmp8 == IDBRANGE_WARNING)
        {
          result.result = IDBRANGE_WARNING;
          logging::Message::Args args;
          logging::Message message(2);
          args.add(errorMsg);
          message.format(args);
          result.message = message;
        }
        else if (tmp8 > 0)
        {
          result.stats.fErrorNo = tmp8;
          err = (tmp8 != 0);
        }

        if (err)
          throw std::runtime_error(errorMsg);

        uint32_t tmp32;
        *bsRecv >> tmp32;
        pmState[tmp32] = true;

        uint64_t blocksChanged = 0;
        *bsRecv >> blocksChanged;
        result.stats.fBlocksChanged += blocksChanged;

        if (tmp32 == (uint32_t)pmNum)
        {
          fWEClient->write(bsSend, (uint32_t)pmNum);
          pmState[pmNum] = false;
          break;
        }
      }
      catch (runtime_error& ex)
      {
        err = true;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add(ex.what());
        message.format(args);
        result.result = VACCUM_ERROR;
        result.message = message;
        break;
      }
      catch (...)
      {
        err = true;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add("Unknown error caught when communicating with WES");
        message.format(args);
        result.result = VACCUM_ERROR;
        result.message = message;
        break;
      }
    }
  }

  return err;
}

bool VaccumPartitionPackageProcessor::receiveAll(DMLResult& result, const uint64_t uniqueId,
                                                 std::vector<int>& pmIds, std::map<unsigned, bool>& pmState,
                                                 const uint32_t tableOid)
{
  bool err = false;

  // check how many message we need to receive
  uint32_t msgNotRecv = 0;
  for (const int& pmId : pmIds)
  {
    if (!pmState[pmId])
      msgNotRecv++;
  }

  boost::shared_ptr<messageqcpp::ByteStream> bsRecv;
  string errMsg;

  if (msgNotRecv > 0)
  {
    logging::LoggingID logid(DMLLoggingId, fSessionID, fSessionID);

    if (msgNotRecv > fWEClient->getPmCount())
    {
      logging::Message::Args args1;
      logging::Message msg(1);
      args1.add("Update outstanding messages exceed PM count , need to receive messages:PMcount = ");
      ostringstream oss;
      oss << msgNotRecv << ":" << fWEClient->getPmCount();
      args1.add(oss.str());
      msg.format(args1);
      logging::Logger logger(logid.fSubsysID);
      logger.logMessage(logging::LOG_TYPE_ERROR, msg, logid);
      err = true;
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Update Failed: ");
      args.add("One of WriteEngineServer went away.");
      message.format(args);
      result.result = VACCUM_ERROR;
      result.message = message;
      return err;
    }

    bsRecv.reset(new messageqcpp::ByteStream());

    uint32_t msgRecv = 0;
    while (1)
    {
      if (msgRecv == msgNotRecv)
        break;

      bsRecv.reset(new messageqcpp::ByteStream());

      try
      {
        fWEClient->read(uniqueId, bsRecv);

        if (bsRecv->empty())
        {
          err = true;
          errMsg = "Lost connection to Write Engine Server while updating";
          throw std::runtime_error(errMsg);
        }

        messageqcpp::ByteStream::byte tmp8;
        *bsRecv >> tmp8;
        *bsRecv >> errMsg;

        if (tmp8 == IDBRANGE_WARNING)
        {
          result.result = IDBRANGE_WARNING;
          logging::Message::Args args;
          logging::Message message(2);
          args.add(errMsg);
          message.format(args);
          result.message = message;
        }
        else
        {
          result.stats.fErrorNo = tmp8;
          err = (tmp8 != 0);
        }

        if (err)
        {
          throw std::runtime_error(errMsg);
        }
        messageqcpp::ByteStream::quadbyte tmp32;

        uint64_t blocksChanged = 0;
        *bsRecv >> blocksChanged;

        *bsRecv >> tmp32;
        pmState[tmp32] = true;

        msgRecv++;
        result.stats.fBlocksChanged += blocksChanged;
      }
      catch (runtime_error& ex)
      {
        err = true;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add(ex.what());
        message.format(args);
        result.result = VACCUM_ERROR;
        result.message = message;
        break;
      }
      catch (...)
      {
        err = true;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add("Unknown error caught when communicating with WES");
        message.format(args);
        result.result = VACCUM_ERROR;
        result.message = message;
        break;
      }
    }
  }

  return err;
}

}  // namespace dmlpackageprocessor
