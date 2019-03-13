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
 *   $Id: deletepackageprocessor.cpp 9673 2013-07-09 15:59:49Z chao $
 *
 *
 ***********************************************************************/

#include <iostream>
#include <boost/scoped_ptr.hpp>
using namespace std;
#include "deletepackageprocessor.h"
#include "writeengine.h"
#include "joblistfactory.h"
#include "messagelog.h"
#include "dataconvert.h"
#include "simplecolumn.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "stopwatch.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "rowgroup.h"
#include "bytestream.h"
#include "columnresult.h"
#include "we_messages.h"
#include "oamcache.h"
#include "tablelockdata.h"
#include "bytestream.h"

using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace dataconvert;
using namespace joblist;
using namespace BRM;
using namespace rowgroup;
using namespace messageqcpp;
using namespace oam;
namespace dmlpackageprocessor
{
DMLPackageProcessor::DMLResult
DeletePackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
{
    SUMMARY_INFO("DeletePackageProcessor::processPackage");

    DMLResult result;
    result.result = NO_ERROR;
    BRM::TxnID txnid;
    // set-up the transaction
    txnid.id  = cpackage.get_TxnID();
    txnid.valid = true;
    fSessionID = cpackage.get_SessionID();
    //StopWatch timer;
    VERBOSE_INFO("DeletePackageProcessor is processing CalpontDMLPackage ...");
    TablelockData* tablelockData = TablelockData::makeTablelockData(fSessionID);
    uint64_t uniqueId = 0;

    //Bug 5070. Added exception handling
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
        result.result = DELETE_ERROR;
        result.message = message;
        fSessionManager.rolledback(txnid);
        return result;
    }
    catch ( ... )
    {
        logging::Message::Args args;
        logging::Message message(9);
        args.add("Unknown error occured while getting unique number.");
        message.format(args);
        result.result = DELETE_ERROR;
        result.message = message;
        fSessionManager.rolledback(txnid);
        return result;
    }

    uint64_t tableLockId = 0;
    // get the table object from the package
    DMLTable* tablePtr =  cpackage.get_Table();
    std::string schemaName = tablePtr->get_SchemaName();
    std::string tableName = tablePtr->get_TableName();
    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
    CalpontSystemCatalog::TableName aTableName;
    aTableName.table = tableName;
    aTableName.schema = schemaName;
    fWEClient->addQueue(uniqueId);
    execplan::CalpontSystemCatalog::ROPair roPair;

    try
    {
        string stmt = cpackage.get_SQLStatement() + "|" + schemaName + "|";
        SQLLogger sqlLogger(stmt, DMLLoggingId, fSessionID, txnid.id);

        if ( 0 != tablePtr )
        {

            roPair = csc->tableRID(aTableName);

            tableLockId = tablelockData->getTablelockId(roPair.objnum); //check whether this table is locked already for this session

            if (tableLockId == 0)
            {
                //cout << "tablelock is not found in cache " << endl;
                uint32_t  processID = ::getpid();
                int32_t   txnId = txnid.id;
                std::string  processName("DMLProc");
                int32_t sessionId = fSessionID;
                int i = 0;
                OamCache* oamcache = OamCache::makeOamCache();
                std::vector<int> pmList = oamcache->getModuleIds();
                std::vector<uint32_t> pms;

                for (unsigned i = 0; i < pmList.size(); i++)
                {
                    pms.push_back((uint32_t)pmList[i]);
                }

                try
                {
                    tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
                }
                catch (std::exception&)
                {
                    throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
                }

                if ( tableLockId  == 0 )
                {
                    int waitPeriod = 10;
                    int sleepTime = 100; // sleep 100 milliseconds between checks
                    int numTries = 30;  // try 30 times (3 seconds)
                    waitPeriod = Config::getWaitPeriod();
                    numTries = 	waitPeriod * 10;
                    struct timespec rm_ts;

                    rm_ts.tv_sec = sleepTime / 1000;
                    rm_ts.tv_nsec = sleepTime % 1000 * 1000000;

                    for (; i < numTries; i++)
                    {
#ifdef _MSC_VER
                        Sleep(rm_ts.tv_sec * 1000);
#else
                        struct timespec abs_ts;

                        do
                        {
                            abs_ts.tv_sec = rm_ts.tv_sec;
                            abs_ts.tv_nsec = rm_ts.tv_nsec;
                        }
                        while (nanosleep(&abs_ts, &rm_ts) < 0);

#endif

                        try
                        {
                            processID = ::getpid();
                            txnId = txnid.id;
                            sessionId = fSessionID;
                            processName = "DMLProc";
                            tableLockId = fDbrm->getTableLock(pms, roPair.objnum, &processName, &processID, &sessionId, &txnId, BRM::LOADING );
                        }
                        catch (std::exception&)
                        {
                            throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
                        }

                        if (tableLockId > 0)
                            break;
                    }

                    if (i >= numTries) //error out
                    {
                        result.result = DELETE_ERROR;
                        logging::Message::Args args;
                        string strOp("delete");
                        args.add(strOp);
                        args.add(processName);
                        args.add((uint64_t)processID);
                        args.add(sessionId);
                        throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED, args));
                    }
                }
            }

            //cout << " tablelock is obtained with id " << tableLockId << endl;
            tablelockData->setTablelock(roPair.objnum, tableLockId);
            //@Bug 4491 start AI sequence for autoincrement column
            const CalpontSystemCatalog::RIDList ridList = csc->columnRIDs(aTableName);
            CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
            CalpontSystemCatalog::ColType colType;

            while (rid_iterator != ridList.end())
            {
                // If user hit ctrl+c in the mysql console, fRollbackPending will be true.
                if (fRollbackPending)
                {
                    result.result = JOB_CANCELED;
                    break;
                }

                CalpontSystemCatalog::ROPair roPair = *rid_iterator;
                colType = csc->colType(roPair.objnum);

                if (colType.autoincrement)
                {
                    try
                    {
                        uint64_t nextVal = csc->nextAutoIncrValue(aTableName);
                        fDbrm->startAISequence(roPair.objnum, nextVal, colType.colWidth, colType.colDataType);
                        break; //Only one autoincrement column per table
                    }
                    catch (std::exception& ex)
                    {
                        result.result = DELETE_ERROR;
                        throw std::runtime_error(ex.what());
                    }
                }

                ++rid_iterator;
            }

            uint64_t  rowsProcessed = 0;

            rowsProcessed = fixUpRows(cpackage, result, uniqueId, roPair.objnum);

            //@Bug 4994 Cancelled job is not error
            if (result.result == JOB_CANCELED)
                throw std::runtime_error("Query execution was interrupted");

            if ((result.result != 0) && (result.result != DMLPackageProcessor::IDBRANGE_WARNING))
                throw std::runtime_error(result.message.msg());

            result.rowCount = rowsProcessed;

            // Log the DML statement.
            logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement(), cpackage.get_SchemaName());
        }
    }
    catch (exception& ex)
    {
        cerr << "DeletePackageProcessor::processPackage: " << ex.what() << endl;

        //@Bug 4994 Cancelled job is not error
        if (result.result == 0)
        {
            result.result = DELETE_ERROR;
        }

        result.message = Message(ex.what());
    }
    catch (...)
    {
        cerr << "DeletePackageProcessor::processPackage: caught unknown exception!" << endl;
        logging::Message::Args args;
        logging::Message message(7);
        args.add( "Delete Failed: ");
        args.add( "encountered unknown exception" );
        args.add(result.message.msg());
        args.add("");
        message.format( args );

        result.result = DELETE_ERROR;
        result.message = message;
    }

    //timer.finish();
    //@Bug 1886,2870 Flush VM cache only once per statement.
    std::map<uint32_t, uint32_t> oids;
    int rc = 0;

    if (result.result == NO_ERROR)
    {
        rc = flushDataFiles( result.result, oids, uniqueId, txnid, roPair.objnum);

        if (rc != NO_ERROR)
        {
            cerr << "UpdatePackageProcessor::processPackage: write data to disk failed" << endl;

            if (!fRollbackPending)
            {
                logging::Message::Args args;
                logging::Message message(7);
                args.add("Delete Failed: ");
                args.add("error when writing data to disk");
                args.add("");
                args.add("");
                message.format(args);

                result.result = UPDATE_ERROR;
                result.message = message;
            }

            result.rowCount = 0;
            rc = endTransaction(uniqueId, txnid, false);

            if ( (rc != NO_ERROR) && (!fRollbackPending))
            {
                logging::Message::Args args;
                logging::Message message(7);
                args.add("Delete Failed: ");
                args.add("error when cleaning up data files");
                args.add("");
                args.add("");
                message.format(args);

                result.result = UPDATE_ERROR;
                result.message = message;
                result.rowCount = 0;

            }
        }
        else
        {
            if (fRollbackPending)
                rc = endTransaction(uniqueId, txnid, false);
            else
                rc = endTransaction(uniqueId, txnid, true);
        }
    }
    else
    {
        rc = flushDataFiles( result.result, oids, uniqueId, txnid, roPair.objnum);
        result.rowCount = 0;
        rc = endTransaction(uniqueId, txnid, false);
    }


    if (fRollbackPending)
    {
        result.result = JOB_CANCELED;
        logging::Message::Args args1;
        args1.add("Query execution was interrupted");
        result.message.format(args1);
    }

    fWEClient->removeQueue(uniqueId);

    VERBOSE_INFO("Finished Processing Delete DML Package");
    return result;
}

uint64_t DeletePackageProcessor::fixUpRows (dmlpackage::CalpontDMLPackage& cpackage, DMLResult& result, const uint64_t uniqueId, const uint32_t tableOid)
{
    ByteStream msg, msgBk, emsgBs;
    rowgroup::RGData rgData;
    ByteStream::quadbyte qb = 4;
    msg << qb;
    boost::scoped_ptr<rowgroup::RowGroup> rowGroup;
    uint64_t  rowsProcessed = 0;
    uint32_t dbroot = 1;
    bool metaData = false;
    oam::OamCache* oamCache = oam::OamCache::makeOamCache();
    std::vector<int> fPMs = oamCache->getModuleIds();
    std::map<unsigned, bool> pmStateDel;
    string emsg;
    string emsgStr;
    bool err = false;

    //boost::scoped_ptr<messageqcpp::MessageQueueClient> fExeMgr;
    //fExeMgr.reset( new messageqcpp::MessageQueueClient("ExeMgr1"));
    try
    {
        for (unsigned i = 0; i < fPMs.size(); i++)
        {
            pmStateDel[fPMs[i]] = true;
        }

        fExeMgr->write(msg);
        fExeMgr->write(*(cpackage.get_ExecutionPlan()));
        //cout << "sending to ExeMgr plan with length " << (cpackage.get_ExecutionPlan())->length() << endl;
        msg.restart();
        emsgBs.restart();
        msg = fExeMgr->read(); //error handling

        if (msg.length() == 4)
        {
            msg >> qb;

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
            args.add("Delete Failed: Error from ExeMgr");
            args.add((int)qb);
            message.format(args);
            result.result = DELETE_ERROR;
            result.message = message;
            return 0;
        }

        emsgBs = fExeMgr->read();

        if (emsgBs.length() == 0)
        {
            logging::Message::Args args;
            logging::Message message(2);
            args.add("Delete Failed: ");
            args.add("Lost connection to ExeMgr");
            message.format(args);
            result.result = DELETE_ERROR;
            result.message = message;
            return 0;
        }

        emsgBs >> emsgStr;

        while (true)
        {
            if (fRollbackPending)
            {
                result.result = JOB_CANCELED;
                err = true;
                break;
            }

            msg.restart();
            msgBk.restart();
            msg = fExeMgr->read();
            msgBk = msg;

            if ( msg.length() == 0 )
            {
                logging::Message::Args args;
                logging::Message message(2);
                args.add("Delete Failed: ");
                args.add("Lost connection to ExeMgr");
                message.format(args);
                result.result = DELETE_ERROR;
                result.message = message;
                //return rowsProcessed;
                break;
            }
            else
            {
                if (rowGroup.get() == NULL)
                {
                    //This is meta data, need to send to all PMs.
                    metaData = true;
                    //cout << "sending meta data" << endl;
                    err = processRowgroup(msgBk, result, uniqueId, cpackage, pmStateDel, metaData, dbroot);
                    rowGroup.reset(new rowgroup::RowGroup());
                    rowGroup->deserialize(msg);
                    qb = 100;
                    msg.restart();
                    msg << qb;
                    //cout << "Projecting rows" << endl;
                    fExeMgr->write(msg);
                    metaData = false;
                    continue;
                }

                // XXXST: take out the 'true' when all jobsteps have been made st-compatible
                rgData.deserialize(msg, true);
                rowGroup->setData(&rgData);
                //rowGroup->setData(const_cast<uint8_t*>(msg.buf()));
                err = (rowGroup->getStatus() != 0);

                if (err)
                {
                    //msgBk.advance(rowGroup->getDataSize());
                    string errorMsg;
                    msg >> errorMsg;
                    logging::Message::Args args;
                    logging::Message message(2);
                    args.add("Delete Failed: ");
                    args.add(errorMsg);
                    message.format(args);
                    result.result = DELETE_ERROR;
                    result.message = message;
                    DMLResult tmpResult;
                    receiveAll( tmpResult, uniqueId, fPMs, pmStateDel, tableOid);
                    //@Bug 4358 get rid of broken pipe error.
                    //msg.restart();
                    //msg << qb;
                    //fExeMgr->write(msg);
                    //return rowsProcessed;
                    break;
                }

                if (rowGroup->getRGData() == NULL)
                {
                    msg.restart();
                }

                if (rowGroup->getRowCount() == 0)  //done fetching
                {
                    err = receiveAll( result, uniqueId, fPMs, pmStateDel, tableOid);
                    //return rowsProcessed;
                    break;
                }

                if (rowGroup->getBaseRid() == (uint64_t) (-1))
                {
                    continue;  // @bug4247, not valid row ids, may from small side outer
                }

                dbroot = rowGroup->getDBRoot();
                err = processRowgroup(msgBk, result, uniqueId, cpackage, pmStateDel, metaData, dbroot);

                if (err)
                {
                    DMLResult tmpResult;
                    receiveAll( tmpResult, uniqueId, fPMs, pmStateDel, tableOid);
                    //@Bug 4358 get rid of broken pipe error.
                    //msg.restart();
                    //msg << qb;
                    //fExeMgr->write(msg);
                    //return rowsProcessed;
                    break;
                }

                rowsProcessed += rowGroup->getRowCount();
            }
        }

        if (fRollbackPending)
        {
            err = true;
            // Response to user
            cerr << "DeletePackageProcessor::processPackage::fixupRows Rollback Pending" << endl;

            //@Bug 4994 Cancelled job is not error
            result.result = JOB_CANCELED;
            // Log
            LoggingID logid( DMLLoggingId, fSessionID, cpackage.get_TxnID());
            logging::Message::Args args1;
            logging::Message msg1(1);
            args1.add("SQL statement canceled by user");
            msg1.format( args1 );
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(LOG_TYPE_DEBUG, msg1, logid);

            // Clean out the pipe;
            DMLResult tmpResult;
            receiveAll( tmpResult, uniqueId, fPMs, pmStateDel, tableOid);
        }

        if (!err)
        {
            // get stats from ExeMgr
            qb = 3;
            msg.restart();
            msg << qb;
            fExeMgr->write(msg);
            msg = fExeMgr->read();
            msg >> result.queryStats;
            msg >> result.extendedStats;
            msg >> result.miniStats;
            result.stats.unserialize(msg);
        }

        //@Bug 4358 get rid of broken pipe error by sending a dummy bs.
        if (err)
        {
            msg.restart();
            msg << qb;
            fExeMgr->write(msg);
        }

        return rowsProcessed;
    }
    catch (runtime_error& ex)
    {
        cerr << "DeletePackageProcessor::processPackage::fixupRows" << ex.what() << endl;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Delete Failed: ");
        args.add(ex.what());
        message.format(args);
        result.result = DELETE_ERROR;
        result.message = message;
        qb = 0;
        msg.restart();
        msg << qb;
        fExeMgr->write(msg);
        return rowsProcessed;
    }
    catch ( ... )
    {
        cerr << "DeletePackageProcessor::processPackage::fixupRows" << endl;
        logging::Message::Args args;
        logging::Message message(2);
        args.add("Update Failed: ");
        args.add("Unknown error caught when communicating with ExeMgr");
        message.format(args);
        result.result = DELETE_ERROR;
        result.message = message;
        qb = 0;
        msg.restart();
        msg << qb;
        fExeMgr->write(msg);
        return rowsProcessed;
    }

    return rowsProcessed;
}

bool DeletePackageProcessor::processRowgroup(ByteStream& aRowGroup, DMLResult& result, const uint64_t uniqueId,
        dmlpackage::CalpontDMLPackage& cpackage, std::map<unsigned, bool>& pmStateDel, bool isMeta, uint32_t dbroot)
{
    bool rc = false;
    //cout << "Get dbroot " << dbroot << endl;
    int pmNum = (*fDbRootPMMap)[dbroot];
    DMLTable* tablePtr =  cpackage.get_Table();
    ByteStream bytestream;
    bytestream << (ByteStream::byte)WE_SVR_DELETE;
    bytestream << uniqueId;
    bytestream << (ByteStream::quadbyte) pmNum;
    bytestream << uint32_t(cpackage.get_SessionID());
    bytestream << (ByteStream::quadbyte) cpackage.get_TxnID();
    bytestream << tablePtr->get_SchemaName();
    bytestream << tablePtr->get_TableName();
    bytestream += aRowGroup;
    //cout << "sending rows to pm " << pmNum << " with msg length " << bytestream.length() << endl;
    uint32_t msgRecived = 0;
    boost::shared_ptr<messageqcpp::ByteStream> bsIn;
    bsIn.reset(new ByteStream());
    ByteStream::byte tmp8;
    string errorMsg;
    ByteStream::quadbyte tmp32;
    uint64_t blocksChanged = 0;

    if (isMeta) //send to all PMs
    {
        fWEClient->write_to_all(bytestream);

        while (1)
        {
            if (msgRecived == fWEClient->getPmCount())
                break;

            fWEClient->read(uniqueId, bsIn);

            if ( bsIn->length() == 0 ) //read error
            {
                rc = true;
                break;
            }
            else
            {
                *bsIn >> tmp8;
                rc = (tmp8 != 0);

                if (rc != 0)
                {
                    *bsIn >> errorMsg;
                    break;
                }
                else
                    msgRecived++;
            }
        }

        return rc;
    }

    if (pmStateDel[pmNum])
    {
        try
        {
            fWEClient->write(bytestream, (uint32_t)pmNum);
            //cout << "sent tp pm " << pmNum<<endl;
            pmStateDel[pmNum] = false;
        }
        catch (runtime_error& ex) //write error
        {
            rc = true;
            logging::Message::Args args;
            logging::Message message(2);
            args.add("Delete Failed: ");
            args.add(ex.what());
            message.format(args);
            result.result = DELETE_ERROR;
            result.message = message;
        }
        catch (...)
        {
            rc = true;
            logging::Message::Args args;
            logging::Message message(2);
            args.add("Delete Failed: ");
            args.add("Unknown error caught when communicating with WES");
            message.format(args);
            result.result = DELETE_ERROR;
            result.message = message;
        }
    }
    else
    {
        while (1)
        {
            bsIn.reset(new ByteStream());

            try
            {
                fWEClient->read(uniqueId, bsIn);

                if ( bsIn->length() == 0 ) //read error
                {
                    rc = true;
                    errorMsg = "Lost connection to Write Engine Server while deleting";
                    throw std::runtime_error(errorMsg);
                }
                else
                {
                    *bsIn >> tmp8;
                    rc = (tmp8 != 0);
                    *bsIn >> errorMsg;
                    *bsIn >> tmp32;
                    *bsIn >> blocksChanged;
                    result.stats.fBlocksChanged += blocksChanged;
                    result.stats.fErrorNo = tmp8;

                    //cout << "received from pm " << (uint32_t)tmp32 << " and rc = " << rc << endl;
                    pmStateDel[tmp32] = true;

                    if (rc != 0)
                    {
                        throw std::runtime_error(errorMsg);
                    }

                    if ( tmp32 == (uint32_t)pmNum )
                    {
                        fWEClient->write(bytestream, (uint32_t)pmNum);
                        pmStateDel[pmNum] = false;
                        break;
                    }
                }
            }
            catch (runtime_error& ex) //write error
            {
                rc = true;
                logging::Message::Args args;
                logging::Message message(2);
                args.add("Delete Failed: ");
                args.add(ex.what());
                message.format(args);
                result.result = DELETE_ERROR;
                result.message = message;
                break;
            }
            catch (...)
            {
                rc = true;
                logging::Message::Args args;
                logging::Message message(2);
                args.add("Delete Failed: ");
                args.add("Unknown error caught when communicating with WES");
                message.format(args);
                result.result = DELETE_ERROR;
                result.message = message;
                break;
            }
        }
    }

    return rc;
}

bool DeletePackageProcessor::receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs,
                                        std::map<unsigned, bool>& pmStateDel, const uint32_t tableOid)
{
    //check how many message we need to receive
    uint32_t messagesNotReceived = 0;
    bool err = false;

    for (unsigned i = 0; i < fPMs.size(); i++)
    {
        if (!pmStateDel[fPMs[i]])
            messagesNotReceived++;
    }

    boost::shared_ptr<messageqcpp::ByteStream> bsIn;
    uint32_t msgReceived = 0;
    ByteStream::byte tmp8;
    string errorMsg;

    if (messagesNotReceived > 0)
    {
        LoggingID logid( DMLLoggingId, fSessionID, fSessionID);

        if ( messagesNotReceived > fWEClient->getPmCount())
        {
            logging::Message::Args args1;
            logging::Message msg(1);
            args1.add("Delete outstanding messages exceed PM count , need to receive messages:PMcount = ");
            ostringstream oss;
            oss << messagesNotReceived << ":" << fPMCount;
            args1.add(oss.str());
            msg.format( args1 );
            logging::Logger logger(logid.fSubsysID);
            logger.logMessage(LOG_TYPE_ERROR, msg, logid);
            err = true;
            logging::Message::Args args;
            logging::Message message(2);
            args.add("Update Failed: ");
            args.add("One of WriteEngineServer went away.");
            message.format(args);
            result.result = DELETE_ERROR;
            result.message = message;
            return err;
        }

        bsIn.reset(new ByteStream());
        ByteStream::quadbyte tmp32;
        uint64_t blocksChanged = 0;

        while (1)
        {
            if (msgReceived == messagesNotReceived)
                break;

            bsIn.reset(new ByteStream());

            try
            {
                fWEClient->read(uniqueId, bsIn);

                if ( bsIn->length() == 0 ) //read error
                {
                    err = true;
                    errorMsg = "Lost connection to Write Engine Server while deleting";
                    throw std::runtime_error(errorMsg);
                }
                else
                {
                    *bsIn >> tmp8;
                    err = (tmp8 != 0);
                    *bsIn >> errorMsg;
                    *bsIn >> tmp32;
                    *bsIn >> blocksChanged;
                    //cout << "Received response from pm " << tmp32 << endl;
                    pmStateDel[tmp32] = true;

                    if (err)
                    {
                        throw std::runtime_error(errorMsg);
                    }

                    msgReceived++;
                    result.stats.fBlocksChanged += blocksChanged;
                    result.stats.fErrorNo = tmp8;
                }
            }
            catch (runtime_error& ex) //write error
            {
                err = true;
                logging::Message::Args args;
                logging::Message message(2);
                args.add("Delete Failed: ");
                args.add(ex.what());
                message.format(args);
                result.result = DELETE_ERROR;
                result.message = message;
                break;
            }
            catch (...)
            {
                err = true;
                logging::Message::Args args;
                logging::Message message(2);
                args.add("Delete Failed: ");
                args.add("Unknown error caught when communicating with WES");
                message.format(args);
                result.result = DELETE_ERROR;
                result.message = message;
                break;
            }
        }
    }

    return err;
}
} // namespace dmlpackageprocessor
