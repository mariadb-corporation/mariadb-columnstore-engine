/* Copyright (C) 2014 InfiniDB, Inc.

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

/*******************************************************************************
* $Id: we_ddlcommandproc.h 3043 2011-08-29 22:03:03Z chao $
*
*******************************************************************************/
#ifndef WE_DMLCOMMANDPROC_H__
#define WE_DMLCOMMANDPROC_H__

#include <unistd.h>
#include <boost/scoped_ptr.hpp>
#include "bytestream.h"
#include "we_messages.h"
#include "dbrm.h"
#include "we_message_handlers.h"
#include "calpontdmlpackage.h"
#include "updatedmlpackage.h"
#include "calpontsystemcatalog.h"
#include "insertdmlpackage.h"
#include "liboamcpp.h"
#include "dataconvert.h"
#include "writeengine.h"
#include "we_convertor.h"
#include "we_dbrootextenttracker.h"
#include "we_rbmetawriter.h"
#include "rowgroup.h"
#include "we_log.h"

#if defined(_MSC_VER) && defined(xxxDDLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

class WE_DMLCommandProc
{
public:
    typedef std::vector<std::string> ColValues;

    EXPORT WE_DMLCommandProc();
    EXPORT WE_DMLCommandProc(const WE_DMLCommandProc& rhs);
    EXPORT ~WE_DMLCommandProc();
    inline void isFirstBatchPm (bool firstBatch)
    {
        fIsFirstBatchPm = firstBatch;
    }

    inline bool isFirstBatchPm ()
    {
        return fIsFirstBatchPm;
    }

    //Convert rid from logical block rid to file relative rid
    inline void convertToRelativeRid (uint64_t& rid, const uint8_t extentNum, const uint16_t blockNum)
    {
        rid = rid + extentNum * extentRows + blockNum * 8192;
    }

    EXPORT uint8_t processSingleInsert(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t commitVersion(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t rollbackBlocks(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t rollbackVersion(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processBatchInsert(messageqcpp::ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId);
    EXPORT uint8_t processBatchInsertBinary(messageqcpp::ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId);
    EXPORT uint8_t commitBatchAutoOn(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t commitBatchAutoOff(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t rollbackBatchAutoOn(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t rollbackBatchAutoOff(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processBatchInsertHwm(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processUpdate(messageqcpp::ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId, uint64_t& blocksChanged);
    EXPORT uint8_t processUpdate1(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processFlushFiles(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processDelete(messageqcpp::ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId, uint64_t& blocksChanged);
    EXPORT uint8_t processRemoveMeta(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processBulkRollback(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t processBulkRollbackCleanup(messageqcpp::ByteStream& bs, std::string& err);
    EXPORT uint8_t updateSyscolumnNextval(ByteStream& bs, std::string& err);
    EXPORT uint8_t processPurgeFDCache(ByteStream& bs, std::string& err);
    EXPORT uint8_t processEndTransaction(ByteStream& bs, std::string& err);
    EXPORT uint8_t processFixRows(ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId);
    EXPORT uint8_t getWrittenLbids(messageqcpp::ByteStream& bs, std::string& err, ByteStream::quadbyte& PMId);
    int validateColumnHWMs(
        execplan::CalpontSystemCatalog::RIDList& ridList,
        boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr,
        const std::vector<DBRootExtentInfo>& segFileInfo,
        const char* stage );
private:
    WriteEngineWrapper fWEWrapper;
    boost::scoped_ptr<RBMetaWriter> fRBMetaWriter;
    std::vector<boost::shared_ptr<DBRootExtentTracker> >   dbRootExtTrackerVec;
    inline bool isDictCol ( execplan::CalpontSystemCatalog::ColType colType )
    {
        if (((colType.colDataType == execplan::CalpontSystemCatalog::CHAR) && (colType.colWidth > 8))
                || ((colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR) && (colType.colWidth > 7))
                || ((colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL) && (colType.precision > 18))
                || ((colType.colDataType == execplan::CalpontSystemCatalog::UDECIMAL) && (colType.precision > 18))
                || (colType.colDataType == execplan::CalpontSystemCatalog::VARBINARY)
                || (colType.colDataType == execplan::CalpontSystemCatalog::BLOB)
                || (colType.colDataType == execplan::CalpontSystemCatalog::TEXT))
        {
            return true;
        }
        else
            return false;
    }
    uint8_t processBatchInsertHwmFlushChunks(uint32_t tableOID, int txnID,
            const std::vector<BRM::FileInfo>& files,
            const std::vector<BRM::OID_t>& oidsToFlush,
            std::string& err);

    bool fIsFirstBatchPm;
    std::map<uint32_t, rowgroup::RowGroup*> rowGroups;
    std::map<uint32_t, dmlpackage::UpdateDMLPackage> cpackages;
    BRM::DBRM fDbrm;
    unsigned  extentsPerSegmentFile, extentRows, filesPerColumnPartition, dbrootCnt;
    Log fLog;
};

}
#undef EXPORT
#endif
