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

// $Id: iomanager.cpp 2147 2013-08-14 20:44:44Z bwilkinson $
//
// C++ Implementation: iomanager
//
// Description:
//
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
//
//
//

#include "config.h"

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#ifdef __linux__
#include <sys/mount.h>
#include <linux/fs.h>
#endif
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif
#include <stdexcept>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <sstream>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <set>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#ifdef _MSC_VER
typedef int pthread_t;
#else
#include <pthread.h>
#endif
//#define NDEBUG
#include <cassert>

using namespace std;

#include "configcpp.h"
using namespace config;

#include "messageobj.h"
#include "messageids.h"
using namespace logging;

#include "brmtypes.h"

#include "pp_logger.h"

#include "fsutils.h"

#include "rwlock_local.h"

#include "iomanager.h"
#include "liboamcpp.h"

#include "idbcompress.h"
using namespace compress;

#include "IDBDataFile.h"
#include "IDBPolicy.h"
#include "IDBLogger.h"
using namespace idbdatafile;

typedef tr1::unordered_set<BRM::OID_t> USOID;

namespace primitiveprocessor
{
extern Logger* mlp;
extern int directIOFlag;
extern int noVB;
}

#ifndef O_BINARY
#  define O_BINARY 0
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

namespace
{

using namespace dbbc;
using namespace std;

const std::string boldStart = "\033[0;1m";
const std::string boldStop = "\033[0;39m";

const uint32_t MAX_OPEN_FILES = 16384;
const uint32_t DECREASE_OPEN_FILES = 4096;

void timespec_sub(const struct timespec& tv1,
                  const struct timespec& tv2,
                  double& tm)
{
    tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9 * (tv2.tv_nsec - tv1.tv_nsec);
}

struct IOMThreadArg
{
    ioManager* iom;
    int32_t thdId;
};

typedef IOMThreadArg IOMThreadArg_t;

/* structures shared across all iomanagers */
class FdEntry
{
public:
    FdEntry() : oid(0), dbroot(0), partNum(0), segNum(0),
        fp(0), c(0), inUse(0), compType(0)
    {
        cmpMTime = 0;
    }

    FdEntry(const BRM::OID_t o, const uint16_t d, const uint32_t p, const uint16_t s, const int ct, IDBDataFile* f) :
        oid(o), dbroot(d), partNum(p), segNum(s), fp(f), c(0), inUse(0), compType(0)
    {
        cmpMTime = 0;

        if (oid >= 1000)
            compType = ct;
    }

    ~FdEntry()
    {
        delete fp;
        fp = 0;
    }

    BRM::OID_t oid;
    uint16_t dbroot;
    uint32_t partNum;
    uint16_t segNum;
    IDBDataFile* fp;
    uint32_t c;
    int inUse;

    CompChunkPtrList ptrList;

    int compType;
    bool isCompressed() const
    {
        return (oid >= 1000 && compType != 0);
    }
    time_t cmpMTime;
    friend ostream& operator<<(ostream& out, const FdEntry& o)
    {
        out << " o: " << o.oid
            << " f: " << o.fp
            << " d: " << o.dbroot
            << " p: " << o.partNum
            << " s: " << o.segNum
            << " c: " << o.c
            << " t: " << o.compType
            << " m: " << o.cmpMTime;
        return out;
    }
};

struct fdCacheMapLessThan
{
    bool operator()(const FdEntry& lhs, const FdEntry& rhs) const
    {
        if (lhs.oid < rhs.oid)
            return true;

        if (lhs.oid == rhs.oid && lhs.dbroot < rhs.dbroot)
            return true;

        if (lhs.oid == rhs.oid && lhs.dbroot == rhs.dbroot && lhs.partNum < rhs.partNum)
            return true;

        if (lhs.oid == rhs.oid && lhs.dbroot == rhs.dbroot && lhs.partNum == rhs.partNum && lhs.segNum < rhs.segNum)
            return true;

        return false;

    }
};

struct fdMapEqual
{
    bool operator()(const FdEntry& lhs, const FdEntry& rhs) const
    {
        return (lhs.oid == rhs.oid &&
                lhs.dbroot == rhs.dbroot &&
                lhs.partNum == rhs.partNum &&
                lhs.segNum == rhs.segNum);
    }

};

typedef boost::shared_ptr<FdEntry> SPFdEntry_t;
typedef std::map<FdEntry, SPFdEntry_t, fdCacheMapLessThan> FdCacheType_t;

struct FdCountEntry
{
    FdCountEntry() {}
    FdCountEntry(const BRM::OID_t o, const uint16_t d, const uint32_t p, const uint16_t s, const uint32_t c,
                 const FdCacheType_t::iterator it) : oid(o), dbroot(d), partNum(p), segNum(s), cnt(c), fdit(it) {}
    ~FdCountEntry() {}

    BRM::OID_t oid;
    uint16_t dbroot;
    uint32_t partNum;
    uint16_t segNum;
    uint32_t cnt;
    FdCacheType_t::iterator fdit;

    friend ostream& operator<<(ostream& out, const FdCountEntry& o)
    {
        out << " o: " << o.oid
            << " d: " << o.dbroot
            << " p: " << o.partNum
            << " s: " << o.segNum
            << " c: " << o.cnt;

        return out;
    }

}; // FdCountEntry

typedef FdCountEntry FdCountEntry_t;

struct fdCountCompare
{
    bool operator() (const FdCountEntry_t& lhs, const FdCountEntry_t& rhs)
    {
        return lhs.cnt > rhs.cnt;
    }
};

typedef multiset<FdCountEntry_t, fdCountCompare> FdCacheCountType_t;

FdCacheType_t fdcache;
boost::mutex fdMapMutex;
rwlock::RWLock_local localLock;

char* alignTo(const char* in, int av)
{
    ptrdiff_t inx = reinterpret_cast<ptrdiff_t>(in);
    ptrdiff_t avx = static_cast<ptrdiff_t>(av);

    if ((inx % avx) != 0)
    {
        inx &= ~(avx - 1);
        inx += avx;
    }

    char* outx = reinterpret_cast<char*>(inx);
    return outx;
}

void waitForRetry(long count)
{
    usleep(5000 * count);
    return;
}


//Must hold the FD cache lock!
int updateptrs(char* ptr, FdCacheType_t::iterator fdit, const IDBCompressInterface& decompressor)
{
    ssize_t i;
    uint32_t progress;


    // ptr is taken from buffer, already been checked: realbuff.get() == 0
    if (ptr == 0)
        return -1;

    // already checked before: fdit->second->isCompressed()
    if (fdit->second.get() == 0)
        return -2;

    IDBDataFile* fp = fdit->second->fp;

    if (fp == INVALID_HANDLE_VALUE)
    {
        Message::Args args;
        args.add("updateptrs got invalid fp.");
        primitiveprocessor::mlp->logInfoMessage(logging::M0006, args);
        return -3;
    }

    //We need to read one extra block because we need the first ptr in the 3rd block
    // to know if we're done.
    //FIXME: re-work all of this so we don't have to re-read the 3rd block.
    progress = 0;

    while (progress < 4096 * 3)
    {
        i = fp->pread(&ptr[progress], progress, (4096 * 3) - progress);

        if (i <= 0)
            break;

        progress += i;
    }

    if (progress != 4096 * 3)
        return -4;   // let it retry. Not likely, but ...

    fdit->second->cmpMTime = 0;
    time_t mtime = fp->mtime();

    if ( mtime != (time_t) - 1 )
        fdit->second->cmpMTime = mtime;

    int gplRc = 0;
    gplRc = decompressor.getPtrList(&ptr[4096], 4096, fdit->second->ptrList);

    if (gplRc != 0)
        return -5; // go for a retry.

    if (fdit->second->ptrList.size() == 0)
        return -6; // go for a retry.

    uint64_t numHdrs = fdit->second->ptrList[0].first / 4096ULL - 2ULL;

    if (numHdrs > 0)
    {
        boost::scoped_array<char> nextHdrBufsa(new char[numHdrs * 4096 + 4095]);
        char* nextHdrBufPtr = 0;

        nextHdrBufPtr = alignTo(nextHdrBufsa.get(), 4096);

        progress = 0;

        while (progress < numHdrs * 4096)
        {
            i = fp->pread(&nextHdrBufPtr[progress], (4096 * 2) + progress,
                          (numHdrs * 4096) - progress);

            if (i <= 0)
                break;

            progress += i;
        }

        if (progress != numHdrs * 4096)
            return -8;

        CompChunkPtrList nextPtrList;
        gplRc = decompressor.getPtrList(&nextHdrBufPtr[0], numHdrs * 4096, nextPtrList);

        if (gplRc != 0)
            return -7; // go for a retry.

        fdit->second->ptrList.insert(fdit->second->ptrList.end(), nextPtrList.begin(), nextPtrList.end());
    }

    return 0;
}

void* thr_popper(ioManager* arg)
{
    ioManager* iom = arg;
    FileBufferMgr* fbm;
    int totalRqst = 0;
    fileRequest* fr = 0;
    BRM::LBID_t lbid = 0;
    BRM::OID_t oid = 0;
    BRM::VER_t ver = 0;
    BRM::QueryContext qc;
    BRM::VER_t txn = 0;
    int compType = 0;
    int blocksLoaded = 0;
    int blocksRead = 0;
    const unsigned pageSize = 4096;
    fbm = &iom->fileBufferManager();
    char fileName[WriteEngine::FILE_NAME_SIZE];
    const uint64_t fileBlockSize = BLOCK_SIZE;
    bool flg = false;
    bool useCache;
    uint16_t dbroot = 0;
    uint32_t partNum = 0;
    uint16_t segNum = 0;
    uint32_t offset = 0;
    char* fileNamePtr = fileName;
    uint64_t longSeekOffset = 0;
    int err;
    uint32_t dlen = 0, acc, readSize, blocksThisRead, j;
    uint32_t blocksRequested = 0;
    ssize_t i;
    char* alignedbuff = 0;
    boost::scoped_array<char> realbuff;
    pthread_t threadId = 0;
    ostringstream iomLogFileName;
    ofstream lFile;
    struct timespec rqst1;
    struct timespec rqst2;
    struct timespec tm;
    struct timespec tm2;
    double tm3;
    double rqst3;
    bool locked = false;
    SPFdEntry_t fe;
    IDBCompressInterface decompressor;
    vector<CacheInsert_t> cacheInsertOps;
    bool copyLocked = false;

    if (iom->IOTrace())
    {
#ifdef _MSC_VER
        threadId = GetCurrentThreadId();
        iomLogFileName << "C:/Calpont/log/trace/iom." << threadId;
#else
        threadId = pthread_self();
        iomLogFileName << "/var/log/mariadb/columnstore/trace/iom." << threadId;
#endif
        lFile.open(iomLogFileName.str().c_str(), ios_base::app | ios_base::ate);
    }

    FdCacheType_t::iterator fdit;
    IDBDataFile* fp = 0;
    uint32_t maxCompSz = IDBCompressInterface::maxCompressedSize(iom->blocksPerRead * BLOCK_SIZE);
    uint32_t readBufferSz = maxCompSz + pageSize;

    realbuff.reset(new char[readBufferSz]);

    if (realbuff.get() == 0)
    {
        cerr << "thr_popper: Can't allocate space for a whole extent in memory" << endl;
        return 0;
    }

    alignedbuff = alignTo(realbuff.get(), 4096);

    if ((((ptrdiff_t)alignedbuff - (ptrdiff_t)realbuff.get()) >= (ptrdiff_t)pageSize) ||
            (((ptrdiff_t)alignedbuff % pageSize) != 0))
        throw runtime_error("aligned buffer size is not matching the page size.");

    uint8_t* uCmpBuf = 0;
    uCmpBuf = new uint8_t[4 * 1024 * 1024 + 4];

    for ( ; ; )
    {
        if (copyLocked)
        {
            iom->dbrm()->releaseLBIDRange(lbid, blocksRequested);
            copyLocked = false;
        }

        if (locked)
        {
            localLock.read_unlock();
            locked = false;
        }

        fr = iom->getNextRequest();

        localLock.read_lock();
        locked = true;

        if (iom->IOTrace())
            clock_gettime(CLOCK_REALTIME, &rqst1);

        lbid = fr->Lbid();
        qc = fr->Ver();
        txn = fr->Txn();
        flg = fr->Flg();
        compType = fr->CompType();
        useCache = fr->useCache();
        blocksLoaded = 0;
        blocksRead = 0;
        dlen = fr->BlocksRequested();
        blocksRequested = fr->BlocksRequested();
        oid = 0;
        dbroot = 0;
        partNum = 0;
        segNum = 0;
        offset = 0;

        // special case for getBlock.
        iom->dbrm()->lockLBIDRange(lbid, blocksRequested);
        copyLocked = true;

        // special case for getBlock.
        if (blocksRequested == 1)
        {
            BRM::VER_t outVer;
            iom->dbrm()->vssLookup((BRM::LBID_t) lbid, qc, txn, &outVer, &flg);
            ver = outVer;
            fr->versioned(flg);
        }
        else
        {
            fr->versioned(false);
            ver = qc.currentScn;
        }

        err = iom->localLbidLookup(lbid,
                                   ver,
                                   flg,
                                   oid,
                                   dbroot,
                                   partNum,
                                   segNum,
                                   offset);

        if (err == BRM::ERR_SNAPSHOT_TOO_OLD)
        {
            ostringstream errMsg;
            errMsg << "thr_popper: version " << ver << " of LBID " << lbid <<
                   "is too old";
            iom->handleBlockReadError(fr, errMsg.str(), &copyLocked);
            continue;
        }
        else if (err < 0)
        {
            ostringstream errMsg;
            errMsg << "thr_popper: BRM lookup failure; lbid=" <<
                   lbid << "; ver=" << ver << "; flg=" << (flg ? 1 : 0);
            iom->handleBlockReadError( fr, errMsg.str(), &copyLocked, fileRequest::BRM_LOOKUP_ERROR);
            continue;
        }

#ifdef IDB_COMP_POC_DEBUG
        {
            boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);

            if (compType != 0) cout << boldStart;

            cout << "fileRequest: " << *fr << endl;

            if (compType != 0) cout << boldStop;
        }
#endif
        const uint32_t extentSize = iom->getExtentRows();
        FdEntry fdKey(oid, dbroot, partNum, segNum, compType, NULL);
        //cout << "Looking for " << fdKey << endl
        //   << "O: " << oid << " D: " << dbroot << " P: " << partNum << " S: " << segNum << endl;

        fdMapMutex.lock();
        fdit = fdcache.find(fdKey);

        if (fdit == fdcache.end())
        {
            try
            {
                iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
            }
            catch (exception& exc)
            {
                fdMapMutex.unlock();
                Message::Args args;
                args.add(oid);
                args.add(exc.what());
                primitiveprocessor::mlp->logMessage(logging::M0053, args, true);
                ostringstream errMsg;
                errMsg << "thr_popper: Error building filename for OID " <<
                       oid << "; " << exc.what();
                iom->handleBlockReadError( fr, errMsg.str(), &copyLocked );
                continue;
            }

#ifdef IDB_COMP_USE_CMP_SUFFIX

            if (compType != 0)
            {
                char* ptr = strrchr(fileNamePtr, '.');
                idbassert(ptr);
                strcpy(ptr, ".cmp");
            }

#endif

            if (oid > 3000)
            {
                //TODO: should syscat columns be considered when reducing open file count
                //  They are always needed why should they be closed?
                if (fdcache.size() >= iom->MaxOpenFiles())
                {
                    FdCacheCountType_t fdCountSort;

                    for (FdCacheType_t::iterator it = fdcache.begin(); it != fdcache.end(); it++)
                    {
                        struct FdCountEntry fdc(it->second->oid,
                                                it->second->dbroot,
                                                it->second->partNum,
                                                it->second->segNum,
                                                it->second->c,
                                                it);

                        fdCountSort.insert(fdc);
                    }

                    if (iom->FDCacheTrace())
                    {
                        iom->FDTraceFile()
                                << "Before flushing sz: " << fdcache.size()
                                << " delCount: " << iom->DecreaseOpenFilesCount()
                                << endl;

                        for (FdCacheType_t::iterator it = fdcache.begin(); it != fdcache.end(); it++)
                            iom->FDTraceFile() << *(*it).second << endl;

                        iom->FDTraceFile() << "==================" << endl << endl;
                    }

                    // TODO: should we consider a minimum number of open files
                    //      currently, there is nothing to prevent all open files
                    //      from being closed by the IOManager.

                    uint32_t delCount = 0;

                    for (FdCacheCountType_t::reverse_iterator rit = fdCountSort.rbegin();
                            rit != fdCountSort.rend() &&
                            fdcache.size() > 0 &&
                            delCount < iom->DecreaseOpenFilesCount();
                            rit++)
                    {
                        FdEntry oldfdKey(rit->oid, rit->dbroot, rit->partNum, rit->segNum, 0, NULL);
                        FdCacheType_t::iterator it = fdcache.find(oldfdKey);

                        if (it != fdcache.end())
                        {
                            if (iom->FDCacheTrace())
                            {
                                if (!rit->fdit->second->inUse)
                                    iom->FDTraceFile()
                                            << "Removing dc: " << delCount << " sz: " << fdcache.size()
                                            << *(*it).second << " u: " << rit->fdit->second->inUse << endl;
                                else
                                    iom->FDTraceFile()
                                            << "Skip Remove in use dc: " << delCount << " sz: " << fdcache.size()
                                            << *(*it).second << " u: " << rit->fdit->second->inUse << endl;
                            }

                            if (rit->fdit->second->inUse <= 0)
                            {
                                fdcache.erase(it);
                                delCount++;
                            }
                        }
                    } // for (FdCacheCountType_t...

                    if (iom->FDCacheTrace())
                    {
                        iom->FDTraceFile() << "After flushing sz: " << fdcache.size() << endl;

                        for (FdCacheType_t::iterator it = fdcache.begin(); it != fdcache.end(); it++)
                        {
                            iom->FDTraceFile()
                                    << *(*it).second << endl;
                        }

                        iom->FDTraceFile() << "==================" << endl << endl;
                    }

                    fdCountSort.clear();

                } // if (fdcache.size()...
            } // if (oid > 3000)

            int opts = primitiveprocessor::directIOFlag ? IDBDataFile::USE_ODIRECT : 0;
            fp = NULL;
            uint32_t openRetries = 0;
            int saveErrno = 0;

            while (fp == NULL && openRetries++ < 5)
            {
                fp = IDBDataFile::open(
                         IDBPolicy::getType( fileNamePtr, IDBPolicy::PRIMPROC ),
                         fileNamePtr,
                         "r",
                         opts);
                saveErrno = errno;

                if (fp == NULL)
                    sleep(1);
            }

            if ( fp == NULL )
            {
                Message::Args args;
                fdit = fdcache.end();
                fdMapMutex.unlock();
                args.add(oid);
                args.add(string(fileNamePtr) + ":" + strerror(saveErrno));
                primitiveprocessor::mlp->logMessage(logging::M0053, args, true);
                ostringstream errMsg;
                errMsg << "thr_popper: Error opening file for OID " << oid << "; "
                       << fileNamePtr << "; " << strerror(saveErrno);
                int errorCode = fileRequest::FAILED;

                if (saveErrno == EINVAL)
                    errorCode = fileRequest::FS_EINVAL;
                else if (saveErrno == ENOENT)
                    errorCode = fileRequest::FS_ENOENT;

                iom->handleBlockReadError(fr, errMsg.str(), &copyLocked, errorCode);
                continue;
            }

            fe.reset( new FdEntry(oid, dbroot, partNum, segNum, compType, fp) );
            fe->inUse++;
            fdcache[fdKey] = fe;
            fdit = fdcache.find(fdKey);
            fe.reset();
        }

        else
        {
            if (fdit->second.get())
            {
                fdit->second->c++;
                fdit->second->inUse++;
                fp = fdit->second->fp;
            }
            else
            {
                Message::Args args;
                fdit = fdcache.end();
                fdMapMutex.unlock();
                args.add(oid);
                ostringstream errMsg;
                errMsg << "Null FD cache entry. (dbroot, partNum, segNum, compType) = ("
                       << dbroot << ", " << partNum << ", " << segNum << ", " << compType << ")";
                args.add(errMsg.str());
                primitiveprocessor::mlp->logMessage(logging::M0053, args, true);
                iom->handleBlockReadError( fr, errMsg.str(), &copyLocked );
                continue;
            }
        }

        fdMapMutex.unlock();

#ifdef SHARED_NOTHING_DEMO_2

        // change offset if it's shared nothing
        /* Get the extent #, divide by # of PMs, calculate base offset for the new extent #,
        	add extent offset */
        if (oid >= 10000)
            offset = (((offset / extentSize) / iom->pmCount) * extentSize) + (offset % extentSize);

#endif

        longSeekOffset = (uint64_t)offset * (uint64_t)fileBlockSize;
        lldiv_t cmpOffFact = lldiv(longSeekOffset, (4LL * 1024LL * 1024LL));
        totalRqst++;

        uint32_t readCount = 0;
        uint32_t bytesRead = 0;
        uint32_t compressedBytesRead = 0; // @Bug 3149.  IOMTrace was not reporting bytesRead correctly for compressed columns.
        uint32_t jend = blocksRequested / iom->blocksPerRead;

        if (iom->IOTrace())
            clock_gettime(CLOCK_REALTIME, &tm);

        ostringstream errMsg;
        bool errorOccurred = false;
        string errorString;
#ifdef IDB_COMP_POC_DEBUG
        bool debugWrite = false;
#endif

#ifdef EM_AS_A_TABLE_POC__
        dlen = 1;
#endif

        if (blocksRequested % iom->blocksPerRead)
            jend++;

        for (j = 0; j < jend; j++)
        {

            int decompRetryCount = 0;
            int retryReadHeadersCount = 0;

decompRetry:
            blocksThisRead = std::min(dlen, iom->blocksPerRead);
            readSize = blocksThisRead * BLOCK_SIZE;

            acc = 0;

            while (acc < readSize)
            {
#if defined(EM_AS_A_TABLE_POC__)

                if (oid == 1084)
                {
                    uint32_t h;
                    int32_t o = 0;
                    int32_t* ip;
                    ip = (int32_t*)(&alignedbuff[acc]);

                    for (o = 0; o < 2048; o++)
                    {
                        if (iom->dbrm()->getHWM(o + 3000, h) == 0)
                            *ip++ = h;
                        else
                            *ip++ = numeric_limits<int32_t>::min() + 1;
                    }

                    i = BLOCK_SIZE;
                }
                else
                    i = pread(fd, &alignedbuff[acc], readSize - acc, longSeekOffset);

#else

                if (fdit->second->isCompressed())
                {
retryReadHeaders:
                    //hdrs may have been modified since we cached them in fdit->second...
                    time_t cur_mtime = numeric_limits<time_t>::max();
                    int updatePtrsRc = 0;
                    fdMapMutex.lock();
                    time_t fp_mtime = fp->mtime();

                    if ( fp_mtime != (time_t) - 1)
                        cur_mtime = fp_mtime;

                    if (decompRetryCount > 0 || retryReadHeadersCount > 0 || cur_mtime > fdit->second->cmpMTime)
                        updatePtrsRc = updateptrs(&alignedbuff[0], fdit, decompressor);

                    fdMapMutex.unlock();

                    int idx = cmpOffFact.quot;

                    if (updatePtrsRc != 0 || idx >= (signed)fdit->second->ptrList.size())
                    {
                        // Due to race condition, the header on disk may not upated yet.
                        // Log an info message and retry.
                        if (retryReadHeadersCount == 0)
                        {
                            Message::Args args;
                            args.add(oid);
                            ostringstream infoMsg;
                            iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
                            infoMsg << "retry updateptrs for " << fileNamePtr
                                    << ". rc=" << updatePtrsRc << ", idx="
                                    << idx << ", ptr.size=" << fdit->second->ptrList.size();
                            args.add(infoMsg.str());
                            primitiveprocessor::mlp->logInfoMessage(logging::M0061, args);
                        }

                        if (++retryReadHeadersCount < 30)
                        {
                            waitForRetry(retryReadHeadersCount);
                            fdit->second->cmpMTime = 0;
                            goto retryReadHeaders;
                        }
                        else
                        {
                            // still fail after all the retries.
                            errorOccurred = true;
                            errMsg << "Error reading compression header. rc=" << updatePtrsRc << ", idx="
                                   << idx << ", ptr.size=" << fdit->second->ptrList.size();
                            errorString = errMsg.str();
                            break;
                        }
                    }

                    //FIXME: make sure alignedbuff can hold fdit->second->ptrList[idx].second bytes
                    if (fdit->second->ptrList[idx].second > maxCompSz)
                    {
                        errorOccurred = true;
                        errMsg << "aligned buff too small. dataSize="
                               << fdit->second->ptrList[idx].second
                               << ", buffSize=" << maxCompSz;
                        errorString = errMsg.str();
                        break;
                    }

                    i = fp->pread(&alignedbuff[0], fdit->second->ptrList[idx].first, fdit->second->ptrList[idx].second );
#ifdef IDB_COMP_POC_DEBUG
                    {
                        boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
                        cout << boldStart << "pread1.1(" << fp << ", 0x" << hex << (ptrdiff_t)&alignedbuff[0] << dec << ", " << fdit->second->ptrList[idx].second <<
                             ", " << fdit->second->ptrList[idx].first << ") = " << i << ' ' << cmpOffFact.quot << ' ' << cmpOffFact.rem << boldStop << endl;
                    }
#endif

                    // @bug3407, give it some retries if pread failed.
                    if (i != (ssize_t)fdit->second->ptrList[idx].second)
                    {
                        // log an info message
                        if (retryReadHeadersCount == 0)
                        {
                            Message::Args args;
                            args.add(oid);
                            ostringstream infoMsg;
                            iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
                            infoMsg << " Read from " << fileNamePtr << " failed at chunk " << idx
                                    << ". (offset, size, actuall read) = ("
                                    << fdit->second->ptrList[idx].first << ", "
                                    << fdit->second->ptrList[idx].second << ", " << i << ")";
                            args.add(infoMsg.str());
                            primitiveprocessor::mlp->logInfoMessage(logging::M0061, args);
                        }

                        if (++retryReadHeadersCount < 30)
                        {
                            waitForRetry(retryReadHeadersCount);
                            fdit->second->cmpMTime = 0;
                            goto retryReadHeaders;
                        }
                        else
                        {
                            errorOccurred = true;
                            errMsg << "Error reading chunk " << idx;
                            errorString = errMsg.str();
                            break;
                        }
                    }

                    compressedBytesRead += i; // @Bug 3149.
                    i = readSize;
                }
                else
                {
                    i = fp->pread(&alignedbuff[acc], longSeekOffset, readSize - acc);
#ifdef IDB_COMP_POC_DEBUG
                    {
                        boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
                        cout << "pread1.2(" << fp << ", 0x" << hex << (ptrdiff_t)&alignedbuff[acc] << dec << ", " << (readSize - acc) <<
                             ", " << longSeekOffset << ") = " << i << ' ' << cmpOffFact.quot << ' ' << cmpOffFact.rem << endl;
                    }
#endif
                }

#endif

                if (i < 0 && errno == EINTR)
                {
                    continue;
                }
                else if (i < 0)
                {
                    try
                    {
                        iom->buildOidFileName(oid, dbroot, partNum, segNum,  fileNamePtr);
                    }
                    catch (exception& exc)
                    {
                        cerr << "FileName Err:" << exc.what() << endl;
                        strcpy(fileNamePtr, "unknown filename");
                    }

                    errorString   = strerror(errno);
                    errorOccurred = true;
                    errMsg << "thr_popper: Error reading file for OID " << oid << "; "
                           << " fp " << fp << "; offset " << longSeekOffset << "; fileName "
                           << fileNamePtr << "; " << errorString;
                    break; // break from "while(acc..." loop
                }
                else if (i == 0)
                {
                    iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
                    errorString   = "early EOF";
                    errorOccurred = true;
                    errMsg << "thr_popper: Early EOF reading file for OID " <<
                           oid << "; " << fileNamePtr;
                    break; // break from "while(acc..." loop
                }

                acc += i;
                longSeekOffset += (uint64_t)i;
                readCount++;
                bytesRead += i;
            } // while(acc...

            //..Break out of "for (j..." read loop if error occurred
            if (errorOccurred)
            {
                Message::Args args;
                args.add(oid);
                args.add(errorString);
                primitiveprocessor::mlp->logMessage(logging::M0061, args, true);
                iom->handleBlockReadError( fr, errMsg.str(), &copyLocked );
                break;
            }

            blocksRead += blocksThisRead;

            if (iom->IOTrace())
                clock_gettime(CLOCK_REALTIME, &tm2);

            /* New bulk VSS lookup code */
            {
                vector<BRM::LBID_t> lbids;
                vector<BRM::VER_t> versions;
                vector<bool> isLocked;

                for (i = 0; (uint32_t) i < blocksThisRead; i++)
                    lbids.push_back((BRM::LBID_t) (lbid + i) + (j * iom->blocksPerRead));

                if (blocksRequested > 1 || !flg)  // prefetch, or an unversioned single-block read
                    iom->dbrm()->bulkGetCurrentVersion(lbids, &versions, &isLocked);
                else     // a single-block read that was versioned
                {
                    versions.push_back(ver);
                    isLocked.push_back(false);
                }

                uint8_t* ptr = (uint8_t*)&alignedbuff[0];

                if (blocksThisRead > 0 && fdit->second->isCompressed())
                {
#ifdef _MSC_VER
                    unsigned int blen = 4 * 1024 * 1024 + 4;
#else
                    uint32_t blen = 4 * 1024 * 1024 + 4;
#endif
#ifdef IDB_COMP_POC_DEBUG
                    {
                        boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
                        cout << "decompress(0x" << hex << (ptrdiff_t)&alignedbuff[0] << dec << ", " << fdit->second->ptrList[cmpOffFact.quot].second << ", 0x" << hex << (ptrdiff_t)uCmpBuf << dec << ", " << blen << ")" << endl;
                    }
#endif
                    int dcrc = decompressor.uncompressBlock(&alignedbuff[0],
                                                            fdit->second->ptrList[cmpOffFact.quot].second, uCmpBuf, blen);

                    if (dcrc != 0)
                    {
#ifdef IDB_COMP_POC_DEBUG
                        boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
#endif

                        if (++decompRetryCount < 30)
                        {
                            blocksRead -= blocksThisRead;
                            waitForRetry(decompRetryCount);

                            // log an info message every 10 retries
                            if (decompRetryCount == 1)
                            {
                                Message::Args args;
                                args.add(oid);
                                ostringstream infoMsg;
                                iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
                                infoMsg << "decompress retry for " << fileNamePtr
                                        << " decompRetry chunk " << cmpOffFact.quot
                                        << " code=" << dcrc;
                                args.add(infoMsg.str());
                                primitiveprocessor::mlp->logInfoMessage(logging::M0061, args);
                            }

                            goto decompRetry;
                        }

                        cout << boldStart << "decomp returned " << dcrc << boldStop << endl;

                        errorOccurred = true;
                        Message::Args args;
                        args.add(oid);
                        errMsg << "Error decompressing block " << cmpOffFact.quot << " code=" << dcrc << " part=" << partNum << " seg=" << segNum;
                        args.add(errMsg.str());
                        primitiveprocessor::mlp->logMessage(logging::M0061, args, true);
                        iom->handleBlockReadError( fr, errMsg.str(), &copyLocked );
                        break;
                    }

                    //FIXME: why doesn't this work??? (See later for why)
                    //ptr = &uCmpBuf[cmpOffFact.rem];
                    memcpy(ptr, &uCmpBuf[cmpOffFact.rem], blocksThisRead * BLOCK_SIZE);

                    // log the retries, if any
                    if (retryReadHeadersCount > 0 || decompRetryCount > 0)
                    {
                        Message::Args args;
                        args.add(oid);
                        ostringstream infoMsg;
                        iom->buildOidFileName(oid, dbroot, partNum, segNum, fileNamePtr);
                        infoMsg << "Successfully uncompress " << fileNamePtr << " chunk "
                                << cmpOffFact.quot << " @";

                        if (retryReadHeadersCount > 0)
                            infoMsg << " HeaderRetry:" << retryReadHeadersCount;

                        if (decompRetryCount > 0)
                            infoMsg << " UncompressRetry:" << decompRetryCount;

                        args.add(infoMsg.str());
                        primitiveprocessor::mlp->logInfoMessage(logging::M0006, args);
                    }
                }

                for (i = 0; useCache && (uint32_t) i < lbids.size(); i++)
                {
                    if (!isLocked[i])
                    {
#ifdef IDB_COMP_POC_DEBUG
                        {
                            if (debugWrite)
                            {
                                boost::mutex::scoped_lock lk(primitiveprocessor::compDebugMutex);
                                cout << boldStart << "i = " << i << ", ptr = 0x" << hex << (ptrdiff_t)&ptr[i * BLOCK_SIZE] << dec << boldStop << endl;
                                cout << boldStart;
#if 0
                                int32_t* i32p;
                                i32p = (int32_t*)&ptr[i * BLOCK_SIZE];

                                for (int iy = 0; iy < 2; iy++)
                                {
                                    for (int ix = 0; ix < 8; ix++, i32p++)
                                        cout << *i32p << ' ';

                                    cout << endl;
                                }

#else
                                int64_t* i64p;
                                i64p = (int64_t*)&ptr[i * BLOCK_SIZE];

                                for (int iy = 0; iy < 2; iy++)
                                {
                                    for (int ix = 0; ix < 8; ix++, i64p++)
                                        cout << *i64p << ' ';

                                    cout << endl;
                                }

#endif
                                cout << boldStop << endl;
                            }
                        }
#endif
                        cacheInsertOps.push_back(CacheInsert_t(lbids[i], versions[i], (uint8_t*)
                                                               &alignedbuff[i * BLOCK_SIZE]));
                    }
                }

                if (useCache)
                {
                    blocksLoaded += fbm->bulkInsert(cacheInsertOps);
                    cacheInsertOps.clear();
                }
            }

            dlen -= blocksThisRead;

        } // for (j...

        fdMapMutex.lock();

        if (fdit->second.get())
            fdit->second->inUse--;

        fdit = fdcache.end();
        fdMapMutex.unlock();

        if (errorOccurred)
            continue;

        try
        {
            iom->dbrm()->releaseLBIDRange(lbid, blocksRequested);
            copyLocked = false;
        }
        catch (exception& e)
        {
            cout << "releaseRange: " << e.what() << endl;
        }

        fr->BlocksRead(blocksRead);
        fr->BlocksLoaded(blocksLoaded);

        //FIXME: This is why some code above doesn't work...
        if (fr->data != 0 && blocksRequested == 1)
            memcpy(fr->data, alignedbuff, BLOCK_SIZE);

        fr->frMutex().lock();
        fr->SetPredicate(fileRequest::COMPLETE);
        fr->frCond().notify_one();
        fr->frMutex().unlock();

        if (iom->IOTrace())
        {
            clock_gettime(CLOCK_REALTIME, &rqst2);
            timespec_sub(tm, tm2, tm3);
            timespec_sub(rqst1, rqst2, rqst3);

            // @Bug 3149.  IOMTrace was not reporting bytesRead correctly for compressed columns.
            uint32_t reportBytesRead = (compressedBytesRead > 0) ? compressedBytesRead : bytesRead;

            lFile
                    << left  << setw(5) << setfill(' ') << oid
                    << right << setw(5) << setfill(' ') << offset / extentSize << " "
                    << right << setw(11) << setfill(' ') << lbid << " "
                    << right << setw(9) << bytesRead / (readCount << 13) << " "
                    << right << setw(9)	<< blocksRequested << " "
                    << right << setw(10) << fixed << tm3 << " "
                    << right << fixed << ((double)(rqst1.tv_sec + (1.e-9 * rqst1.tv_nsec))) << " "
                    << right << setw(10) << fixed << rqst3 << " "
                    << right << setw(10) << fixed << longSeekOffset << " "
                    << right << setw(10) << fixed << reportBytesRead << " "
                    << right << setw(3) << fixed << dbroot << " "
                    << right << setw(3) << fixed << partNum << " "
                    << right << setw(3) << fixed << segNum << " "
                    << endl;
        }

    } // for(;;)

    delete [] uCmpBuf;

    lFile.close();

    //reaching here is an error...

    return 0;
} // end thr_popper

} // anonymous namespace

namespace dbbc
{

void setReadLock()
{
    localLock.read_lock();
}

void releaseReadLock()
{
    localLock.read_unlock();
}

void dropFDCache()
{
    localLock.write_lock();
    fdcache.clear();
    localLock.write_unlock();
}
void purgeFDCache(std::vector<BRM::FileInfo>& files)
{
    localLock.write_lock();

    FdCacheType_t::iterator fdit;

    for ( uint32_t i = 0; i < files.size(); i++)
    {
        FdEntry fdKey(files[i].oid, files[i].dbRoot, files[i].partitionNum, files[i].segmentNum, files[i].compType, NULL);
        fdit = fdcache.find(fdKey);

        if (fdit != fdcache.end())
            fdcache.erase(fdit);
    }

    localLock.write_unlock();
}

ioManager::ioManager(FileBufferMgr& fbm,
                     fileBlockRequestQueue& fbrq,
                     int thrCount,
                     int bsPerRead):
    blocksPerRead(bsPerRead),
    fIOMfbMgr(fbm),
    fIOMRequestQueue(fbrq),
    fFileOp(false)
{
    if (thrCount <= 0)
        thrCount = 1;

    if (thrCount > 256)
        thrCount = 256;

    fConfig = Config::makeConfig();
    string val = fConfig->getConfig("DBBC", "IOMTracing");
    int temp = 0;

    if (val.length() > 0) temp = static_cast<int>(Config::fromText(val));

    if (temp > 0)
        fIOTrace = true;
    else
        fIOTrace = false;

    val = fConfig->getConfig("DBBC", "MaxOpenFiles");
    temp = 0;
    fMaxOpenFiles = MAX_OPEN_FILES;

    if (val.length() > 0) temp = static_cast<int>(Config::fromText(val));

    if (temp > 0)
        fMaxOpenFiles = temp;

    val = fConfig->getConfig("DBBC", "DecreaseOpenFilesCount");
    temp = 0;
    fDecreaseOpenFilesCount = DECREASE_OPEN_FILES;

    if (val.length() > 0) temp = static_cast<int>(Config::fromText(val));

    if (temp > 0)
        fDecreaseOpenFilesCount = temp;

    // limit the number of files closed
    if (fDecreaseOpenFilesCount > (uint32_t)(0.75 * fMaxOpenFiles))
        fDecreaseOpenFilesCount = (uint32_t)(0.75 * fMaxOpenFiles);

    val = fConfig->getConfig("DBBC", "FDCacheTrace");
    temp = 0;
    fFDCacheTrace = false;

    if (val.length() > 0) temp = static_cast<int>(Config::fromText(val));

    if (temp > 0)
    {
        fFDCacheTrace = true;
#ifdef _MSC_VER
        FDTraceFile().open("C:/Calpont/log/trace/fdcache", ios_base::ate | ios_base::app);
#else
        FDTraceFile().open("/var/log/mariadb/columnstore/trace/fdcache", ios_base::ate | ios_base::app);
#endif
    }

    fThreadCount = thrCount;
    go();
}

void ioManager::buildOidFileName(const BRM::OID_t oid, const uint16_t dbRoot, const uint16_t partNum, const uint32_t segNum, char* file_name)
{
    if (fFileOp.getFileName(oid, file_name, dbRoot, partNum, segNum) != WriteEngine::NO_ERROR)
    {
        file_name[0] = 0;
        throw std::runtime_error("fileOp.getFileName failed");
    }

    //cout << "Oid2Filename o: " << oid << " n: " << file_name << endl;
}

const int ioManager::localLbidLookup(BRM::LBID_t lbid,
                                     BRM::VER_t verid,
                                     bool vbFlag,
                                     BRM::OID_t& oid,
                                     uint16_t& dbRoot,
                                     uint32_t& partitionNum,
                                     uint16_t& segmentNum,
                                     uint32_t& fileBlockOffset)
{
    if (primitiveprocessor::noVB > 0)
        vbFlag = false;

    int rc = fdbrm.lookupLocal(lbid,
                               verid,
                               vbFlag,
                               oid,
                               dbRoot,
                               partitionNum,
                               segmentNum,
                               fileBlockOffset);

    return rc;
}

struct LambdaKludge
{
    LambdaKludge(ioManager* i) : iom(i) { }
    ~LambdaKludge()
    {
        iom = NULL;
    }
    ioManager* iom;
    void operator()()
    {
        thr_popper(iom);
    }
};

void ioManager::createReaders()
{
    int idx;

    for (idx = 0; idx < fThreadCount; idx++)
    {
        try
        {
            fThreadArr.create_thread(LambdaKludge(this));
        }
        catch (exception& e)
        {
            cerr << "IOM::createReaders() caught " << e.what() << endl;
            idx--;
            sleep(1);
            continue;
        }
    }
}

ioManager::~ioManager()
{
    stop();
}

void ioManager::go(void)
{
    createReaders();
}


//FIXME: is this right? what does this method do?
void ioManager::stop()
{
    for (int idx = 0; idx < fThreadCount; idx++)
    {
        (void)0; //pthread_detach(fThreadArr[idx]);
    }
}


fileRequest* ioManager::getNextRequest()
{
    fileRequest* blk = 0;

    try
    {
        blk = fIOMRequestQueue.pop();
        return blk;
    }
    catch (exception&)
    {
        cerr << "ioManager::getNextRequest() ERROR " << endl;
    }

    return blk;

}

//------------------------------------------------------------------------------
// Prints stderr msg and updates fileRequest object to reflect an error.
// Lastly, notifies waiting thread that fileRequest has been completed.
//------------------------------------------------------------------------------
void ioManager::handleBlockReadError( fileRequest* fr,
                                      const string& errMsg, bool* copyLocked, int errorCode )
{
    try
    {
        dbrm()->releaseLBIDRange(fr->Lbid(), fr->BlocksRequested());
        *copyLocked = false;
    }
    catch (exception& e)
    {
        cout << "releaseRange on read error: " << e.what() << endl;
    }

    cerr << errMsg << endl;
    fr->RequestStatus(errorCode);
    fr->RequestStatusStr(errMsg);

    fr->frMutex().lock();
    fr->SetPredicate(fileRequest::COMPLETE);
    fr->frCond().notify_one();
    fr->frMutex().unlock();
}

}
// vim:ts=4 sw=4:
