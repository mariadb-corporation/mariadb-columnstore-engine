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
 *   $Id: primitiveserver.h 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *
 ***********************************************************************/
/** @file */
#pragma once

#include <map>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <unordered_map>
#include <boost/thread.hpp>

#include "threadpool.h"
#include "fair_threadpool.h"
#include "messagequeue.h"
#include "blockrequestprocessor.h"
#include "batchprimitiveprocessor.h"

#include "service.h"

#include "oamcache.h"
extern oam::OamCache* oamCache;

namespace primitiveprocessor
{
extern dbbc::BlockRequestProcessor** BRPp;
extern BRM::DBRM* brm;
extern boost::mutex bppLock;
extern uint32_t highPriorityThreads, medPriorityThreads, lowPriorityThreads;

class BPPSendThread;

class PrimitiveServer;

class BPPV
{
 public:
  BPPV(PrimitiveServer* ps);
  ~BPPV();
  boost::shared_ptr<BatchPrimitiveProcessor> next();
  void add(boost::shared_ptr<BatchPrimitiveProcessor> a);
  const std::vector<boost::shared_ptr<BatchPrimitiveProcessor> >& get();
  inline boost::shared_ptr<BPPSendThread> getSendThread()
  {
    return sendThread;
  }
  void abort();
  bool aborted();
  std::atomic<bool> joinDataReceived{false};

 private:
  std::vector<boost::shared_ptr<BatchPrimitiveProcessor> > v;
  boost::shared_ptr<BPPSendThread> sendThread;

  // the instance other instances are created from
  boost::shared_ptr<BatchPrimitiveProcessor> unusedInstance;

  // pos keeps the position of the last BPP returned by next(),
  // next() will start searching at this pos on the next call.
  uint32_t pos;
};

typedef boost::shared_ptr<BPPV> SBPPV;
typedef std::map<uint32_t, SBPPV> BPPMap;
extern BPPMap bppMap;

void prefetchBlocks(uint64_t lbid, const int compType, uint32_t* rCount);
void prefetchExtent(uint64_t lbid, uint32_t ver, uint32_t txn, uint32_t* rCount);
void loadBlock(uint64_t lbid, BRM::QueryContext q, uint32_t txn, int compType, void* bufferPtr,
               bool* pWasBlockInCache, uint32_t* rCount = nullptr, bool LBIDTrace = false,
               uint32_t sessionID = 0, bool doPrefetch = true, VSSCache* vssCache = nullptr);
void loadBlockAsync(uint64_t lbid, const BRM::QueryContext& q, uint32_t txn, int CompType, uint32_t* cCount,
                    uint32_t* rCount, bool LBIDTrace, uint32_t sessionID, boost::mutex* m,
                    uint32_t* busyLoaders, boost::shared_ptr<BPPSendThread> sendThread,
                    VSSCache* vssCache = nullptr);
uint32_t loadBlocks(BRM::LBID_t* lbids, BRM::QueryContext q, BRM::VER_t txn, int compType,
                    uint8_t** bufferPtrs, uint32_t* rCount, bool LBIDTrace, uint32_t sessionID,
                    uint32_t blockCount, bool* wasVersioned, bool doPrefetch = true,
                    VSSCache* vssCache = nullptr);
uint32_t cacheNum(uint64_t lbid);
void buildFileName(BRM::OID_t oid, char* fileName);

/** @brief process primitives as they arrive
 */
class PrimitiveServer
{
 public:
  /** @brief ctor
   */
  PrimitiveServer(int serverThreads, int serverQueueSize, int processorWeight, int processorQueueSize,
                  bool rotatingDestination, uint32_t BRPBlocks = (1024 * 1024 * 2), int BRPThreads = 64,
                  int cacheCount = 8, int maxBlocksPerRead = 128, int readAheadBlocks = 256,
                  uint32_t deleteBlocks = 0, bool ptTrace = false, double prefetchThreshold = 0,
                  uint64_t pmSmallSide = 0);

  /** @brief dtor
   */
  ~PrimitiveServer();

  /** @brief start the primitive server
   *
   */
  void start(Service* p, utils::USpaceSpinLock& startupRaceLock);

  /** @brief get a pointer the shared processor thread pool
   */
  inline boost::shared_ptr<threadpool::FairThreadPool> getProcessorThreadPool() const
  {
    return fProcessorPool;
  }

  inline std::shared_ptr<threadpool::PriorityThreadPool> getOOBProcessorThreadPool() const
  {
    return fOOBPool;
  }

  int ReadAheadBlocks() const
  {
    return fReadAheadBlocks;
  }
  bool rotatingDestination() const
  {
    return fRotatingDestination;
  }
  bool PTTrace() const
  {
    return fPTTrace;
  }
  double prefetchThreshold() const
  {
    return fPrefetchThreshold;
  }
  uint32_t ProcessorThreads() const
  {
    return highPriorityThreads + medPriorityThreads + lowPriorityThreads;
  }

 protected:
 private:
  /** @brief the thread pool used to listen for
   * incoming primitive commands
   */
  threadpool::ThreadPool fServerpool;

  /** @brief the thread pool used to process
   * primitive commands
   */
  boost::shared_ptr<threadpool::FairThreadPool> fProcessorPool;
  std::shared_ptr<threadpool::PriorityThreadPool> fOOBPool;

  int fServerThreads;
  int fServerQueueSize;
  int fProcessorWeight;
  int fProcessorQueueSize;
  int fMaxBlocksPerRead;
  int fReadAheadBlocks;
  bool fRotatingDestination;
  bool fPTTrace;
  double fPrefetchThreshold;
  uint64_t fPMSmallSide;
};

}  // namespace primitiveprocessor
