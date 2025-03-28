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

#include "diskjoinstep.h"
#include "exceptclasses.h"
#include "resourcemanager.h"

using namespace std;
using namespace rowgroup;
using namespace joiner;
using namespace logging;

namespace joblist
{
DiskJoinStep::DiskJoinStep()
{
}

DiskJoinStep::DiskJoinStep(TupleHashJoinStep* t, int djsIndex, int joinIndex, bool lastOne)
 : JobStep(*t), thjs(t), mainThread(0), joinerIndex(joinIndex), closedOutput(false)
{
  /*
      grab all relevant vars from THJS
      make largeRG and outputRG
      make the RG mappings
      init a JoinPartition
      load the existing RGData into JoinPartition
  */

  largeRG = thjs->largeRG + thjs->outputRG;

  if (lastOne)
    outputRG = thjs->outputRG;
  else
    outputRG = largeRG;

  smallRG = thjs->smallRGs[joinerIndex];
  largeKeyCols = thjs->largeSideKeys[joinerIndex];
  smallKeyCols = thjs->smallSideKeys[joinerIndex];

  /* Should not be necessary if we can use THJS's logic to do the join */
  fe = thjs->getJoinFilter(joinerIndex);

  if (fe)
  {
    joinFERG = thjs->joinFilterRG;
    SjoinFEMapping = makeMapping(smallRG, joinFERG);
    LjoinFEMapping = makeMapping(largeRG, joinFERG);
  }

  joiner = thjs->djsJoiners[djsIndex];
  joinType = joiner->getJoinType();
  typeless = joiner->isTypelessJoin();
  joiner->clearData();
  joiner->setInUM();

  LOMapping = makeMapping(largeRG, outputRG);
  SOMapping = makeMapping(smallRG, outputRG);

  /* Link up */
  largeDL = thjs->fifos[djsIndex];
  outputDL = thjs->fifos[djsIndex + 1];
  smallDL = thjs->smallDLs[joinerIndex];
  largeIt = largeDL->getIterator();

  smallUsage = thjs->djsSmallUsage;
  smallLimit = thjs->djsSmallLimit;
  largeLimit = thjs->djsLargeLimit;
  partitionSize = thjs->djsPartitionSize;
  maxPartitionTreeDepth = thjs->djsMaxPartitionTreeDepth;

  if (smallLimit == 0)
    smallLimit = numeric_limits<int64_t>::max();

  if (largeLimit == 0)
    largeLimit = numeric_limits<int64_t>::max();

  uint64_t totalUMMemory = thjs->resourceManager->getConfiguredUMMemLimit();
  jp.reset(new JoinPartition(largeRG, smallRG, smallKeyCols, largeKeyCols, typeless,
                             (joinType & ANTI) && (joinType & MATCHNULLS), (bool)fe, totalUMMemory,
                             partitionSize, maxPartitionTreeDepth));

  if (cancelled())
  {
    // drain inputs, close output
    smallReader();  // only small input is supplying input at this point
    outputDL->endOfInput();
    closedOutput = true;
  }

  largeIterationCount = 0;
  lastLargeIteration = false;
  fMiniInfo.clear();
  fExtendedInfo.clear();
}

DiskJoinStep::~DiskJoinStep()
{
  abort();

  if (mainThread)
  {
    jobstepThreadPool.join(mainThread);
    mainThread = 0;
  }

  if (jp)
    atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
}

void DiskJoinStep::loadExistingData(vector<RGData>& data)
{
  int64_t memUsage;
  uint32_t i;

  for (i = 0; i < data.size() && !cancelled(); i++)
  {
    memUsage = jp->insertSmallSideRGData(data[i]);
    atomicops::atomicAdd(smallUsage.get(), memUsage);
  }
}

void DiskJoinStep::run()
{
  mainThread = jobstepThreadPool.invoke(Runner(this));
}

void DiskJoinStep::join()
{
  if (mainThread)
  {
    jobstepThreadPool.join(mainThread);
    mainThread = 0;
  }

  if (jp)
  {
    atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
    // int64_t memUsage;
    // memUsage = atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
    // cout << "join(): small side usage was: " << jp->getSmallSideDiskUsage() << " final shared mem usage = "
    // << memUsage << endl;
    jp.reset();
  }
}

void DiskJoinStep::smallReader()
{
  RGData rgData;
  bool more = true;
  int64_t memUsage = 0, combinedMemUsage = 0;
  RowGroup l_smallRG = smallRG;

  try
  {
    while (more && !cancelled())
    {
      more = smallDL->next(0, &rgData);

      if (more)
      {
        l_smallRG.setData(&rgData);
        memUsage = jp->insertSmallSideRGData(rgData);
        combinedMemUsage = atomicops::atomicAdd(smallUsage.get(), memUsage);

        if (combinedMemUsage > smallLimit)
        {
          errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_DISK_USAGE_LIMIT));
          status(ERR_DBJ_DISK_USAGE_LIMIT);
          cout << "DJS small reader: exceeded disk space limit" << endl;
          abort();
        }
      }
    }

    if (LIKELY(!cancelled()))
    {
      memUsage = jp->doneInsertingSmallData();
      combinedMemUsage = atomicops::atomicAdd(smallUsage.get(), memUsage);

      if (combinedMemUsage > smallLimit)
      {
        errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_DISK_USAGE_LIMIT));
        status(ERR_DBJ_DISK_USAGE_LIMIT);
        cout << "DJS small reader: exceeded disk space limit" << endl;
        abort();
      }
    }
  }  // try
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_ALWAYS_CRITICAL,
                    "DiskJoinStep::smallReader()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
    abort();
  }

  while (more)
    more = smallDL->next(0, &rgData);
}

void DiskJoinStep::largeReader()
{
  RGData rgData;
  bool more = true;
  int64_t largeSize = 0;
  RowGroup l_largeRG = largeRG;

  largeIterationCount++;

  try
  {
    while (more && !cancelled() && largeSize < largeLimit)
    {
      more = largeDL->next(largeIt, &rgData);

      if (more)
      {
        l_largeRG.setData(&rgData);

        largeSize += jp->insertLargeSideRGData(rgData);
      }
    }

    jp->doneInsertingLargeData();

    if (!more)
      lastLargeIteration = true;
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_ALWAYS_CRITICAL,
                    "DiskJoinStep::largeReader()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
    abort();
  }

  if (UNLIKELY(cancelled()))
    while (more)
      more = largeDL->next(largeIt, &rgData);
}

void DiskJoinStep::loadFcn(const uint32_t threadID, const uint32_t smallSideSizeLimit,
                           const std::vector<joiner::JoinPartition*>& joinPartitions)
{
  boost::shared_ptr<LoaderOutput> out;

  try
  {
    uint32_t partitionIndex = 0;
    bool partitionDone = true;
    RowGroup& rowGroup = smallRG;

    // Iterate over partitions.
    while (partitionIndex < joinPartitions.size() && !cancelled())
    {
      uint64_t currentSize = 0;
      auto* joinPartition = joinPartitions[partitionIndex];
      out.reset(new LoaderOutput());

      if (partitionDone)
        joinPartition->setNextSmallOffset(0);

      while (true)
      {
        messageqcpp::ByteStream bs;
        RGData rgData;

        joinPartition->readByteStream(0, &bs);
        if (!bs.length())
        {
          partitionDone = true;
          break;
        }

        rgData.deserialize(bs);
        rowGroup.setData(&rgData);

        // Check that current `RowGroup` has rows.
        if (!rowGroup.getRowCount())
        {
          partitionDone = true;
          break;
        }

        currentSize += rowGroup.getDataSize();
        out->smallData.push_back(rgData);

        if (currentSize > smallSideSizeLimit)
        {
#ifdef DEBUG_DJS
          cout << "Memory limit exceeded for the partition: " << joinPartition->getUniqueID() << endl;
          cout << "Current size: " << currentSize << " Memory limit: " << partitionSize << endl;
#endif
          partitionDone = false;
          currentSize = 0;
          break;
        }
      }

      if (!out->smallData.size())
      {
        ++partitionIndex;
        partitionDone = true;
        continue;
      }

      // Initialize `LoaderOutput` and add it to `FIFO`.
      out->partitionID = joinPartition->getUniqueID();
      out->jp = joinPartition;
      loadFIFO[threadID]->insert(out);

      // If this partition is done - take a next one.
      if (partitionDone)
        ++partitionIndex;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_ALWAYS_CRITICAL,
                    "DiskJoinStep::loadFcn()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
    abort();
  }

  loadFIFO[threadID]->endOfInput();
}

void DiskJoinStep::buildFcn(const uint32_t threadID)
{
  boost::shared_ptr<LoaderOutput> in;
  boost::shared_ptr<BuilderOutput> out;
  bool more = true;
  int it = loadFIFO[threadID]->getIterator();
  int i, j;
  Row smallRow;
  RowGroup l_smallRG = smallRG;

  l_smallRG.initRow(&smallRow);

  while (true)
  {
    more = loadFIFO[threadID]->next(it, &in);

    if (!more || cancelled())
      goto out;

    out.reset(new BuilderOutput());
    out->smallData = in->smallData;
    out->partitionID = in->partitionID;
    out->jp = in->jp;
    out->tupleJoiner = joiner->copyForDiskJoin();

    for (i = 0; i < (int)in->smallData.size(); i++)
    {
      l_smallRG.setData(&in->smallData[i]);
      l_smallRG.getRow(0, &smallRow);

      for (j = 0; j < (int)l_smallRG.getRowCount(); j++, smallRow.nextRow())
        out->tupleJoiner->insert(smallRow, (largeIterationCount == 1));
    }

    out->tupleJoiner->doneInserting();
    buildFIFO[threadID]->insert(out);
  }

out:

  while (more)
    more = loadFIFO[threadID]->next(it, &in);

  buildFIFO[threadID]->endOfInput();
}

void DiskJoinStep::joinFcn(const uint32_t threadID)
{
  // This function mostly serves as an adapter between the
  // input data and the joinOneRG() fcn in THJS.
  boost::shared_ptr<BuilderOutput> in;
  bool more = true;
  int it = buildFIFO[threadID]->getIterator();
  int i;
  vector<RGData> joinResults;
  RowGroup l_largeRG = largeRG, l_smallRG = smallRG;
  RowGroup l_outputRG = outputRG;
  Row l_largeRow;
  Row l_joinFERow, l_outputRow, baseRow;
  vector<vector<Row::Pointer>> joinMatches;
  auto new_row = new Row[1];
  std::shared_ptr<Row[]> smallRowTemplates(new_row);
  vector<std::shared_ptr<TupleJoiner>> joiners;
  std::shared_ptr<std::shared_ptr<int[]>[]> colMappings, fergMappings;
  boost::scoped_array<boost::scoped_array<uint8_t>> smallNullMem;
  boost::scoped_array<uint8_t> joinFEMem;
  Row smallNullRow;

  boost::scoped_array<uint8_t> baseRowMem;

  if (joiner->hasFEFilter())
  {
    joinFERG.initRow(&l_joinFERow, true);
    joinFEMem.reset(new uint8_t[l_joinFERow.getSize()]);
    l_joinFERow.setData(rowgroup::Row::Pointer(joinFEMem.get()));
  }

  outputRG.initRow(&l_outputRow);
  outputRG.initRow(&baseRow, true);

  largeRG.initRow(&l_largeRow);

  baseRowMem.reset(new uint8_t[baseRow.getSize()]);
  baseRow.setData(rowgroup::Row::Pointer(baseRowMem.get()));
  joinMatches.emplace_back(vector<Row::Pointer>());
  smallRG.initRow(&smallRowTemplates[0]);
  joiners.resize(1);

  colMappings.reset(new std::shared_ptr<int[]>[2]);
  colMappings[0] = SOMapping;
  colMappings[1] = LOMapping;

  if (fe)
  {
    fergMappings.reset(new std::shared_ptr<int[]>[2]);
    fergMappings[0] = SjoinFEMapping;
    fergMappings[1] = LjoinFEMapping;
  }

  l_smallRG.initRow(&smallNullRow, true);
  smallNullMem.reset(new boost::scoped_array<uint8_t>[1]);
  smallNullMem[0].reset(new uint8_t[smallNullRow.getSize()]);
  smallNullRow.setData(rowgroup::Row::Pointer(smallNullMem[0].get()));
  smallNullRow.initToNull();

  try
  {
    while (true)
    {
      more = buildFIFO[threadID]->next(it, &in);

      if (!more || cancelled())
        goto out;

      joiners[0] = in->tupleJoiner;
      boost::shared_ptr<RGData> largeData;
      largeData = in->jp->getNextLargeRGData();

      while (largeData)
      {
        l_largeRG.setData(largeData.get());
        thjs->joinOneRG(0, joinResults, l_largeRG, l_outputRG, l_largeRow, l_joinFERow, l_outputRow, baseRow,
                        joinMatches, smallRowTemplates, outputDL.get(), &joiners, &colMappings, &fergMappings,
                        &smallNullMem);

        if (joinResults.size())
          outputResult(joinResults);

        thjs->returnMemory();
        joinResults.clear();
        largeData = in->jp->getNextLargeRGData();
      }

      if (joinType & SMALLOUTER)
      {
        if (!lastLargeIteration)
        {
          /* TODO: an optimization would be to detect whether any new rows were marked and if not
             suppress the save operation */
          vector<Row::Pointer> unmatched;
          in->tupleJoiner->getUnmarkedRows(&unmatched);
          // cout << "***** saving partition " << in->partitionID << " unmarked count=" << unmatched.size() <<
          // " total count="
          //	<< in->tupleJoiner->size() << " vector size=" << in->smallData.size() <<  endl;
          in->jp->saveSmallSidePartition(in->smallData);
        }
        else
        {
          // cout << "finishing small-outer output" << endl;
          vector<Row::Pointer> unmatched;
          RGData rgData(l_outputRG);
          Row outputRow;

          l_outputRG.setData(&rgData);
          l_outputRG.resetRowGroup(0);
          l_outputRG.initRow(&outputRow);
          l_outputRG.getRow(0, &outputRow);

          l_largeRG.initRow(&l_largeRow, true);
          boost::scoped_array<uint8_t> largeNullMem(new uint8_t[l_largeRow.getSize()]);
          l_largeRow.setData(rowgroup::Row::Pointer(largeNullMem.get()));
          l_largeRow.initToNull();

          in->tupleJoiner->getUnmarkedRows(&unmatched);

          // cout << " small-outer count=" << unmatched.size() << endl;
          for (i = 0; i < (int)unmatched.size(); i++)
          {
            smallRowTemplates[0].setData(unmatched[i]);
            applyMapping(LOMapping, l_largeRow, &outputRow);
            applyMapping(SOMapping, smallRowTemplates[0], &outputRow);
            l_outputRG.incRowCount();

            if (l_outputRG.getRowCount() == 8192)
            {
              outputResult(rgData);
              // cout << "inserting a full RG" << endl;
              if (thjs)
              {
                if (!thjs->getMemory(l_outputRG.getMaxDataSizeWithStrings()))
                {
                  // FIXME: This is also looks wrong.
                  // calculate guess of size required for error message
                  uint64_t memReqd = (l_outputRG.getMaxDataSizeWithStrings()) / 1048576;
                  Message::Args args;
                  args.add(memReqd);
                  args.add(thjs->resourceManager->getConfiguredUMMemLimit() / 1048576);
                  std::cerr << logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_JOIN_RESULT_TOO_BIG,
                                                                           args)
                            << " @" << __FILE__ << ":" << __LINE__;
                  throw logging::IDBExcept(logging::ERR_JOIN_RESULT_TOO_BIG, args);
                }
              }

              rgData.reinit(l_outputRG);
              l_outputRG.setData(&rgData);
              l_outputRG.resetRowGroup(0);
              l_outputRG.getRow(0, &outputRow);
            }
            else
              outputRow.nextRow();
          }

          if (l_outputRG.getRowCount())
            outputResult(rgData);
          if (thjs)
            thjs->returnMemory();
        }
      }
    }
  }  // the try stmt above; need to reformat.
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_ALWAYS_CRITICAL,
                    "DiskJoinStep::joinFcn()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
    abort();
  }

out:

  while (more)
    more = buildFIFO[threadID]->next(it, &in);

  if (cancelled())
  {
    reportStats();
    outputDL->endOfInput();
    closedOutput = true;
  }
}

void DiskJoinStep::initializeFIFO(const uint32_t threadCount)
{
  loadFIFO.clear();
  buildFIFO.clear();

  for (uint32_t i = 0; i < threadCount; ++i)
  {
    boost::shared_ptr<LoaderOutputFIFO> lFIFO(new LoaderOutputFIFO(1, 1));
    boost::shared_ptr<BuilderOutputFIFO> bFIFO(new BuilderOutputFIFO(1, 1));

    loadFIFO.push_back(lFIFO);
    buildFIFO.push_back(bFIFO);
  }
}

void DiskJoinStep::processJoinPartitions(const uint32_t threadID, const uint32_t smallSideSizeLimitPerThread,
                                         const std::vector<JoinPartition*>& joinPartitions)
{
  std::vector<uint64_t> pipelineThreads;
  pipelineThreads.reserve(3);
  pipelineThreads.push_back(
      jobstepThreadPool.invoke(Loader(this, threadID, smallSideSizeLimitPerThread, joinPartitions)));
  pipelineThreads.push_back(jobstepThreadPool.invoke(Builder(this, threadID)));
  pipelineThreads.push_back(jobstepThreadPool.invoke(Joiner(this, threadID)));
  jobstepThreadPool.join(pipelineThreads);
}

void DiskJoinStep::prepareJobs(const std::vector<JoinPartition*>& joinPartitions,
                               JoinPartitionJobs& joinPartitionsJobs)
{
  const uint32_t issuedThreads = jobstepThreadPool.getIssuedThreads();
  const uint32_t maxNumOfThreads = jobstepThreadPool.getMaxThreads();
  const uint32_t numOfThreads =
      std::min(std::min(maxNumOfJoinThreads, std::max(maxNumOfThreads - issuedThreads, (uint32_t)1)),
               (uint32_t)joinPartitions.size());
  const uint32_t workSize = joinPartitions.size() / numOfThreads;

  uint32_t offset = 0;
  joinPartitionsJobs.reserve(numOfThreads);
  for (uint32_t threadNum = 0; threadNum < numOfThreads; ++threadNum, offset += workSize)
  {
    auto start = joinPartitions.begin() + offset;
    auto end = start + workSize;
    std::vector<JoinPartition*> joinPartitionJob(start, end);
    joinPartitionsJobs.push_back(std::move(joinPartitionJob));
  }

  for (uint32_t i = 0, e = joinPartitions.size() % numOfThreads; i < e; ++i, ++offset)
    joinPartitionsJobs[i].push_back(joinPartitions[offset]);
}

void DiskJoinStep::outputResult(const std::vector<rowgroup::RGData>& result)
{
  std::lock_guard<std::mutex> lk(outputMutex);
  for (const auto& rgData : result)
    outputDL->insert(rgData);
}

void DiskJoinStep::outputResult(const rowgroup::RGData& result)
{
  std::lock_guard<std::mutex> lk(outputMutex);
  outputDL->insert(result);
}

void DiskJoinStep::spawnJobs(const std::vector<std::vector<JoinPartition*>>& joinPartitionsJobs,
                             const uint32_t smallSideSizeLimitPerThread)
{
  const uint32_t threadsCount = joinPartitionsJobs.size();
  std::vector<uint64_t> processorThreadsId;
  processorThreadsId.reserve(threadsCount);
  for (uint32_t threadID = 0; threadID < threadsCount; ++threadID)
  {
    processorThreadsId.push_back(jobstepThreadPool.invoke(
        JoinPartitionsProcessor(this, threadID, smallSideSizeLimitPerThread, joinPartitionsJobs[threadID])));
  }

  jobstepThreadPool.join(processorThreadsId);
}

void DiskJoinStep::mainRunner()
{
  try
  {
    smallReader();

    while (!lastLargeIteration && !cancelled())
    {
      jp->initForLargeSideFeed();
      largeReader();

      jp->initForProcessing();

      // Collect all join partitions.
      std::vector<JoinPartition*> joinPartitions;
      jp->collectJoinPartitions(joinPartitions);

      // Split partitions for each threads.
      JoinPartitionJobs joinPartitionsJobs;
      prepareJobs(joinPartitions, joinPartitionsJobs);

      // Initialize data lists.
      const uint32_t numOfThreads = joinPartitionsJobs.size();
      initializeFIFO(numOfThreads);

      // Spawn jobs.
      const uint32_t smallSideSizeLimitPerThread = partitionSize / numOfThreads;
      spawnJobs(joinPartitionsJobs, smallSideSizeLimitPerThread);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_ALWAYS_CRITICAL,
                    "DiskJoinStep::mainRunner()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
    abort();
  }

  // make sure all inputs were drained & output closed
  if (UNLIKELY(cancelled()))
  {
    try
    {
      jp->initForLargeSideFeed();
    }
    catch (...)
    {
    }  // doesn't matter if this fails to open the large-file

    largeReader();  // large reader will only drain the fifo when cancelled()

    if (!closedOutput)
    {
      outputDL->endOfInput();
      closedOutput = true;
    }
  }

  if (LIKELY(!closedOutput))
  {
    outputDL->endOfInput();
    closedOutput = true;
  }
}

const string DiskJoinStep::toString() const
{
  return "DiskJoinStep\n";
}

void DiskJoinStep::reportStats()
{
  ostringstream os1, os2;

  os1 << "DiskJoinStep: joined (large) " << alias() << " to (small) " << joiner->getTableName()
      << ". Processing stages: " << largeIterationCount
      << ", disk usage small/large: " << jp->getMaxSmallSize() << "/" << jp->getMaxLargeSize()
      << ", total bytes read/written: " << jp->getBytesRead() << "/" << jp->getBytesWritten() << endl;
  fExtendedInfo = os1.str();

  /* TODO: Can this report anything more useful in miniInfo? */
  int64_t bytesToReport = jp->getBytesRead() + jp->getBytesWritten();
  char units;

  if (bytesToReport > (1 << 30))
  {
    bytesToReport >>= 30;
    units = 'G';
  }
  else if (bytesToReport > (1 << 20))
  {
    bytesToReport >>= 20;
    units = 'M';
  }
  else if (bytesToReport > (1 << 10))
  {
    bytesToReport >>= 10;
    units = 'K';
  }
  else
    units = ' ';

  os2 << "DJS UM " << alias() << "-" << joiner->getTableName() << " - - " << bytesToReport << units
      << " - - -------- -\n";

  fMiniInfo = os2.str();

  if (traceOn())
    logEnd(os1.str().c_str());
}

}  // namespace joblist
