/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation.

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

#include "jobstep.h"
#include "tuplehashjoin.h"
#include "joinpartition.h"
#include "threadnaming.h"
#include <mutex>

#pragma once

namespace joblist
{
using JoinPartitionJobs = std::vector<std::vector<joiner::JoinPartition*>>;

class DiskJoinStep : public JobStep
{
 public:
  DiskJoinStep();
  DiskJoinStep(TupleHashJoinStep*, int djsIndex, int joinerIndex, bool lastOne);
  ~DiskJoinStep() override;

  void run() override;
  void join() override;
  const std::string toString() const override;

  void loadExistingData(std::vector<rowgroup::RGData>& data);
  uint32_t getIterationCount()
  {
    return largeIterationCount;
  }

 protected:
 private:
  void initializeFIFO(uint32_t threadCount);
  void processJoinPartitions(const uint32_t threadID, const uint32_t smallSideSizeLimitPerThread,
                             const std::vector<joiner::JoinPartition*>& joinPartitions);
  void prepareJobs(const std::vector<joiner::JoinPartition*>& joinPartitions,
                   JoinPartitionJobs& joinPartitionsJobs);
  void outputResult(const std::vector<rowgroup::RGData>& result);
  void outputResult(const rowgroup::RGData& result);
  void spawnJobs(const std::vector<std::vector<joiner::JoinPartition*>>& joinPartitionsJobs,
                 const uint32_t smallSideSizeLimitPerThread);
  boost::shared_ptr<joiner::JoinPartition> jp;
  rowgroup::RowGroup largeRG, smallRG, outputRG, joinFERG;
  std::vector<uint32_t> largeKeyCols, smallKeyCols;
  boost::shared_ptr<RowGroupDL> largeDL, outputDL;
  RowGroupDL* smallDL;

  std::shared_ptr<int[]> LOMapping, SOMapping, SjoinFEMapping, LjoinFEMapping;
  TupleHashJoinStep* thjs;
  boost::shared_ptr<funcexp::FuncExpWrapper> fe;
  bool typeless;
  JoinType joinType;
  std::shared_ptr<joiner::TupleJoiner> joiner;  // the same instance THJS uses

  /* main thread, started by JobStep::run() */
  void mainRunner();
  struct Runner
  {
    explicit Runner(DiskJoinStep* d) : djs(d)
    {
    }
    void operator()()
    {
      utils::setThreadName("DJSMainRunner");
      djs->mainRunner();
    }
    DiskJoinStep* djs;
  };

  void smallReader();
  void largeReader();
  int largeIt;
  bool lastLargeIteration;
  uint32_t largeIterationCount;

  uint64_t mainThread;  // thread handle from thread pool

  struct JoinPartitionsProcessor
  {
    JoinPartitionsProcessor(DiskJoinStep* djs, const uint32_t threadID, const uint32_t smallSideSizeLimit,
                            const std::vector<joiner::JoinPartition*>& joinPartitions)
     : djs(djs), threadID(threadID), smallSideSizeLimit(smallSideSizeLimit), joinPartitions(joinPartitions)
    {
    }

    void operator()()
    {
      utils::setThreadName("DJSJoinPartitionsProcessor");
      djs->processJoinPartitions(threadID, smallSideSizeLimit, joinPartitions);
    }

    DiskJoinStep* djs;
    uint32_t threadID;
    uint32_t smallSideSizeLimit;
    std::vector<joiner::JoinPartition*> joinPartitions;
  };

  /* Loader structs */
  struct LoaderOutput
  {
    std::vector<rowgroup::RGData> smallData;
    uint64_t partitionID;
    joiner::JoinPartition* jp;
  };

  using LoaderOutputFIFO = joblist::FIFO<boost::shared_ptr<LoaderOutput>>;
  std::vector<boost::shared_ptr<LoaderOutputFIFO>> loadFIFO;

  struct Loader
  {
    Loader(DiskJoinStep* d, const uint32_t threadID, const uint32_t smallSideSizeLimit,
           const std::vector<joiner::JoinPartition*>& joinPartitions)
     : djs(d), threadID(threadID), smallSideSizeLimit(smallSideSizeLimit), joinPartitions(joinPartitions)
    {
    }
    void operator()()
    {
      utils::setThreadName("DJSLoader");
      djs->loadFcn(threadID, smallSideSizeLimit, joinPartitions);
    }

    DiskJoinStep* djs;
    uint32_t threadID;
    uint32_t smallSideSizeLimit;
    std::vector<joiner::JoinPartition*> joinPartitions;
  };
  void loadFcn(const uint32_t threadID, const uint32_t smallSideSizeLimit,
               const std::vector<joiner::JoinPartition*>& joinPartitions);

  /* Builder structs */
  struct BuilderOutput
  {
    std::shared_ptr<joiner::TupleJoiner> tupleJoiner;
    std::vector<rowgroup::RGData> smallData;
    uint64_t partitionID;
    joiner::JoinPartition* jp;
  };

  using BuilderOutputFIFO = joblist::FIFO<boost::shared_ptr<BuilderOutput>>;
  std::vector<boost::shared_ptr<BuilderOutputFIFO>> buildFIFO;

  struct Builder
  {
    Builder(DiskJoinStep* d, const uint32_t threadID) : djs(d), threadID(threadID)
    {
    }
    void operator()()
    {
      utils::setThreadName("DJSBuilder");
      djs->buildFcn(threadID);
    }
    DiskJoinStep* djs;
    uint32_t threadID;
  };
  void buildFcn(const uint32_t threadID);

  /* Joining structs */
  struct Joiner
  {
    Joiner(DiskJoinStep* d, const uint32_t threadID) : djs(d), threadID(threadID)
    {
    }
    void operator()()
    {
      utils::setThreadName("DJSJoiner");
      djs->joinFcn(threadID);
    }
    DiskJoinStep* djs;
    uint32_t threadID;
  };
  void joinFcn(const uint32_t threadID);

  // limits & usage
  boost::shared_ptr<int64_t> smallUsage;
  int64_t smallLimit;
  int64_t largeLimit;
  uint64_t partitionSize;
  uint32_t maxPartitionTreeDepth;

  void reportStats();

  uint32_t joinerIndex;
  bool closedOutput;

  std::mutex outputMutex;
  const uint32_t maxNumOfJoinThreads = 32;
};

}  // namespace joblist
