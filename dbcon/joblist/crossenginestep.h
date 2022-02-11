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

//  $Id: crossenginestep.h 9620 2013-06-13 15:51:52Z pleblanc $

#ifndef JOBLIST_CROSSENGINESTEP_H
#define JOBLIST_CROSSENGINESTEP_H

#include <my_config.h>
#include <mysql.h>

#include <boost/scoped_array.hpp>

#include "jobstep.h"
#include "primitivestep.h"
#include "threadnaming.h"

// forward reference
namespace utils
{
class LibMySQL;
}

namespace execplan
{
class ParseTree;
class ReturnedColumn;
}  // namespace execplan

namespace funcexp
{
class FuncExp;
}

namespace joblist
{
/** @brief class CrossEngineStep
 *
 */
class CrossEngineStep : public BatchPrimitive, public TupleDeliveryStep
{
 public:
  /** @brief CrossEngineStep constructor
   */
  CrossEngineStep(const std::string& schema, const std::string& table, const std::string& alias,
                  const JobInfo& jobInfo);

  /** @brief CrossEngineStep destructor
   */
  ~CrossEngineStep();

  /** @brief virtual void Run method
   */
  void run();

  /** @brief virtual void join method
   */
  void join();

  /** @brief virtual string toString method
   */
  const std::string toString() const;

  // from BatchPrimitive
  bool getFeederFlag() const
  {
    return false;
  }
  uint64_t getLastTupleId() const
  {
    return 0;
  }
  uint32_t getStepCount() const
  {
    return 1;
  }
  void setBPP(JobStep* jobStep);
  void setFirstStepType(PrimitiveStepType firstStepType)
  {
  }
  void setIsProjectionOnly()
  {
  }
  void setLastTupleId(uint64_t id)
  {
  }
  void setOutputType(BPSOutputType outputType)
  {
  }
  void setProjectBPP(JobStep* jobStep1, JobStep* jobStep2);
  void setStepCount()
  {
  }
  void setSwallowRows(const bool swallowRows)
  {
  }
  void setBppStep()
  {
  }
  void dec(DistributedEngineComm* dec)
  {
  }
  const OIDVector& getProjectOids() const
  {
    return fOIDVector;
  }
  uint64_t blksSkipped() const
  {
    return 0;
  }
  bool wasStepRun() const
  {
    return fRunExecuted;
  }
  BPSOutputType getOutputType() const
  {
    return ROW_GROUP;
  }
  uint64_t getRows() const
  {
    return fRowsReturned;
  }
  const std::string& schemaName() const
  {
    return fSchema;
  }
  const std::string& tableName() const
  {
    return fTable;
  }
  const std::string& tableAlias() const
  {
    return fAlias;
  }
  void setJobInfo(const JobInfo* jobInfo)
  {
  }
  void setOutputRowGroup(const rowgroup::RowGroup&);
  const rowgroup::RowGroup& getOutputRowGroup() const;

  // from DECEventListener
  void newPMOnline(uint32_t)
  {
  }

  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;
  uint32_t nextBand(messageqcpp::ByteStream& bs);

  void addFcnJoinExp(const std::vector<execplan::SRCP>&);
  void addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>&);
  void setFE1Input(const rowgroup::RowGroup&);
  void setFcnExpGroup3(const std::vector<execplan::SRCP>&);
  void setFE23Output(const rowgroup::RowGroup&);

  void addFilter(JobStep* jobStep);
  void addProject(JobStep* jobStep);

 protected:
  virtual void execute();
  virtual void getMysqldInfo(const JobInfo&);
  virtual void makeMappings();
  virtual void addFilterStr(const std::vector<const execplan::Filter*>&, const std::string&);
  virtual std::string makeQuery();
  virtual void setField(int, const char*, unsigned long, MYSQL_FIELD*, rowgroup::Row&);
  inline void addRow(rowgroup::RGData&);
  // inline  void addRow(boost::shared_array<uint8_t>&);
  template <typename T>
  T convertValueNum(const char*, const execplan::CalpontSystemCatalog::ColType&);
  virtual void formatMiniStats();
  virtual void printCalTrace();

  uint64_t fRowsRetrieved;
  uint64_t fRowsReturned;
  uint64_t fRowsPerGroup;

  // output rowgroup and row
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::RowGroup fRowGroupDelivered;
  rowgroup::RowGroup fRowGroupAdded;
  rowgroup::Row fRowDelivered;

  // for datalist
  RowGroupDL* fOutputDL;
  uint64_t fOutputIterator;

  class Runner
  {
   public:
    Runner(CrossEngineStep* step) : fStep(step)
    {
    }
    void operator()()
    {
      utils::setThreadName("CESRunner");
      fStep->execute();
    }

    CrossEngineStep* fStep;
  };

  uint64_t fRunner;  // thread pool handle
  OIDVector fOIDVector;
  bool fEndOfResult;
  bool fRunExecuted;

  // MySQL server info
  std::string fHost;
  std::string fUser;
  std::string fPasswd;
  std::string fSchema;
  std::string fTable;
  std::string fAlias;
  unsigned int fPort;

  // returned columns and primitive filters
  std::string fWhereClause;
  std::string fSelectClause;

  // Function & Expression columns
  std::vector<boost::shared_ptr<execplan::ParseTree> > fFeFilters;
  std::vector<boost::shared_ptr<execplan::ReturnedColumn> > fFeSelects;
  std::vector<boost::shared_ptr<execplan::ReturnedColumn> > fFeFcnJoin;
  std::map<uint32_t, uint32_t> fColumnMap;  // projected key position (k->p)
  uint64_t fColumnCount;
  boost::scoped_array<int> fFe1Column;
  boost::shared_array<int> fFeMapping1;
  boost::shared_array<int> fFeMapping3;
  rowgroup::RowGroup fRowGroupFe1;
  rowgroup::RowGroup fRowGroupFe3;

  funcexp::FuncExp* fFeInstance;
  utils::LibMySQL* mysql;
};

}  // namespace joblist

#endif  // JOBLIST_CROSSENGINESTEP_H

// vim:ts=4 sw=4:
