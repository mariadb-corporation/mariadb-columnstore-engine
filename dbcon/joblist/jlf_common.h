/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

//  $Id: jlf_common.h 9702 2013-07-17 19:08:07Z xlou $

/** @file jlf_common.h
 *
 */
#ifndef JLF_COMMON_H__
#define JLF_COMMON_H__

#include <map>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/uuid/uuid.hpp>

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn.h"

#include "dbrm.h"

#include "joblist.h"
#include "jobstep.h"
#include "groupconcat.h"
#include "jl_logger.h"

#include "resourcemanager.h"
#include "rowgroup.h"

// forward reference
namespace execplan
{
class AggregateColumn;
class SimpleColumn;
}  // namespace execplan

namespace joblist
{
// for output error messages to screen.
const std::string boldStart = "\033[0;1m";
const std::string boldStop = "\033[0;39m";

const int8_t CONST_COL_NONE = 0;
const int8_t CONST_COL_EXIST = 1;
const int8_t CONST_COL_ONLY = 2;

// pretend all expressions belong to "virtual" table EXPRESSION, (CNX_EXP_TABLE_ID, expression)
// CNX_EXP_TABLE_ID(999) is not for user table or column, there will be no confilict in queries.
const int32_t CNX_EXP_TABLE_ID = 999;

struct TupleInfo
{
  TupleInfo(uint32_t w = 0, uint32_t o = 0, uint32_t k = -1, uint32_t t = -1, uint32_t s = 0, uint32_t p = 0,
            execplan::CalpontSystemCatalog::ColDataType dt = execplan::CalpontSystemCatalog::BIT,
            uint32_t csn = 8)
   : width(w), oid(o), key(k), tkey(t), scale(s), precision(p), dtype(dt), csNum(csn)
  {
  }
  ~TupleInfo()
  {
  }

  uint32_t width;
  uint32_t oid;
  uint32_t key;
  uint32_t tkey;
  uint32_t scale;
  uint32_t precision;
  execplan::CalpontSystemCatalog::ColDataType dtype;
  uint32_t csNum;  // For collations
};

// for compound join
struct JoinData
{
  int64_t fJoinId;
  std::vector<uint32_t> fLeftKeys;
  std::vector<uint32_t> fRightKeys;
  std::vector<JoinType> fTypes;  // joblisttypes.h: INNER, LEFTOUTER, RIGHTOUTER
  bool fTypeless;

  JoinData() : fJoinId(-1), fTypeless(false)
  {
  }
};

typedef std::stack<JobStepVector> JobStepVectorStack;
typedef std::map<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::OID> DictOidToColOidMap;
typedef std::vector<TupleInfo> TupleInfoVector;
typedef std::map<uint32_t, TupleInfo> TupleInfoMap;

// for subquery support
struct UniqId
{
  int fId;              // OID for real table, sequence # for subquery
                        //	std::string fName;   // name (table alias + [column name, if column])
  std::string fTable;   // table name (table alias)
  std::string fSchema;  // schema name
  std::string fView;    // view name
  uint32_t fPseudo;     // pseudo type
                        //	uint64_t	fEngine; // InfiniDB == 0
  uint64_t fSubId;      // subquery ID

  UniqId() : fId(-1), fSubId(-1)
  {
  }
  UniqId(int i, const std::string& t, const std::string& s, const std::string& v, uint32_t pi = 0,
         uint64_t l = -1)
   : fId(i), fTable(t), fSchema(s), fView(v), fPseudo(pi), fSubId(l)
  {
  }
  UniqId(const execplan::SimpleColumn* sc);
  UniqId(int o, const execplan::SimpleColumn* sc);

  std::string toString() const;
};
bool operator<(const struct UniqId& x, const struct UniqId& y);
bool operator==(const struct UniqId& x, const struct UniqId& y);
typedef std::map<UniqId, uint32_t> TupleKeyMap;

// typedef vector<SRCP> RetColsVector;
typedef execplan::CalpontSelectExecutionPlan::ReturnedColumnList RetColsVector;

// join data between table pairs
typedef std::map<std::pair<uint32_t, uint32_t>, JoinData> TableJoinMap;

struct TupleKeyInfo
{
  uint32_t nextKey;
  TupleKeyMap tupleKeyMap;
  std::vector<UniqId> tupleKeyVec;
  std::vector<std::string> tupleKeyToName;
  std::vector<bool> crossEngine;

  // TODO: better organize these structs
  std::map<uint32_t, execplan::CalpontSystemCatalog::OID> tupleKeyToTableOid;
  std::map<uint32_t, execplan::CalpontSystemCatalog::ColType> colType;
  std::map<uint32_t, execplan::CalpontSystemCatalog::ColType> token2DictTypeMap;
  std::map<uint32_t, std::string> keyName;
  std::map<uint32_t, uint32_t> colKeyToTblKey;
  std::map<uint32_t, uint32_t> dictKeyMap;  // map token key to dictionary key
  DictOidToColOidMap dictOidToColOid;       // map dictionary OID to column OID
  std::map<uint32_t, uint32_t> pseudoType;  // key to pseudo column type
  std::set<uint32_t> functionJoinKeys;      // key used in function join
  TupleInfoMap tupleInfoMap;

  TupleKeyInfo() : nextKey(0)
  {
  }
};

//------------------------------------------------------------------------------
/** @brief This struct maintains state for the query processing
 *
 */
//------------------------------------------------------------------------------
struct JobInfo
{
  JobInfo(ResourceManager* r)
   : rm(r)
   , sessionId(0)
   , txnId(0)
   , statementId(0)
   , maxBuckets(rm->getHjMaxBuckets())
   , maxElems(rm->getHjMaxElems())
   , flushInterval(rm->getJLFlushInterval())
   , fifoSize(rm->getJlFifoSize())
   , logger(new Logger())
   , traceFlags(0)
   , projectingTableOID(0)
   , isExeMgr(false)
   , trace(false)
   , tryTuples(false)
   , constantCol(CONST_COL_NONE)
   , hasDistinct(false)
   , hasAggregation(false)
   , limitStart(0)
   , limitCount(-1)
   , joinNum(0)
   , subLevel(0)
   , subNum(0)
   , subId(0)
   , pJobInfo(NULL)
   , constantFalse(false)
   , cntStarPos(-1)
   , stringScanThreshold(1)
   , wfqLimitStart(0)
   , wfqLimitCount(-1)
  {
  }
  ResourceManager* rm;
  uint32_t sessionId;
  uint32_t txnId;
  BRM::QueryContext verId;
  uint32_t statementId;
  std::string queryType;
  boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
  int maxBuckets;
  uint64_t maxElems;
  JobStepVectorStack stack;
  uint32_t flushInterval;
  uint32_t fifoSize;
  //...joblist does not use scanLbidReqLimit and SdanLbidReqThreshold.
  //...They are actually used by pcolscan and pdictionaryscan, but
  //...we have joblist get and report the values here since they
  //...are global to the job.
  SPJL logger;
  uint32_t traceFlags;
  SErrorInfo errorInfo;
  execplan::CalpontSystemCatalog::OID* projectingTableOID;  // DeliveryWSDLs get a reference to this
  bool isExeMgr;
  bool trace;
  bool tryTuples;
  int8_t constantCol;
  TupleInfoVector pjColList;

  // aggregation
  bool hasDistinct;
  bool hasAggregation;
  std::vector<uint32_t> groupByColVec;
  std::vector<uint32_t> distinctColVec;
  std::vector<uint32_t> expressionVec;
  std::vector<std::pair<uint32_t, int> > returnedColVec;

  // order by and limit
  std::vector<std::pair<uint32_t, bool> > orderByColVec;
  uint64_t limitStart;
  uint64_t limitCount;
  uint32_t orderByThreads;

  // tupleInfo
  boost::shared_ptr<TupleKeyInfo> keyInfo;

  // skip dictionary step if the real string is not necessary to projected.
  // In most case, the string is used for return or comparison, so default is false.
  //     when setting to false, no need to check: false overwrites true;
  //     When setting to true, need check: true cannot overwrite false.
  std::map<uint32_t, bool> tokenOnly;

  // unique ID list of the tables in from clause
  std::vector<uint32_t> tableList;

  // table join map
  TableJoinMap tableJoinMap;

  // for expression
  JobStepVector crossTableExpressions;
  JobStepVector returnedExpressions;

  // @bug3683, function join
  std::vector<JobStep*> functionJoins;  // store expressions can be converted to joins

  // for function on aggregation
  RetColsVector deliveredCols;    // columns to be sent to connector
  RetColsVector nonConstCols;     // none constant columns
  RetColsVector nonConstDelCols;  // delivered none constant columns
  RetColsVector projectionCols;   // columns for projection
  std::multimap<execplan::ReturnedColumn*, execplan::ReturnedColumn*> cloneAggregateColMap;
  std::vector<std::pair<int, int> > aggEidIndexList;

  // table pairs with incompatible join which is treated as expression
  std::map<uint32_t, uint32_t> incompatibleJoinMap;

  // bug 1573 & 3391, having
  SJSTEP havingStep;
  JobStepVector havingStepVec;

  // bug 2634, 5311 and 5374, outjoin and predicates
  std::set<uint32_t> outerOnTable;
  std::set<uint32_t> tableHasIsNull;
  JobStepVector outerJoinExpressions;

  // bug 3759, join in order
  // mixed outer join
  std::map<int, uint64_t> tableSize;
  int64_t joinNum;

  // for subquery
  boost::shared_ptr<int> subCount;  // # of subqueries in the query statement
  int subLevel;                     // subquery level
  int subNum;                       // # of subqueries @ level n
  int subId;                        // id of current subquery
  JobInfo* pJobInfo;                // jobinfo of outer query
  bool constantFalse;               // has constant false filter
  std::string subAlias;             // unique alias to identify the subquery
  JobStepVector correlateSteps;
  JobStepVector selectAndFromSubs;
  std::set<uint64_t> returnColSet;
  std::map<UniqId, execplan::CalpontSystemCatalog::ColType> vtableColTypes;

  // step to process orderby, limit and fill in constants
  SJSTEP annexStep;

  // @bug3475, aggregate constant column <position, aggregate column>
  std::map<uint64_t, execplan::SRCP> constAggregate;
  int64_t cntStarPos;  // position of count(*)

  // @bug3321, dictionary scan setting, HWM = stringScanThreshold -1
  uint64_t stringScanThreshold;

  // @bug3362, group_concat
  RetColsVector groupConcatCols;
  GroupConcatInfo groupConcatInfo;

  // @bug3736, column map
  std::map<uint32_t, std::vector<uint32_t> > columnMap;

  // @bug3438, joblist for trace/stats
  JobList* jobListPtr;  // just reference, NOT delete by JobInfo

  // WORKAROUND for join FE limitation (join Id to expression tables map)
  std::map<uint32_t, std::set<uint32_t> > joinFeTableMap;

  uint32_t stringTableThreshold;

  // @bug4531, Window Function support
  RetColsVector windowCols;
  RetColsVector windowExps;
  RetColsVector windowDels;
  std::set<uint64_t> windowSet;
  RetColsVector wfqOrderby;
  uint64_t wfqLimitStart;
  uint64_t wfqLimitCount;
  // workaround for expression of windowfunction in IN/EXISTS sub-query
  // std::map<uint32_t, RetColsVector>  exprWinfuncListMap;

  // Flag to tell us we are in local PM only query mode
  uint32_t localQuery;

  boost::uuids::uuid uuid;

  // @bug4021, column map for all pseudo column queries
  std::map<uint64_t, execplan::SRCP> tableColMap;
  std::set<uint64_t> pseudoColTable;

  /* Disk-based join vars */
  boost::shared_ptr<int64_t> smallSideUsage;
  boost::shared_ptr<int64_t> umMemLimit;
  int64_t smallSideLimit;  // need to get these from a session var in execplan
  int64_t largeSideLimit;
  uint64_t partitionSize;
  bool isDML;
  long timeZone;

  // This is for tracking any dynamically allocated ParseTree objects
  // in simpleScalarFilterToParseTree() for later deletion in
  // ~csep() or csep.unserialize()
  std::vector<execplan::ParseTree*> dynamicParseTreeVec;

 private:
  // defaults okay
  // JobInfo(const JobInfo& rhs);
  // JobInfo& operator=(const JobInfo& rhs);
};

//------------------------------------------------------------------------------
// namespace scoped functions
//------------------------------------------------------------------------------

/** @brief Returns the table alias for the specified column
 *
 */
std::string extractTableAlias(const execplan::SimpleColumn* sc);

/** @brief Returns the table alias for the specified column
 *
 */
std::string extractTableAlias(const execplan::SSC& sc);

/** @brief Returns OID associated with colType if it is a dictionary column
 *
 */
execplan::CalpontSystemCatalog::OID isDictCol(const execplan::CalpontSystemCatalog::ColType& colType);

/** @brief Determines if colType is a character column
 *
 */
bool isCharCol(const execplan::CalpontSystemCatalog::ColType& colType);

/** @brief Returns OID associated with a table
 *
 */
execplan::CalpontSystemCatalog::OID tableOid(const execplan::SimpleColumn* sc,
                                             boost::shared_ptr<execplan::CalpontSystemCatalog> cat);

/** @brief Returns the unique ID to be used in tupleInfo
 *
 */
uint32_t getTupleKey(JobInfo& jobInfo, const execplan::SimpleColumn* sc, bool add = false);
uint32_t getTableKey(const JobInfo& jobInfo, execplan::CalpontSystemCatalog::OID tableOid,
                     const std::string& alias, const std::string& schema, const std::string& view);
uint32_t getTupleKey(JobInfo& jobInfo, const execplan::SRCP& srcp, bool add = false);
uint32_t getTableKey(const JobInfo& jobInfo, uint32_t cid);
uint32_t getTableKey(JobInfo& jobInfo, JobStep* js);

void updateTableKey(uint32_t cid, uint32_t tid, JobInfo& jobInfo);

uint32_t getExpTupleKey(const JobInfo& jobInfo, uint64_t eid, bool cr = false);

uint32_t makeTableKey(JobInfo& jobInfo, const execplan::SimpleColumn* sc);
uint32_t makeTableKey(JobInfo& jobInfo, execplan::CalpontSystemCatalog::OID tableOid,
                      const std::string& tbl_name, const std::string& tbl_alias, const std::string& sch_name,
                      const std::string& vw_name, uint64_t engine = 0);

/** @brief Returns the tupleInfo associate with the (table, column) key pair
 *
 */
TupleInfo getTupleInfo(uint32_t columnKey, const JobInfo& jobInfo);

/** @brief set tuple info for simple column
 *
 */
TupleInfo setTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct,
                       execplan::CalpontSystemCatalog::OID col_oid, JobInfo& jobInfo,
                       execplan::CalpontSystemCatalog::OID tbl_oid, const execplan::SimpleColumn* sc,
                       const std::string& alias);

/** @brief set tuple info for expressions
 *
 */
TupleInfo setExpTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct, uint64_t expressionId,
                          const std::string& alias, JobInfo& jobInfo, bool rc = false);

TupleInfo setExpTupleInfo(const execplan::ReturnedColumn* rc, JobInfo& jobInfo);

/** @brief add an aggregate column info
 *
 */
void addAggregateColumn(execplan::ReturnedColumn*, int, RetColsVector&, JobInfo&);

void makeJobSteps(execplan::CalpontSelectExecutionPlan* csep, JobInfo& jobInfo, JobStepVector& querySteps,
                  JobStepVector& projectSteps, DeliveredTableMap& deliverySteps);

void makeUnionJobSteps(execplan::CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
                       JobStepVector& querySteps, JobStepVector&, DeliveredTableMap& deliverySteps);

void updateDerivedColumn(JobInfo&, execplan::SimpleColumn*, execplan::CalpontSystemCatalog::ColType&);

bool filterWithDictionary(execplan::CalpontSystemCatalog::OID dictOid, uint64_t n);

bool compatibleColumnTypes(const execplan::CalpontSystemCatalog::ColType& ct1,
                           const execplan::CalpontSystemCatalog::ColType& ct2, bool forJoin = true);
bool compatibleColumnTypes(const execplan::CalpontSystemCatalog::ColDataType& dt1, uint32_t scale1,
                           const execplan::CalpontSystemCatalog::ColDataType& dt2, uint32_t scale2,
                           bool forJoin = true);

}  // namespace joblist

#endif
