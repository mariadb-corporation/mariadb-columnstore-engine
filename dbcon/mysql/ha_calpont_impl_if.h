/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporaton

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

// $Id: ha_calpont_impl_if.h 9701 2013-07-17 18:51:55Z zzhu $
/** @file */
#ifndef HA_CALPONT_IMPL_IF_H__
#define HA_CALPONT_IMPL_IF_H__
#include <string>
#include <stdint.h>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <iosfwd>
#include <boost/shared_ptr.hpp>
#include <stack>
#include <vector>

#include "idb_mysql.h"

struct st_ha_create_information;

#include "configcpp.h"
#include "idberrorinfo.h"
#include "calpontselectexecutionplan.h"
#include "windowfunctioncolumn.h"
#include "pseudocolumn.h"
#include "querystats.h"
#include "sm.h"
#include "functor.h"

/** Debug macro */
#ifdef INFINIDB_DEBUG
#define IDEBUG(x) {x;}
#else
#define IDEBUG(x) {}
#endif

namespace execplan
{
	class ReturnedColumn;
	class SimpleColumn;
	class SimpleFilter;
	class AggregateColumn;
	class FunctionColumn;
	class ArithmeticColumn;
	class ParseTree;
	class ConstantColumn;
	class RowColumn;
}

namespace cal_impl_if
{
class SubQuery;
class View;

struct JoinInfo
{
	execplan::CalpontSystemCatalog::TableAliasName tn;
	uint32_t joinTimes;
	std::vector<uint32_t> IDs;
};

enum ClauseType
{
	INIT = 0,
	SELECT,
	FROM,
	WHERE,
	HAVING,
	GROUP_BY,
	ORDER_BY
};

typedef std::vector<JoinInfo> JoinInfoVec;
typedef std::map<execplan::CalpontSystemCatalog::TableAliasName, std::pair<int, TABLE_LIST*> > TableMap;


struct gp_walk_info
{
	std::vector <std::string> selectCols;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList returnedCols;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList groupByCols;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList subGroupByCols;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList orderByCols;
	execplan::CalpontSelectExecutionPlan::ColumnMap columnMap;
	// This vector temporarily hold the projection columns to be added
	// to the returnedCols vector for subquery processing. It will be appended
	// to the end of returnedCols when the processing is finished.
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList additionalRetCols;
	// the sequence # of the subselect local columns need to be set after
	// the additionRetCols are merged to the returnedCols.
	std::vector<execplan::ReturnedColumn*> localCols;
	std::stack<execplan::ReturnedColumn*> rcWorkStack;
	std::stack<execplan::ParseTree*> ptWorkStack;
	boost::shared_ptr<execplan::SimpleColumn> scsp;
	uint32_t sessionid;
	bool fatalParseError;
	std::string parseErrorText;
	// for outer join walk. the column that is not of the outerTable has the returnAll flag set.
	std::set<execplan::CalpontSystemCatalog::TableAliasName> innerTables;
	// the followinig members are used for table mode
	bool condPush;
	bool dropCond;
	std::string funcName;
	std::vector<execplan::AggregateColumn*> count_asterisk_list;
	std::vector<execplan::FunctionColumn*> no_parm_func_list;
	std::vector<execplan::ReturnedColumn*> windowFuncList;
	TableMap tableMap;
	boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
	int8_t internalDecimalScale;
	THD* thd;
	uint64_t subSelectType; // the type of sub select filter that owns the gwi
	SubQuery* subQuery;
	execplan::CalpontSelectExecutionPlan::SelectList derivedTbList;
	execplan::CalpontSelectExecutionPlan::TableList tbList;
	std::vector<execplan::CalpontSystemCatalog::TableAliasName> correlatedTbNameVec;
	ClauseType clauseType;
	execplan::CalpontSystemCatalog::TableAliasName viewName;
	bool aggOnSelect;
	bool hasWindowFunc;
	bool hasSubSelect;
	SubQuery* lastSub;
	std::vector<View*> viewList;
	std::map<std::string, execplan::ParseTree*> derivedTbFilterMap;
	uint32_t derivedTbCnt;
	std::vector<execplan::SCSEP> subselectList;

	// Kludge for Bug 750
	int32_t recursionLevel;
	int32_t recursionHWM;
	std::stack<int32_t> rcBookMarkStack;

    // Kludge for MCOL-1472
    bool inCaseStmt;

	gp_walk_info() : sessionid(0),
				   fatalParseError(false),
				   condPush(false),
				   dropCond(false),
				   internalDecimalScale(4),
				   thd(0),
				   subSelectType(uint64_t(-1)),
				   subQuery(0),
				   clauseType(INIT),
				   aggOnSelect(false),
				   hasWindowFunc(false),
				   hasSubSelect(false),
				   lastSub(0),
				   derivedTbCnt(0),
				   recursionLevel(-1),
				   recursionHWM(0),
                   inCaseStmt(false)
	{}

	~gp_walk_info() {}
};

struct cal_table_info
{
	enum RowSources { FROM_ENGINE, FROM_FILE };

	cal_table_info() : tpl_ctx(0),
						 //tpl_scan_ctx(0),
						 c(0),
						 msTablePtr(0),
						 conn_hndl(0),
						 condInfo(0),
						 moreRows(false)
	{ }
	~cal_table_info() {}
	sm::cpsm_tplh_t* tpl_ctx;
	sm::sp_cpsm_tplsch_t tpl_scan_ctx;
	unsigned c; // for debug purpose
	TABLE* msTablePtr; // no ownership
	sm::cpsm_conhdl_t* conn_hndl;
	gp_walk_info* condInfo;
	execplan::SCSEP csep;
	bool moreRows; //are there more rows to consume (b/c of limit)
};

typedef std::tr1::unordered_map<TABLE*, cal_table_info> CalTableMap;
typedef std::vector<std::string> ColValuesList;
typedef std::vector<std::string> ColNameList;
typedef std::map<uint32_t, ColValuesList> TableValuesMap;
typedef std::bitset<4096> NullValuesBitset;
struct cal_connection_info
{
	enum AlterTableState { NOT_ALTER, ALTER_SECOND_RENAME, ALTER_FIRST_RENAME };
	cal_connection_info() : cal_conn_hndl(0), 
	                        queryState(0),
	                        currentTable(0),
	                        traceFlags(0),
	                        alterTableState(NOT_ALTER),
	                        isAlter(false),
	                        bulkInsertRows(0),
	                        singleInsert(true),
	                        isLoaddataInfile( false ),
	                        dmlProc(0),
	                        rowsHaveInserted(0),
	                        rc(0),
	                        tableOid(0), 
	                        localPm(-1),
	                        isSlaveNode(false),
	                        expressionId(0),
	                        mysqld_pid(getpid()),
	                        cpimport_pid(0),
	                        filePtr(0),
	                        headerLength(0),
	                        useXbit(false),
	                        utf8(false),
	                        useCpimport(1),
	                        delimiter('\7')
	{
		// check if this is a slave mysql daemon
		isSlaveNode = checkSlave();
	}

	static bool checkSlave()
	{
		config::Config* cf = config::Config::makeConfig();
		std::string configVal = cf->getConfig("Installation", "MySQLRep");
		bool isMysqlRep = (configVal == "y" || configVal == "Y");
		if (!isMysqlRep) return false;
		configVal = cf->getConfig("SystemConfig", "PrimaryUMModuleName");
		std::string module = execplan::ClientRotator::getModule();
		if (boost::iequals(configVal, module))
			return false;
		return true;
	}

	sm::cpsm_conhdl_t* cal_conn_hndl;
	int queryState;
	CalTableMap tableMap;
	sm::tableid_t currentTable;
	uint32_t traceFlags;
	std::string queryStats;
	AlterTableState alterTableState;
	bool isAlter;
	ha_rows  bulkInsertRows;
	bool singleInsert;
	bool isLoaddataInfile;
	std::string extendedStats;
	std::string miniStats;
	messageqcpp::MessageQueueClient* dmlProc;
	ha_rows rowsHaveInserted;
	ColNameList colNameList;
	TableValuesMap tableValuesMap;
  	NullValuesBitset nullValuesBitset;
	int rc;
	uint32_t tableOid;
	querystats::QueryStats stats;
	std::string warningMsg;
	int64_t localPm;
	bool isSlaveNode;
	uint32_t expressionId; // for F&E
	pid_t mysqld_pid;
	pid_t cpimport_pid;
	int fdt[2];
#ifdef _MSC_VER
    // Used for launching cpimport for Load Data Infile
	HANDLE cpimport_stdin_Rd;
	HANDLE cpimport_stdin_Wr;
	HANDLE cpimport_stdout_Rd;
	HANDLE cpimport_stdout_Wr;
	PROCESS_INFORMATION cpimportProcInfo;
#endif
	FILE * filePtr;
	uint8_t headerLength;
	bool useXbit;
	bool utf8;
	uint8_t useCpimport;
	char delimiter;
	char enclosed_by;
	std::vector <execplan::CalpontSystemCatalog::ColType> columnTypes; 
};

typedef std::tr1::unordered_map<int, cal_connection_info> CalConnMap;

const std::string infinidb_err_msg = "\nThe query includes syntax that is not supported by MariaDB Columnstore. Use 'show warnings;' to get more information. Review the MariaDB Columnstore Syntax guide for additional information on supported distributed syntax or consider changing the MariaDB Columnstore Operating Mode (infinidb_vtable_mode).";

int cp_get_plan(THD* thd, execplan::SCSEP& csep);
int cp_get_table_plan(THD* thd, execplan::SCSEP& csep, cal_impl_if::cal_table_info& ti);
int getSelectPlan(gp_walk_info& gwi, SELECT_LEX& select_lex, execplan::SCSEP& csep, bool isUnion=false);
void setError(THD* thd, uint32_t errcode, const std::string errmsg, gp_walk_info* gwi);
void setError(THD* thd, uint32_t errcode, const std::string errmsg);
void gp_walk(const Item *item, void *arg);
void parse_item (Item *item, std::vector<Item_field*>& field_vec, bool& hasNonSupportItem, uint16& parseInfo);
execplan::ReturnedColumn* buildReturnedColumn(Item* item, gp_walk_info& gwi, bool& nonSupport);
const std::string bestTableName(const Item_field* ifp);
bool isInfiniDB(TABLE* table_ptr);

// execution plan util functions prototypes
execplan::ReturnedColumn* buildReturnedColumn(Item* item, gp_walk_info& gwi, bool& nonSupport);
execplan::ReturnedColumn* buildFunctionColumn(Item_func* item, gp_walk_info& gwi, bool& nonSupport);
execplan::ArithmeticColumn* buildArithmeticColumn(Item_func* item, gp_walk_info &gwi, bool& nonSupport);
execplan::ConstantColumn* buildDecimalColumn(Item *item, gp_walk_info &gwi);
execplan::SimpleColumn* buildSimpleColumn(Item_field* item, gp_walk_info& gwi);
execplan::FunctionColumn* buildCaseFunction(Item_func* item, gp_walk_info& gwi, bool& nonSupport);
execplan::ParseTree* buildParseTree(Item_func* item, gp_walk_info& gwi, bool& nonSupport);
execplan::ReturnedColumn* buildAggregateColumn(Item* item, gp_walk_info& gwi);
execplan::ReturnedColumn* buildWindowFunctionColumn(Item* item, gp_walk_info& gwi, bool& nonSupport);
execplan::ReturnedColumn* buildPseudoColumn(Item* item, gp_walk_info& gwi, bool& nonSupport, uint32_t pseudoType);
void addIntervalArgs(Item_func* ifp, funcexp::FunctionParm& functionParms);
void castCharArgs(Item_func* ifp, funcexp::FunctionParm& functionParms);
void castDecimalArgs(Item_func* ifp, funcexp::FunctionParm& functionParms);
void castTypeArgs(Item_func* ifp, funcexp::FunctionParm& functionParms);
void parse_item (Item *item, std::vector<Item_field*>& field_vec, bool& hasNonSupportItem, uint16& parseInfo);
bool isPredicateFunction(Item* item, gp_walk_info* gwip);
execplan::ParseTree* buildRowPredicate(execplan::RowColumn* lhs, execplan::RowColumn* rhs, std::string predicateOp);
bool buildRowColumnFilter(gp_walk_info* gwip, execplan::RowColumn* rhs, execplan::RowColumn* lhs, Item_func* ifp);
bool buildPredicateItem(Item_func* ifp, gp_walk_info* gwip);
void collectAllCols(gp_walk_info& gwi, Item_field* ifp);
void buildSubselectFunc(Item_func* ifp, gp_walk_info* gwip);
uint32_t buildOuterJoin(gp_walk_info& gwi, SELECT_LEX& select_lex);
std::string getViewName(TABLE_LIST* table_ptr);
bool buildConstPredicate(Item_func* ifp, execplan::ReturnedColumn* rhs, gp_walk_info* gwip);
execplan::CalpontSystemCatalog::ColType fieldType_MysqlToIDB (const Field* field);
execplan::CalpontSystemCatalog::ColType colType_MysqlToIDB (const Item* item);
execplan::SPTP getIntervalType(int interval_type);
uint32_t isPseudoColumn(std::string funcName);
void setDerivedTable(execplan::ParseTree* n);
execplan::ParseTree* setDerivedFilter(execplan::ParseTree*& n, 
									  std::map<std::string, execplan::ParseTree*>& obj,
									  execplan::CalpontSelectExecutionPlan::SelectList& derivedTbList);
void derivedTableOptimization(execplan::SCSEP& csep);

#ifdef DEBUG_WALK_COND
void debug_walk(const Item *item, void *arg);
#endif
}

#endif

