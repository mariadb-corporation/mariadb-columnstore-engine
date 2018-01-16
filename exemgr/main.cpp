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

/**********************************************************************
*   $Id: main.cpp 1000 2013-07-24 21:05:51Z pleblanc $
*
*
***********************************************************************/
/**
 * @brief execution plan manager main program
 *
 * This is the ?main? program for dealing with execution plans and
 * result sets. It sits in a loop waiting for CalpontExecutionPlan
 * from the FEP. It then passes the CalpontExecutionPlan to the
 * JobListFactory from which a JobList is obtained. This is passed
 * to to the Query Manager running in the real-time portion of the
 * EC. The ExecutionPlanManager waits until the Query Manager
 * returns a result set for the job list. These results are passed
 * into the CalpontResultFactory, which outputs a CalpontResultSet.
 * The ExecutionPlanManager passes the CalpontResultSet into the
 * VendorResultFactory which produces a result set tailored to the
 * specific DBMS front end in use. The ExecutionPlanManager then
 * sends the VendorResultSet back to the Calpont Database Connector
 * on the Front-End Processor where it is returned to the DBMS
 * front-end.
 */
#include <iostream>
#include <map>
#include <string>
#include <stdexcept>
#include <csignal>
#include <unistd.h>
#include <sys/time.h>
#ifndef _MSC_VER
#include <sys/resource.h>
#else
#include <io.h>
#include <direct.h>
#endif
//#define NDEBUG
#include <cassert>
#include <clocale>
#include <ctime>
using namespace std;

#include <boost/thread.hpp>
#include <boost/tokenizer.hpp>
#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;
#include "messagequeue.h"
#include "iosocket.h"
#include "bytestream.h"
using namespace messageqcpp;
#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn.h"
using namespace execplan;
#include "joblist.h"
#include "joblistfactory.h"
#include "distributedenginecomm.h"
#include "resourcemanager.h"
using namespace joblist;
#include "liboamcpp.h"
using namespace oam;
#include "logger.h"
#include "sqllogger.h"
#include "idberrorinfo.h"
using namespace logging;
#include "querystats.h"
using namespace querystats;
#include "MonitorProcMem.h"
#include "querytele.h"
using namespace querytele;
#include "oamcache.h"

#include "activestatementcounter.h"
#include "femsghandler.h"

#include "utils_utf8.h"
#include "boost/filesystem.hpp"

#include "threadpool.h"

namespace {

//If any flags other than the table mode flags are set, produce output to screeen
const uint32_t flagsWantOutput = (0xffffffff &
	~CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH &
	~CalpontSelectExecutionPlan::TRACE_TUPLE_OFF);

int gDebug;

const unsigned logDefaultMsg           = logging::M0000;
const unsigned logDbProfStartStatement = logging::M0028;
const unsigned logDbProfEndStatement   = logging::M0029;
const unsigned logStartSql             = logging::M0041;
const unsigned logEndSql               = logging::M0042;
const unsigned logRssTooBig            = logging::M0044;
const unsigned logDbProfQueryStats     = logging::M0047;
const unsigned logExeMgrExcpt          = logging::M0055;

logging::Logger msgLog(16);

typedef map<uint32_t, size_t> SessionMemMap_t;
SessionMemMap_t sessionMemMap; // track memory% usage during a query
mutex sessionMemMapMutex;

//...The FrontEnd may establish more than 1 connection (which results in
//   more than 1 ExeMgr thread) per session.  These threads will share
//   the same CalpontSystemCatalog object for that session.  Here, we
//   define a map to track how many threads are sharing each session, so
//   that we know when we can safely delete a CalpontSystemCatalog object
//   shared by multiple threads per session.
typedef map<uint32_t, uint32_t> ThreadCntPerSessionMap_t;
ThreadCntPerSessionMap_t        threadCntPerSessionMap;
mutex threadCntPerSessionMapMutex;

//This var is only accessed using thread-safe inc/dec calls
ActiveStatementCounter *statementsRunningCount;

DistributedEngineComm *ec;

ResourceManager *rm = ResourceManager::instance(true);

int toInt(const string& val)
{
	if (val.length() == 0) return -1;
	return static_cast<int>(Config::fromText(val));
}

const string ExeMgr("ExeMgr1");

const string prettyPrintMiniInfo(const string& in)
{
	//1. take the string and tok it by '\n'
	//2. for each part in each line calc the longest part
	//3. padding to each longest value, output a header and the lines
	typedef boost::tokenizer<boost::char_separator<char> > my_tokenizer;
	boost::char_separator<char> sep1("\n");
	my_tokenizer tok1(in, sep1);
	vector<string> lines;
	string header = "Desc Mode Table TableOID ReferencedColumns PIO LIO PBE Elapsed Rows";
	const int header_parts = 10;
	lines.push_back(header);
	for (my_tokenizer::const_iterator iter1 = tok1.begin(); iter1 != tok1.end(); ++iter1)
	{
		if (!iter1->empty())
			lines.push_back(*iter1);
	}
	vector<unsigned> lens;
	for (int i = 0; i < header_parts; i++)
		lens.push_back(0);
	vector<vector<string> > lineparts;
	vector<string>::iterator iter2;
	int j;
	for (iter2 = lines.begin(), j = 0; iter2 != lines.end(); ++iter2, j++)
	{
		boost::char_separator<char> sep2(" ");
		my_tokenizer tok2(*iter2, sep2);
		int i;
		vector<string> parts;
		my_tokenizer::iterator iter3;
		for (iter3 = tok2.begin(), i = 0; iter3 != tok2.end(); ++iter3, i++)
		{
			if (i >= header_parts) break;
			string part(*iter3);
			if (j != 0 && i == 8)
				part.resize(part.size() - 3);
			assert(i < header_parts);
			if (part.size() > lens[i])
				lens[i] = part.size();
			parts.push_back(part);
		}
		assert(i == header_parts);
		lineparts.push_back(parts);
	}

	ostringstream oss;

	vector<vector<string> >::iterator iter1 = lineparts.begin();
	vector<vector<string> >::iterator end1 = lineparts.end();

	oss << "\n";
	while (iter1 != end1)
	{
		vector<string>::iterator iter2 = iter1->begin();
		vector<string>::iterator end2 = iter1->end();
		assert(distance(iter2, end2) == header_parts);
		int i = 0;
		while (iter2 != end2)
		{
			assert(i < header_parts);
			oss << setw(lens[i]) << left << *iter2 << " ";
			++iter2;
			i++;
		}
		oss << "\n";
		++iter1;
	}

	return oss.str();
}

const string timeNow()
{
	time_t outputTime = time(0);
	struct tm ltm;
	char buf[32];	//ctime(3) says at least 26
	size_t len = 0;
#ifdef _MSC_VER
	asctime_s(buf, 32, localtime_r(&outputTime, &ltm));
#else
	asctime_r(localtime_r(&outputTime, &ltm), buf);
#endif
	len = strlen(buf);
	if (len > 0) --len;
	if (buf[len] == '\n') buf[len] = 0;
	return buf;
}

QueryTeleServerParms gTeleServerParms;

class SessionThread
{
public:

	SessionThread(const IOSocket& ios, DistributedEngineComm* ec, ResourceManager* rm) :
		fIos(ios), fEc(ec),
		fRm(rm),
		fStatsRetrieved(false),
		fTeleClient(gTeleServerParms),
		fOamCachePtr(oam::OamCache::makeOamCache())
	{
	}

private:

	IOSocket fIos;
	DistributedEngineComm *fEc;
	ResourceManager*  fRm;
	querystats::QueryStats fStats;

	// Variables used to store return stats
	bool       fStatsRetrieved;

	QueryTeleClient fTeleClient;

	oam::OamCache* fOamCachePtr;	//this ptr is copyable...

	//...Reinitialize stats for start of a new query
	void initStats ( uint32_t sessionId, string& sqlText )
	{
		initMaxMemPct ( sessionId );

		fStats.reset();
		fStats.setStartTime();
		fStats.fSessionID = sessionId;
		fStats.fQuery = sqlText;
		fStatsRetrieved  = false;
	}

	//...Get % memory usage during latest query for sesssionId.
	//...SessionId >= 0x80000000 is system catalog query we can ignore.
	static uint64_t getMaxMemPct ( uint32_t sessionId )
	{
		uint64_t maxMemoryPct = 0;

		if ( sessionId < 0x80000000 )
		{
			mutex::scoped_lock lk(sessionMemMapMutex);
			SessionMemMap_t::iterator mapIter =
				sessionMemMap.find( sessionId );
			if ( mapIter != sessionMemMap.end() )
			{
				maxMemoryPct = (uint64_t)mapIter->second;
			}
		}

		return maxMemoryPct;
	}

	//...Delete sessionMemMap entry for the specified session's memory % use.
	//...SessionId >= 0x80000000 is system catalog query we can ignore.
	static void deleteMaxMemPct ( uint32_t sessionId )
	{
		if ( sessionId < 0x80000000 )
		{
			mutex::scoped_lock lk(sessionMemMapMutex);
			SessionMemMap_t::iterator mapIter =
				sessionMemMap.find( sessionId );
			if ( mapIter != sessionMemMap.end() )
			{
				sessionMemMap.erase( sessionId );
			}
		}
	}

	//...Get and log query stats to specified output stream
	const string formatQueryStats (
		SJLP&    jl,			// joblist associated with query
		const    string& label, // header label to print in front of log output
		bool     includeNewLine,//include line breaks in query stats string
		bool     vtableModeOn,
		bool     wantExtendedStats,
		uint64_t rowsReturned)
	{
		ostringstream os;

		// Get stats if not already acquired for current query
		if ( !fStatsRetrieved )
		{
			if (wantExtendedStats)
			{
				//wait for the ei data to be written by another thread (brain-dead)
				struct timespec req = { 0, 250000 }; //250 usec
#ifdef _MSC_VER
				Sleep(20); //20ms on Windows
#else
				nanosleep(&req, 0);
#endif
			}
			// Get % memory usage during current query for sessionId
			jl->querySummary( wantExtendedStats );
			fStats = jl->queryStats();
			fStats.fMaxMemPct = getMaxMemPct( fStats.fSessionID );
			fStats.fRows = rowsReturned;
			fStatsRetrieved = true;
		}

		string queryMode;
		queryMode = (vtableModeOn ? "Distributed" : "Standard");

		// Log stats to specified output stream
		os << label <<
			": MaxMemPct-"     << fStats.fMaxMemPct <<
			"; NumTempFiles-"  << fStats.fNumFiles  <<
			"; TempFileSpace-" << roundBytes(fStats.fFileBytes) <<
			"; ApproxPhyI/O-"  << fStats.fPhyIO     <<
			"; CacheI/O-"      << fStats.fCacheIO   <<
			"; BlocksTouched-" << fStats.fMsgRcvCnt;
		if ( includeNewLine )
			os << endl << "      ";	// insert line break
		else
			os << "; ";				// continue without line break
		os << "PartitionBlocksEliminated-" << fStats.fCPBlocksSkipped <<
			"; MsgBytesIn-"    << roundBytes(fStats.fMsgBytesIn) <<
			"; MsgBytesOut-"   << roundBytes(fStats.fMsgBytesOut) <<
			"; Mode-"          << queryMode;

		return os.str();
	}

	//...Increment the number of threads using the specified sessionId
	static void incThreadCntPerSession(uint32_t sessionId)
	{
		mutex::scoped_lock lk(threadCntPerSessionMapMutex);
		ThreadCntPerSessionMap_t::iterator mapIter =
			threadCntPerSessionMap.find(sessionId);

		if (mapIter == threadCntPerSessionMap.end())
			threadCntPerSessionMap.insert(ThreadCntPerSessionMap_t::value_type(sessionId, 1));
		else
			mapIter->second++;
	}

	//...Decrement the number of threads using the specified sessionId.
	//...When the thread count for a sessionId reaches 0, the corresponding
	//...CalpontSystemCatalog objects are deleted.
	//...The user query and its associated catalog query have a different
	//...session Id where the highest bit is flipped.
	//...The object with id(sessionId | 0x80000000) cannot be removed before
	//...user query session completes because the sysdata may be used for
	//...debugging/stats purpose, such as result graph, etc.
	static void decThreadCntPerSession(uint32_t sessionId)
	{
		mutex::scoped_lock lk(threadCntPerSessionMapMutex);
		ThreadCntPerSessionMap_t::iterator mapIter =
			threadCntPerSessionMap.find(sessionId);

		if (mapIter != threadCntPerSessionMap.end())
		{
			if (--mapIter->second == 0)
			{
				threadCntPerSessionMap.erase(mapIter);
				CalpontSystemCatalog::removeCalpontSystemCatalog(sessionId);
				CalpontSystemCatalog::removeCalpontSystemCatalog((sessionId ^ 0x80000000));
			}
		}
	}

	//...Init sessionMemMap entry for specified session to 0 memory %.
	//...SessionId >= 0x80000000 is system catalog query we can ignore.
	static void initMaxMemPct ( uint32_t sessionId )
	{
		if ( sessionId < 0x80000000 )
		{
			// cout << "Setting pct to 0 for session " << sessionId << endl;
			mutex::scoped_lock lk(sessionMemMapMutex);
			SessionMemMap_t::iterator mapIter = sessionMemMap.find( sessionId );
			if ( mapIter == sessionMemMap.end() )
			{
				sessionMemMap[sessionId] = 0;
			}
			else
			{
				mapIter->second = 0;
			}
		}
	}

	//... Round off to human readable format (KB, MB, or GB).
	const string roundBytes(uint64_t value) const
	{
		const char* units[] = {"B", "KB", "MB", "GB", "TB"};
		uint64_t i = 0, up = 0;
		uint64_t roundedValue = value;
		while (roundedValue > 1024 && i < 4)
		{
			up = (roundedValue & 512);
			roundedValue /= 1024;
			i++;
		}

		if (up)
			roundedValue++;

		ostringstream oss;
		oss << roundedValue << units[i];
		return oss.str();
	}

	//...Round off to nearest (1024*1024) MB
	uint64_t roundMB ( uint64_t value ) const
	{
		uint64_t roundedValue = value >> 20;
		if (value & 524288)
			roundedValue++;

		return roundedValue;
	}

	void setRMParms ( const CalpontSelectExecutionPlan::RMParmVec& parms )
	{
		for (CalpontSelectExecutionPlan::RMParmVec::const_iterator it = parms.begin();
				it != parms.end(); ++it)
		{
			switch (it->id)
			{
				case PMSMALLSIDEMEMORY:
				{
					fRm->addHJPmMaxSmallSideMap(it->sessionId, it->value);
					break;
				}
				case UMSMALLSIDEMEMORY:
				{
					fRm->addHJUmMaxSmallSideMap(it->sessionId, it->value);
					break;
				}
				default: ;
			}
		}
	}

	void buildSysCache(const CalpontSelectExecutionPlan& csep, boost::shared_ptr<CalpontSystemCatalog> csc)
	{
		const CalpontSelectExecutionPlan::ColumnMap& colMap = csep.columnMap();
		CalpontSelectExecutionPlan::ColumnMap::const_iterator it;
		string schemaName;
		for (it = colMap.begin(); it != colMap.end(); ++it)
		{
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>((it->second).get());
			if (sc)
			{
				schemaName = sc->schemaName();
				// only the first time a schema is got will actually query
				// system catalog. System catalog keeps a schema name map.
				// if a schema exists, the call getSchemaInfo returns without
				// doing anything.
				if (!schemaName.empty())
					csc->getSchemaInfo(schemaName);
			}
		}
		CalpontSelectExecutionPlan::SelectList::const_iterator subIt;
		for (subIt = csep.derivedTableList().begin(); subIt != csep.derivedTableList().end(); ++ subIt)
		{
			buildSysCache(*(dynamic_cast<CalpontSelectExecutionPlan*>(subIt->get())), csc);
		}
	}

public:

	void operator()()
	{
		ByteStream bs, inbs;
		CalpontSelectExecutionPlan csep;
		csep.sessionID(0);
		SJLP jl;
		bool incSessionThreadCnt = true;

		bool selfJoin = false; 
		bool tryTuples = false;
		bool usingTuples = false;
		bool stmtCounted = false;

		try
		{
			for (;;)
			{
				selfJoin = false;
				tryTuples = false;
				usingTuples = false;

				jl.reset();
				bs = fIos.read();
				if (bs.length() == 0)
				{
					if (gDebug > 1 || (gDebug && !csep.isInternal()))
						cout << "### Got a close(1) for session id " << csep.sessionID() << endl;
					// connection closed by client
					fIos.close();
					break;
				}
				else if (bs.length() < 4) //Not a CalpontSelectExecutionPlan
				{
					if (gDebug)
						cout << "### Got a not-a-plan for session id " << csep.sessionID()
							 << " with length " << bs.length() <<  endl;
					fIos.close();
					break;
				}
				else if (bs.length() == 4) //possible tuple flag
				{
					ByteStream::quadbyte qb;
					bs >> qb;
					if (qb == 4) //UM wants new tuple i/f
					{
						if (gDebug)
							cout << "### UM wants tuples" << endl;
						tryTuples = true;
						// now wait for the CSEP...
						bs = fIos.read();
					}
					else if (qb == 5) //somebody wants stats
					{
						bs.restart();
						qb = statementsRunningCount->cur();
						bs << qb;
						qb = statementsRunningCount->waiting();
						bs << qb;
						fIos.write(bs);
						fIos.close();
						break;
					}
					else
					{
						if (gDebug)
							cout << "### Got a not-a-plan value " << qb << endl;
						fIos.close();
						break;
					}
				}
new_plan:
				csep.unserialize(bs);

				QueryTeleStats qts;
				if ( !csep.isInternal() &&
					(csep.queryType() == "SELECT" || csep.queryType() == "INSERT_SELECT") )
				{
					qts.query_uuid = csep.uuid();
					qts.msg_type = QueryTeleStats::QT_START;
					qts.start_time = QueryTeleClient::timeNowms();
					qts.query = csep.data();
					qts.session_id = csep.sessionID();
					qts.query_type = csep.queryType();
					qts.system_name = fOamCachePtr->getSystemName();
					qts.module_name = fOamCachePtr->getModuleName();
					qts.local_query = csep.localQuery();
					qts.schema_name = csep.schemaName();
					fTeleClient.postQueryTele(qts);
				}

				if (gDebug > 1 || (gDebug && !csep.isInternal()))
					cout << "### For session id " << csep.sessionID() << ", got a CSEP" << endl;

				setRMParms(csep.rmParms());
				// @bug 1021. try to get schema cache for a come in query.
				// skip system catalog queries.
				if (!csep.isInternal())
				{
					boost::shared_ptr<CalpontSystemCatalog> csc =
						CalpontSystemCatalog::makeCalpontSystemCatalog(csep.sessionID());
					buildSysCache(csep, csc);
				}

				// As soon as we have a session id for this thread, update the
				// thread count per session; only do this once per thread.
				// Mask 0x80000000 is for associate user query and csc query
				if (incSessionThreadCnt)
				{
					incThreadCntPerSession( csep.sessionID() |0x80000000 );
					incSessionThreadCnt = false;
				}

				bool needDbProfEndStatementMsg = false;
				Message::Args args;
				string sqlText = csep.data();
				LoggingID li(16, csep.sessionID(), csep.txnID());

				// Initialize stats for this query, including
				// init sessionMemMap entry for this session to 0 memory %.
				// We will need this later for traceOn() or if we receive a
				// table request with qb=3 (see below). This is also recorded
				// as query start time.
				initStats( csep.sessionID(), sqlText );
				fStats.fQueryType = csep.queryType();

				// Log start and end statement if tracing is enabled.  Keep in
				// mind the trace flag won't be set for system catalog queries.
				if (csep.traceOn())
				{
					args.reset();
					args.add((int)csep.statementID());
					args.add((int)csep.verID().currentScn);
					args.add(sqlText);
					msgLog.logMessage(LOG_TYPE_DEBUG,
									  logDbProfStartStatement,
									  args,
									  li);
					needDbProfEndStatementMsg = true;
				}

				//Don't log subsequent self joins after first.
				if (selfJoin)
					sqlText = "";

				ostringstream oss;
				oss << sqlText << "; |" << csep.schemaName() <<"|";
				SQLLogger sqlLog(oss.str(), li);

				statementsRunningCount->incr(stmtCounted);

				if (tryTuples)
				{
					try // @bug2244: try/catch around fIos.write() calls responding to makeTupleList
					{
						string emsg("NOERROR");
						ByteStream emsgBs;
						ByteStream::quadbyte tflg = 0;
						jl = JobListFactory::makeJobList(
								&csep, fRm, true, true);
						// assign query stats
						jl->queryStats(fStats);

						ByteStream tbs;

						if ((jl->status()) == 0 && (jl->putEngineComm(fEc) == 0))
						{
							usingTuples = true;

							//Tell the FE that we're sending tuples back, not TableBands
							tbs << tflg;
							fIos.write(tbs);
							emsgBs.reset();
							emsgBs << emsg;
							fIos.write(emsgBs);
							TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jl.get());
							assert(tjlp);
							tbs.restart();
							tbs << tjlp->getOutputRowGroup();
							fIos.write(tbs);
						}
						else
						{
							statementsRunningCount->decr(stmtCounted);
							tflg = jl->status();
							emsg = jl->errMsg();
							tbs << tflg;
							fIos.write(tbs);
							emsgBs.reset();
							emsgBs << emsg;
							fIos.write(emsgBs);
							cerr << "ExeMgr: could not build a tuple joblist: " << emsg << endl;
							continue;
						}
					}
					catch (std::exception& ex)
					{
						ostringstream errMsg;
						errMsg << "ExeMgr: error writing makeJoblist "
							"response; " <<  ex.what();
						throw runtime_error( errMsg.str() );
					}
					catch (...)
					{
						ostringstream errMsg;
						errMsg << "ExeMgr: unknown error writing makeJoblist "
							"response; ";
						throw runtime_error( errMsg.str() );
					}

					if (!usingTuples)
					{
						if (gDebug)
							cout << "### UM wanted tuples but it didn't work out :-(" << endl;
					}
					else
					{
						if (gDebug)
							cout << "### UM wanted tuples and we'll do our best;-)" << endl;
					}
				}
				else
				{
					usingTuples = false;
					jl = JobListFactory::makeJobList(&csep, fRm, false, true);
					if (jl->status() == 0)
					{
						string emsg;
						if (jl->putEngineComm(fEc) != 0)
							throw runtime_error(jl->errMsg());
					}
					else
					{
						throw runtime_error("ExeMgr: could not build a JobList!");
					}
				}


				jl->doQuery();

				CalpontSystemCatalog::OID tableOID;
				bool swallowRows = false;
				DeliveredTableMap tm;
				uint64_t totalBytesSent = 0;
				uint64_t totalRowCount = 0;

				// Project each table as the FE asks for it
				for (;;)
				{
					bs = fIos.read();
					if (bs.length() == 0)
					{
						if (gDebug > 1 || (gDebug && !csep.isInternal()))
							cout << "### Got a close(2) for session id " << csep.sessionID() << endl;
						break;
					}

					if (gDebug && bs.length() > 4)
						cout << "### For session id " << csep.sessionID() << ", got too many bytes = " <<
							bs.length() << endl;

					//TODO: Holy crud! Can this be right?
					//@bug 1444 Yes, if there is a self-join
					if (bs.length() > 4)
					{
						selfJoin = true;
						statementsRunningCount->decr(stmtCounted);
						goto new_plan;
					}

					assert(bs.length() == 4);

					ByteStream::quadbyte qb;
					try // @bug2244: try/catch around fIos.write() calls responding to qb command
					{
						bs >> qb;
						if (gDebug > 1 || (gDebug && !csep.isInternal()))
							cout << "### For session id " << csep.sessionID() << ", got a command = " << qb << endl;
						if (qb == 0)
						{
							// No more tables, query is done
							break;
						}
						else if (qb == 1)
						{
							// super-secret flag indicating that the UM is going to scarf down all the rows in the
							//   query.
							swallowRows = true;
							tm = jl->deliveredTables();
							continue;
						}
						else if (qb == 2)
						{
							// UM just wants any table
							assert(swallowRows);
							DeliveredTableMap::iterator iter = tm.begin();
							if (iter == tm.end())
							{
								if (gDebug > 1 || (gDebug && !csep.isInternal()))
									cout << "### For session id " << csep.sessionID() << ", returning end flag" << endl;
								bs.restart();
								bs << (ByteStream::byte)1;
								fIos.write(bs);
								continue;
							}
							tableOID = iter->first;
						}
						else if (qb == 3) //special option-UM wants job stats string
						{
							string statsString;

							//Log stats string to be sent back to front end
							statsString = formatQueryStats(
								jl,
								"Query Stats",
								false,
								!(csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_TUPLE_OFF),
								(csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_LOG),
								totalRowCount
								);

							bs.restart();
							bs << statsString;
							if ((csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_LOG) != 0)
							{
								bs << jl->extendedInfo();
								bs << prettyPrintMiniInfo(jl->miniInfo());
							}
							else
							{
								string empty;
								bs << empty;
								bs << empty;
							}

							// send stats to connector for inserting to the querystats table
							fStats.serialize(bs);
							fIos.write(bs);
							continue;
						}
						// for table mode handling
						else if (qb == 4)
						{
							statementsRunningCount->decr(stmtCounted);
							bs = fIos.read();
							goto new_plan;
						}
						else // (qb > 3)
						{
							//Return table bands for the requested tableOID
							tableOID = static_cast<CalpontSystemCatalog::OID>(qb);
						}
					}
					catch (std::exception& ex)
					{
						ostringstream errMsg;
						errMsg << "ExeMgr: error writing qb response "
							"for qb cmd " << qb <<
							"; " <<  ex.what();
						throw runtime_error( errMsg.str() );
					}
					catch (...)
					{
						ostringstream errMsg;
						errMsg << "ExeMgr: unknown error writing qb response "
							"for qb cmd " << qb;
						throw runtime_error( errMsg.str() );
					}

					if (swallowRows) tm.erase(tableOID);

					FEMsgHandler msgHandler(jl, &fIos);
					if (tableOID == 100)
						msgHandler.start();

					//...Loop serializing table bands projected for the tableOID
					for (;;)
					{
						uint32_t rowCount;

						rowCount = jl->projectTable(tableOID, bs);

						msgHandler.stop();
						if (jl->status()) {
							IDBErrorInfo* errInfo = IDBErrorInfo::instance();

							if (jl->errMsg().length() != 0)
								bs << jl->errMsg();
							else
								bs << errInfo->errorMsg(jl->status());
						}

						try // @bug2244: try/catch around fIos.write() calls projecting rows
						{
							if (csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_NO_ROWS3)
							{
								// Skip the write to the front end until the last empty band.  Used to time queries
								// through without any front end waiting.
								if(tableOID < 3000 || rowCount == 0)
									fIos.write(bs);
							}
							else
							{
								fIos.write(bs);
							}
						}
						catch (std::exception& ex)
						{
							msgHandler.stop();
							ostringstream errMsg;
							errMsg << "ExeMgr: error projecting rows "
								"for tableOID: " << tableOID <<
								"; rowCnt: " << rowCount <<
								"; prevTotRowCnt: " << totalRowCount <<
								"; " << ex.what();
							jl->abort();
							while (rowCount)
								rowCount = jl->projectTable(tableOID, bs);
							if (tableOID == 100 && msgHandler.aborted()) {
								/* TODO: modularize the cleanup code, as well as
								 * the rest of this fcn */

								decThreadCntPerSession( csep.sessionID() | 0x80000000 );
								statementsRunningCount->decr(stmtCounted);
								fIos.close();
								return;
						}
							//cout << "connection drop\n";
							throw runtime_error( errMsg.str() );
						}
						catch (...)
						{
							ostringstream errMsg;
							msgHandler.stop();
							errMsg << "ExeMgr: unknown error projecting rows "
								"for tableOID: " <<
								tableOID << "; rowCnt: " << rowCount <<
								"; prevTotRowCnt: " << totalRowCount;
							jl->abort();
							while (rowCount)
								rowCount = jl->projectTable(tableOID, bs);
							throw runtime_error( errMsg.str() );
						}
						totalRowCount += rowCount;
						totalBytesSent += bs.length();

						if (rowCount == 0)
						{
							msgHandler.stop();
							// No more bands, table is done
							bs.reset();
							// @bug 2083 decr active statement count here for table mode.
							if (!usingTuples)
								statementsRunningCount->decr(stmtCounted);
							break;
						}
						else
						{
							bs.restart();
						}
					} // End of loop to project and serialize table bands for a table
				} // End of loop to process tables

				// @bug 828
				if (csep.traceOn())
					jl->graph(csep.sessionID());

				if (needDbProfEndStatementMsg)
				{
					string ss;
					ostringstream prefix;
					prefix << "ses:" << csep.sessionID() << " Query Totals";

					//Log stats string to standard out
					ss = formatQueryStats(
						jl,
						prefix.str(),
						true,
						!(csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_TUPLE_OFF),
						(csep.traceFlags() & CalpontSelectExecutionPlan::TRACE_LOG),
						totalRowCount);
					//@Bug 1306. Added timing info for real time tracking.
					cout << ss << " at " << timeNow() << endl;

					// log query status to debug log file
					args.reset();
					args.add((int)csep.statementID());
					args.add(fStats.fMaxMemPct);
					args.add(fStats.fNumFiles);
					args.add(fStats.fFileBytes); // log raw byte count instead of MB
					args.add(fStats.fPhyIO);
					args.add(fStats.fCacheIO);
					args.add(fStats.fMsgRcvCnt);
					args.add(fStats.fMsgBytesIn);
					args.add(fStats.fMsgBytesOut);
					args.add(fStats.fCPBlocksSkipped);
					msgLog.logMessage(LOG_TYPE_DEBUG,
										logDbProfQueryStats,
										args,
										li);
					//@bug 1327
					deleteMaxMemPct( csep.sessionID() );
					// Calling reset here, will cause joblist destructor to be
					// called, which "joins" the threads.  We need to do that
					// here to make sure all syslogging from all the threads
					// are complete; and that our logDbProfEndStatement will
					// appear "last" in the syslog for this SQL statement.
					jl.reset();
					args.reset();
					args.add((int)csep.statementID());
					msgLog.logMessage(LOG_TYPE_DEBUG,
									  logDbProfEndStatement,
									  args,
									  li);
				}
				else
					// delete sessionMemMap entry for this session's memory % use
					deleteMaxMemPct( csep.sessionID() );

				string endtime(timeNow());
				if ((csep.traceFlags() & flagsWantOutput) && (csep.sessionID() < 0x80000000))
				{
					cout << "For session " << csep.sessionID() << ": " <<
						totalBytesSent <<
						" bytes sent back at " << endtime << endl;

					// @bug 663 - Implemented caltraceon(16) to replace the
					// $FIFO_SINK compiler definition in pColStep.
					// This option consumes rows in the project steps.
					if (csep.traceFlags() &
						CalpontSelectExecutionPlan::TRACE_NO_ROWS4)
					{
						cout << endl;
						cout << "**** No data returned to DM.  Rows consumed "
							"in ProjectSteps - caltrace(16) is on (FIFO_SINK)."
							" ****" << endl;
						cout << endl;
					}
					else if (csep.traceFlags() &
							CalpontSelectExecutionPlan::TRACE_NO_ROWS3)
					{
						cout << endl;
						cout << "**** No data returned to DM - caltrace(8) is "
							"on (SWALLOW_ROWS_EXEMGR). ****" << endl;
						cout << endl;
					}
				}

				statementsRunningCount->decr(stmtCounted);

				if ( !csep.isInternal() &&
					(csep.queryType() == "SELECT" || csep.queryType() == "INSERT_SELECT") )
				{
					qts.msg_type = QueryTeleStats::QT_SUMMARY;
					qts.max_mem_pct = fStats.fMaxMemPct;
					qts.num_files = fStats.fNumFiles;
					qts.phy_io = fStats.fPhyIO;
					qts.cache_io = fStats.fCacheIO;
					qts.msg_rcv_cnt = fStats.fMsgRcvCnt;
					qts.cp_blocks_skipped = fStats.fCPBlocksSkipped;
					qts.msg_bytes_in = fStats.fMsgBytesIn;
					qts.msg_bytes_out = fStats.fMsgBytesOut;
					qts.rows = totalRowCount;
					qts.end_time = QueryTeleClient::timeNowms();
					qts.session_id = csep.sessionID();
					qts.query_type = csep.queryType();
					qts.query = csep.data();
					qts.system_name = fOamCachePtr->getSystemName();
					qts.module_name = fOamCachePtr->getModuleName();
					qts.local_query = csep.localQuery();
					fTeleClient.postQueryTele(qts);
				}

			}
			jl.reset();
			// Release CSC object (for sessionID) that was added by makeJobList()
			// Mask 0x80000000 is for associate user query and csc query
			decThreadCntPerSession( csep.sessionID() | 0x80000000 );

		}
		catch (std::exception& ex)
		{
			decThreadCntPerSession( csep.sessionID() | 0x80000000 );
			statementsRunningCount->decr(stmtCounted);
			cerr << "### ExeMgr ses:" << csep.sessionID() << " caught: " << ex.what() << endl;
			Message::Args args;
			LoggingID li(16, csep.sessionID(), csep.txnID());
			args.add(ex.what());
			msgLog.logMessage(LOG_TYPE_CRITICAL, logExeMgrExcpt, args, li);
			fIos.close();
		}
		catch (...)
		{
			decThreadCntPerSession( csep.sessionID() | 0x80000000 );
			statementsRunningCount->decr(stmtCounted);
			cerr << "### Exception caught!" << endl;
			Message::Args args;
			LoggingID li(16, csep.sessionID(), csep.txnID());
			args.add("ExeMgr caught unknown exception");
			msgLog.logMessage(LOG_TYPE_CRITICAL, logExeMgrExcpt, args, li);
			fIos.close();
		}
	}
};


class RssMonFcn : public utils::MonitorProcMem
{
public:
    RssMonFcn(size_t maxPct, int pauseSeconds) :
        MonitorProcMem(maxPct, 0, 21, pauseSeconds) {}

    /*virtual*/
    void operator()() const
    {
        for (;;)
        {
            size_t rssMb = rss();
            size_t pct = rssMb * 100 / fMemTotal;

            if (pct > fMaxPct)
            {
                if (fMaxPct >= 95)
                {
                    cerr << "Too much memory allocated!" << endl;
                    Message::Args args;
                    args.add((int)pct);
                    args.add((int)fMaxPct);
                    msgLog.logMessage(LOG_TYPE_CRITICAL, logRssTooBig, args, LoggingID(16));
                    exit(1);
                }
                if (statementsRunningCount->cur() == 0)
                {
                    cerr << "Too much memory allocated!" << endl;
                    Message::Args args;
                    args.add((int)pct);
                    args.add((int)fMaxPct);
                    msgLog.logMessage(LOG_TYPE_WARNING, logRssTooBig, args, LoggingID(16));
                    exit(1);
                }
                cerr << "Too much memory allocated, but stmts running" << endl;
            }

            // Update sessionMemMap entries lower than current mem % use
            mutex::scoped_lock lk(sessionMemMapMutex);
            for ( SessionMemMap_t::iterator mapIter = sessionMemMap.begin();
                mapIter != sessionMemMap.end();
                ++mapIter )
            {
                if ( pct > mapIter->second )
                {
                    mapIter->second = pct;
                }
            }
            lk.unlock();

            pause_();
        }
    }
};

void exit_(int)
{
    exit(0);
}

void added_a_pm(int)
{
    logging::LoggingID logid(21, 0, 0);
    logging::Message::Args args1;
    logging::Message msg(1);
    args1.add("exeMgr caught SIGHUP. Resetting connections");
    msg.format( args1 );
    cout << msg.msg().c_str() << endl;
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);
    if (ec) {
        //set BUSY_INIT state while processing the add pm configuration change
        Oam oam;
        try
        {
            oam.processInitComplete("ExeMgr", oam::BUSY_INIT);
        }
        catch (...)
        {}

		oam::OamCache *oamCache = oam::OamCache::makeOamCache();
		oamCache->forceReload();
        ec->Setup();

        //set ACTIVE state
        try
        {
            oam.processInitComplete("ExeMgr");
        }
        catch (...)
        {}
    }
}

void printTotalUmMemory(int sig)
{
    int64_t num = rm->availableMemory();
    cout << "Total UM memory available: " << num << endl;
}

void setupSignalHandlers()
{
#ifdef _MSC_VER
    signal(SIGSEGV, exit_);
    signal(SIGINT, exit_);
    signal(SIGTERM, exit_);
#else
    struct sigaction ign;

    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;

    sigaction(SIGPIPE, &ign, 0);

    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = added_a_pm;
    sigaction(SIGHUP, &ign, 0);
    ign.sa_handler = printTotalUmMemory;
    sigaction(SIGUSR1, &ign, 0);
#endif
}

void setupCwd(ResourceManager* rm)
{
    string workdir = rm->getScWorkingDir();
    (void)chdir(workdir.c_str());
    if (access(".", W_OK) != 0)
        (void)chdir("/tmp");
}

void startRssMon(size_t maxPct, int pauseSeconds)
{
    new boost::thread(RssMonFcn(maxPct, pauseSeconds));
}

int setupResources()
{
#ifdef _MSC_VER
    //FIXME:
#else
    struct rlimit rlim;

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        return -1;
    }

    rlim.rlim_cur = rlim.rlim_max = 65536;
    if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        return -2;
    }

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        return -3;
    }

    if (rlim.rlim_cur != 65536) {
        return -4;
    }
#endif
    return 0;
}

}

void cleanTempDir()
{
	config::Config *config = config::Config::makeConfig();
	string allowDJS = config->getConfig("HashJoin", "AllowDiskBasedJoin");
	string tmpPrefix = config->getConfig("HashJoin", "TempFilePath");

	if (allowDJS == "N" || allowDJS == "n")
		return;

	if (tmpPrefix.empty())
		tmpPrefix = "/tmp/infinidb";

	tmpPrefix += "/";

	assert(tmpPrefix != "/");

	/* This is quite scary as ExeMgr usually runs as root */
	boost::filesystem::remove_all(tmpPrefix);
	boost::filesystem::create_directories(tmpPrefix);
}


int main(int argc, char* argv[])
{
	// get and set locale language
    string systemLang = "C";
	systemLang = funcexp::utf8::idb_setlocale();

	gDebug = 0;
	bool eFlg = false;
	int c;

	opterr = 0;

	while ((c = getopt(argc, argv, "ed")) != EOF)
		switch (c)
		{
		case 'd':
			gDebug++;
			break;
		case 'e':
			eFlg = true;
			break;
		case '?':
		default:
			break;
		}

	//set BUSY_INIT state
	{
		Oam oam;
		try
		{
			oam.processInitComplete("ExeMgr", oam::BUSY_INIT);
		}
		catch (...)
		{
		}
	}

#ifdef _MSC_VER
	//FIXME:
#else
	// Make sure CSC thinks it's on a UM or else bucket reuse stuff below will stall
	if (!eFlg)
		setenv("CALPONT_CSC_IDENT", "um", 1);
#endif
	setupSignalHandlers();
    int err = setupResources();
    string errMsg;
    switch (err)
    {
        case -1:
        case -3:
            errMsg = "Error getting file limits, please see non-root install documentation";
            break;
        case -2:
            errMsg = "Error setting file limits, please see non-root install documentation";
            break;
        case -4:
            errMsg = "Could not install file limits to required value, please see non-root install documentation";
            break;
        default:
            break;
    }

    if (err < 0)
    {
        Oam oam;
        logging::Message::Args args;
        logging::Message message;
        args.add( errMsg );
        message.format(args);
        logging::LoggingID lid(16);
        logging::MessageLog ml(lid);
        ml.logCriticalMessage( message );
        cerr << errMsg << endl;
        try
        {
            oam.processInitFailure();
        }
        catch (...)
        {
        }
        return 2;
    }


	setupCwd(rm);

	cleanTempDir();

	MsgMap msgMap;
	msgMap[logDefaultMsg]           = Message(logDefaultMsg);
	msgMap[logDbProfStartStatement] = Message(logDbProfStartStatement);
	msgMap[logDbProfEndStatement]   = Message(logDbProfEndStatement);
	msgMap[logStartSql]             = Message(logStartSql);
	msgMap[logEndSql]               = Message(logEndSql);
	msgMap[logRssTooBig]            = Message(logRssTooBig);
	msgMap[logDbProfQueryStats]     = Message(logDbProfQueryStats);
	msgMap[logExeMgrExcpt]          = Message(logExeMgrExcpt);
	msgLog.msgMap(msgMap);

	ec = DistributedEngineComm::instance(rm, true);
	ec->Open();

	bool tellUser = true;

	MessageQueueServer* mqs;

	statementsRunningCount = new ActiveStatementCounter(rm->getEmExecQueueSize());
	for (;;)
	{
		try {
			mqs = new MessageQueueServer(ExeMgr, rm->getConfig(), ByteStream::BlockSize, 64);
			break;
		}
		catch (runtime_error& re) {
			string what = re.what();
			if (what.find("Address already in use") != string::npos)
			{
				if (tellUser)
				{
					cerr << "Address already in use, retrying..." << endl;
					tellUser = false;
				}
				sleep(5);
			}
			else
			{
				throw;
			}
		}
	}

    // class jobstepThreadPool is used by other processes. We can't call 
    // resourcemanaager (rm) functions during the static creation of threadpool 
    // because rm has a "isExeMgr" flag that is set upon creation (rm is a singleton). 
    // From  the pools perspective, it has no idea if it is ExeMgr doing the 
    // creation, so it has no idea which way to set the flag. So we set the max here.
	JobStep::jobstepThreadPool.setMaxThreads(rm->getJLThreadPoolSize());
    JobStep::jobstepThreadPool.setName("ExeMgrJobList");
	if (rm->getJlThreadPoolDebug() == "Y" || rm->getJlThreadPoolDebug() == "y")
	{
		JobStep::jobstepThreadPool.setDebug(true);
		JobStep::jobstepThreadPool.invoke(ThreadPoolMonitor(&JobStep::jobstepThreadPool));
	}
	
	int serverThreads = rm->getEmServerThreads();
	int maxPct = rm->getEmMaxPct();
	int pauseSeconds = rm->getEmSecondsBetweenMemChecks();
	int priority = rm->getEmPriority();

    FEMsgHandler::threadPool.setMaxThreads(serverThreads);
    FEMsgHandler::threadPool.setName("FEMsgHandler");
    
    if (maxPct > 0)
		startRssMon(maxPct, pauseSeconds);

#ifndef _MSC_VER
	setpriority(PRIO_PROCESS, 0, priority);
#endif

	string teleServerHost(rm->getConfig()->getConfig("QueryTele", "Host"));
	if (!teleServerHost.empty())
	{
		int teleServerPort = toInt(rm->getConfig()->getConfig("QueryTele", "Port"));
		if (teleServerPort > 0)
		{
			gTeleServerParms.host = teleServerHost;
			gTeleServerParms.port = teleServerPort;
		}
	}

	cout << "Starting ExeMgr: st = " << serverThreads << 
		", qs = " << rm->getEmExecQueueSize() << ", mx = " << maxPct << ", cf = " <<
		rm->getConfig()->configFile() << endl;

	//set ACTIVE state
	{
		Oam oam;
		try
		{
			oam.processInitComplete("ExeMgr");
		}
		catch (...)
		{
		}
	}

	threadpool::ThreadPool exeMgrThreadPool(serverThreads, 0);
    exeMgrThreadPool.setName("ExeMgrServer");

	if (rm->getExeMgrThreadPoolDebug() == "Y" || rm->getExeMgrThreadPoolDebug() == "y")
	{
		exeMgrThreadPool.setDebug(true);
		exeMgrThreadPool.invoke(ThreadPoolMonitor(&exeMgrThreadPool));
	}

    for (;;)
	{
		IOSocket ios;
		ios = mqs->accept();
		exeMgrThreadPool.invoke(SessionThread(ios, ec, rm));
	}
	exeMgrThreadPool.wait();

	return 0;
}
// vim:ts=4 sw=4:

