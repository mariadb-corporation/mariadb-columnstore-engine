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

/*
 * $Id: ha_calpont_impl.cpp 9642 2013-06-24 14:57:42Z rdempsey $
 */

//#define DEBUG_WALK_COND
#include <my_config.h>
#ifndef _MSC_VER
#include <unistd.h>
#endif
#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#include <stdio.h> 
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <time.h>
//#define NDEBUG
#include <cassert>
#include <vector>
#include <map>
#include <limits>
#if defined(__linux__)
#include <wait.h>			//wait()
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/stat.h>   	// For stat().
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "idb_mysql.h"

#define NEED_CALPONT_INTERFACE
#include "ha_calpont_impl.h"

#include "ha_calpont_impl_if.h"
using namespace cal_impl_if;

#include "calpontselectexecutionplan.h"
#include "logicoperator.h"
#include "parsetree.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "sm.h"

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "dmlpackage.h"
#include "calpontdmlpackage.h"
#include "insertdmlpackage.h"
#include "vendordmlstatement.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "configcpp.h"
using namespace config;

#include "rowgroup.h"
using namespace rowgroup;

#include "brmtypes.h"
using namespace BRM;

#include "querystats.h"
using namespace querystats;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn_int.h"
#include "simplecolumn_decimal.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "selectfilter.h"
using namespace execplan;

#include "joblisttypes.h"
using namespace joblist;

#include "cacheutils.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "resourcemanager.h"

#include "funcexp.h"
#include "functor.h"
using namespace funcexp;

#include "installdir.h"
#include "columnstoreversion.h"

namespace cal_impl_if
{
	extern bool nonConstFunc(Item_func* ifp);
}

namespace
{
// Calpont vtable non-support error message
const string infinidb_autoswitch_warning = "The query includes syntax that is not supported by MariaDB Columnstore distributed mode. The execution was switched to standard mode with downgraded performance.";

// copied from item_timefunc.cc
static const string interval_names[]=
{
  "year", "quarter", "month", "week", "day",
  "hour", "minute", "second", "microsecond",
  "year_month", "day_hour", "day_minute",
  "day_second", "hour_minute", "hour_second",
  "minute_second", "day_microsecond",
  "hour_microsecond", "minute_microsecond",
  "second_microsecond"
};

const unsigned NONSUPPORTED_ERR_THRESH = 2000;

//TODO: make this session-safe (put in connMap?)
vector<RMParam> rmParms;
ResourceManager *rm = ResourceManager::instance();
bool useHdfs = rm->useHdfs();
//convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
	return CalpontSystemCatalog::idb_tid2sid(tid);
}

void storeNumericField(Field** f, int64_t value, CalpontSystemCatalog::ColType& ct)
{
	// unset null bit first
	if ((*f)->null_ptr)
		*(*f)->null_ptr &= ~(*f)->null_bit;
	// For unsigned, use the ColType returned in the row rather than the
	// unsigned_flag set by mysql. This is because mysql gets it wrong for SUM()
	// Hopefully, in all other cases we get it right.
	switch ((*f)->type())
	{
		case MYSQL_TYPE_NEWDECIMAL:
		{
			Field_new_decimal* f2 = (Field_new_decimal*)*f;
			// @bug4388 stick to InfiniDB's scale in case mysql gives wrong scale due
			// to create vtable limitation.
			if (f2->dec < ct.scale)
				f2->dec = ct.scale;
			char buf[256];
			dataconvert::DataConvert::decimalToString(value, (unsigned)ct.scale, buf, 256, ct.colDataType);
			f2->store(buf, strlen(buf), f2->charset());
			break;
		}
		case MYSQL_TYPE_TINY: //TINYINT type
		{
			Field_tiny* f2 = (Field_tiny*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_SHORT: //SMALLINT type
		{
			Field_short* f2 = (Field_short*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_LONG: //INT type
		{
			Field_long* f2 = (Field_long*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_LONGLONG: //BIGINT type
		{
			Field_longlong* f2 = (Field_longlong*)*f;
			longlong int_val = (longlong)value;
			f2->store(int_val, f2->unsigned_flag);
			break;
		}
		case MYSQL_TYPE_FLOAT: // FLOAT type
		{
			Field_float* f2 = (Field_float*)*f;
			float float_val = *(float*)(&value);
			f2->store(float_val);
			break;
		}
		case MYSQL_TYPE_DOUBLE: // DOUBLE type
		{
			Field_double* f2 = (Field_double*)*f;
			double double_val = *(double*)(&value);
			f2->store(double_val);
			break;
		}
		case MYSQL_TYPE_VARCHAR:
		{
			Field_varstring* f2 = (Field_varstring*)*f;
			char tmp[25];
			if (ct.colDataType == CalpontSystemCatalog::DECIMAL)
				dataconvert::DataConvert::decimalToString(value, (unsigned)ct.scale, tmp, 25, ct.colDataType);
			else
				snprintf(tmp, 25, "%ld", value);
			f2->store(tmp, strlen(tmp), f2->charset());
			break;
		}
		default:
		{
			Field_longlong* f2 = (Field_longlong*)*f;
			longlong int_val = (longlong)value;
				f2->store(int_val, f2->unsigned_flag);
			break;
		}
	}
}

//
// @bug 2244. Log exception related to lost connection to ExeMgr.
// Log exception error from calls to sm::tpl_scan_fetch in fetchNextRow()
//
void tpl_scan_fetch_LogException( cal_table_info& ti, cal_connection_info* ci, std::exception* ex)
{
	time_t t = time(0);
	char datestr[50];
	ctime_r(&t, datestr);
	datestr[ strlen(datestr)-1 ] = '\0'; // strip off trailing newline

	uint32_t sesID   = 0;
	string connHndl("No connection handle to use");

	if (ti.conn_hndl) {
		connHndl = "ti connection used";
		sesID = ti.conn_hndl->sessionID;
	}

	else if (ci->cal_conn_hndl) {
		connHndl = "ci connection used";
		sesID = ci->cal_conn_hndl->sessionID;
	}

	int64_t rowsRet = -1;
	if (ti.tpl_scan_ctx)
		rowsRet = ti.tpl_scan_ctx->rowsreturned;

	if (ex)
		cerr << datestr << ": sm::tpl_scan_fetch error getting rows for sessionID: " <<
			sesID << "; " << connHndl << "; rowsReturned: " << rowsRet <<
			"; reason-" << ex->what() << endl;
	else
		cerr << datestr << ": sm::tpl_scan_fetch unknown error getting rows for sessionID: " <<
			sesID << "; " << connHndl << "; rowsReturned: " << rowsRet << endl;
}

const char hexdig[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', };

int vbin2hex(const uint8_t* p, const unsigned l, char* o)
{
	for (unsigned i = 0; i < l; i++, p++)
	{
		*o++ = hexdig[*p >> 4];
		*o++ = hexdig[*p & 0xf];
	}
	return 0;
}

int fetchNextRow(uchar *buf, cal_table_info& ti, cal_connection_info* ci)
{
	int rc = HA_ERR_END_OF_FILE;
	int num_attr = ti.msTablePtr->s->fields;
	sm::status_t sm_stat;

	try {
		if (ti.conn_hndl)
		{
			sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, ti.conn_hndl);
		}
		else if (ci->cal_conn_hndl)
		{
			sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, ci->cal_conn_hndl, (int*)(&current_thd->killed));
		}
		else
			throw runtime_error("internal error");
	} catch (std::exception& ex) {
// @bug 2244. Always log this msg for now, as we try to track down when/why we are
//			losing socket connection with ExeMgr
//#ifdef INFINIDB_DEBUG
		tpl_scan_fetch_LogException( ti, ci, &ex);
//#endif
		sm_stat = sm::CALPONT_INTERNAL_ERROR;
	} catch (...) {
// @bug 2244. Always log this msg for now, as we try to track down when/why we are
//			losing socket connection with ExeMgr
//#ifdef INFINIDB_DEBUG
		tpl_scan_fetch_LogException( ti, ci, 0 );
//#endif
		sm_stat = sm::CALPONT_INTERNAL_ERROR;
	}

	if (sm_stat == sm::STATUS_OK)
	{
		Field **f;
		f = ti.msTablePtr->field;
		//set all fields to null in null col bitmap
		memset(buf, -1, ti.msTablePtr->s->null_bytes);
		std::vector<CalpontSystemCatalog::ColType> &colTypes = ti.tpl_scan_ctx->ctp;
		int64_t intColVal = 0;
		uint64_t uintColVal = 0;
		char tmp[256];

		RowGroup *rowGroup = ti.tpl_scan_ctx->rowGroup;

		// table mode mysql expects all columns of the table. mapping between columnoid and position in rowgroup
		// set coltype.position to be the position in rowgroup. only set once.
		if (ti.tpl_scan_ctx->rowsreturned == 0 &&
			 (ti.tpl_scan_ctx->traceFlags & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF))
		{
			for (uint32_t i = 0; i < rowGroup->getColumnCount(); i++)
			{
				int oid = rowGroup->getOIDs()[i];
				int j = 0;
				for (; j < num_attr; j++)
				{
					// mysql should haved eliminated duplicate projection columns
					if (oid == colTypes[j].columnOID || oid == colTypes[j].ddn.dictOID)
					{
						colTypes[j].colPosition = i;
						break;
					}
				}
			}
		}

		rowgroup::Row row;
		rowGroup->initRow(&row);
		rowGroup->getRow(ti.tpl_scan_ctx->rowsreturned, &row);
		int s;
		for (int p = 0; p < num_attr; p++, f++)
		{
			//This col is going to be written
			bitmap_set_bit(ti.msTablePtr->write_set, (*f)->field_index);

			// get coltype if not there yet
			if (colTypes[0].colWidth == 0)
			{
				for (short c = 0; c < num_attr; c++)
				{
					colTypes[c].colPosition = c;
					colTypes[c].colWidth = rowGroup->getColumnWidth(c);
					colTypes[c].colDataType = rowGroup->getColTypes()[c];
					colTypes[c].columnOID = rowGroup->getOIDs()[c];
					colTypes[c].scale = rowGroup->getScale()[c];
					colTypes[c].precision = rowGroup->getPrecision()[c];
				}
			}
			CalpontSystemCatalog::ColType colType(colTypes[p]);

			// table mode handling
			if (ti.tpl_scan_ctx->traceFlags & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF)
			{
				if (colType.colPosition == -1) // not projected by tuplejoblist
					continue;
				else
					s = colType.colPosition;
			}
			else
			{
				s = p;
			}

			// precision == -16 is borrowed as skip null check indicator for bit ops.
			if (row.isNullValue(s) && colType.precision != -16)
			{
				// @2835. Handle empty string and null confusion. store empty string for string column
				if (colType.colDataType == CalpontSystemCatalog::CHAR ||
					  colType.colDataType == CalpontSystemCatalog::VARCHAR ||
					  colType.colDataType == CalpontSystemCatalog::VARBINARY)
				{
					Field_varstring* f2 = (Field_varstring*)*f;
					f2->store(tmp, 0, f2->charset());
				}
				continue;
			}

			// fetch and store data
			switch (colType.colDataType)
			{
				case CalpontSystemCatalog::DATE:
				{
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					intColVal = row.getUintField<4>(s);
					DataConvert::dateToString(intColVal, tmp, 255);
					Field_varstring* f2 = (Field_varstring*)*f;
					f2->store(tmp, strlen(tmp), f2->charset());
					break;
				}
				case CalpontSystemCatalog::DATETIME:
				{
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					intColVal = row.getUintField<8>(s);
					DataConvert::datetimeToString(intColVal, tmp, 255);

                    /* setting the field_length is a sort-of hack. The length
                     * at this point can be long enough to include mseconds.
                     * ColumnStore doesn't fully support mseconds yet so if
                     * they are requested, trim them off.
                     * At a later date we should set this more intelligently
                     * based on the result set.
                     */
                    /* MCOL-683: UTF-8 datetime no msecs is 57, this sometimes happens! */
                    if (((*f)->field_length > 19) && ((*f)->field_length != 57))
                        (*f)->field_length = strlen(tmp);

					Field_varstring* f2 = (Field_varstring*)*f;
					f2->store(tmp, strlen(tmp), f2->charset());
					break;
				}
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
				{
					Field_varstring* f2 = (Field_varstring*)*f;
					switch (colType.colWidth)
					{
						case 1:
							intColVal = row.getUintField<1>(s);
							f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset());
							break;
						case 2:
							intColVal = row.getUintField<2>(s);
							f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset());
							break;
						case 4:
							intColVal = row.getUintField<4>(s);
							f2->store((char*)(&intColVal), strlen((char*)(&intColVal)), f2->charset());
							break;
						case 8:
							//make sure we don't send strlen off into the weeds...
							intColVal = row.getUintField<8>(s);
							memcpy(tmp, &intColVal, 8);
							tmp[8] = 0;
							f2->store(tmp, strlen(tmp), f2->charset());
							break;
						default:
                            f2->store((const char*)row.getStringPointer(s), row.getStringLength(s), f2->charset());
					}
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					break;
				}
				case CalpontSystemCatalog::VARBINARY:
				{
					Field_varstring* f2 = (Field_varstring*)*f;

					if (current_thd->variables.infinidb_varbin_always_hex)
					{
						uint32_t l;
						const uint8_t* p = row.getVarBinaryField(l, s);
						uint32_t ll = l * 2;
						boost::scoped_array<char> sca(new char[ll]);
						vbin2hex(p, l, sca.get());
						f2->store(sca.get(), ll, f2->charset());
					}
					else
						f2->store((const char*)row.getVarBinaryField(s), row.getVarBinaryLength(s), f2->charset());

					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					break;
				}
				case CalpontSystemCatalog::BIGINT:
				{
					intColVal = row.getIntField<8>(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
				case CalpontSystemCatalog::UBIGINT:
				{
					uintColVal = row.getUintField<8>(s);
					storeNumericField(f, uintColVal, colType);
					break;
				}
				case CalpontSystemCatalog::INT:
				{
					intColVal = row.getIntField<4>(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
				case CalpontSystemCatalog::UINT:
				{
					uintColVal = row.getUintField<4>(s);
					storeNumericField(f, uintColVal, colType);
					break;
				}
				case CalpontSystemCatalog::SMALLINT:
				{
					intColVal = row.getIntField<2>(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
				case CalpontSystemCatalog::USMALLINT:
				{
					uintColVal = row.getUintField<2>(s);
					storeNumericField(f, uintColVal, colType);
					break;
				}
				case CalpontSystemCatalog::TINYINT:
				{
					intColVal = row.getIntField<1>(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
				case CalpontSystemCatalog::UTINYINT:
				{
					uintColVal = row.getUintField<1>(s);
					storeNumericField(f, uintColVal, colType);
					break;
				}
				//In this case, we're trying to load a double output column with float data. This is the
				// case when you do sum(floatcol), e.g.
				case CalpontSystemCatalog::FLOAT:
				case CalpontSystemCatalog::UFLOAT:
				{
					float dl = row.getFloatField(s);
					if (dl == std::numeric_limits<float>::infinity())
						continue;

					//int64_t* icvp = (int64_t*)&dl;
					//intColVal = *icvp;
					Field_float* f2 = (Field_float*)*f;
					// bug 3485, reserve enough space for the longest float value
					// -3.402823466E+38 to -1.175494351E-38, 0, and
					// 1.175494351E-38 to 3.402823466E+38.
					(*f)->field_length = 40;
					//float float_val = *(float*)(&value);
					//f2->store(float_val);
					if (f2->decimals() < (uint32_t)row.getScale(s))
						f2->dec = (uint32_t)row.getScale(s);
					f2->store(dl);
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					break;

					//storeNumericField(f, intColVal, colType);
					//break;
				}
				case CalpontSystemCatalog::DOUBLE:
				case CalpontSystemCatalog::UDOUBLE:
				{
					double dl = row.getDoubleField(s);
					if (dl == std::numeric_limits<double>::infinity())
						continue;
					Field_double* f2 = (Field_double*)*f;
					// bug 3483, reserve enough space for the longest double value
					// -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and
					// 2.2250738585072014E-308 to 1.7976931348623157E+308.
					(*f)->field_length = 310;
					//double double_val = *(double*)(&value);
					//f2->store(double_val);
					if (f2->decimals() < (uint32_t)row.getScale(s))
						f2->dec = (uint32_t)row.getScale(s);
					f2->store(dl);
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					break;


					//int64_t* icvp = (int64_t*)&dl;
					//intColVal = *icvp;
					//storeNumericField(f, intColVal, colType);
					//break;
				}
				case CalpontSystemCatalog::DECIMAL:
				case CalpontSystemCatalog::UDECIMAL:
				{
					intColVal = row.getIntField(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
				case CalpontSystemCatalog::BLOB:
				case CalpontSystemCatalog::TEXT:
				{
					Field_blob *f2 = (Field_blob*)*f;
					f2->set_ptr(row.getVarBinaryLength(s), (unsigned char*)row.getVarBinaryField(s));
					if ((*f)->null_ptr)
						*(*f)->null_ptr &= ~(*f)->null_bit;
					break;
				}
				default:	// treat as int64
				{
					intColVal = row.getUintField<8>(s);
					storeNumericField(f, intColVal, colType);
					break;
				}
			}
		}

		ti.tpl_scan_ctx->rowsreturned++;
		ti.c++;
#ifdef INFINIDB_DEBUG
		if ((ti.c % 1000000) == 0)
			cerr << "fetchNextRow so far table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c << endl;
#endif
		ti.moreRows = true;
		rc = 0;
	}
	else if (sm_stat == sm::SQL_NOT_FOUND)
	{
		IDEBUG( cerr << "fetchNextRow done for table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c << endl );
		ti.c = 0;
		ti.moreRows = false;
		rc = HA_ERR_END_OF_FILE;
	}
	else if (sm_stat == sm::CALPONT_INTERNAL_ERROR)
	{
		ti.moreRows = false;
		rc = ER_INTERNAL_ERROR;
		ci->rc = rc;
	}
	else if ((uint32_t)sm_stat == logging::ERR_LOST_CONN_EXEMGR)
	{
		ti.moreRows = false;
		rc = logging::ERR_LOST_CONN_EXEMGR;
		sm::sm_init(tid2sid(current_thd->thread_id), &ci->cal_conn_hndl,
					current_thd->variables.infinidb_local_query);
		idbassert(ci->cal_conn_hndl != 0);
		ci->rc = rc;
	}
	else if (sm_stat == sm::SQL_KILLED)
	{
		// query was aborted by the user. treat it the same as limit query. close
		// connection after rnd_close.
		ti.c = 0;
		ti.moreRows = false;
		rc = HA_ERR_END_OF_FILE;
		ci->rc = rc;
	}
	else
	{
		ti.moreRows = false;
		rc = sm_stat;
		ci->rc = rc;
	}

	return rc;
}

void makeUpdateScalarJoin(const ParseTree* n, void* obj)
{
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;

	SimpleColumn *scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
	SimpleColumn *scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
	CalpontSystemCatalog::TableAliasName* updatedTables = reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);
	if ( scLeft && scRight )
	{
		if ( (strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0 ) && (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0)
		&& (strcasecmp(scLeft->tableAlias().c_str(),updatedTables->alias.c_str()) == 0))
		{
			uint64_t lJoinInfo = sf->lhs()->joinInfo();
			lJoinInfo |= JOIN_SCALAR;
			//lJoinInfo |= JOIN_OUTER_SELECT;
			//lJoinInfo |= JOIN_CORRELATED;
			sf->lhs()->joinInfo(lJoinInfo);
		}
		else if ( (strcasecmp(scRight->tableName().c_str(),updatedTables->table.c_str()) == 0) && (strcasecmp(scRight->schemaName().c_str(),updatedTables->schema.c_str())==0)
		&& (strcasecmp(scRight->tableAlias().c_str(),updatedTables->alias.c_str())==0))
		{
			uint64_t rJoinInfo = sf->rhs()->joinInfo();
			rJoinInfo |= JOIN_SCALAR;
			//rJoinInfo |= JOIN_OUTER_SELECT;
			//rJoinInfo |= JOIN_CORRELATED;
			sf->rhs()->joinInfo(rJoinInfo);
		}
		else
			return;
	}
	else
		return;
}

void makeUpdateSemiJoin(const ParseTree* n, void* obj)
{
	TreeNode *tn = n->data();
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	if (!sf)
		return;

	SimpleColumn *scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
	SimpleColumn *scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
	CalpontSystemCatalog::TableAliasName* updatedTables = reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);
	//@Bug 3279. Added a check for column filters.
	if ( scLeft && scRight && (strcasecmp(scRight->tableAlias().c_str(),scLeft->tableAlias().c_str()) != 0))
	{
		if ( (strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0 ) && (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0)
		&& (strcasecmp(scLeft->tableAlias().c_str(),updatedTables->alias.c_str()) == 0))
		{
			uint64_t lJoinInfo = sf->lhs()->joinInfo();
			lJoinInfo |= JOIN_SEMI;
			//lJoinInfo |= JOIN_OUTER_SELECT;
			//lJoinInfo |= JOIN_CORRELATED;
			sf->lhs()->joinInfo(lJoinInfo);
		}
		else if ( (strcasecmp(scRight->tableName().c_str(),updatedTables->table.c_str()) == 0) && (strcasecmp(scRight->schemaName().c_str(),updatedTables->schema.c_str())==0)
		&& (strcasecmp(scRight->tableAlias().c_str(),updatedTables->alias.c_str())==0))
		{
			uint64_t rJoinInfo = sf->rhs()->joinInfo();
			rJoinInfo |= JOIN_SEMI;
			//rJoinInfo |= JOIN_OUTER_SELECT;
			//rJoinInfo |= JOIN_CORRELATED;
			sf->rhs()->joinInfo(rJoinInfo);
		}
		else
			return;
	}
	else
		return;
}

uint32_t doUpdateDelete(THD *thd)
{
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	//@bug 5660. Error out DDL/DML on slave node, or on local query node
	if (ci->isSlaveNode && !thd->slave_thread)
	{
		string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
		setError(thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
		return ER_CHECK_NOT_IMPLEMENTED;
	}

	//@Bug 4387. Check BRM status before start statement.
	scoped_ptr<DBRM> dbrmp(new DBRM());
	int rc = dbrmp->isReadWrite();
	thd->infinidb_vtable.isInfiniDBDML = true;

	if (rc != 0 )
	{
		setError(current_thd, ER_READ_ONLY_MODE, "Cannot execute the statement. DBRM is read only!");
		return ER_READ_ONLY_MODE;
	}

	// stats start
	ci->stats.reset();
	ci->stats.setStartTime();
	ci->stats.fUser = thd->main_security_ctx.user;
	if (thd->main_security_ctx.host)
		ci->stats.fHost = thd->main_security_ctx.host;
	else if (thd->main_security_ctx.host_or_ip)
		ci->stats.fHost = thd->main_security_ctx.host_or_ip;
	else
		ci->stats.fHost = "unknown";
	try {
		ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
	} catch (std::exception& e)
	{
		string msg = string("Columnstore User Priority - ") + e.what();
		push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
	}
	ci->stats.fSessionID = tid2sid(thd->thread_id);

	LEX* lex = thd->lex;
	idbassert(lex != 0);

	// Error out DELETE on VIEW. It's currently not supported.
	// @note DELETE on VIEW works natually (for simple cases at least), but we choose to turn it off
	// for now - ZZ.
	TABLE_LIST* tables = thd->lex->query_tables;
	for (; tables; tables= tables->next_local)
	{
		if (tables->view)
		{
			Message::Args args;
			if (((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
				args.add("Update");
			else
				args.add("Delete");
			string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
			setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
			return ER_CHECK_NOT_IMPLEMENTED;
		}

/*
#if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
		if ((strcmp((*tables->table->s->db_plugin)->name.str, "InfiniDB") != 0) && (strcmp((*tables->table->s->db_plugin)->name.str, "MEMORY") != 0) &&
					   (tables->table->s->table_category != TABLE_CATEGORY_TEMPORARY) )
#else
		if ((strcmp(tables->table->s->db_plugin->name.str, "InfiniDB") != 0) && (strcmp(tables->table->s->db_plugin->name.str, "MEMORY") != 0) &&
					   (tables->table->s->table_category != TABLE_CATEGORY_TEMPORARY) )
#endif
		{
			Message::Args args;
			args.add("Non Calpont table(s)");
			string emsg(IDBErrorInfo::instance()->errorMsg(ERR_DML_NOT_SUPPORT_FEATURE, args));
			setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
			return ER_CHECK_NOT_IMPLEMENTED;
		}
*/
	}

	// @bug 1127. Re-construct update stmt using lex instead of using the original query.
//	string dmlStmt="";
	string dmlStmt=string(idb_mysql_query_str(thd));
	string schemaName;
	string tableName("");
	string aliasName("");
	UpdateSqlStatement updateSqlStmt;
	ColumnAssignmentList* colAssignmentListPtr = new ColumnAssignmentList();
	bool isFromCol = false;
	bool isFromSameTable = true;
	execplan::SCSEP updateCP(new execplan::CalpontSelectExecutionPlan());

	updateCP->isDML(true);
	//@Bug 2753. the memory already freed by destructor of UpdateSqlStatement
	if (((thd->lex)->sql_command == SQLCOM_UPDATE) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
	{
		ColumnAssignment* columnAssignmentPtr;
		Item_field *item;
//		TABLE_LIST* table_ptr = thd->lex->select_lex.get_table_list();
        List_iterator_fast<Item> field_it(thd->lex->select_lex.item_list);
        List_iterator_fast<Item> value_it(thd->lex->value_list);
//      dmlStmt += "update ";
        updateCP->queryType(CalpontSelectExecutionPlan::UPDATE);
        ci->stats.fQueryType = updateCP->queryType();
        uint32_t cnt = 0;

//        for (; table_ptr; table_ptr= table_ptr->next_local)
//        {
//            dmlStmt += string(table_ptr->table_name);
//            if (table_ptr->next_local)
//                dmlStmt += ", ";
//        }

//		dmlStmt += " set ";

        while ((item= (Item_field *) field_it++))
        {
            cnt++;
//            dmlStmt += string(item->name) + "=";

            string tmpTableName = bestTableName(item);

            //@Bug 5312 populate aliasname with tablename if it is empty
            if (!item->table_name)
                aliasName = tmpTableName;
            else
                aliasName = item->table_name;

            if (strcasecmp(tableName.c_str(), "") == 0)
            {
                tableName = tmpTableName;
            }
            else if (strcasecmp(tableName.c_str(), tmpTableName.c_str()) != 0)
            {
                //@ Bug3326 error out for multi table update
                string emsg(IDBErrorInfo::instance()->errorMsg(ERR_UPDATE_NOT_SUPPORT_FEATURE));
                thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED, emsg.c_str());
                ci->rc = -1;
                thd->set_row_count_func(0);
                return -1;
            }
            if (!item->db_name)
            {
                //@Bug 5312. if subselect, wait until the schema info is available.
                if (thd->derived_tables_processing)
                    return 0;
                else
                {
                    thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED, "The statement cannot be processed without schema.");
                    ci->rc = -1;
                    thd->set_row_count_func(0);
                    return -1;
                }
            }
            else
                schemaName = string(item->db_name);

            columnAssignmentPtr = new ColumnAssignment();
            columnAssignmentPtr->fColumn = string(item->name);
            columnAssignmentPtr->fOperator = "=";
            columnAssignmentPtr->fFuncScale = 0;
            Item *value= value_it++;
            if (value->type() ==  Item::STRING_ITEM)
            {
                //@Bug 2587 use val_str to replace value->name to get rid of 255 limit
                String val, *str;
                str = value->val_str(&val);
                columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
                columnAssignmentPtr->fFromCol = false;
            }
            else if ( value->type() ==  Item::VARBIN_ITEM )
            {
                String val, *str;
                str = value->val_str(&val);
                columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
                columnAssignmentPtr->fFromCol = false;
            }
            else if ( value->type() ==  Item::FUNC_ITEM )
            {
                //Bug 2092 handle negative values
                Item_func* ifp = (Item_func*)value;
                if (ifp->result_type() == DECIMAL_RESULT)
                    columnAssignmentPtr->fFuncScale = ifp->decimals; //decimal scale

                vector <Item_field*> tmpVec;
                bool hasNonSupportItem = false;
                uint16_t parseInfo = 0;
                parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo);
                // const f&e evaluate here. @bug3513. Rule out special functions that takes
                // no argument but needs to be sent to the back end to process. Like rand(),
                // sysdate() etc.
                if (!hasNonSupportItem && !cal_impl_if::nonConstFunc(ifp) && tmpVec.size() == 0)
                {
                    gp_walk_info gwi;
                    gwi.thd = thd;
                    SRCP srcp(buildReturnedColumn(value, gwi, gwi.fatalParseError));
                    ConstantColumn *constCol = dynamic_cast<ConstantColumn*>(srcp.get());
                    if (constCol )
                    {
                        columnAssignmentPtr->fScalarExpression  = constCol->constval();
                        isFromCol = false;
                        columnAssignmentPtr->fFromCol = false;
                    }
                    else
                    {
                        isFromCol = true;
                        columnAssignmentPtr->fFromCol = true;

                    }
                }
                else
                {
                    isFromCol = true;
                    columnAssignmentPtr->fFromCol = true;
                }

                if ( isFromCol )
                {
                    string sectableName("");
                    string secschemaName ("");
                    for ( unsigned i = 0; i < tmpVec.size(); i++ )
                    {
                        sectableName = bestTableName(tmpVec[i]);
                        if ( tmpVec[i]->db_name )
                        {
                            secschemaName = string(tmpVec[i]->db_name);
                        }
                        if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) ||
                             (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
                        {
                            isFromSameTable = false;
                            break;
                        }
                    }
                }
            }
            else if ( value->type() ==  Item::INT_ITEM )
            {
                std::ostringstream oss;
                if (value->unsigned_flag)
                {
                    oss << value->val_uint();
                }
                else
                {
                    oss << value->val_int();
                }
//                dmlStmt += oss.str();
                columnAssignmentPtr->fScalarExpression = oss.str();
                columnAssignmentPtr->fFromCol = false;
            }
            else if ( value->type() ==  Item::FIELD_ITEM)
            {
                isFromCol = true;
                columnAssignmentPtr->fFromCol = true;
                Item_field* setIt = reinterpret_cast<Item_field*> (value);
                string sectableName = string(setIt->table_name);
                if ( setIt->db_name ) //derived table
                {
                    string secschemaName = string(setIt->db_name);
                    if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) || (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
                    {
                        isFromSameTable = false;
                    }
                }
                else
                {
                    isFromSameTable = false;
                }
            }
            else if ( value->type() ==  Item::NULL_ITEM )
            {
//                dmlStmt += "NULL";
                columnAssignmentPtr->fScalarExpression = "NULL";
                columnAssignmentPtr->fFromCol = false;
            }
            else if ( value->type() == Item::SUBSELECT_ITEM )
            {
                isFromCol = true;
                columnAssignmentPtr->fFromCol = true;
//                Item_field* setIt = reinterpret_cast<Item_field*> (value);
//                string sectableName = string(setIt->table_name);
//                string secschemaName = string(setIt->db_name);
//                if ( (strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) || (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
//                {
                    isFromSameTable = false;
//                }
            }
            //@Bug 4449 handle default value
            else if (value->type() == Item::DEFAULT_VALUE_ITEM)
            {
                Item_field* tmp = (Item_field*)value;

                if (!tmp->field_name) //null
                {
                    columnAssignmentPtr->fScalarExpression = "NULL";
                    columnAssignmentPtr->fFromCol = false;
                }
                else
                {
                    String val, *str;
                    str = value->val_str(&val);
                    columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
                    columnAssignmentPtr->fFromCol = false;
                }
            }
            else if (value->type() == Item::WINDOW_FUNC_ITEM)
            {
                setError(thd, ER_INTERNAL_ERROR,
                         logging::IDBErrorInfo::instance()->errorMsg(ERR_WF_UPDATE));
                return ER_CHECK_NOT_IMPLEMENTED;
                //return 0;
            }
            else
            {
                String val, *str;
                str = value->val_str(&val);
                if (str)
                {
                    columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
                    columnAssignmentPtr->fFromCol = false;
                }
                else
                {
                    columnAssignmentPtr->fScalarExpression = "NULL";
                    columnAssignmentPtr->fFromCol = false;
                }
            }

            colAssignmentListPtr->push_back ( columnAssignmentPtr );
//            if (cnt < thd->lex->select_lex.item_list.elements)
//                dmlStmt += ", ";
        }
	}
	else
	{
//		dmlStmt = string(idb_mysql_query_str(thd));
		updateCP->queryType(CalpontSelectExecutionPlan::DELETE);
		ci->stats.fQueryType = updateCP->queryType();
	}
	//save table oid for commit/rollback to use
	uint32_t sessionID = tid2sid(thd->thread_id);
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->identity(execplan::CalpontSystemCatalog::FE);
	CalpontSystemCatalog::TableName aTableName;
	TABLE_LIST *first_table = 0;
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		aTableName.schema = schemaName;
		aTableName.table = tableName;
	}
	else
	{
		first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
		aTableName.schema = first_table->table->s->db.str;
		aTableName.table = first_table->table->s->table_name.str;
	}
	CalpontSystemCatalog::ROPair roPair;
	try {
			roPair = csc->tableRID( aTableName );
	}
	catch (IDBExcept &ie) {
//		setError(thd, ER_UNKNOWN_TABLE, ie.what());
		setError(thd, ER_INTERNAL_ERROR, ie.what());
		return ER_INTERNAL_ERROR;
	}
	catch (std::exception&ex) {
		setError(thd, ER_INTERNAL_ERROR,
					logging::IDBErrorInfo::instance()->errorMsg(ERR_SYSTEM_CATALOG) + ex.what());
		return ER_INTERNAL_ERROR;
	}

	ci->tableOid = roPair.objnum;
	CalpontDMLPackage* pDMLPackage = 0;
//	dmlStmt += ";";
	IDEBUG( cout << "STMT: " << dmlStmt << " and sessionID " << thd->thread_id <<  endl );
	VendorDMLStatement dmlStatement(dmlStmt, sessionID);
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
		dmlStatement.set_DMLStatementType( DML_UPDATE );
	else
		dmlStatement.set_DMLStatementType( DML_DELETE );

	TableName* qualifiedTablName = new TableName();


	UpdateSqlStatement updateStmt;
	//@Bug 2753. To make sure the momory is freed.
	updateStmt.fColAssignmentListPtr = colAssignmentListPtr;
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		qualifiedTablName->fName = tableName;
		qualifiedTablName->fSchema = schemaName;
		updateStmt.fNamePtr = qualifiedTablName;
		pDMLPackage = CalpontDMLFactory::makeCalpontUpdatePackageFromMysqlBuffer( dmlStatement, updateStmt );
	}
	else if ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) //@Bug 6121 error out on multi tables delete.
	{
		if ( (thd->lex->select_lex.join) != 0)
		{
			multi_delete* deleteTable = (multi_delete*)((thd->lex->select_lex.join)->result);
			first_table= (TABLE_LIST*) deleteTable->get_tables();
			if (deleteTable->get_num_of_tables() == 1)
			{
				schemaName = first_table->db;
				tableName = first_table->table_name;
				aliasName = first_table->alias;
				qualifiedTablName->fName = tableName;
				qualifiedTablName->fSchema = schemaName;
				pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
			}
			else
			{
				string emsg("Deleting rows from multiple tables in a single statement is currently not supported.");
                thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
				ci->rc = 1;
                thd->set_row_count_func(0);
				return 0;
			}
		}
		else
		{
			first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
			schemaName = first_table->table->s->db.str;
			tableName = first_table->table->s->table_name.str;
			aliasName = first_table->alias;
			qualifiedTablName->fName = tableName;
			qualifiedTablName->fSchema = schemaName;
			pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
		}
	}
	else
	{
		first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
		schemaName = first_table->table->s->db.str;
		tableName = first_table->table->s->table_name.str;
		aliasName = first_table->alias;
		qualifiedTablName->fName = tableName;
		qualifiedTablName->fSchema = schemaName;
		pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
	}
	if (!pDMLPackage)
	{
		string emsg("Fatal parse error in vtable mode in DMLParser ");
		setError(thd, ER_INTERNAL_ERROR, emsg);
		return ER_INTERNAL_ERROR;
	}
	pDMLPackage->set_TableName(tableName);

	pDMLPackage->set_SchemaName(schemaName);

	pDMLPackage->set_IsFromCol( true );
	//cout << " setting 	isFromCol to " << isFromCol << endl;
	string origStmt(idb_mysql_query_str(thd));
	origStmt += ";";
	pDMLPackage->set_SQLStatement( origStmt );

	//Save the item list
	List<Item> items;
	SELECT_LEX select_lex;
	if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		items = (thd->lex->select_lex.item_list);
		thd->lex->select_lex.item_list = thd->lex->value_list;
	}
	select_lex = lex->select_lex;
	
		
	//@Bug 2808 Error out on order by or limit clause
	//@bug5096. support dml limit.
	if (/*( select_lex.explicit_limit ) || */( select_lex.order_list.elements != 0 ) )
	{
		string emsg("DML Statement with order by clause is not currently supported.");
        thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
		ci->rc = 1;
        thd->set_row_count_func(0);
		return 0;
	}

	//thd->infinidb_vtable.isInfiniDBDML = true;
	THD::infinidb_state origState = thd->infinidb_vtable.vtable_state;
	//if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
	{
		gp_walk_info gwi;
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;
		gwi.thd = thd;
		//updateCP->subType (CalpontSelectExecutionPlan::SINGLEROW_SUBS); //set scalar
		updateCP->subType (CalpontSelectExecutionPlan::SELECT_SUBS);
		//@Bug 2975.
		SessionManager sm;
		BRM::TxnID txnID;
		txnID = sm.getTxnID(sessionID);
		if (!txnID.valid)
		{
			txnID.id = 0;
			txnID.valid = true;
		}
		QueryContext verID;
		verID = sm.verID();

		updateCP->txnID(txnID.id);
		updateCP->verID(verID);
		updateCP->sessionID(sessionID);
		updateCP->data(idb_mysql_query_str(thd));
		try {
			updateCP->priority(	ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
		}catch(std::exception& e)
		{
			string msg = string("Columnstore User Priority - ") + e.what();
			push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
		}

		gwi.clauseType = WHERE;

		if (getSelectPlan(gwi, select_lex, updateCP) != 0) //@Bug 3030 Modify the error message for unsupported functions
		{
			if (thd->infinidb_vtable.isUpdateWithDerive)
			{
				// @bug 4457. MySQL inconsistence! for some queries, some structures are only available
				// in the derived_tables_processing phase. So by pass the phase for DML only when the
				// execution plan can not be successfully generated. recover lex before returning;
				thd->lex->select_lex.item_list = items;
				thd->infinidb_vtable.vtable_state = origState;
				return 0;
			}

			//check different error code
			// avoid double set IDB error
			string emsg;
			if (gwi.parseErrorText.find("IDB-") == string::npos)
			{
				Message::Args args;
				args.add(gwi.parseErrorText);
				emsg = IDBErrorInfo::instance()->errorMsg(ER_INTERNAL_ERROR, args);
			}
			else
			{
				emsg = gwi.parseErrorText;
			}
            thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
			ci->rc = -1;
            thd->set_row_count_func(0);
			return -1;

		}
//cout<< "Plan before is " << endl << *updateCP << endl;
		//set the large side by putting the updated table at the first
		CalpontSelectExecutionPlan::TableList tbList = updateCP->tableList();

		CalpontSelectExecutionPlan::TableList::iterator iter = tbList.begin();
		bool notFirst = false;
		while ( iter != tbList.end() )
		{
			if ( ( iter != tbList.begin() ) && (iter->schema == schemaName) && ( iter->table == tableName ) && ( iter->alias == aliasName ) )
			{
				notFirst = true;
				tbList.erase(iter);
				break;
			}
			iter++;
		}
		if ( notFirst )
		{
			CalpontSystemCatalog::TableAliasName tn = make_aliastable(schemaName, tableName, aliasName);
			iter = tbList.begin();
			tbList.insert( iter, 1, tn );
		}
		updateCP->tableList( tbList );
		updateCP->overrideLargeSideEstimate( true );
		//loop through returnedcols to find out constant columns
		CalpontSelectExecutionPlan::ReturnedColumnList returnedCols = updateCP->returnedCols();
		CalpontSelectExecutionPlan::ReturnedColumnList::iterator coliter = returnedCols.begin();
		while ( coliter != returnedCols.end() )
		{
			ConstantColumn *returnCol = dynamic_cast<ConstantColumn*>((*coliter).get());
			if (returnCol )
			{
				returnedCols.erase(coliter);
				coliter = returnedCols.begin();
				//cout << "constant column " << endl;
			}
			else
				coliter++;
		}
		if ((updateCP->columnMap()).empty())
			throw runtime_error ("column map is empty!");
		if (returnedCols.empty())
			returnedCols.push_back((updateCP->columnMap()).begin()->second);
		//@Bug 6123. get the correct returned columnlist	
		if (( (thd->lex)->sql_command == SQLCOM_DELETE ) || ( (thd->lex)->sql_command == SQLCOM_DELETE_MULTI ) ) 
		{
			returnedCols.clear();
			//choose the smallest column to project
			CalpontSystemCatalog::TableName deleteTableName;
			deleteTableName.schema = schemaName;
			deleteTableName.table = tableName;
			CalpontSystemCatalog::RIDList colrids;
			try {
				colrids = csc->columnRIDs(deleteTableName);
			}
			catch (IDBExcept &ie) {
                thd->raise_error_printf(ER_INTERNAL_ERROR, ie.what());
				ci->rc = -1;
                thd->set_row_count_func(0);
				return 0;
			}
			int minColWidth = -1;
			int minWidthColOffset = 0;
			for (unsigned int j = 0; j < colrids.size(); j++)
			{
				CalpontSystemCatalog::ColType ct = csc->colType(colrids[j].objnum);
				if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
					continue;

				if (minColWidth == -1 || ct.colWidth < minColWidth)
				{
					minColWidth = ct.colWidth;
					minWidthColOffset= j;
				}
			}
			CalpontSystemCatalog::TableColName tcn = csc->colName(colrids[minWidthColOffset].objnum);
			SimpleColumn *sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, csc->sessionID());
			sc->tableAlias(aliasName);
			sc->resultType(csc->colType(colrids[minWidthColOffset].objnum));
			SRCP srcp;
			srcp.reset(sc);
			returnedCols.push_back(srcp);
			//cout << "tablename:alias = " << tcn.table<<":"<<aliasName<<endl;
		}
		updateCP->returnedCols( returnedCols );
		if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
		{
			const ParseTree* ptsub = updateCP->filters();
			if ( !isFromSameTable )
			{
				//cout << "set scalar" << endl;
				//walk tree to set scalar
				if (ptsub)
					ptsub->walk(makeUpdateScalarJoin, &tbList[0] );
			}
			else
			{
				//cout << "set semi" << endl;
				if (ptsub)
					ptsub->walk(makeUpdateSemiJoin, &tbList[0] );
			}
			//cout<< "Plan is " << endl << *updateCP << endl;
			if (( (thd->lex)->sql_command == SQLCOM_UPDATE ) || ( (thd->lex)->sql_command == SQLCOM_UPDATE_MULTI ) )
				thd->lex->select_lex.item_list = items;
		}
		//cout<< "Plan is " << endl << *updateCP << endl;
	}

	//updateCP->traceFlags(1);
	//cout<< "Plan is " << endl << *updateCP << endl;
	pDMLPackage->HasFilter(true);
	pDMLPackage->uuid(updateCP->uuid());

	ByteStream bytestream, bytestream1;
	bytestream << sessionID;
	boost::shared_ptr<messageqcpp::ByteStream> plan = pDMLPackage->get_ExecutionPlan();
	updateCP->rmParms(rmParms);
	updateCP->serialize(*plan);
	// recover original vtable state
	thd->infinidb_vtable.vtable_state = origState;
	//cout << "plan has bytes " << plan->length() << endl;
	pDMLPackage->write(bytestream);

	delete pDMLPackage;

	ByteStream::byte b = 0;
	ByteStream::octbyte rows = 0;
	std::string errorMsg;
	long long dmlRowCount = 0;
	if ( thd->killed > 0 )
	{
		return 0;
	}
	//querystats::QueryStats stats;
	string tableLockInfo;

	// Send the request to DMLProc and wait for a response.
	try
	{
		timespec* tsp=0;
#ifndef _MSC_VER
		timespec ts;
		ts.tv_sec = 3L;
		ts.tv_nsec = 0L;
		tsp = &ts;
#else
		//FIXME: @#$%^&! mysql has buggered up timespec!
		// The definition in my_pthread.h isn't the same as in winport/unistd.h...
		struct timespec_foo
		{
			long tv_sec;
			long tv_nsec;
		} ts_foo;
		ts_foo.tv_sec = 3;
		ts_foo.tv_nsec = 0;
		//This is only to get the compiler to not carp below at the read() call.
		// The messagequeue lib uses the correct struct
		tsp = reinterpret_cast<timespec*>(&ts_foo);
#endif
		bool isTimeOut = true;
		int maxRetries = 2;
		std::string exMsg;
		// We try twice to get a response from dmlproc.
		// Every (3) seconds, check for ctrl+c
		for (int retry = 0; bytestream1.length() == 0 && retry < maxRetries; ++ retry)
		{
			try
			{
				if (!ci->dmlProc)
				{
					ci->dmlProc = new MessageQueueClient("DMLProc");
					//cout << "doUpdateDelete start new DMLProc client " << ci->dmlProc << " for session " << sessionID << endl;
				}
				else
				{
					delete ci->dmlProc;
					ci->dmlProc = NULL;
					ci->dmlProc = new MessageQueueClient("DMLProc");
				}

				// Send the request to DMLProc
				ci->dmlProc->write(bytestream);
				// Get an answer from DMLProc
				while (isTimeOut)
				{
					isTimeOut = false;
					bytestream1 = ci->dmlProc->read(tsp, &isTimeOut);
					if (b == 0 && thd->killed > 0 && isTimeOut)
					{
						// We send the CTRL+C command to DMLProc out of band
						// (on a different connection) This will cause
						// DMLProc to stop processing and return an error on the
						// original connection which will cause a rollback.
						messageqcpp::MessageQueueClient ctrlCProc("DMLProc");
						//cout << "doUpdateDelete start new DMLProc client for ctrl-c " <<  " for session " << sessionID << endl;
						VendorDMLStatement cmdStmt( "CTRL+C", DML_COMMAND, sessionID);
						CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
						ByteStream bytestream;
						bytestream << static_cast<uint32_t>(sessionID);
						pDMLPackage->write(bytestream);
						delete pDMLPackage;
						b = 1;
						retry = maxRetries;
						errorMsg = "Command canceled by user";

						try
						{
							ctrlCProc.write(bytestream);
						}
						catch (runtime_error&)
						{
							errorMsg = "Lost connection to DMLProc while doing ctrl+c";
						}
						catch (...)
						{
							errorMsg = "Unknown error caught while doing ctrl+c";
						}
//						break;
					}
				}
			}
			catch (runtime_error& ex)
			{
				// An exception causes a retry, so fall thru
				exMsg = ex.what();
			}
			if (bytestream1.length() == 0 && thd->killed <= 0)
			{
				//cout << "line 1442. received 0 byte from DMLProc and retry = "<< retry << endl;
				// Seems dmlProc isn't playing. Reset it and try again.
				delete ci->dmlProc;
				ci->dmlProc = NULL;
				isTimeOut = true; //@Bug 4742
			}
		}

		if (bytestream1.length() == 0)
		{
			// If we didn't get anything, error
			b = 1;
			if (exMsg.length() > 0)
			{
				errorMsg = exMsg;
			}
			else
			{
				errorMsg = "Lost connection to DMLProc";
			}
		}
		else
		{
			bytestream1 >> b;
			bytestream1 >> rows;
			bytestream1 >> errorMsg;
			if (b == 0)
			{
				bytestream1 >> tableLockInfo;
				bytestream1 >> ci->queryStats;
				bytestream1 >> ci->extendedStats;
				bytestream1 >> ci->miniStats;
				ci->stats.unserialize(bytestream1);
			}
		}

		dmlRowCount = rows;

		if (thd->killed && b == 0)
		{
			b = dmlpackageprocessor::DMLPackageProcessor::JOB_CANCELED;
			errorMsg = "Command canceled by user";
		}
	}
	catch (runtime_error& ex)
	{
		cout << ex.what() << endl;
		b = 1;
		delete ci->dmlProc;
		ci->dmlProc = NULL;
		errorMsg = ex.what();
	}
	catch ( ... )
	{
		//cout << "... exception while writing to DMLProc" << endl;
		b = 1;
		delete ci->dmlProc;
		ci->dmlProc = NULL;
		errorMsg =  "Unknown error caught";
	}

	// If autocommit is on then go ahead and tell the engine to commit the transaction
	//@Bug 1960 If error occurs, the commit is just to release the active transaction.
	//@Bug 2241. Rollback transaction when it failed
	//@Bug 4605. If error, always rollback.
	if (b != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR)
	{
		std::string command;
		if ((useHdfs) && (b == 0))
			command = "COMMIT";
		else if ((useHdfs) && (b != 0))
			command = "ROLLBACK";
        else if ((b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) && thd->is_strict_mode())
            command = "ROLLBACK";
		else if ((!(current_thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) && (( b == 0 ) || (b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)) )
			command = "COMMIT";
		else if (( b != 0 ) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) )
			command = "ROLLBACK";
		else
			command = "";

		if ( command != "")
		{
			VendorDMLStatement cmdStmt(command, DML_COMMAND, sessionID);
			CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
			pDMLPackage->setTableOid (ci->tableOid);
			ByteStream bytestream;
			bytestream << static_cast<uint32_t>(sessionID);
			pDMLPackage->write(bytestream);
			delete pDMLPackage;

			ByteStream::byte bc;
			std::string errMsg;
			try
			{
				if (!ci->dmlProc)
				{
					ci->dmlProc = new MessageQueueClient("DMLProc");
					//cout << " doupdateDelete command use a new dml client " << ci->dmlProc << endl;
				}
				ci->dmlProc->write(bytestream);
				bytestream1 = ci->dmlProc->read();
				bytestream1 >> bc;
				bytestream1 >> rows;
				bytestream1 >> errMsg;
				if ( b == 0 )
				{
					b = bc;
					errorMsg = errMsg;
				}
			}
			catch (runtime_error&)
			{
				errorMsg = "Lost connection to DMLProc";
				b = 1;
				delete ci->dmlProc;
				ci->dmlProc = NULL;
			}
			catch (...)
			{
				errorMsg = "Unknown error caught";
				b = 1;
			}
		}
	}

	//@Bug 2241 Display an error message to user

	if ( ( b != 0 ) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
	{
		//@Bug 2540. Set error status instead of warning
        thd->raise_error_printf(ER_INTERNAL_ERROR, errorMsg.c_str());
		ci->rc = b;
        thd->set_row_count_func(0);
        thd->get_stmt_da()->set_overwrite_status(true);
		//cout << " error status " << ci->rc << endl;
	}
    if (b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
    {
        if (thd->is_strict_mode())
        {
            thd->set_row_count_func(0);
            ci->rc = b;
            // Turn this on as MariaDB doesn't do it until the next phase
            thd->abort_on_warning= thd->is_strict_mode();
        }
        else
        {
            thd->set_row_count_func(dmlRowCount+thd->get_row_count_func());
        }
        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, ER_WARN_DATA_OUT_OF_RANGE, errorMsg.c_str());
    }
  	else
	{
//		if (dmlRowCount != 0) //Bug 5117. Handling self join.
			thd->set_row_count_func(dmlRowCount+thd->get_row_count_func());


		//cout << " error status " << ci->rc << " and rowcount = " << dmlRowCount << endl;
	}

	// @bug 4027. comment out the following because this will cause mysql
	// kernel assertion failure. not sure why this is here in the first place.
//	if (thd->derived_tables)
//	{
//      thd->get_stmt_da()->set_overwrite_status(true);
//		thd->get_stmt_da()->set_ok_status( thd, dmlRowCount, 0, "");
//	}
	// insert query stats
	ci->stats.setEndTime();
	try
	{
		ci->stats.insert();
	}
	catch (std::exception& e)
	{
		string msg = string("Columnstore Query Stats - ") + e.what();
		push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
	}
	delete ci->dmlProc;
	ci->dmlProc = NULL;
	return 0;
}

} //anon namespace

extern "C"
{
#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgetstats(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	unsigned long l = ci->queryStats.size();
	if (l == 0)
	{
		*is_null = 1;
		return 0;
	}
	if (l > 255) l = 255;
	memcpy(result, ci->queryStats.c_str(), l);
	*length = l;
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgetstats_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETSTATS() takes no arguments");
		return 1;
	}
	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgetstats_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long calsettrace(UDF_INIT* initid, UDF_ARGS* args,
							char* is_null, char* error)
{
	THD* thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	long long oldTrace = ci->traceFlags;
	ci->traceFlags = (uint32_t)(*((long long*)args->args[0]));
	// keep the vtablemode bit
	ci->traceFlags |= (oldTrace & CalpontSelectExecutionPlan::TRACE_TUPLE_OFF);
	ci->traceFlags |= (oldTrace & CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH);
	return oldTrace;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calsettrace_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1 || args->arg_type[0] != INT_RESULT)
	{
		strcpy(message,"CALSETTRACE() requires one INTEGER argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calsettrace_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
// Return 1 if system is ready for reads or 0 if not.
long long mcssystemready(UDF_INIT* initid, UDF_ARGS* args,
                        char* is_null, char* error)
{
    long long rtn = 0;
    Oam oam;
    DBRM dbrm(true);
    SystemStatus systemstatus;

    try
    {
        oam.getSystemStatus(systemstatus);
        if (systemstatus.SystemOpState == ACTIVE
            && dbrm.getSystemReady()
            && dbrm.getSystemQueryReady())
        {
            return 1;
        }
    }
    catch (...)
    {
        *error = 1;
    }
    return rtn;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool mcssystemready_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
    return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void mcssystemready_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
// Return non-zero if system is read only; 0 if writeable
long long mcssystemreadonly(UDF_INIT* initid, UDF_ARGS* args,
                            char* is_null, char* error)
{
    long long rtn = 0;
    DBRM dbrm(true);

    try
    {
        if (dbrm.getSystemSuspended())
        {
            rtn = 1;
        }
        if (dbrm.isReadWrite() > 0) // Returns 0 for writable, 5 for read only
        {
            rtn = 2;
        }
    }
    catch (...)
    {
        *error = 1;
        rtn = 1;
    }
    return rtn;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool mcssystemreadonly_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
    return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void mcssystemreadonly_deinit(UDF_INIT* initid)
{
}

#define MAXSTRINGLENGTH 50

const char* PmSmallSideMaxMemory = "pmmaxmemorysmallside";

const char* SetParmsPrelude = "Updated ";
const char* SetParmsError = "Invalid parameter: ";
const char* InvalidParmSize = "Invalid parameter size: Input value cannot be larger than ";

const size_t Plen = strlen(SetParmsPrelude);
const size_t Elen = strlen(SetParmsError);

const char* invalidParmSizeMessage(uint64_t size, size_t& len)
{
    static char str[sizeof(InvalidParmSize) + 12] = {0};
	ostringstream os;
	os << InvalidParmSize << size;
	len = os.str().length();
    strcpy(str, os.str().c_str());
	return str;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calsetparms(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	char parameter[MAXSTRINGLENGTH];
	char valuestr[MAXSTRINGLENGTH];
	size_t plen = args->lengths[0];
	size_t vlen = args->lengths[1];

  	memcpy(parameter,args->args[0], plen);
  	memcpy(valuestr,args->args[1], vlen);

	parameter[plen] = '\0';
	valuestr[vlen] = '\0';

	uint64_t value = Config::uFromText(valuestr);

	THD* thd = current_thd;
	uint32_t sessionID = tid2sid(thd->thread_id);

	const char* msg = SetParmsError;
	size_t mlen = Elen;
	bool includeInput = true;

	string pstr(parameter);
	algorithm::to_lower(pstr);
	if (pstr == PmSmallSideMaxMemory)
	{
	  joblist::ResourceManager *rm = joblist::ResourceManager::instance();
		if (rm->getHjTotalUmMaxMemorySmallSide() >= value)
		{
			rmParms.push_back(RMParam(sessionID, execplan::PMSMALLSIDEMEMORY, value));

			msg = SetParmsPrelude;
			mlen = Plen;
		}
		else
		{
			msg = invalidParmSizeMessage(rm->getHjTotalUmMaxMemorySmallSide(), mlen);
			includeInput = false;
		}
	}

	memcpy(result, msg, mlen);
	if (includeInput)
	{
		memcpy(result + mlen , parameter, plen);
		mlen += plen;
		memcpy(result + mlen++, " ", 1);
		memcpy(result + mlen, valuestr, vlen);
		*length = mlen + vlen;
	}
	else
		*length = mlen;
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calsetparms_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 2 || args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)
	{
		strcpy(message,"CALSETPARMS() requires two string arguments");
		return 1;
	}
	initid->max_length=MAXSTRINGLENGTH;

	char valuestr[MAXSTRINGLENGTH];
	size_t vlen = args->lengths[1];

  	memcpy(valuestr,args->args[1], vlen--);

	for (size_t i = 0; i < vlen; ++i)
		if (!isdigit(valuestr[i]))
		{
			strcpy(message,"CALSETPARMS() second argument must be numeric or end in G, M or K");
			return 1;
		}

	if (!isdigit(valuestr[vlen]))
	{
		switch (valuestr[vlen])
		{
		case 'G':
		case 'g':
		case 'M':
		case 'm':
		case 'K':
		case 'k':
		case '\0':
			break;
		default:
			strcpy(message,"CALSETPARMS() second argument must be numeric or end in G, M or K");
			return 1;
		}
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calsetparms_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calviewtablelock_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 2 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT))
	{
		strcpy(message,"CALVIEWTABLELOCK() requires two string arguments");
		return 1;
	}
	else if ((args->arg_count == 1) && (args->arg_type[0] != STRING_RESULT ) )
	{
		strcpy(message,"CALVIEWTABLELOCK() requires one string argument");
		return 1;
	}
	else if (args->arg_count > 2 )
	{
		strcpy(message,"CALVIEWTABLELOCK() takes one or two arguments only");
		return 1;
	}
	else if (args->arg_count == 0 )
	{
		strcpy(message,"CALVIEWTABLELOCK() requires at least one argument");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calviewtablelock(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);		CalpontSystemCatalog::TableName tableName;
	if ( args->arg_count == 2 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
	}
	else if ( args->arg_count == 1 )
	{
		tableName.table = args->args[0];
		if (thd->db)
			tableName.schema = thd->db;
		else
		{
			string msg("No schema information provided");
			memcpy(result,msg.c_str(), msg.length());
			*length = msg.length();
			return result;
		}
	}
	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "viewtablelock starts a new client " << ci->dmlProc << " for session " << thd->thread_id << endl;
	}

	string lockinfo = ha_calpont_impl_viewtablelock(*ci, tableName);

	memcpy(result,lockinfo.c_str(), lockinfo.length());
	*length = lockinfo.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calviewtablelock_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calcleartablelock_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if ((args->arg_count != 1) || (args->arg_type[0] != INT_RESULT))
	{
		strcpy(message,
			"CALCLEARTABLELOCK() requires one integer argument (the lockID)");
		return 1;
	}
	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calcleartablelock(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());

	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(
		thd->infinidb_vtable.cal_conn_info);
	long long lockID = *reinterpret_cast<long long*>(args->args[0]);

	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "cleartablelock starts a new client " << ci->dmlProc << " for session " << thd->thread_id << endl;
	}

	unsigned long long uLockID = lockID;
	string lockinfo = ha_calpont_impl_cleartablelock(*ci, uLockID);

	memcpy(result,lockinfo.c_str(), lockinfo.length());
	*length = lockinfo.length();
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calcleartablelock_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool callastinsertid_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count == 2 && (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT))
	{
		strcpy(message,"CALLASTINSRTID() requires two string arguments");
		return 1;
	}
	else if ((args->arg_count == 1) && (args->arg_type[0] != STRING_RESULT ) )
	{
		strcpy(message,"CALLASTINSERTID() requires one string argument");
		return 1;
	}
	else if (args->arg_count > 2 )
	{
		strcpy(message,"CALLASTINSERTID() takes one or two arguments only");
		return 1;
	}
	else if (args->arg_count == 0 )
	{
		strcpy(message,"CALLASTINSERTID() requires at least one argument");
		return 1;
	}

	initid->maybe_null = 1;
	initid->max_length = 255;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long callastinsertid(UDF_INIT* initid, UDF_ARGS* args,
					char* is_null, char* error)
{
	THD* thd = current_thd;

	CalpontSystemCatalog::TableName tableName;
	uint64_t nextVal = 0;
	if ( args->arg_count == 2 )
	{
		tableName.schema = args->args[0];
		tableName.table = args->args[1];
	}
	else if ( args->arg_count == 1 )
	{
		tableName.table = args->args[0];
		if (thd->db)
			tableName.schema = thd->db;
		else
		{
			return -1;
		}
	}

	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(thd->thread_id));
	csc->identity(execplan::CalpontSystemCatalog::FE);

	try
	{
		nextVal = csc->nextAutoIncrValue(tableName);
	}
	catch (std::exception&)
	{
		string msg("No such table found during autincrement");
		setError(thd, ER_INTERNAL_ERROR, msg);
		return nextVal;
	}

	if (nextVal == AUTOINCR_SATURATED)
	{
		setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_EXCEED_LIMIT));
		return nextVal;
	}
	//@Bug 3559. Return a message for table without autoincrement column.
	if (nextVal == 0)
	{
		string msg("Autoincrement does not exist for this table.");
		setError(thd, ER_INTERNAL_ERROR, msg);
		return nextVal;
	}

	return (nextVal-1);
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void callastinsertid_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calflushcache_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long calflushcache(UDF_INIT* initid, UDF_ARGS* args,
							char* is_null, char* error)
{
	return static_cast<long long>(cacheutils::flushPrimProcCache());
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calflushcache_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALFLUSHCACHE() takes no arguments");
		return 1;
	}

	return 0;
}

static const unsigned long TraceSize = 16 * 1024;

//mysqld will call this with only 766 bytes available in result no matter what we asked for in calgettrace_init()
// if we return a pointer that is not result, mysqld will take our pointer and use it, freeing up result
#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgettrace(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	const string* msgp;
	int flags = 0;
	if (args->arg_count > 0)
	{
		if (args->arg_type[0] == INT_RESULT)
		{
			flags = *reinterpret_cast<int*>(args->args[0]);
		}
	}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (flags > 0)
		//msgp = &connMap[sessionID].extendedStats;
		msgp = &ci->extendedStats;
	else
		//msgp = &connMap[sessionID].miniStats;
		msgp = &ci->miniStats;
	unsigned long l = msgp->size();
	if (l == 0)
	{
		*is_null = 1;
		return 0;
	}
	if (l > TraceSize) l = TraceSize;
	*length = l;
	return msgp->c_str();
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgettrace_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
#if 0
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETTRACE() takes no arguments");
		return 1;
	}
#endif
	initid->maybe_null = 1;
	initid->max_length = TraceSize;

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgettrace_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgetversion(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	string version(columnstore_version);
	*length = version.size();
	memcpy(result, version.c_str(), *length);
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgetversion_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETVERSION() takes no arguments");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgetversion_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* calgetsqlcount(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	idbassert(ci != 0);

	MessageQueueClient* mqc = 0;
	mqc = new MessageQueueClient("ExeMgr1");

	ByteStream msg;
	ByteStream::quadbyte runningSql, waitingSql;
	ByteStream::quadbyte qb = 5;
	msg << qb;
	mqc->write(msg);

	//get ExeMgr response
	msg.restart();
	msg = mqc->read();
	if (msg.length() == 0)
	{
		memcpy(result, "Lost connection to ExeMgr", *length);
		return result;
	}
	msg >> runningSql;
	msg >> waitingSql;
	delete mqc;
 
	char ans[128];
	sprintf(ans, "Running SQL statements %d, Waiting SQL statments %d", runningSql, waitingSql);
	*length = strlen(ans);
	memcpy(result, ans, *length);
	return result;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool calgetsqlcount_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"CALGETSQLCOUNT() takes no arguments");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void calgetsqlcount_deinit(UDF_INIT* initid)
{
}


} //extern "C"

int ha_calpont_impl_open(const char *name, int mode, uint32_t test_if_locked)
{
	IDEBUG ( cout << "ha_calpont_impl_open: " << name << ", " << mode << ", " << test_if_locked << endl );
	Config::makeConfig();
	return 0;
}

int ha_calpont_impl_close(void)
{
	IDEBUG( cout << "ha_calpont_impl_close" << endl );
	return 0;
}

int ha_calpont_impl_discover_existence(const char *schema, const char *name)
{
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog();
	try
	{
		const CalpontSystemCatalog::OID oid = csc->lookupTableOID(make_table(schema, name));
		if (oid)
			return 1;
	}
	catch ( ... )
	{
	}
	return 0;
}

int ha_calpont_impl_rnd_init(TABLE* table)
{
#ifdef DEBUG_SETENV
	string home(getenv("HOME"));
	if (!getenv("CALPONT_HOME"))
	{
		string calpontHome(home + "/Calpont/etc/");
		setenv("CALPONT_HOME", calpontHome.c_str(), 1);
	}

	if (!getenv("CALPONT_CONFIG_FILE"))
	{
		string calpontConfigFile(home + "/Calpont/etc/Columnstore.xml");
		setenv("CALPONT_CONFIG_FILE", calpontConfigFile.c_str(), 1);
	}

	if (!getenv("CALPONT_CSC_IDENT"))
		setenv("CALPONT_CSC_IDENT", "dm", 1);
#endif

	IDEBUG( cout << "rnd_init for table " << table->s->table_name.str << endl );
	THD* thd = current_thd;

	//check whether the system is ready to process statement.
#ifndef _MSC_VER
	static DBRM dbrm(true);
	bool bSystemQueryReady = dbrm.getSystemQueryReady();
    if (bSystemQueryReady == 0)
	{
		// Still not ready
		setError(thd, ER_INTERNAL_ERROR, "The system is not yet ready to accept queries");
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR;
		return ER_INTERNAL_ERROR;
	}
	else
	if (bSystemQueryReady < 0)
	{
		// Still not ready
		setError(thd, ER_INTERNAL_ERROR, "DBRM is not responding. Cannot accept queries");
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR;
		return ER_INTERNAL_ERROR;
	}
#endif
	// prevent "create table as select" from running on slave
	thd->infinidb_vtable.hasInfiniDBTable = true;

	/* If this node is the slave, ignore DML to IDB tables */
	if (thd->slave_thread && (
	    thd->lex->sql_command == SQLCOM_INSERT ||
	    thd->lex->sql_command == SQLCOM_INSERT_SELECT ||
	    thd->lex->sql_command == SQLCOM_UPDATE ||
	    thd->lex->sql_command == SQLCOM_UPDATE_MULTI ||
	    thd->lex->sql_command == SQLCOM_DELETE ||
	    thd->lex->sql_command == SQLCOM_DELETE_MULTI ||
	    thd->lex->sql_command == SQLCOM_TRUNCATE ||
	    thd->lex->sql_command == SQLCOM_LOAD))
		return 0;

	// @bug 3005. if the table is not $vtable, then this could be a UDF defined on the connector.
	// watch this for other complications
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE &&
	    string(table->s->table_name.str).find("$vtable") != 0)
		return 0;

	// return error is error status is already set
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR)
		return ER_INTERNAL_ERROR;

	// by pass the extra union trips. return 0
	if (thd->infinidb_vtable.isUnion && thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		return 0;

	// @bug 2232. Basic SP support. Error out non support sp cases.
	// @bug 3939. Only error out for sp with select. Let pass for alter table in sp.
	if (thd->infinidb_vtable.call_sp && (thd->lex)->sql_command != SQLCOM_ALTER_TABLE)
	{
		setError(thd, ER_CHECK_NOT_IMPLEMENTED, "This stored procedure syntax is not supported by Columnstore in this version");
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR;
		return ER_INTERNAL_ERROR;
	}

	// mysql reads table twice for order by
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_PHASE1 ||
		thd->infinidb_vtable.vtable_state == THD::INFINIDB_ORDER_BY)
		return 0;

	if ( (thd->lex)->sql_command == SQLCOM_ALTER_TABLE )
		return 0;

	//Update and delete code
	if ( ((thd->lex)->sql_command == SQLCOM_UPDATE)  || ((thd->lex)->sql_command == SQLCOM_DELETE) || ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
		return doUpdateDelete(thd);

	uint32_t sessionID = tid2sid(thd->thread_id);
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->identity(CalpontSystemCatalog::FE);

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	idbassert(ci != 0);

	// MySQL sometimes calls rnd_init multiple times, plan should only be
	// generated and sent once.
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE &&
	    !thd->infinidb_vtable.isNewQuery)
		return 0;

	if(thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before closing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// canceling query. ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		return 0;
	}

	sm::tableid_t tableid = 0;
	cal_table_info ti;
	sm::cpsm_conhdl_t* hndl;
	SCSEP csep;

	// update traceFlags according to the autoswitch state. replication query
	// on slave are in table mode (create table as...)
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE ||
		(thd->slave_thread && thd->infinidb_vtable.vtable_state == THD::INFINIDB_INIT))
	{
		ci->traceFlags |= CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_DISABLE_VTABLE;
	}
	else
	{
		ci->traceFlags = (ci->traceFlags | CalpontSelectExecutionPlan::TRACE_TUPLE_OFF)^
		                  CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
	}

	bool localQuery = (thd->variables.infinidb_local_query>0 ? true : false);

	// table mode
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
	{
		ti = ci->tableMap[table];

		// get connection handle for this table handler
		// re-establish table handle
		if (ti.conn_hndl)
		{
			sm::sm_cleanup(ti.conn_hndl);
			ti.conn_hndl = 0;
		}

		sm::sm_init(sessionID, &ti.conn_hndl, localQuery);
		ti.conn_hndl->csc = csc;
		hndl = ti.conn_hndl;

		try {
			ti.conn_hndl->connect();
		} catch (...) {
			setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto error;
		}

		// get filter plan for table
		if (ti.csep.get() == 0)
		{
			ti.csep.reset(new CalpontSelectExecutionPlan());

			SessionManager sm;
			BRM::TxnID txnID;
			txnID = sm.getTxnID(sessionID);
			if (!txnID.valid)
			{
				txnID.id = 0;
				txnID.valid = true;
			}
			QueryContext verID;
			verID = sm.verID();

			ti.csep->txnID(txnID.id);
			ti.csep->verID(verID);
			ti.csep->sessionID(sessionID);
			if (thd->db)
				ti.csep->schemaName(thd->db);
			ti.csep->traceFlags(ci->traceFlags);
			ti.msTablePtr = table;

			// send plan whenever rnd_init is called
			cp_get_table_plan(thd, ti.csep, ti);
		}

		IDEBUG( cerr << table->s->table_name.str << " send plan:" << endl );
		IDEBUG( cerr << *ti.csep << endl );
		csep = ti.csep;

		// for ExeMgr logging sqltext. only log once for the query although multi plans may be sent
		if (ci->tableMap.size() == 1)
			ti.csep->data(idb_mysql_query_str(thd));
	}
	// vtable mode
	else
	{
		//if (!ci->cal_conn_hndl || thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE)
		{
			ci->stats.reset(); // reset query stats
			ci->stats.setStartTime();
			ci->stats.fUser = thd->main_security_ctx.user;
			if (thd->main_security_ctx.host)
				ci->stats.fHost = thd->main_security_ctx.host;
			else if (thd->main_security_ctx.host_or_ip)
				ci->stats.fHost = thd->main_security_ctx.host_or_ip;
			else
				ci->stats.fHost = "unknown";
			try {
				ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
			} catch (std::exception& e)
			{
				string msg = string("Columnstore User Priority - ") + e.what();
				ci->warningMsg = msg;
			}

			// if the previous query has error, re-establish the connection
			if (ci->queryState != 0)
			{
				sm::sm_cleanup(ci->cal_conn_hndl);
				ci->cal_conn_hndl = 0;
			}
		}

		sm::sm_init(sessionID, &ci->cal_conn_hndl, localQuery);
		idbassert(ci->cal_conn_hndl != 0);
		ci->cal_conn_hndl->csc = csc;
		idbassert(ci->cal_conn_hndl->exeMgr != 0);

		try {
			ci->cal_conn_hndl->connect();
		} catch (...) {
			setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto error;
		}
		hndl = ci->cal_conn_hndl;

		if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_SELECT_VTABLE)
		{
			//CalpontSelectExecutionPlan csep;
			if (!csep)
				csep.reset(new CalpontSelectExecutionPlan());

			SessionManager sm;
			BRM::TxnID txnID;
			txnID = sm.getTxnID(sessionID);
			if (!txnID.valid)
			{
				txnID.id = 0;
				txnID.valid = true;
			}
			QueryContext verID;
			verID = sm.verID();

			csep->txnID(txnID.id);
			csep->verID(verID);
			csep->sessionID(sessionID);
			if (thd->db)
				csep->schemaName(thd->db);

			csep->traceFlags(ci->traceFlags);
			if (thd->infinidb_vtable.isInsertSelect)
				csep->queryType(CalpontSelectExecutionPlan::INSERT_SELECT);

			//get plan
			int status = cp_get_plan(thd, csep);
			//if (cp_get_plan(thd, csep) != 0)
			if (status > 0)
				goto internal_error;
			else if (status < 0)
				return 0;

			// @bug 2547. don't need to send the plan if it's impossible where for all unions.
			if (thd->infinidb_vtable.impossibleWhereOnUnion)
				return 0;
			string query;
			query.assign(thd->infinidb_vtable.original_query.ptr(),
							  thd->infinidb_vtable.original_query.length());
			csep->data(query);
			try {
				csep->priority(	ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
			}catch (std::exception& e)
			{
				string msg = string("Columnstore User Priority - ") + e.what();
				push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
			}

#ifdef PLAN_HEX_FILE
			// plan serialization
			ifstream ifs("/tmp/li1-plan.hex");
			ByteStream bs1;
			ifs >> bs1;
			ifs.close();
			csep->unserialize(bs1);
#endif

			if (ci->traceFlags & 1)
			{
				cerr << "---------------- EXECUTION PLAN ----------------" << endl;
				cerr << *csep << endl ;
				cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
			}
			else
			{
				IDEBUG( cout << "---------------- EXECUTION PLAN ----------------" << endl );
				IDEBUG( cerr << *csep << endl );
				IDEBUG( cout << "-------------- EXECUTION PLAN END --------------\n" << endl );
			}
		}
	}// end of execution plan generation

	if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_SELECT_VTABLE)
	{
		ByteStream msg;
		ByteStream emsgBs;

		while (true)
		{
			try {
				ByteStream::quadbyte qb = 4;
				msg << qb;
				hndl->exeMgr->write(msg);
				msg.restart();
				csep->rmParms(rmParms);

				//send plan
				csep->serialize(msg);
				hndl->exeMgr->write(msg);

				//get ExeMgr status back to indicate a vtable joblist success or not
				msg.restart();
				emsgBs.restart();
				msg = hndl->exeMgr->read();
				emsgBs = hndl->exeMgr->read();
				string emsg;

				if (msg.length() == 0 || emsgBs.length() == 0)
				{
					emsg = "Lost connection to ExeMgr. Please contact your administrator";
					setError(thd, ER_INTERNAL_ERROR, emsg);
					return ER_INTERNAL_ERROR;
				}
				string emsgStr;
				emsgBs >> emsgStr;
				bool err = false;

				if (msg.length() == 4)
				{
					msg >> qb;
					if (qb != 0)
					{
						err = true;
						// for makejoblist error, stats contains only error code and insert from here
						// because table fetch is not started
						ci->stats.setEndTime();
						ci->stats.fQuery = csep->data();
						ci->stats.fQueryType = csep->queryType();
						ci->stats.fErrorNo = qb;
						try {
							ci->stats.insert();
						} catch (std::exception& e)
						{
							string msg = string("Columnstore Query Stats - ") + e.what();
							push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
						}
					}
				}
				else
				{
					err = true;
				}
				if (err)
				{
					setError(thd, ER_INTERNAL_ERROR, emsgStr);
					return ER_INTERNAL_ERROR;
				}

				rmParms.clear();
				if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
				{
					ci->tableMap[table] = ti;
				}
				else
				{
					ci->queryState = 1;
				}
				break;
			} catch (...) {
				sm::sm_cleanup(hndl);
				hndl = 0;

				sm::sm_init(sessionID, &hndl, localQuery);
				idbassert(hndl != 0);
				hndl->csc = csc;
				if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
					ti.conn_hndl = hndl;
				else
					ci->cal_conn_hndl = hndl;
				try {
					hndl->connect();
				} catch (...) {
					setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
					CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
					goto error;
				}
				msg.restart();
			}
		}
	}

	// set query state to be in_process. Sometimes mysql calls rnd_init multiple
	// times, this makes sure plan only being generated and sent once. It will be
	// reset when query finishes in sm::end_query
    thd->infinidb_vtable.isNewQuery = false;

	// common path for both vtable select phase and table mode -- open scan handle
	ti = ci->tableMap[table];
	ti.msTablePtr = table;
	if ((thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE)||
	    (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE) ||
	    (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_QUERY))
	{
		if (ti.tpl_ctx == 0)
		{
			ti.tpl_ctx = new sm::cpsm_tplh_t();
			ti.tpl_scan_ctx = sm::sp_cpsm_tplsch_t(new sm::cpsm_tplsch_t());
		}

		// make sure rowgroup is null so the new meta data can be taken. This is for some case mysql
		// call rnd_init for a table more than once.
		ti.tpl_scan_ctx->rowGroup = NULL;

		try {
			tableid = execplan::IDB_VTABLE_ID;
		} catch (...) {
			string emsg = "No table ID found for table " + string(table->s->table_name.str);
			setError(thd, ER_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}

		try {
			sm::tpl_open(tableid, ti.tpl_ctx, hndl);
			sm::tpl_scan_open(tableid, ti.tpl_scan_ctx, hndl);
		} catch (std::exception& e)
		{
			string emsg = "table can not be opened: " + string(e.what());
			setError(thd, ER_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}
		catch (...)
		{
			string emsg = "table can not be opened";
			setError(thd, ER_INTERNAL_ERROR, emsg);
			CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
			goto internal_error;
		}

		ti.tpl_scan_ctx->traceFlags = ci->traceFlags;
		if ((ti.tpl_scan_ctx->ctp).size() == 0)
		{
			uint32_t num_attr = table->s->fields;
			for (uint32_t i=0; i < num_attr; i++)
			{
				CalpontSystemCatalog::ColType ctype;
				ti.tpl_scan_ctx->ctp.push_back(ctype);
			}

			// populate coltypes here for table mode because tableband gives treeoid for dictionary column
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
			{
				CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(make_table(table->s->db.str, table->s->table_name.str), true);
				if (oidlist.size() != num_attr)
				{
					string emsg = "Size mismatch probably caused by front end out of sync";
					setError(thd, ER_INTERNAL_ERROR, emsg);
					CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
					goto internal_error;
				}
				for (unsigned int j = 0; j < oidlist.size(); j++)
				{
					CalpontSystemCatalog::ColType ctype = csc->colType(oidlist[j].objnum);
					ti.tpl_scan_ctx->ctp[ctype.colPosition] = ctype;
					ti.tpl_scan_ctx->ctp[ctype.colPosition].colPosition = -1;
				}
			}
		}
	}

	ci->tableMap[table] = ti;
	return 0;

error:
	if (ci->cal_conn_hndl)
	{
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}
	// do we need to close all connection handle of the table map?
	return ER_INTERNAL_ERROR;

internal_error:
	if (ci->cal_conn_hndl)
	{
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}
	return ER_INTERNAL_ERROR;
}

int ha_calpont_impl_rnd_next(uchar *buf, TABLE* table)
{
	THD* thd = current_thd;

	/* If this node is the slave, ignore DML to IDB tables */
	if (thd->slave_thread && (
	    thd->lex->sql_command == SQLCOM_INSERT ||
	    thd->lex->sql_command == SQLCOM_INSERT_SELECT ||
	    thd->lex->sql_command == SQLCOM_UPDATE ||
	    thd->lex->sql_command == SQLCOM_UPDATE_MULTI ||
	    thd->lex->sql_command == SQLCOM_DELETE ||
	    thd->lex->sql_command == SQLCOM_DELETE_MULTI ||
	    thd->lex->sql_command == SQLCOM_TRUNCATE ||
	    thd->lex->sql_command == SQLCOM_LOAD))
		return 0;


	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR)
		return ER_INTERNAL_ERROR;
	// @bug 3005
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE &&
	    string(table->s->table_name.str).find("$vtable") != 0)
		return HA_ERR_END_OF_FILE;
	if (((thd->lex)->sql_command == SQLCOM_UPDATE)  || ((thd->lex)->sql_command == SQLCOM_DELETE) ||
	    ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) || ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
		return HA_ERR_END_OF_FILE;

	// @bug 2547
	if (thd->infinidb_vtable.impossibleWhereOnUnion)
		return HA_ERR_END_OF_FILE;

	// @bug 2232. Basic SP support
	// @bug 3939. Only error out for sp with select. Let pass for alter table in sp.
	if (thd->infinidb_vtable.call_sp && (thd->lex)->sql_command != SQLCOM_ALTER_TABLE)
	{
		setError(thd, ER_CHECK_NOT_IMPLEMENTED, "This stored procedure syntax is not supported by Columnstore in this version");
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_ERROR;
		return ER_INTERNAL_ERROR;
	}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	// @bug 3078
    if(thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// cancel query. ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		return 0;
	}

	if (ci->alterTableState > 0) return HA_ERR_END_OF_FILE;

	cal_table_info ti;
	ti = ci->tableMap[table];
	int rc = HA_ERR_END_OF_FILE;

	if (!ti.tpl_ctx || !ti.tpl_scan_ctx)
	{
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		return ER_INTERNAL_ERROR;
	}

	idbassert(ti.msTablePtr == table);

	try {
		rc = fetchNextRow(buf, ti, ci);
	} catch (std::exception& e)
	{
		string emsg = string("Error while fetching from ExeMgr: ") + e.what();
		setError(thd, ER_INTERNAL_ERROR, emsg);
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		return ER_INTERNAL_ERROR;
	}
	ci->tableMap[table] = ti;

	if (rc != 0 && rc != HA_ERR_END_OF_FILE)
	{
		string emsg;
		// remove this check when all error handling migrated to the new framework.
		if (rc >= 1000)
			emsg = ti.tpl_scan_ctx->errMsg;
		else
		{
			logging::ErrorCodes errorcodes;
			emsg = errorcodes.errorString(rc);
		}
		setError(thd, ER_INTERNAL_ERROR, emsg);
		//setError(thd, ER_INTERNAL_ERROR, "testing");
		ci->stats.fErrorNo = rc;
		CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
		rc = ER_INTERNAL_ERROR;
	}

	return rc;
}

int ha_calpont_impl_rnd_end(TABLE* table)
{
	int rc = 0;
	THD* thd = current_thd;
	cal_connection_info* ci = NULL;

	if (thd->slave_thread && (
	    thd->lex->sql_command == SQLCOM_INSERT ||
	    thd->lex->sql_command == SQLCOM_INSERT_SELECT ||
	    thd->lex->sql_command == SQLCOM_UPDATE ||
	    thd->lex->sql_command == SQLCOM_UPDATE_MULTI ||
	    thd->lex->sql_command == SQLCOM_DELETE ||
	    thd->lex->sql_command == SQLCOM_DELETE_MULTI ||
	    thd->lex->sql_command == SQLCOM_TRUNCATE ||
	    thd->lex->sql_command == SQLCOM_LOAD))
		return 0;

	thd->infinidb_vtable.isNewQuery = true;

	if (thd->infinidb_vtable.cal_conn_info)
		ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ORDER_BY )
	{
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_SELECT_VTABLE;	// flip back to normal state
		return rc;
	}
//	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_PHASE1)
//		return rc;

	if ( (thd->lex)->sql_command == SQLCOM_ALTER_TABLE )
		return rc;

	if (((thd->lex)->sql_command == SQLCOM_UPDATE)  ||
	    ((thd->lex)->sql_command == SQLCOM_DELETE) ||
	    ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI) ||
	    ((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI))
		return rc;

	if (((thd->lex)->sql_command == SQLCOM_INSERT) ||
	    ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) )
	{
		// @bug 4022. error handling for select part of dml
		if (ci->cal_conn_hndl && ci->rc)
		{
			// send ExeMgr a signal before closing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// this is error handling, so ignore connection failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
			return rc;
		}
	}

	if (!ci)
	{
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
		ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	}

	// @bug 3078. Also session limit variable works the same as ctrl+c
    if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD ||
	    ((thd->lex)->sql_command != SQLCOM_INSERT &&
	    (thd->lex)->sql_command != SQLCOM_INSERT_SELECT &&
	    thd->variables.select_limit != (uint64_t)-1))
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before closing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// this is the end of query. Ignore the exception if exemgr connection failed
				// for whatever reason.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
			// clear querystats because no query stats available for cancelled query
			ci->queryStats = "";
		}
		return 0;
	}

	IDEBUG( cerr << "rnd_end for table " << table->s->table_name.str << endl );

	cal_table_info ti = ci->tableMap[table];
	sm::cpsm_conhdl_t* hndl;

	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		hndl = ti.conn_hndl;
	else
		hndl = ci->cal_conn_hndl;

	if (ti.tpl_ctx)
	{
		if (ti.tpl_scan_ctx.get())
		{
			try {
				sm::tpl_scan_close(ti.tpl_scan_ctx);
			} catch (...) {
				rc = ER_INTERNAL_ERROR;
			}
		}
		ti.tpl_scan_ctx.reset();
		try {
			sm::tpl_close(ti.tpl_ctx, &hndl, ci->stats);
			// set conn hndl back. could be changed in tpl_close
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
				ti.conn_hndl = hndl;
			else
				ci->cal_conn_hndl = hndl;
			ti.tpl_ctx = 0;
		}	catch (IDBExcept& e)
		{
			if (e.errorCode() == ERR_CROSS_ENGINE_CONNECT || e.errorCode() == ERR_CROSS_ENGINE_CONFIG)
			{
				string msg = string("Columnstore Query Stats - ") + e.what();
				push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
			}
			else
			{
				setError(thd, ER_INTERNAL_ERROR, e.what());
				rc = ER_INTERNAL_ERROR;
			}
		}
		catch (std::exception& e) {
			setError(thd, ER_INTERNAL_ERROR, e.what());
			rc = ER_INTERNAL_ERROR;
		}
		catch (...)
		{
			setError(thd, ER_INTERNAL_ERROR, "Internal error throwed in rnd_end");
			rc = ER_INTERNAL_ERROR;
		}
	}
	ti.tpl_ctx = 0;

	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE &&
		  thd->infinidb_vtable.has_order_by)
	thd->infinidb_vtable.vtable_state = THD::INFINIDB_ORDER_BY;

	ci->tableMap[table] = ti;

	// push warnings from CREATE phase
	if (!ci->warningMsg.empty())
		push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, ci->warningMsg.c_str());
	ci->warningMsg.clear();
	// reset expressionId just in case
	ci->expressionId = 0;
	return rc;
}

int ha_calpont_impl_create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	THD *thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	// @bug1940 Do nothing for select query. Support of set default engine to IDB.
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE ||
	    string(name).find("@0024vtable") != string::npos)
	    return 0;

	//@Bug 1948. Mysql calls create table to create a new table with new signature.
	if (ci->alterTableState > 0) return 0;

	// Just to be sure
	if (!table_arg)
	{
		setError(thd, ER_INTERNAL_ERROR, "ha_calpont_impl_create_: table_arg is NULL");
		return 1;
	}
	if (!table_arg->s)
	{
		setError(thd, ER_INTERNAL_ERROR, "ha_calpont_impl_create_: table_arg->s is NULL");
		return 1;
	}

	int rc = ha_calpont_impl_create_(name, table_arg, create_info, *ci);

	return rc;
}

int ha_calpont_impl_delete_table(const char *name)
{
	THD *thd = current_thd;
	char *dbName = NULL;

	if (!name)
	{
		setError(thd, ER_INTERNAL_ERROR, "Drop Table with NULL name not permitted");
		return 1;
	}

	//if this is an InfiniDB tmp table ('#sql*.frm') just leave...
	if (!memcmp((uchar*)name, tmp_file_prefix, tmp_file_prefix_length)) return 0;

	// @bug1940 Do nothing for select query. Support of set default engine to IDB.
	if (string(name).find("@0024vtable") != string::npos)
	    return 0;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (!thd) return 0;
	if (!thd->lex) return 0;
	if (!idb_mysql_query_str(thd)) return 0;

	if (thd->lex->sql_command == SQLCOM_DROP_DB)
	{
		dbName = thd->lex->name.str;
	}
	else
	{
		TABLE_LIST *first_table= (TABLE_LIST*) thd->lex->select_lex.table_list.first;
		dbName = first_table->db;
	}

	if (!dbName)
	{
		setError(thd, ER_INTERNAL_ERROR, "Drop Table with NULL schema not permitted");
		return 1;
	}

	if (!ci) return 0;

	//@Bug 1948,2306. if alter table want to drop the old table, InfiniDB does not need to drop.
	if ( ci->isAlter ){
		 ci->isAlter=false;
		 return 0;
	}
	// @bug 1793. make vtable droppable in calpontsys. "$vtable" ==> "@0024vtable" passed in as name.
	if (strcmp(dbName, "calpontsys") == 0 && string(name).find("@0024vtable") == string::npos)
	{
		std::string stmt(idb_mysql_query_str(thd));
		algorithm::to_upper(stmt);
		//@Bug 2432. systables can be dropped with restrict
		if (stmt.find(" RESTRICT") != string::npos)
		{
			return 0;
		}
		setError(thd, ER_INTERNAL_ERROR, "Calpont system tables can only be dropped with restrict.");
		return 1;
	}

	int rc = ha_calpont_impl_delete_table_(dbName, name, *ci);
	return rc;
}
int ha_calpont_impl_write_row(uchar *buf, TABLE* table)
{
	THD *thd = current_thd;
	//sleep(100);
	// Error out INSERT on VIEW. It's currently not supported.
	// @note INSERT on VIEW works natually (for simple cases at least), but we choose to turn it off
	// for now - ZZ.
	TABLE_LIST* tables = thd->lex->query_tables;
	for (; tables; tables= tables->next_local)
	{
		if (tables->view)
		{
			Message::Args args;
			args.add("Insert");
			string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
			setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
			return ER_CHECK_NOT_IMPLEMENTED;
		}
	}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (thd->slave_thread) return 0;



	if (ci->alterTableState > 0) return 0;
	ha_rows rowsInserted = 0;
	int rc = 0;

	if ( (ci->useCpimport > 0) && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) && (!ci->singleInsert) && ((ci->isLoaddataInfile) ||
			((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
	    ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT)) )
	{
		rc = ha_calpont_impl_write_batch_row_(buf, table, *ci);
	}
	else
	{
		if ( !ci->dmlProc )
		{
			ci->dmlProc = new MessageQueueClient("DMLProc");
			//cout << "write_row starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
		}

		rc = ha_calpont_impl_write_row_(buf, table, *ci, rowsInserted);
			
	}

	//@Bug 2438 Added a variable rowsHaveInserted to keep track of how many rows have been inserted already.
	if ( !ci->singleInsert && ( rc == 0 ) && ( rowsInserted > 0 ))
	{
		ci->rowsHaveInserted += rowsInserted;
	}
	return rc;
}

int ha_calpont_impl_update_row()
{
	//@Bug 2540. Return the correct error code.
	THD *thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	int rc = ci->rc;
	if ( rc != 0 )
		ci->rc = 0;
	return ( rc );
}

int ha_calpont_impl_delete_row()
{
	//@Bug 2540. Return the correct error code.
	THD *thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	int rc = ci->rc;
	if ( rc !=0 )
		ci->rc = 0;

	return ( rc );
}

void ha_calpont_impl_start_bulk_insert(ha_rows rows, TABLE* table)
{
	THD *thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	// clear rows variable
	ci->rowsHaveInserted = 0;

	if (ci->alterTableState > 0) return;

	if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_ALTER_VTABLE)
		thd->infinidb_vtable.isInfiniDBDML = true;

	if (thd->slave_thread) return;

	//@bug 5660. Error out DDL/DML on slave node, or on local query node
	if (ci->isSlaveNode && thd->infinidb_vtable.vtable_state != THD::INFINIDB_ALTER_VTABLE)
	{
		string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
		setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
		return;
	}

	//@bug 4771. reject REPLACE key word
	if ((thd->lex)->sql_command == SQLCOM_REPLACE_SELECT)
	{
		setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, "REPLACE statement is not supported in Columnstore.");
	}

	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(thd->thread_id));
	csc->identity(execplan::CalpontSystemCatalog::FE);
	//@Bug 2515.
	//Check command instead of vtable state
	if ((thd->lex)->sql_command == SQLCOM_INSERT)
	{
		string insertStmt = idb_mysql_query_str(thd);
		algorithm::to_lower(insertStmt);
		string intoStr("into");
		size_t found = insertStmt.find(intoStr);
		if (found != string::npos)
			insertStmt.erase(found);

		found = insertStmt.find("ignore");
		if (found!=string::npos)
		{
			setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, "IGNORE option in insert statement is not supported in Columnstore.");
		}

		if ( rows > 1 )
		{
			ci->singleInsert = false;
		}
	}
	else if ( (thd->lex)->sql_command == SQLCOM_LOAD || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT)
	{
		ci->singleInsert = false;
		ci->isLoaddataInfile = true;
	}

	ci->bulkInsertRows = rows;
	if ((((thd->lex)->sql_command == SQLCOM_INSERT) ||
	    ((thd->lex)->sql_command == SQLCOM_LOAD) ||
	    (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) && !ci->singleInsert )
	{
    	ci->useCpimport = thd->variables.infinidb_use_import_for_batchinsert;

		if (((thd->lex)->sql_command == SQLCOM_INSERT) && (rows > 0))
			ci->useCpimport = 0;
			
		if ((ci->useCpimport > 0) && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))) //If autocommit on batch insert will use cpimport to load data
		{
			//store table info to connection info
			CalpontSystemCatalog::TableName tableName;
			tableName.schema = table->s->db.str;
			tableName.table = table->s->table_name.str;
			ci->useXbit = false;
			ci->utf8 = false;
			CalpontSystemCatalog::RIDList colrids;
			try {
				colrids = csc->columnRIDs(tableName);
			}
			catch (IDBExcept &ie) {
				// TODO Can't use ERR_UNKNOWN_TABLE because it needs two
				// arguments to format. Update setError to take vararg.
//				setError(thd, ER_UNKNOWN_TABLE, ie.what());
				setError(thd, ER_INTERNAL_ERROR, ie.what());
				ci->rc = 5;
				ci->singleInsert = true;
				return;
			}
			ci->useXbit = table->s->db_options_in_use & HA_OPTION_PACK_RECORD;
			//@bug 6122 Check how many columns have not null constraint. columnn with not null constraint will not show up in header.
			unsigned int numberNotNull = 0; 
			for (unsigned int j = 0; j < colrids.size(); j++)
			{
				CalpontSystemCatalog::ColType ctype = csc->colType(colrids[j].objnum);
				ci->columnTypes.push_back(ctype);
				if ((( ctype.colDataType == CalpontSystemCatalog::VARCHAR ) || ( ctype.colDataType == CalpontSystemCatalog::VARBINARY )) && !ci->useXbit )
					ci->useXbit = true;
				if (ctype.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
					numberNotNull++;
			}

			// The length of the record header is:(1 + number of columns + 7) / 8 bytes
			if (ci->useXbit)
				ci->headerLength = (1 + colrids.size() + 7 - 1-numberNotNull) /8; //xbit is used
			else
				ci->headerLength = (1 + colrids.size() + 7 - numberNotNull) /8;

			if ((strncmp(table->s->table_charset->comment, "UTF-8", 5) == 0) || (strncmp(table->s->table_charset->comment, "utf-8", 5) == 0))
			{
				ci->utf8 = true;
			}
			//Log the statement to debug.log
			LoggingID logid( 24, tid2sid(thd->thread_id), 0);
			logging::Message::Args args1;
			logging::Message msg(1);
			args1.add("Start SQL statement: ");
			ostringstream oss;
			oss << idb_mysql_query_str(thd) << "; |" << table->s->db.str<<"|";
			args1.add(oss.str());
			msg.format( args1 );
			Logger logger(logid.fSubsysID);
			logger.logMessage(LOG_TYPE_DEBUG, msg, logid);

			//start process cpimport mode 1
			ci->mysqld_pid = getpid();
			
			//get delimiter
			if (char(thd->variables.infinidb_import_for_batchinsert_delimiter) != '\007')
				ci->delimiter = char(thd->variables.infinidb_import_for_batchinsert_delimiter);
			else
				ci->delimiter = '\007';
			//get enclosed by
			if (char(thd->variables.infinidb_import_for_batchinsert_enclosed_by) != 8)
				ci->enclosed_by = char(thd->variables.infinidb_import_for_batchinsert_enclosed_by);
			else
				ci->enclosed_by = 8;
			//cout << "current set up is usecpimport:delimiter = " << (int)ci->useCpimport<<":"<<	ci->delimiter <<endl;
			//set up for cpimport
			std::vector<char*> Cmds;
			std::string aCmdLine(startup::StartUp::installDir());
			//If local module type is not PM and Local PM query is set, error out
			char escapechar[2] = "";
			if (ci->enclosed_by == 34)	// Double quotes
				strcat(escapechar, "\\");
			if (thd->variables.infinidb_local_query > 0  )
			{
				OamCache * oamcache = OamCache::makeOamCache();
				int localModuleId = oamcache->getLocalPMId();
				if (localModuleId == 0)
				{
					setError(current_thd, ER_INTERNAL_ERROR, logging::IDBErrorInfo::instance()->errorMsg(ERR_LOCAL_QUERY_UM));
					ci->singleInsert = true;
					LoggingID logid( 24, tid2sid(thd->thread_id), 0);
					logging::Message::Args args1;
					logging::Message msg(1);
					args1.add("End SQL statement");
					msg.format( args1 );
					Logger logger(logid.fSubsysID);
					logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
					return;
				}
				else
				{
#ifdef _MSC_VER
					aCmdLine = aCmdLine + "/bin/cpimport.exe -N -P " + to_string(localModuleId)+ " -s " + ci->delimiter + " -e 0" + " -E " + escapechar + ci->enclosed_by + " ";
#else
					aCmdLine = aCmdLine + "/bin/cpimport -m 1 -N -P " + to_string(localModuleId)+ " -s " + ci->delimiter + " -e 0" + " -E " + escapechar + ci->enclosed_by + " ";
#endif
				}
			}
			else
			{
#ifdef _MSC_VER
				aCmdLine = aCmdLine + "/bin/cpimport.exe -N -s " + ci->delimiter + " -e 0" + " -E " + escapechar + ci->enclosed_by + " ";
#else
				aCmdLine = aCmdLine + "/bin/cpimport -m 1 -N -s " + ci->delimiter + " -e 0" + " -E " + escapechar + ci->enclosed_by + " ";
#endif					
			}
			aCmdLine = aCmdLine + table->s->db.str + " " + table->s->table_name.str ;

			//cout << "aCmdLine = " << aCmdLine << endl;
			std::istringstream ss(aCmdLine);
			std::string arg;
			std::vector<std::string> v2(20, "");
            unsigned int i = 0;
			while (ss >> arg)
			{
                v2[i++] = arg;
            }
            for (unsigned int j = 0; j < i; ++j)
            {
                Cmds.push_back(const_cast<char*>(v2[j].c_str()));
            }

			Cmds.push_back(0); //null terminate
				
#ifdef _MSC_VER
			BOOL bSuccess = false; 
			BOOL bInitialized = false;
		    SECURITY_ATTRIBUTES saAttr; 
			saAttr.nLength = sizeof(SECURITY_ATTRIBUTES); 
			saAttr.bInheritHandle = TRUE; 
			saAttr.lpSecurityDescriptor = NULL; 
			HANDLE handleList[2];
			const char* pSectionMsg;
			// Create a pipe for the child process's STDOUT. 
#if 0       // We don't need stdout to come back right now.
			pSectionMsg = "Create Stdout";
			bSuccess = CreatePipe(&ci->cpimport_stdout_Rd, &ci->cpimport_stdout_Wr, &saAttr, 0);
			// Ensure the read handle to the pipe for STDIN is not inherited. 
			if (bSuccess)
			{
				pSectionMsg = "SetHandleInformation(stdout)";
 				bSuccess = SetHandleInformation(ci->cpimport_stdout_Rd, HANDLE_FLAG_INHERIT, 0);
			}
#endif     
            bSuccess = true;
			// Create a pipe for the child process's STDIN. 
			if (bSuccess)
			{
				pSectionMsg = "Create Stdin";
				bSuccess = CreatePipe(&ci->cpimport_stdin_Rd, &ci->cpimport_stdin_Wr, &saAttr, 65536);
				// Ensure the write handle to the pipe for STDIN is not inherited. 
				if (bSuccess)
				{
					pSectionMsg = "SetHandleInformation(stdin)";
 					bSuccess = SetHandleInformation(ci->cpimport_stdin_Wr, HANDLE_FLAG_INHERIT, 0);
				}
			}
			// Launch cpimport
			LPPROC_THREAD_ATTRIBUTE_LIST lpAttributeList = NULL;
			SIZE_T attrSize = 0;
			STARTUPINFOEX siStartInfo;
			// To ensure the child only inherits the STDIN and STDOUT Handles, we add a list of 
			// Handles that can be inherited to the call to CreateProcess
			if (bSuccess)
			{
				pSectionMsg = "InitializeProcThreadAttributeList(NULL)";
			    bSuccess = InitializeProcThreadAttributeList(NULL, 1, 0, &attrSize) ||
				           GetLastError() == ERROR_INSUFFICIENT_BUFFER; // Asks how much buffer to alloc
			}
			if (bSuccess)
			{
				pSectionMsg = "HeapAlloc for AttrList";
				lpAttributeList = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>
					                (HeapAlloc(GetProcessHeap(), 0, attrSize));
				bSuccess = lpAttributeList != NULL;
			}
			if (bSuccess) 
			{
				pSectionMsg = "InitializeProcThreadAttributeList";
			    bSuccess = InitializeProcThreadAttributeList(lpAttributeList, 1, 0, &attrSize);
			}
			if (bSuccess)
			{
   				pSectionMsg = "UpdateProcThreadAttribute";
				bInitialized = true;
				handleList[0] = ci->cpimport_stdin_Rd;
//				handleList[1] = ci->cpimport_stdout_Wr;
				bSuccess = UpdateProcThreadAttribute(lpAttributeList,		
                    0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
//                    handleList, 2*sizeof(HANDLE), NULL, NULL);
                    handleList, sizeof(HANDLE), NULL, NULL);
			}
			if (bSuccess)
			{
     			pSectionMsg = "CreateProcess";
                // In order for GenerateConsoleCtrlEvent (used when job is canceled) to work, 
                // this process must have a Console, which Services don't have. We create this 
                // when we create the child process. Once created, we leave it around for next time.
                // AllocConsole will silently fail if it already exists, so no pain.
                AllocConsole();
				// Set up members of the PROCESS_INFORMATION structure. 
 				memset(&ci->cpimportProcInfo, 0, sizeof(PROCESS_INFORMATION));
 
				// Set up members of the STARTUPINFOEX structure. 
				// This structure specifies the STDIN and STDOUT handles for redirection.
 				memset(&siStartInfo, 0, sizeof(STARTUPINFOEX));
				siStartInfo.StartupInfo.cb = sizeof(STARTUPINFOEX); 
				siStartInfo.lpAttributeList = lpAttributeList;
				siStartInfo.StartupInfo.hStdError = NULL;
				siStartInfo.StartupInfo.hStdOutput = NULL;
//				siStartInfo.StartupInfo.hStdError = ci->cpimport_stdout_Wr;
//				siStartInfo.StartupInfo.hStdOutput = ci->cpimport_stdout_Wr;
				siStartInfo.StartupInfo.hStdInput = ci->cpimport_stdin_Rd;
				siStartInfo.StartupInfo.dwFlags |= STARTF_USESTDHANDLES;
				// Create the child process. 
    			bSuccess = CreateProcess(NULL,         // program. NULL means use command line
										const_cast<LPSTR>(aCmdLine.c_str()), // command line 
										NULL,          // process security attributes 
										NULL,          // primary thread security attributes 
										TRUE,          // handles are inherited 
                                        EXTENDED_STARTUPINFO_PRESENT | CREATE_NEW_PROCESS_GROUP, // creation flags 
										NULL,          // use parent's environment 
										NULL,          // use parent's current directory 
										&siStartInfo.StartupInfo,  // STARTUPINFO pointer 
										&ci->cpimportProcInfo);  // receives PROCESS_INFORMATION 
   
			}
			// We need to clean up the memory created by InitializeProcThreadAttributeList
			// and HeapAlloc
			if (bInitialized) 
				DeleteProcThreadAttributeList(lpAttributeList);
			if (lpAttributeList)
				HeapFree(GetProcessHeap(), 0, lpAttributeList);

			if (!bSuccess)
			{
				// If an error occurs, Log and return. 
				int errnum = GetLastError();
				char errmsg[512];
				FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, errnum, 0, errmsg, 512, NULL);
				ostringstream oss;
				oss <<" : Error in " << pSectionMsg << " (errno-" <<
					errnum << "); " << errmsg;
				setError(current_thd, ER_INTERNAL_ERROR, oss.str());
				ci->singleInsert = true;
				LoggingID logid( 24, tid2sid(thd->thread_id), 0);
				logging::Message::Args args1, args2;
				logging::Message emsg(1), msg(1);
				args1.add(oss.str());
				emsg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_ERROR, emsg, logid);
				args2.add("End SQL statement");
				msg.format( args2 );
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				return;
			}
            // Close the read handle that the child is using. We won't be needing this.
            CloseHandle(ci->cpimport_stdin_Rd);
			// The write functions all want a FILE*
			ci->fdt[1] = _open_osfhandle((intptr_t)ci->cpimport_stdin_Wr, _O_APPEND);
			ci->filePtr = _fdopen(ci->fdt[1], "w");
#else
			long maxFD = -1;
			maxFD = sysconf(_SC_OPEN_MAX);
			if(pipe(ci->fdt)== -1)
			{
				int errnum = errno;
				ostringstream oss;
				oss <<" : Error in creating pipe (errno-" <<
					errnum << "); " << strerror(errnum);
				setError(current_thd, ER_INTERNAL_ERROR, oss.str());
				ci->singleInsert = true;
				LoggingID logid( 24, tid2sid(thd->thread_id), 0);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add("End SQL statement");
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				return;
			}
			//cout << "maxFD = " << maxFD <<endl;
			errno = 0;
			pid_t aChPid = fork();
			if(aChPid == -1)	//an error caused
			{
				int errnum = errno;
				ostringstream oss;
				oss << " : Error in forking cpimport.bin (errno-" <<
					errnum << "); " << strerror(errnum);
				setError(current_thd, ER_INTERNAL_ERROR, oss.str());
				ci->singleInsert = true;
				LoggingID logid( 24, tid2sid(thd->thread_id), 0);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add("End SQL statement");
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				return;
			}
			else if(aChPid == 0)// we are in child
			{
				for(int i=0;i< maxFD;i++)
				{
					if (i != ci->fdt[0])
						close(i);
				}
				errno = 0;
				if (dup2(ci->fdt[0], 0) < 0)	//make stdin be the reading end of the pipe
				{
					setError(current_thd, ER_INTERNAL_ERROR, "dup2 failed");
					ci->singleInsert = true;
					exit (1);
				}
				close(ci->fdt[0]);	// will trigger an EOF on stdin
				ci->fdt[0] = -1;
				open("/dev/null", O_WRONLY);
                open("/dev/null", O_WRONLY);
				errno = 0;
				execv(Cmds[0], &Cmds[0]);	//NOTE - works with full Path

				int execvErrno = errno;
				
				ostringstream oss;
				oss << " : execv error: cpimport.bin invocation failed; "
					<< "(errno-" << errno << "); " << strerror(execvErrno) <<
					"; Check file and try invoking locally.";
				cout << oss.str();
				
				setError(current_thd, ER_INTERNAL_ERROR, "Forking process cpimport failed.");
				ci->singleInsert = true;
				LoggingID logid( 24, tid2sid(thd->thread_id), 0);
				logging::Message::Args args1;
				logging::Message msg(1);
				args1.add("End SQL statement");
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				exit(1);
			}
			else	// parent
			{
				ci->filePtr = fdopen(ci->fdt[1], "w");
				ci->cpimport_pid = aChPid;	// This is the child PID
				//cout << "Child PID is " << aChPid << endl;
				close(ci->fdt[0]);	//close the READER of PARENT
				ci->fdt[0] = -1;
				// now we can send all the data thru FIFO[1], writer of PARENT
			}

			//if(aChPid == 0)
			//cout << "******** Child finished its work ********" << endl;
#endif
		}
		else
		{
			if(!ci->dmlProc)
			{
				ci->dmlProc = new MessageQueueClient("DMLProc");
				//cout << "start_bulk_insert starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
			}
		}
	}

	//Save table oid for commit to use
	if ( ( ((thd->lex)->sql_command == SQLCOM_INSERT) ||  ((thd->lex)->sql_command == SQLCOM_LOAD) || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) )
	{
		// query stats. only collect execution time and rows inserted for insert/load_data_infile
		ci->stats.reset();
		ci->stats.setStartTime();
		ci->stats.fUser = thd->main_security_ctx.user;
		if (thd->main_security_ctx.host)
			ci->stats.fHost = thd->main_security_ctx.host;
		else if (thd->main_security_ctx.host_or_ip)
			ci->stats.fHost = thd->main_security_ctx.host_or_ip;
		else
			ci->stats.fHost = "unknown";
		ci->stats.fSessionID = thd->thread_id;
		ci->stats.fQuery = idb_mysql_query_str(thd);
		try {
			ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
		} catch (std::exception& e)
		{
			string msg = string("Columnstore User Priority - ") + e.what();
			push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
		}

		if ((thd->lex)->sql_command == SQLCOM_INSERT)
			ci->stats.fQueryType = CalpontSelectExecutionPlan::queryTypeToString(CalpontSelectExecutionPlan::INSERT);
		else if ((thd->lex)->sql_command == SQLCOM_LOAD)
			ci->stats.fQueryType = CalpontSelectExecutionPlan::queryTypeToString(CalpontSelectExecutionPlan::LOAD_DATA_INFILE);

		//@Bug 4387. Check BRM status before start statement.
		scoped_ptr<DBRM> dbrmp(new DBRM());
		int rc = dbrmp->isReadWrite();
		if (rc != 0 )
		{
			setError(current_thd, ER_READ_ONLY_MODE, "Cannot execute the statement. DBRM is read only!");
			ci->rc = rc;
			ci->singleInsert = true;
			return;
		}
		uint32_t stateFlags;
		dbrmp->getSystemState(stateFlags);
	    if (stateFlags & SessionManagerServer::SS_SUSPENDED)
	    {
			setError(current_thd, ER_INTERNAL_ERROR, "Writing to the database is disabled.");
			return;
	    }

		CalpontSystemCatalog::TableName tableName;
		tableName.schema = table->s->db.str;
		tableName.table = table->s->table_name.str;
		try {
			CalpontSystemCatalog::ROPair roPair = csc->tableRID( tableName );
			ci->tableOid = roPair.objnum;
		}
		catch (IDBExcept &ie) {
			setError(thd, ER_INTERNAL_ERROR, ie.what());
//			setError(thd, ER_UNKNOWN_TABLE, ie.what());
		}
		catch (std::exception& ex) {
			setError(thd, ER_INTERNAL_ERROR,
					logging::IDBErrorInfo::instance()->errorMsg(ERR_SYSTEM_CATALOG) + ex.what());
		}
	}
	if ( ci->rc != 0 )
		ci->rc = 0;
}



int ha_calpont_impl_end_bulk_insert(bool abort, TABLE* table)
{
	THD *thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (thd->slave_thread) return 0;

	int rc = 0;
	if (ci->rc == 5) //read only dbrm
		return rc;

	// @bug 2378. do not enter for select, reset singleInsert flag after multiple insert.
	// @bug 2515. Check command intead of vtable state
	if ( ( ((thd->lex)->sql_command == SQLCOM_INSERT) ||  ((thd->lex)->sql_command == SQLCOM_LOAD) || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) && !ci->singleInsert )
	{

		//@Bug 2438. Only load data infile calls last batch process
/*		if ( ci->isLoaddataInfile && ((thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) || (ci->useCpimport == 0))) {
			//@Bug 2829 Handle ctrl-C
			if ( thd->killed > 0 )
				abort = true;

			if ( !ci->dmlProc )
			{
				ci->dmlProc = new MessageQueueClient("DMLProc");
				//cout << "end_bulk_insert starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
			}
			rc = ha_calpont_impl_write_last_batch(table, *ci, abort);
		}
	    else if ((ci->useCpimport > 0) && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) && (!ci->singleInsert) && ((ci->isLoaddataInfile) ||
		}  */
	   if ((ci->useCpimport > 0) && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) && (!ci->singleInsert) && ((ci->isLoaddataInfile) ||  
			((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
	    ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT)) )
	    {
#ifdef _MSC_VER
			if (thd->killed > 0)
			{
				errno = 0;
                // GenerateConsoleCtrlEvent sends a signal to cpimport
                BOOL brtn = GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, ci->cpimportProcInfo.dwProcessId);
                if (!brtn)
                {
    				int errnum = GetLastError();
    				char errmsg[512];
    				FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, errnum, 0, errmsg, 512, NULL);
	    			ostringstream oss;
		    		oss <<"GenerateConsoleCtrlEvent: (errno-" << errnum << "); " << errmsg;
			        LoggingID logid( 24, 0, 0);
			        logging::Message::Args args1;
			        logging::Message msg(1);
			        args1.add(oss.str());
			        msg.format( args1 );
			        Logger logger(logid.fSubsysID);
			        logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
                }
				// Close handles to the cpimport process and its primary thread.
				fclose (ci->filePtr);
				ci->filePtr = 0;
				ci->fdt[1] = -1;
				CloseHandle(ci->cpimportProcInfo.hProcess);
				CloseHandle(ci->cpimportProcInfo.hThread);
				WaitForSingleObject(ci->cpimportProcInfo.hProcess, INFINITE);
            }
#else
			if ( (thd->killed > 0) && (ci->cpimport_pid > 0) ) //handle CTRL-C
			{
				//cout << "sending ctrl-c to cpimport" << endl;
				errno = 0;
				kill( ci->cpimport_pid, SIGUSR1 );
				fclose (ci->filePtr);
				ci->filePtr = 0;
				ci->fdt[1] = -1;
				int aStatus;
				waitpid(ci->cpimport_pid, &aStatus, 0); // wait until cpimport finishs
			}
#endif
			else
			{
				//tear down cpimport
#ifdef _MSC_VER
				fclose (ci->filePtr);
				ci->filePtr = 0;
				ci->fdt[1] = -1;
				DWORD exitCode;
				WaitForSingleObject(ci->cpimportProcInfo.hProcess, INFINITE);
				GetExitCodeProcess(ci->cpimportProcInfo.hProcess, &exitCode);
				if (exitCode != 0)
				{
					rc = 1;
					setError(thd, ER_INTERNAL_ERROR, "load failed. The detailed error information is listed in InfiniDBLog.txt.");
				}
				// Close handles to the cpimport process and its primary thread.
				CloseHandle(ci->cpimportProcInfo.hProcess);
				CloseHandle(ci->cpimportProcInfo.hThread);
#else
				fclose (ci->filePtr);
				ci->filePtr = 0;
				ci->fdt[1] = -1;
				int aStatus;
				pid_t aPid = waitpid(ci->cpimport_pid, &aStatus, 0); // wait until cpimport finishs
				if ((aPid == ci->cpimport_pid)|| (aPid == -1))
				{
					ci->cpimport_pid = 0;
					
					if ((WIFEXITED(aStatus)) && (WEXITSTATUS(aStatus) == 0))
					{
						//cout << "\tCpimport exit on success" << endl;
					}
					else
					{
						if (WEXITSTATUS(aStatus) == 2)
						{
							rc = 1;
							ifstream dmlFile;
							ostringstream oss;
							oss << "/tmp/" <<ci->tableOid << ".txt";
							dmlFile.open(oss.str().c_str());
							if (dmlFile.is_open())
							{
								string line;
								getline(dmlFile, line);
								setError(thd, ER_INTERNAL_ERROR, line);
								dmlFile.close();
								remove (oss.str().c_str());
							}
						}
						else
						{
							rc = 1;
							ifstream dmlFile;
							ostringstream oss;
							oss << "/tmp/" <<ci->tableOid << ".txt";
							dmlFile.open(oss.str().c_str());
							if (dmlFile.is_open())
							{
								string line;
								getline(dmlFile, line);
								setError(thd, ER_INTERNAL_ERROR, line);
								dmlFile.close();
								remove (oss.str().c_str());
							}
							else
								setError(thd, ER_INTERNAL_ERROR, "load failed. The detailed error information is listed in err.log.");
						}
					}
				}
#endif
				LoggingID logid( 24, tid2sid(thd->thread_id), 0);
				logging::Message::Args args1;
				logging::Message msg(1);
				if ( rc == 0)
					args1.add("End SQL statement");
				else
					args1.add("End SQL statement with error");
					
				msg.format( args1 );
				Logger logger(logid.fSubsysID);
				logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
				ci->columnTypes.clear();
				//get extra warning count if any
				ifstream dmlFile;
				ostringstream oss;
				oss << "/tmp/" <<ci->tableOid << ".txt";
				dmlFile.open(oss.str().c_str());
				int totalWarnCount = 0;
				int colWarns = 0;
				string line;
				if (dmlFile.is_open())
				{
					while (getline(dmlFile, line))
					{
						colWarns = atoi(line.c_str());
						totalWarnCount += colWarns;
					}
					dmlFile.close();
					remove (oss.str().c_str());
					for (int i=0; i < totalWarnCount; i++)
						push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, "Values saturated");
				}
			}
		}
		else 
		{
			if ( thd->killed > 0 )
				abort = true;
			if ( !ci->dmlProc )
			{
				ci->dmlProc = new MessageQueueClient("DMLProc");
				//cout << "end_bulk_insert starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
			}
			if (((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) || ((thd->lex)->sql_command == SQLCOM_LOAD))
				rc = ha_calpont_impl_write_last_batch(table, *ci, abort);
		}
	}

	// populate query stats for insert and load data infile. insert select has
	// stats entered in sm already
	if (((thd->lex)->sql_command == SQLCOM_INSERT) ||  ((thd->lex)->sql_command == SQLCOM_LOAD))
	{
		ci->stats.setEndTime();
		ci->stats.fErrorNo = rc;
		if (ci->singleInsert)
			ci->stats.fRows = 1;
		else
			ci->stats.fRows = ci->rowsHaveInserted;
		try {
			ci->stats.insert();
		} catch (std::exception& e)
		{
			string msg = string("Columnstore Query Stats - ") + e.what();
			push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
		}
	}

	if (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))){
		ci->singleInsert = true; // reset the flag
		ci->isLoaddataInfile = false;
		ci->tableOid = 0;
		ci->rowsHaveInserted = 0;
		ci->useCpimport = 1;
	}
	return rc;
}

int ha_calpont_impl_commit (handlerton *hton, THD *thd, bool all)
{
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_CREATE_VTABLE ||
	    thd->infinidb_vtable.vtable_state == THD::INFINIDB_ALTER_VTABLE ||
	    thd->infinidb_vtable.vtable_state == THD::INFINIDB_DROP_VTABLE ||
	    thd->infinidb_vtable.vtable_state == THD::INFINIDB_REDO_PHASE1)
	    return 0;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (ci->isAlter)
		return 0;

	//@Bug 5823 check if any active transaction for this session
	scoped_ptr<DBRM> dbrmp(new DBRM());
	BRM::TxnID txnId = dbrmp->getTxnID(tid2sid(thd->thread_id));
	if (!txnId.valid)
	   return 0;

	if ( !ci->dmlProc )
	{
		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "commit starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
	}

	int rc = ha_calpont_impl_commit_(hton, thd, all, *ci);
	thd->server_status&= ~SERVER_STATUS_IN_TRANS;
	ci->singleInsert = true; // reset the flag
	ci->isLoaddataInfile = false;
	ci->tableOid = 0;
	ci->rowsHaveInserted = 0;
	return rc;
}

int ha_calpont_impl_rollback (handlerton *hton, THD *thd, bool all)
{
	// @bug 1738. no need to rollback for select. This is to avoid concurrent session
	// conflict because DML is not thread safe.
	//comment out for bug 3874. Select should never come to rollback. If there is no active transaction,
	//rollback in DMLProc is not doing anything anyway
	//if (!(current_thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
	//{
	//	return 0;
	//}

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if ( !ci->dmlProc ) {

		ci->dmlProc = new MessageQueueClient("DMLProc");
		//cout << "rollback starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
	}

	int rc = ha_calpont_impl_rollback_(hton, thd, all, *ci);
	ci->singleInsert = true; // reset the flag
	ci->isLoaddataInfile = false;
	ci->tableOid = 0;
	ci->rowsHaveInserted = 0;
	thd->server_status&= ~SERVER_STATUS_IN_TRANS;
	return rc;
}

int ha_calpont_impl_close_connection (handlerton *hton, THD *thd)
{
	if (!thd) return 0;

	if (thd->thread_id == 0)
		return 0;

	execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);
	if (!ci) return 0;

	int rc = 0;
	if ( ci->dmlProc )
	{
		rc = ha_calpont_impl_close_connection_(hton, thd, *ci);
		delete ci->dmlProc;
		ci->dmlProc = NULL;
	}

	if (ci->cal_conn_hndl)
	{
		sm::sm_cleanup(ci->cal_conn_hndl);
		ci->cal_conn_hndl = 0;
	}

	return rc;
}

int ha_calpont_impl_rename_table(const char* from, const char* to)
{
	IDEBUG( cout << "ha_calpont_impl_rename_table: " << from << " => " << to << endl );
	THD *thd = current_thd;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	//@Bug 1948. Alter table call rename table twice
	if ( ci->alterTableState == cal_connection_info::ALTER_FIRST_RENAME )
	{
		ci->alterTableState = cal_connection_info::ALTER_SECOND_RENAME;
		IDEBUG( cout << "ha_calpont_impl_rename_table: was in state ALTER_FIRST_RENAME, now in ALTER_SECOND_RENAME" << endl );
		return 0;
	}
	else if (ci->alterTableState == cal_connection_info::ALTER_SECOND_RENAME)
	{
		ci->alterTableState = cal_connection_info::NOT_ALTER;
		IDEBUG( cout << "ha_calpont_impl_rename_table: was in state ALTER_SECOND_RENAME, now in NOT_ALTER" << endl );
		return 0;
	}
	else if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_ALTER_VTABLE )
		return 0;

	int rc = ha_calpont_impl_rename_table_(from, to, *ci);
	return rc;
}

int ha_calpont_impl_delete_row(const uchar *buf)
{
	IDEBUG( cout << "ha_calpont_impl_delete_row" << endl );
	return 0;
}

COND* ha_calpont_impl_cond_push(COND *cond, TABLE* table)
{
	THD *thd = current_thd;

	if (thd->slave_thread && thd->infinidb_vtable.vtable_state == THD::INFINIDB_INIT)
		thd->infinidb_vtable.vtable_state = THD::INFINIDB_DISABLE_VTABLE;

	if (thd->infinidb_vtable.vtable_state != THD::INFINIDB_DISABLE_VTABLE)
		return cond;

	if (((thd->lex)->sql_command == SQLCOM_UPDATE) ||
		((thd->lex)->sql_command == SQLCOM_UPDATE_MULTI) ||
		((thd->lex)->sql_command == SQLCOM_DELETE) ||
		((thd->lex)->sql_command == SQLCOM_DELETE_MULTI))
		return cond;
	string alias;
	alias.assign(table->alias.ptr(), table->alias.length());
	IDEBUG( cout << "ha_calpont_impl_cond_push: " << alias << endl );

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	cal_table_info ti = ci->tableMap[table];

#ifdef DEBUG_WALK_COND
{
		gp_walk_info gwi;
		gwi.condPush = true;
		gwi.sessionid = tid2sid(thd->thread_id);
		cout << "------------------ cond push -----------------------" << endl;
		cond->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
		cout << "------------------------------------------------\n" << endl;
}
#endif
	if (!ti.csep)
	{
		if (!ti.condInfo)
			ti.condInfo = new gp_walk_info();
		gp_walk_info* gwi = ti.condInfo;
		gwi->dropCond = false;
		gwi->fatalParseError = false;
		gwi->condPush = true;
		gwi->thd = thd;
		gwi->sessionid = tid2sid(thd->thread_id);
		cond->traverse_cond(gp_walk, gwi, Item::POSTFIX);
		ci->tableMap[table] = ti;

		if (gwi->fatalParseError)
		{
			IDEBUG( cout << gwi->parseErrorText << endl );
			if (ti.condInfo)
			{
				delete ti.condInfo;
				ti.condInfo = 0;
				ci->tableMap[table] = ti;
			}
			return cond;
		}
		if (gwi->dropCond)
		{
			return cond;
		}
		else
		{
			return NULL;
		}
	}

	return cond;
}

int ha_calpont_impl_external_lock(THD *thd, TABLE* table, int lock_type)
{
	// @bug 3014. Error out locking table command. IDB does not support it now.
	if (thd->lex->sql_command == SQLCOM_LOCK_TABLES)
	{
		setError(current_thd, ER_CHECK_NOT_IMPLEMENTED,
				 logging::IDBErrorInfo::instance()->errorMsg(ERR_LOCK_TABLE));
		return ER_CHECK_NOT_IMPLEMENTED;
	}

	// @info called for every table at the beginning and at the end of a query.
	// used for cleaning up the tableinfo.
	string alias;
	alias.assign(table->alias.ptr(), table->alias.length());
	IDEBUG( cout << "external_lock for " << alias << endl );
	idbassert((thd->infinidb_vtable.vtable_state >= THD::INFINIDB_INIT_CONNECT &&
	           thd->infinidb_vtable.vtable_state <= THD::INFINIDB_REDO_QUERY) ||
	          thd->infinidb_vtable.vtable_state == THD::INFINIDB_ERROR);
	if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_INIT  )
		return 0;

	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

    if(thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
	{
		if (ci->cal_conn_hndl)
		{
			// send ExeMgr a signal before cloing the connection
			ByteStream msg;
			ByteStream::quadbyte qb = 0;
			msg << qb;
			try {
				ci->cal_conn_hndl->exeMgr->write(msg);
			} catch (...)
			{
				// this is the end of the query. Ignore connetion failure.
			}
			sm::sm_cleanup(ci->cal_conn_hndl);
			ci->cal_conn_hndl = 0;
		}
		return 0;
	}

	CalTableMap::iterator mapiter = ci->tableMap.find(table);
#ifdef _MSC_VER
	//FIXME: fix this! (must be related to F_UNLCK define in winport)
	if (mapiter != ci->tableMap.end() && lock_type == 0) // make sure it's the release lock (2nd) call
#else
	if (mapiter != ci->tableMap.end() && lock_type == 2) // make sure it's the release lock (2nd) call
#endif
	{
		// table mode
		if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE)
		{
			if (mapiter->second.conn_hndl)
			{
				if (ci->traceFlags & 1)
					push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, 9999, mapiter->second.conn_hndl->queryStats.c_str());

				ci->queryStats = mapiter->second.conn_hndl->queryStats;
				ci->extendedStats = mapiter->second.conn_hndl->extendedStats;
				ci->miniStats = mapiter->second.conn_hndl->miniStats;
				sm::sm_cleanup(mapiter->second.conn_hndl);
				mapiter->second.conn_hndl = 0;
			}
			if (mapiter->second.condInfo)
			{
				delete mapiter->second.condInfo;
				mapiter->second.condInfo = 0;
			}

			// only push this warning for once
			if (ci->tableMap.size() == 1 &&
				  thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE && thd->infinidb_vtable.autoswitch)
			{
				push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, infinidb_autoswitch_warning.c_str());
			}
		}
		else // vtable mode
		{
			if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE)
			{
				if (!ci->cal_conn_hndl)
					return 0;

				if (ci->traceFlags & 1)
					push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, 9999, ci->cal_conn_hndl->queryStats.c_str());

				ci->queryStats = ci->cal_conn_hndl->queryStats;
				ci->extendedStats = ci->cal_conn_hndl->extendedStats;
				ci->miniStats = ci->cal_conn_hndl->miniStats;
				ci->queryState = 0;
				thd->infinidb_vtable.override_largeside_estimate = false;
			}
		}
		ci->tableMap.erase(table);
	}

	return 0;
}

// for sorting length exceeds blob limit. Just error out for now.
int ha_calpont_impl_rnd_pos(uchar *buf, uchar *pos)
{
	IDEBUG( cout << "ha_calpont_impl_rnd_pos" << endl);
	string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_ORDERBY_TOO_BIG);
	setError(current_thd, ER_INTERNAL_ERROR, emsg);
	return ER_INTERNAL_ERROR;
}

// vim:sw=4 ts=4:

