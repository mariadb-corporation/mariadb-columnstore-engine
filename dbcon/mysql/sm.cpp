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

/***********************************************************************
 *   $Id: sm.cpp 9254 2013-02-04 19:40:31Z rdempsey $
 *
 ***********************************************************************/

#define PREFER_MY_CONFIG_H
#include <mariadb.h>
#include <mysql.h>
#include <my_sys.h>
#include <errmsg.h>
#include <sql_common.h>
#include <unistd.h>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <sstream>
#include <cstdlib>
#include <signal.h>
#include <cstdio>
#if __FreeBSD__
typedef sig_t sighandler_t;
#endif
using namespace std;

#include <boost/thread.hpp>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "bytestream.h"
using namespace messageqcpp;

#include "errorcodes.h"
using namespace logging;

#include "querystats.h"
using namespace querystats;

#include "sm.h"

#include "installdir.h"

namespace
{
using namespace sm;

// @bug 159 fix. clean up routine when error happened
void cleanup(cpsm_conhdl_t* hndl)
{
  // remove system catalog instance for this statement.
  CalpontSystemCatalog::removeCalpontSystemCatalog(hndl->sessionID);
  hndl->queryState = NO_QUERY;
  hndl->resultSet.erase(hndl->resultSet.begin(), hndl->resultSet.end());
}

status_t tpl_scan_fetch_getband(cpsm_conhdl_t* hndl, sp_cpsm_tplsch_t& ntplsch, int* killed)
{
  // @bug 649 check keybandmap first
  map<int, int>::iterator keyBandMapIter = hndl->keyBandMap.find(ntplsch->key);

  try
  {
    if (keyBandMapIter != hndl->keyBandMap.end())
    {
      ByteStream bs;
      ostringstream oss;
      oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->key << '_' << ntplsch->bandID
          << ".band";
      ifstream bandFile(oss.str().c_str(), ios::in);
      bandFile >> bs;
      unlink(oss.str().c_str());

      // not possible for vtable
      ntplsch->deserializeTable(bs);
      ntplsch->bandID++;

      // end of result set
      if (ntplsch->bandID == keyBandMapIter->second)
      {
        hndl->keyBandMap.erase(keyBandMapIter);
        return SQL_NOT_FOUND;
      }
    }
    else
    {
      ByteStream bs;

      // @bug 626. check saveFlag. If SAVING, read band from socket and save to disk
      if (ntplsch->saveFlag == SAVING)
      {
        ByteStream bs;
        // @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
        //			to reestablish a connection with ExeMgr which ends up causing mysql
        //			session to hang.
        bs = hndl->exeMgr->read();
        ostringstream oss;
        oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->tableid << '_'
            << ntplsch->bandsReturned << ".band";
        ofstream saveFile(oss.str().c_str(), ios::out);
        saveFile << bs;
        saveFile.close();
        ntplsch->bandsReturned++;
        // not possible for vtable
        ntplsch->deserializeTable(bs);
      }
      // if SAVED, read from saved file. not possible for vtable
      else if (ntplsch->saveFlag == SAVED)
      {
        ostringstream oss;
        oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->tableid << '_'
            << ntplsch->bandsReturned << ".band";
        ifstream saveFile(oss.str().c_str(), ios::in);
        saveFile >> bs;
        saveFile.close();
        ntplsch->bandsReturned++;
        ntplsch->deserializeTable(bs);
      }
      // most normal path. also the path for vtable
      else
      {
        ntplsch->bs.restart();
        // @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
        //			to reestablish a connection with ExeMgr which ends up causing mysql
        //			session to hang.
        // @bug 3386. need to abort the query when user does ctrl+c
        timespec t;
        t.tv_sec = 5L;
        t.tv_nsec = 0L;

        if (killed && *killed)
          return SQL_KILLED;

        ntplsch->bs = hndl->exeMgr->read();

        if (ntplsch->bs.length() != 0)
        {
          ntplsch->deserializeTable(ntplsch->bs);

          if (ntplsch->rowGroup && ntplsch->rowGroup->getRGData() == NULL)
          {
            ntplsch->bs.restart();
            // @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
            //			to reestablish a connection with ExeMgr which ends up causing mysql
            //			session to hang.
            bool timeout = true;

            while (timeout)
            {
              timeout = false;
              ntplsch->bs = hndl->exeMgr->getClient()->read(&t, &timeout);

              if (killed && *killed)
                return SQL_KILLED;
            }

            if (ntplsch->bs.length() == 0)
            {
              hndl->curFetchTb = 0;
              return logging::ERR_LOST_CONN_EXEMGR;
            }

            ntplsch->deserializeTable(ntplsch->bs);
          }

          uint16_t error = ntplsch->getStatus();

          if (0 != error)
          {
            ntplsch->setErrMsg();
            return error;
          }
        }
        else  // @todo error handling
        {
          hndl->curFetchTb = 0;

          if (ntplsch->saveFlag == NO_SAVE)
            hndl->tidScanMap[ntplsch->tableid] = ntplsch;

          ntplsch->errMsg = IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR);
          return logging::ERR_LOST_CONN_EXEMGR;
        }
      }

      // All done with this table. reset curFetchTb when finish SOCKET reading
      if (ntplsch->getRowCount() == 0)
      {
        hndl->curFetchTb = 0;

        if (ntplsch->saveFlag == NO_SAVE)
          hndl->tidScanMap[ntplsch->tableid] = ntplsch;

        return SQL_NOT_FOUND;
      }
    }
  }
  catch (std::exception& e)
  {
    hndl->curFetchTb = 0;

    if (ntplsch->saveFlag == NO_SAVE)
      hndl->tidScanMap[ntplsch->tableid] = ntplsch;

    ntplsch->errMsg = e.what();
    return logging::ERR_LOST_CONN_EXEMGR;
  }

  ntplsch->rowsreturned = 0;
  return STATUS_OK;
}

void end_query(cpsm_conhdl_t* hndl)
{
  // remove system catalog instance for this statement.
  // @bug 695. turn on system catalog session cache for FE
  // CalpontSystemCatalog::removeCalpontSystemCatalog(hndl->sessionID);
  hndl->queryState = NO_QUERY;
  // reset at the end of query
  hndl->curFetchTb = 0;
  // @bug 626 clear up
  hndl->tidMap.clear();
  hndl->tidScanMap.clear();
  hndl->keyBandMap.clear();

  // Tell ExeMgr we are done with this query
  try
  {
    ByteStream bs;
    ByteStream::quadbyte qb = 0;
    bs << qb;
    hndl->write(bs);
  }
  catch (...)
  {
    throw;
  }
}

// @bug 1054, 863 - SIGPIPE handler
bool sigFlag = false;
void sighandler(int sig_num)
{
  FILE* p;
  char buf[1024];

  string tmpDir = startup::StartUp::tmpDir() + "/f1.dat";
  const char* cstr = tmpDir.c_str();

  if ((p = fopen(cstr, "a")) != NULL)
  {
    snprintf(buf, 1024, "sighandler() hit with %d\n", sig_num);
    fwrite(buf, 1, strlen(buf), p);
    fclose(p);
  }

  sigFlag = true;
  throw runtime_error("zerror");
}

}  // namespace

namespace sm
{
#ifdef _MSC_VER
const std::string DEFAULT_SAVE_PATH = "C:\\Calpont\\tmp";
#else
const std::string DEFAULT_SAVE_PATH = "/var/tmp";
#endif

status_t tpl_open(tableid_t tableid, cpsm_tplh_t* ntplh, cpsm_conhdl_t* conn_hdl)
{
  SMDEBUGLOG << "tpl_open: ntplh: " << ntplh << " conn_hdl: " << conn_hdl << " tableid: " << tableid << endl;

  // if first time enter this function for a statement, set
  // queryState to QUERY_IN_PROCESS and get execution plan.
  if (conn_hdl->queryState == NO_QUERY)
  {
    conn_hdl->queryState = QUERY_IN_PROCESS;
  }

  try
  {
    // @bug 626. check saveFlag, if SAVED, do not project
    if (ntplh->saveFlag != SAVED)
    {
      // Request ExeMgr to start projecting table tableid
      CalpontSystemCatalog::OID tableOID = static_cast<CalpontSystemCatalog::OID>(tableid);
      ByteStream::quadbyte qb = static_cast<ByteStream::quadbyte>(tableOID);
      ByteStream bs;
      bs << qb;
      conn_hdl->write(bs);
    }
  }
  catch (std::exception& ex)
  {
    SMDEBUGLOG << "Exception caught in tpl_open: " << ex.what() << endl;
    cleanup(conn_hdl);
    return CALPONT_INTERNAL_ERROR;
  }

  ntplh->tableid = tableid;

  return STATUS_OK;
}

status_t tpl_scan_open(tableid_t tableid, sp_cpsm_tplsch_t& ntplsch, cpsm_conhdl_t* conn_hdl)
{
#if IDB_SM_DEBUG
  SMDEBUGLOG << "tpl_scan_open: " << conn_hdl << " tableid: " << tableid << endl;
#endif

  // @bug 649. No initialization here. take passed in reference
  ntplsch->tableid = tableid;

  ntplsch->rowsreturned = 0;
  return STATUS_OK;
}

status_t tpl_scan_fetch(sp_cpsm_tplsch_t& ntplsch, cpsm_conhdl_t* conn_hdl, int* killed)
{
  // @770. force end of result set when this is not the first table to be fetched.
  if (ntplsch->traceFlags & CalpontSelectExecutionPlan::TRACE_NO_ROWS2)
    if (conn_hdl->tidScanMap.size() >= 1)
      return SQL_NOT_FOUND;

  // need another band
  status_t status = STATUS_OK;

  if (ntplsch->rowsreturned == ntplsch->getRowCount())
    status = tpl_scan_fetch_getband(conn_hdl, ntplsch, killed);

  return status;
}

status_t tpl_scan_close(sp_cpsm_tplsch_t& ntplsch)
{
#if IDB_SM_DEBUG
  SMDEBUGLOG << "tpl_scan_close: ";

  if (ntplsch)
    SMDEBUGLOG << "tpl_scan_close: ntplsch " << ntplsch;
  SMDEBUGLOG << "tpl_scan_close: tableid: " << ntplsch->tableid << endl;
#endif
  ntplsch.reset();

  return STATUS_OK;
}

status_t tpl_close(cpsm_tplh_t* ntplh, cpsm_conhdl_t** conn_hdl, QueryStats& stats, bool ask_4_stats,
                   bool clear_scan_ctx)
{
  cpsm_conhdl_t* hndl = *conn_hdl;
  SMDEBUGLOG << "tpl_close: hndl" << hndl << " ntplh " << ntplh;

  if (ntplh)
    SMDEBUGLOG << " tableid: " << ntplh->tableid;

  SMDEBUGLOG << endl;
  delete ntplh;

  // determine end of result set and end of statement execution
  if (hndl->queryState == QUERY_IN_PROCESS)
  {
    // Get the query stats
    ByteStream bs;
    // Ask for a stats only if a user explicitly asks
    if (ask_4_stats)
    {
      ByteStream::quadbyte qb = 3;
      bs << qb;
      hndl->write(bs);
    }
    // MCOL-1601 Dispose of unused empty RowGroup
    if (clear_scan_ctx)
    {
      SMDEBUGLOG << "tpl_close() clear_scan_ctx read" << std::endl;
      bs = hndl->exeMgr->read();
    }

    SMDEBUGLOG << "tpl_close hndl->exeMgr: " << hndl->exeMgr << endl;
    // keep reading until we get a string
    // Ask for a stats only if a user explicitly asks
    if (ask_4_stats)
    {
      for (int tries = 0; tries < 10; tries++)
      {
        bs = hndl->exeMgr->read();

        if (bs.length() == 0)
          break;

        try
        {
          bs >> hndl->queryStats;
          bs >> hndl->extendedStats;
          bs >> hndl->miniStats;
          stats.unserialize(bs);
          stats.setEndTime();
          stats.insert();
          break;
        }
        catch (IDBExcept&)
        {
          // @bug4732
          end_query(hndl);
          throw;
        }
        catch (...)
        {
          // querystats messed up. close connection.
          // no need to throw for querystats protocol error, like for tablemode.
          SMDEBUGLOG << "tpl_close() exception whilst getting stats" << endl;
          end_query(hndl);
          sm_cleanup(hndl);
          *conn_hdl = 0;
          return STATUS_OK;
          // throw runtime_error(string("tbl_close catch exception: ") + e.what());
        }
      }
    }  // stats

    end_query(hndl);
  }

  return STATUS_OK;
}

status_t sm_init(uint32_t sid, cpsm_conhdl_t** conn_hdl, uint32_t columnstore_local_query)
{
  // clear file content
#if IDB_SM_DEBUG
  // smlog.close();
  // smlog.open("/tmp/sm.log");
  SMDEBUGLOG << "sm_init: " << endl;
#endif

  // @bug5660 Connection changes related to the local pm setting
  /**
   * when local PM is detected, or columnstore_local_query is set:
   * 1. SELECT query connect to local ExeMgr 127.0.0.1:8601;
   * 2. DML/DDL is disallowed.
   * once local connection is determined, no need to check
   * again because it will not switch back.
   **/
  if (*conn_hdl)
  {
    // existing connection is local, ok.
    if ((*conn_hdl)->exeMgr->localQuery() || !columnstore_local_query)
      return STATUS_OK;
    // if session variable changes to local, re-establish the connection to loopback.
    else
      sm_cleanup(*conn_hdl);
  }

  cpsm_conhdl_t* hndl = new cpsm_conhdl_t(time(0), sid, columnstore_local_query);
  *conn_hdl = hndl;
  hndl->sessionID = sid;

  // profiling statistics
  GET_PF_TIME(hndl->pf.login);

  return STATUS_OK;
}

status_t sm_cleanup(cpsm_conhdl_t* conn_hdl)
{
#if IDB_SM_DEBUG
  SMDEBUGLOG << "sm_cleanup: " << conn_hdl << endl;
#endif

  delete conn_hdl;

  return STATUS_OK;
}

void cpsm_conhdl_t::write(ByteStream bs)
{
#ifdef _MSC_VER
  exeMgr->write(bs);
#else
  sighandler_t old_handler = signal(SIGPIPE, sighandler);
  sigFlag = false;
  exeMgr->write(bs);
  signal(SIGPIPE, old_handler);

  if (sigFlag)
    throw runtime_error("Broken Pipe Error");

#endif
}

}  // namespace sm

/**
 * The following functions exist because QueryStats is used by sm.
 * QueryStats needs to use a MariaDB client connection and has to use the
 * server's built-in client which doesn't have the same API as the general
 * MariaDB client.
 * These functions reproduce the missing functionality.
 * Everywhere else QueryStats is linked to uses the general MariaDB API so
 * these functions aren't needed.
 */

unsigned long mysql_real_escape_string(MYSQL* mysql, char* to, const char* from, unsigned long length)
{
  my_bool overflow;
  return escape_string_for_mysql(mysql->charset, to, length * 2 + 1, from, length, &overflow);
}

// Clone of sql-common/client.c cli_use_result
MYSQL_RES* mysql_use_result(MYSQL* mysql)
{
  MYSQL_RES* result;
  DBUG_ENTER("mysql_use_result (clone)");

  if (!mysql->fields)
    DBUG_RETURN(0);

  if (mysql->status != MYSQL_STATUS_GET_RESULT)
  {
    set_mysql_error(mysql, CR_COMMANDS_OUT_OF_SYNC, unknown_sqlstate);
    DBUG_RETURN(0);
  }

  if (!(result =
            (MYSQL_RES*)my_malloc(PSI_NOT_INSTRUMENTED, sizeof(*result) + sizeof(ulong) * mysql->field_count,
                                  MYF(MY_WME | MY_ZEROFILL))))
    DBUG_RETURN(0);

  result->lengths = (ulong*)(result + 1);
  result->methods = mysql->methods;

  if (!(result->row = (MYSQL_ROW)my_malloc(PSI_NOT_INSTRUMENTED,
                                           sizeof(result->row[0]) * (mysql->field_count + 1), MYF(MY_WME))))
  {
    /* Ptrs: to one row */
    my_free(result);
    DBUG_RETURN(0);
  }

  result->fields = mysql->fields;
  result->field_alloc = mysql->field_alloc;
  result->field_count = mysql->field_count;
  result->current_field = 0;
  result->handle = mysql;
  result->current_row = 0;
  mysql->fields = 0; /* fields is now in result */
  clear_alloc_root(&mysql->field_alloc);
  mysql->status = MYSQL_STATUS_USE_RESULT;
  mysql->unbuffered_fetch_owner = &result->unbuffered_fetch_cancelled;
  DBUG_RETURN(result); /* Data is read to be fetched */
}

MYSQL_FIELD* mysql_fetch_fields(MYSQL_RES* res)
{
  return res->fields;
}
