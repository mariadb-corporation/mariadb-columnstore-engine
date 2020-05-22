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

/*
 * $Id: ha_mcs_dml.cpp 9711 2013-07-23 21:01:27Z chao $
 */

#include <my_config.h>
#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "idb_mysql.h"

#define NEED_CALPONT_INTERFACE
#include "ha_mcs_impl.h"

#include "ha_mcs_impl_if.h"
using namespace cal_impl_if;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "dataconvert.h"
using namespace dataconvert;

#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "resourcemanager.h"
using namespace joblist;
//#include "stopwatch.h"
//using namespace logging;

#include "dbrm.h"

namespace
{
uint64_t fBatchInsertGroupRows = 0; // ResourceManager::instance()->getRowsPerBatch();
// HDFS is never used nowadays, so don't bother
bool useHdfs = false; // ResourceManager::instance()->useHdfs();

//convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
    return execplan::CalpontSystemCatalog::idb_tid2sid(tid);
}

//StopWatch timer;
uint32_t buildValueList (TABLE* table, cal_connection_info& ci )
{
    char attribute_buffer[1024];
    String attribute(attribute_buffer, sizeof(attribute_buffer),
                     &my_charset_bin);
    uint32_t size = 0;
    int columnPos = 0;
    double dbval;
    ci.nullValuesBitset.reset();

    for (Field** field = table->field; *field; field++)
    {
        if ((*field)->is_null())
        {
            ci.tableValuesMap[columnPos].push_back (""); //currently, empty string is treated as null.
            ci.nullValuesBitset[columnPos] = true;
        }
        else
        {
            bitmap_set_bit(table->read_set, (*field)->field_index);
            ci.nullValuesBitset[columnPos] = false;

            // @bug 3798 get real value for float/double type
            if ((*field)->result_type() == REAL_RESULT)
            {
                dbval = (*field)->val_real();
                //int maxlen = (*field)->max_display_length();
                const unsigned maxlen = 1024 + 1 + 1 + 1; //1 for leading zero, 1 for dp, 1 for null
                char buf[maxlen];
                memset(buf, 0, maxlen);
                snprintf(buf, maxlen, "%.1024f", dbval);
                ci.tableValuesMap[columnPos].push_back(buf);
            }
            else
            {
                //fetch different data type
                (*field)->val_str(&attribute, &attribute);

                if (attribute.length() == 0)
                {
                    ci.tableValuesMap[columnPos].push_back (""); //currently, empty string is treated as null.
                }
                else
                {
                    string val(attribute.ptr(), attribute.length());
                    ci.tableValuesMap[columnPos].push_back(val);
                }
            }
        }

        ci.colNameList.push_back((*field)->field_name.str);

        columnPos++;
    }

    size = ci.tableValuesMap[0].size();
    return size;
}


int ProcessCommandStatement(THD* thd, string& dmlStatement, cal_connection_info& ci, std::string schema = "")
{
    int rc = 0;

    ulong sessionID = tid2sid(thd->thread_id);

    CalpontDMLPackage* pDMLPackage;

    //@Bug 2721 and 2722. Log the statement before issuing commit/rollback
    if ( dmlStatement == "LOGGING" )
    {
        char* query_char = idb_mysql_query_str(thd);
        std::string query_str;
        if (!query_char)
        {
            query_str = "<Replication event>";
        }
        else
        {
            query_str = query_char;
        }
        VendorDMLStatement cmdStmt(query_str, DML_COMMAND, sessionID);
        cmdStmt.set_Logging( false );
        pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
        pDMLPackage->set_Logging( false );
        pDMLPackage->set_SchemaName( schema );
    }
    else
    {
        VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
        pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
    }

    pDMLPackage->setTableOid (ci.tableOid);

    if (!ci.singleInsert)
    {
        pDMLPackage->set_isBatchInsert(true);
    }

    if (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
        pDMLPackage->set_isAutocommitOn(true);

    if (useHdfs)
        pDMLPackage->set_isAutocommitOn(true);

    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);

    pDMLPackage->write(bytestream);
    delete pDMLPackage;

    ByteStream::byte b = 0;
    string errormsg;
    ByteStream::octbyte rows;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();

        if ( bytestream.length() == 0 )
        {
            rc = 1;
            thd->killed = KILL_QUERY;
            thd->get_stmt_da()->set_overwrite_status(true);

            thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [1]");
        }
        else
        {
            bytestream >> b;
            bytestream >> rows;
            bytestream >> errormsg;
        }
    }
    catch (runtime_error&)
    {
        rc = 1 ;
        thd->killed = KILL_QUERY;
        thd->get_stmt_da()->set_overwrite_status(true);

        thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [2]");
    }
    catch (...)
    {
        rc = 1;
        thd->killed = KILL_QUERY;
        thd->get_stmt_da()->set_overwrite_status(true);

        thd->raise_error_printf(ER_INTERNAL_ERROR, "Caught unknown error");
    }

    if (( b != 0 ) && (!thd->get_stmt_da()->is_set()))
    {
        rc = 1;
        thd->killed = KILL_QUERY;
        thd->raise_error_printf(ER_INTERNAL_ERROR, errormsg.c_str());
    }

    delete ci.dmlProc;
    ci.dmlProc = NULL;
    return rc;
}

int doProcessInsertValues ( TABLE* table, uint32_t size, cal_connection_info& ci, bool lastBatch = false )
{
    THD* thd = current_thd;
    uint32_t sessionID = tid2sid(thd->thread_id);

    int rc = 0;

    char* query_char = idb_mysql_query_str(thd);
    std::string query_str;
    if (!query_char)
    {
        query_str = "<Replication event>";
    }
    else
    {
        query_str = query_char;
    }

    VendorDMLStatement dmlStmts(query_str, DML_INSERT, table->s->table_name.str,
                                table->s->db.str, size, ci.colNameList.size(), ci.colNameList,
                                ci.tableValuesMap, ci.nullValuesBitset, sessionID);

    CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStmts);
    //@Bug 2466 Move the clean up earlier to avoid the second insert in another session to get the data
    ci.tableValuesMap.clear();
    ci.colNameList.clear();

    if (!pDMLPackage)
    {
        rc = -1;
        string emsg("Calpont DML package cannot build. ");
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
        return rc;
    }

    //@Bug 2721 and 2722 log batch insert statement only once in the bebug file.
    if (( ( ci.isLoaddataInfile ) || ((ci.rowsHaveInserted + size) < ci.bulkInsertRows ) ) && (ci.rowsHaveInserted > 0))
    {
        pDMLPackage->set_Logging( false );
        pDMLPackage->set_Logending( false );
    }
    else if (( ( ci.isLoaddataInfile ) || ((ci.rowsHaveInserted + size) < ci.bulkInsertRows ) ) && (ci.rowsHaveInserted == 0))
    {
        pDMLPackage->set_Logging( true );
        pDMLPackage->set_Logending( false );
    }

    if ( ci.singleInsert )
    {
        pDMLPackage->set_Logging( true );
        pDMLPackage->set_Logending( true );
    }

    if ( !ci.singleInsert )
    {
        pDMLPackage->set_isBatchInsert( true );
    }

    if (thd->is_strict_mode())
    {
        pDMLPackage->set_isWarnToError( true );
    }

    pDMLPackage->setTableOid (ci.tableOid);

    if (lastBatch)
    {
        pDMLPackage->set_Logending( true );

    }

    if (lastBatch && (ci.rowsHaveInserted > 0))
        pDMLPackage->set_Logging( false );

    std::string name = table->s->table_name.str;
    pDMLPackage->set_TableName(name);
    name = table->s->db.str;
    pDMLPackage->set_SchemaName(name);
    pDMLPackage->set_TimeZone(thd->variables.time_zone->get_name()->ptr());

    if (thd->lex->sql_command == SQLCOM_INSERT_SELECT)
        pDMLPackage->set_isInsertSelect(true);

    //Carry session autocommit info in the pkg to use in DMLProc
    //cout << "Thread options = "  << thd->variables.option_bits << " and  OPTION_NOT_AUTOCOMMIT:OPTION_BEGIN = " << OPTION_NOT_AUTOCOMMIT << ":" << OPTION_BEGIN << endl;
    if (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
    {
        //cout << "autocommit is on" << endl;
        pDMLPackage->set_isAutocommitOn(true);
    }
    else if (useHdfs)
    {
        pDMLPackage->set_isAutocommitOn(true);
    }

    ByteStream bytestream, bytestreamRcv;
    bytestream << sessionID;

    pDMLPackage->write(bytestream);
    delete pDMLPackage;

    ByteStream::byte b = 0;
    string errormsg;
    ByteStream::octbyte rows;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestreamRcv = ci.dmlProc->read();

        if ( bytestreamRcv.length() == 0 )
        {
            //check if it is first batch and DMLProc restarted. Only this case, get a new client and resend
            if (ci.rowsHaveInserted == 0)
            {
                delete ci.dmlProc;
                ci.dmlProc = new MessageQueueClient("DMLProc");

                //cout << "doProcessInsertValues starts a client " << ci.dmlProc << " for session " << thd->thread_id << endl;
                try
                {
                    ci.dmlProc->write(bytestream);
                    bytestreamRcv = ci.dmlProc->read();

                    if ( bytestreamRcv.length() == 0 )
                    {
                        rc = -1;
                        b = 1;
                        errormsg = "Lost connection to DMLProc [3]";
                    }
                    else
                    {
                        bytestreamRcv >> b;
                        bytestreamRcv >> rows;
                        bytestreamRcv >> errormsg;
                        rc = b;
                    }
                }
                catch (runtime_error&)
                {
                    rc = -1;
                    thd->get_stmt_da()->set_overwrite_status(true);
                    errormsg = "Lost connection to DMLProc [4]";
                    b = 1;
                }
            }
        }
        else
        {
            bytestreamRcv >> b;
            bytestreamRcv >> rows;
            bytestreamRcv >> errormsg;
            rc = b;
        }
    }
    catch (std::exception& rex)
    {
        //check if it is first batch and DMLProc restarted. Only this case, get a new client and resend
        if (ci.rowsHaveInserted == 0)
        {
            delete ci.dmlProc;
            ci.dmlProc = new MessageQueueClient("DMLProc");

            //cout << "doProcessInsertValues exception starts a client " << ci.dmlProc << " for session " << thd->thread_id << endl;
            try
            {
                ci.dmlProc->write(bytestream);
                bytestreamRcv = ci.dmlProc->read();

                if ( bytestreamRcv.length() == 0 )
                {
                    rc = -1;
                    b = 1;
                    errormsg = string("Lost connection to DMLProc after getting a new client [1:") + rex.what() + "]";
                }
                else
                {
                    bytestreamRcv >> b;
                    bytestreamRcv >> rows;
                    bytestreamRcv >> errormsg;
                    rc = b;
                }
            }
            catch (std::exception& rrex)
            {
                rc = -1;
                thd->get_stmt_da()->set_overwrite_status(true);
                errormsg = string("Lost connection to DMLProc after getting a new client [2:") + rex.what() + " then " + rrex.what() + "]";
                b = 1;
            }
        }
        else //really lost connection
        {
            rc = -1;
            thd->get_stmt_da()->set_overwrite_status(true);
            errormsg = string("Lost connection to DMLProc really [1:") + rex.what() + "]";
            b = 1;
        }
    }
    catch (...)
    {
        rc = -1;
        thd->get_stmt_da()->set_overwrite_status(true);
        errormsg = "Unknown error caught";
        b = 1;
    }


    if ((b != 0) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
    {
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, errormsg.c_str());

    }

    if ( b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING )
    {
        if (!thd->is_strict_mode())
        {
            rc = 0;
        }

        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, ER_WARN_DATA_OUT_OF_RANGE, errormsg.c_str());
    }

    if ( rc != 0 )
        ci.rc = rc;

    if ( b == dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR )
        rc = b;

    return rc;
}

}

int ha_mcs_impl_write_last_batch(TABLE* table, cal_connection_info& ci, bool abort)
{
    int rc = 0;
    THD* thd = current_thd;
    std::string command;
    uint32_t size =  ci.tableValuesMap[0].size();
    //@Bug 2468. Add a logging statement command
    command = "COMMIT";
    std::string schema;
    schema = table->s->db.str;

    //@Bug 6112. if no row to be insert and no rows have been inserted, no need to send to DMLProc
    if ((size == 0) && (ci.rowsHaveInserted == 0))
        return rc;

    //@Bug 2715 Check the saved error code.
    //@Bug 4516 always send the last package to allow DMLProc receive all messages from WES
    if (( ci.rc != 0 ) || abort )
    {
        if (abort) //@Bug 5285. abort is different from error, dmlproc only clean up when erroring out
            rc = doProcessInsertValues( table, size, ci, true);

        //@Bug 2722 Log the statement into datamod log
        //@Bug 4605 if error, rollback and no need to check whether the session is autocommit off

        command = "ROLLBACK";
        rc = ProcessCommandStatement ( thd, command, ci, schema );
        rc = ci.rc;
        ci.rc = 0;

        if (size > 0 )
        {
            ci.tableValuesMap.clear();
            ci.colNameList.clear();
        }

        return rc;
    }
    else
    {
        rc = doProcessInsertValues( table, size, ci, true);
    }

    if ( abort )
    {
        rc = 1;
        thd->get_stmt_da()->set_overwrite_status(true);
        std::string errormsg = "statement is aborted.";
        thd->raise_error_printf(ER_INTERNAL_ERROR, errormsg.c_str());
    }

    if ( rc == dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR  )
        return rc;

    //@Bug 4605
    if ( (rc == 0) && !abort && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))
    {
        ci.rowsHaveInserted += size;
        command = "COMMIT";
        ProcessCommandStatement ( thd, command, ci, schema );
    }
    else if (useHdfs)
    {
        ci.rowsHaveInserted += size;
        command = "COMMIT";
        ProcessCommandStatement ( thd, command, ci, schema );
    }
    else if (( rc != 0) || abort )
    {
        command = "ROLLBACK";
        ProcessCommandStatement ( thd, command, ci, schema );
    }
    else
    {
        return rc;
    }

    return rc;

}

int ha_mcs_impl_write_row_(const uchar* buf, TABLE* table, cal_connection_info& ci, ha_rows& rowsInserted)
{
    int rc = 0;
    //timer.start( "buildValueList");
    ci.colNameList.clear();
    THD* thd = current_thd;
    uint32_t size = 0;
    std::string schema;
    schema = table->s->db.str;

    //@Bug 2086 Added syntax check for '\0'
    try
    {
        size = buildValueList ( table, ci );
    }
    catch (runtime_error& rex)
    {
        rc = 1;
        ci.rc = rc; //@Bug 2790 Save the error infomation.
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, rex.what());
        return rc;
    }

    if (fBatchInsertGroupRows == 0)
        fBatchInsertGroupRows = ResourceManager::instance()->getRowsPerBatch();

    //timer.stop( "buildValueList");
    if ( ci.singleInsert   // Single insert
            || (( ci.bulkInsertRows > 0 ) && (( ( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ) || ( size >= fBatchInsertGroupRows )) )
            //Insert with mutilple value case: processed batch by batch. Last batch is sent also.
            || (( ci.bulkInsertRows == 0 ) && ( size >= fBatchInsertGroupRows ) )  ) // Load data in file is processed batch by batch
    {
        //timer.start( "DMLProc takes");
        //cout <<" sending a batch to DMLProc ... The size is " << size << "  the current bulkInsertRows = " <<  ci.bulkInsertRows << endl;
        //Build dmlpackage
        if (( ci.bulkInsertRows > 0 ) && ( ( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ))
        {
            rc = doProcessInsertValues( table, size, ci, true );
        }
        else
        {
            rc = doProcessInsertValues( table, size, ci );
        }

        if ( rc == 0 )
            rowsInserted = size;
        else
            ci.rc = rc;

        //@Bug 2481. The current active transaction needs to be released if autocommit is on
        //@Bug 2438 Added a check for batchinsert's last batch to send commit if autocommit is on
        std::string command;

        if ( ci.singleInsert  || ( (ci.bulkInsertRows > 0 ) && (( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ) ) )
        {
            if ( thd->killed > 0 )
            {
                command = "ROLLBACK";
                ProcessCommandStatement ( thd, command, ci, schema );
            }
            else if (rc != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR)
            {
                //@Bug 4605
                if ( rc != 0 )
                {
                    command = "ROLLBACK";
                    ProcessCommandStatement ( thd, command, ci, schema );
                }
                else if (( rc == 0 ) && (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))
                {
                    command = "COMMIT";
                    ProcessCommandStatement ( thd, command, ci, schema );
                }
                else if (useHdfs)
                {
                    command = "COMMIT";
                    ProcessCommandStatement ( thd, command, ci, schema );
                }
            }
        }

        //timer.stop( "DMLProc takes");
        //timer.finish();
        return rc;

    }
    else
    {
        return rc;
    }
}

int ha_mcs_impl_write_batch_row_(const uchar* buf, TABLE* table, cal_impl_if::cal_connection_info& ci)
{
    ByteStream rowData;
    int rc = 0;
    //std::ostringstream  data;
    bool nullVal = false;
    const uchar* bufHdr = buf;	// bit flag indicating a field is null. Only those fields that are nullable are represented.
    int32_t headerByte = 0; // Current byte in the bufHdr
    int32_t headerBit = 0;  // current bit in the bufHdr current byte.
    uint16_t colpos = 0;
    buf  = buf + ci.headerLength;  // Number of bytes used for null bits.
    //@Bug 6122 if all columns have not null constraint, there is no information in the header
    std::string escape;
    char nullBits = *bufHdr++;

    if (!ci.useXbit)
    {
        // Skip the first bit. For some reason, mysql reserves the first bit of the first byte, unless there's a varchar column in the table.
        nullBits = nullBits >> 1;
        ++headerBit;
    }

    while (colpos < ci.columnTypes.size()) //test bitmap for null values
    {
        uint8_t numLoop = 7;

        if ((ci.useXbit) || (colpos > 6))
            numLoop++;

        for (uint8_t i = 0; i < numLoop; i++)
        {
            if (colpos == ci.columnTypes.size())
                break;

            //if a column has not null constraint, it will not be in the bit map
            if (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT)
            {
                if (ci.headerLength > 0 && headerByte >= ci.headerLength)
                {
                    // We've used more null bits than allowed. Something is seriously wrong.
                    std::string errormsg = "Null bit header is wrong size";
                    setError(current_thd, ER_INTERNAL_ERROR, errormsg);
                    return -1;
                }

                nullVal = nullBits & 0x01;
                nullBits = nullBits >> 1;
                ++headerBit;

                if (headerBit == 8)
                {
                    nullBits = *bufHdr++;
                    headerBit = 0;
                    ++headerByte;
                }
            }
            else
            {
                nullVal = false;
            }

            switch (ci.columnTypes[colpos].colDataType)
            {
                case CalpontSystemCatalog::DATE: //date fetch
                {

                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    }
                    else
                    {
                        const uchar* tmp1 = buf;
                        uint32_t tmp = (tmp1[2] << 16) + (tmp1[1] << 8) + tmp1[0];

                        int day = tmp & 0x0000001fl;
                        int month = (tmp >> 5) & 0x0000000fl;
                        int year = tmp >> 9;
                        fprintf(ci.filePtr, "%04d-%02d-%02d%c", year, month, day, ci.delimiter);
                    }

                    buf += 3;
                    break;
                }

                case CalpontSystemCatalog::DATETIME:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);

                        if (table->field[colpos]->real_type() == MYSQL_TYPE_DATETIME2)
                            buf += table->field[colpos]->pack_length();
                        else
                            buf += 8;
                    }
                    else
                    {
                        if (table->field[colpos]->real_type() == MYSQL_TYPE_DATETIME2)
                        {
                            // mariadb 10.1 compatibility -- MYSQL_TYPE_DATETIME2 introduced in mysql 5.6
                            MYSQL_TIME ltime;
                            const uchar* pos = buf;
                            longlong tmp = my_datetime_packed_from_binary(pos, table->field[colpos]->decimals());
                            TIME_from_longlong_datetime_packed(&ltime, tmp);

                            if (!ltime.second_part)
                            {
                                fprintf(ci.filePtr, "%04d-%02d-%02d %02d:%02d:%02d%c",
                                        ltime.year, ltime.month, ltime.day,
                                        ltime.hour, ltime.minute, ltime.second, ci.delimiter);
                            }
                            else
                            {
                                fprintf(ci.filePtr, "%04d-%02d-%02d %02d:%02d:%02d.%ld%c",
                                        ltime.year, ltime.month, ltime.day,
                                        ltime.hour, ltime.minute, ltime.second,
                                        ltime.second_part, ci.delimiter);
                            }

                            buf += table->field[colpos]->pack_length();
                        }
                        else
                        {
                            long long value = *((long long*) buf);
                            long datePart = (long) (value / 1000000ll);
                            int day = datePart % 100;
                            int month = (datePart / 100) % 100;
                            int year = datePart / 10000;
                            fprintf(ci.filePtr, "%04d-%02d-%02d ", year, month, day);

                            long timePart = (long) (value - (long long) datePart * 1000000ll);
                            int second = timePart % 100;
                            int min = (timePart / 100) % 100;
                            int hour = timePart / 10000;
                            fprintf(ci.filePtr, "%02d:%02d:%02d%c", hour, min, second, ci.delimiter);
                            buf += 8;
                        }
                    }

                    break;
                }

                case CalpontSystemCatalog::TIME:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);

                        buf += table->field[colpos]->pack_length();
                    }
                    else
                    {
                        MYSQL_TIME ltime;
                        const uchar* pos = buf;
                        longlong tmp = my_time_packed_from_binary(pos, table->field[colpos]->decimals());
                        TIME_from_longlong_time_packed(&ltime, tmp);

                        if (ltime.neg)
                        {
                            fprintf(ci.filePtr, "-");
                        }

                        if (!ltime.second_part)
                        {
                            fprintf(ci.filePtr, "%02d:%02d:%02d%c",
                                    ltime.hour, ltime.minute, ltime.second, ci.delimiter);
                        }
                        else
                        {
                            fprintf(ci.filePtr, "%02d:%02d:%02d.%ld%c",
                                    ltime.hour, ltime.minute, ltime.second,
                                    ltime.second_part, ci.delimiter);
                        }

                        buf += table->field[colpos]->pack_length();
                    }

                    break;
                }

                case CalpontSystemCatalog::TIMESTAMP:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    }
                    else
                    {
                        const uchar* pos = buf;
                        struct timeval tm;
                        my_timestamp_from_binary(&tm, pos, table->field[colpos]->decimals());

                        MySQLTime time;
                        gmtSecToMySQLTime(tm.tv_sec, time, current_thd->variables.time_zone->get_name()->ptr());

                        if (!tm.tv_usec)
                        {
                            fprintf(ci.filePtr, "%04d-%02d-%02d %02d:%02d:%02d%c",
                                    time.year, time.month, time.day,
                                    time.hour, time.minute, time.second, ci.delimiter);
                        }
                        else
                        {
                            fprintf(ci.filePtr, "%04d-%02d-%02d %02d:%02d:%02d.%ld%c",
                                    time.year, time.month, time.day,
                                    time.hour, time.minute, time.second,
                                    tm.tv_usec, ci.delimiter);
                        }
                    }

                    buf += table->field[colpos]->pack_length();

                    break;
                }

                case CalpontSystemCatalog::CHAR:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    }
                    else
                    {
                        if (current_thd->variables.sql_mode & MODE_PAD_CHAR_TO_FULL_LENGTH)
                        {
                            // Pad to the full length of the field
                            if (ci.utf8)
                                escape.assign((char*)buf, ci.columnTypes[colpos].colWidth * 3);
                            else
                                escape.assign((char*)buf, ci.columnTypes[colpos].colWidth);

                            boost::replace_all(escape, "\\", "\\\\");

                            fprintf(ci.filePtr, "%c%.*s%c%c", ci.enclosed_by, (int)escape.length(),
                                    escape.c_str(), ci.enclosed_by, ci.delimiter);
                        }
                        else
                        {
                            // Get the actual data length
                            Field* field = table->field[colpos];
                            bitmap_set_bit(table->read_set, field->field_index);
                            String attribute;
                            field->val_str(&attribute);

                            escape.assign((char*)buf, attribute.length());
                            boost::replace_all(escape, "\\", "\\\\");

                            fprintf(ci.filePtr, "%c%.*s%c%c", ci.enclosed_by, (int)escape.length(),
                                    escape.c_str(), ci.enclosed_by, ci.delimiter);
                        }
                    }

                    if (ci.utf8)
                        buf += (ci.columnTypes[colpos].colWidth * 3);
                    else
                        buf += ci.columnTypes[colpos].colWidth;

                    break;
                }

                case CalpontSystemCatalog::VARCHAR:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);

                        if (!ci.utf8)
                        {
                            if (ci.columnTypes[colpos].colWidth < 256)
                            {
                                buf++;
                            }
                            else
                            {
                                buf = buf + 2 ;
                            }
                        }
                        else //utf8
                        {
                            if (ci.columnTypes[colpos].colWidth < 86)
                            {
                                buf++;
                            }
                            else
                            {
                                buf = buf + 2 ;
                            }
                        }
                    }
                    else
                    {
                        int dataLength = 0;

                        if (!ci.utf8)
                        {
                            if (ci.columnTypes[colpos].colWidth < 256)
                            {
                                dataLength = *(uint8_t*) buf;
                                buf++;
                            }
                            else
                            {
                                dataLength = *(uint16_t*) buf;
                                buf = buf + 2 ;
                            }
                            escape.assign((char*)buf, dataLength);
                            boost::replace_all(escape, "\\", "\\\\");
                            fprintf(ci.filePtr, "%c%.*s%c%c", ci.enclosed_by, (int)escape.length(), escape.c_str(), ci.enclosed_by, ci.delimiter);
                        }
                        else //utf8
                        {
                            if (ci.columnTypes[colpos].colWidth < 86)
                            {
                                dataLength = *(uint8_t*) buf;
                                buf++;
                            }
                            else
                            {
                                dataLength = *(uint16_t*) buf;
                                buf = buf + 2 ;
                            }

                            escape.assign((char*)buf, dataLength);
                            boost::replace_all(escape, "\\", "\\\\");

                            fprintf(ci.filePtr, "%c%.*s%c%c", ci.enclosed_by, (int)escape.length(), escape.c_str(), ci.enclosed_by, ci.delimiter);
                        }
                    }
                    if (ci.utf8)
                        buf += (ci.columnTypes[colpos].colWidth * 3);
                    else
                        buf += ci.columnTypes[colpos].colWidth;
                    break;
                }

                case CalpontSystemCatalog::BIGINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%lld%c", *((long long*)buf), ci.delimiter);

                    buf += 8;
                    break;
                }

                case CalpontSystemCatalog::UBIGINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%llu%c", *((long long unsigned*)buf), ci.delimiter);

                    buf += 8;
                    break;
                }

                case CalpontSystemCatalog::INT:
                case CalpontSystemCatalog::MEDINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%d%c", *((int32_t*)buf), ci.delimiter);

                    buf += 4;
                    break;
                }

                case CalpontSystemCatalog::UINT:
                case CalpontSystemCatalog::UMEDINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                    {
                        fprintf(ci.filePtr, "%u%c", *((uint32_t*)buf), ci.delimiter);
                        //printf("%u|", *((uint32_t*)buf));
                        //cout << *((uint32_t*)buf) << endl;
                    }

                    buf += 4;
                    break;
                }

                case CalpontSystemCatalog::SMALLINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%d%c", *((int16_t*)buf), ci.delimiter);

                    buf += 2;
                    break;
                }

                case CalpontSystemCatalog::USMALLINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%u%c", *((uint16_t*)buf), ci.delimiter);

                    buf += 2;
                    break;
                }

                case CalpontSystemCatalog::TINYINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%d%c", *((int8_t*)buf), ci.delimiter);

                    buf += 1;
                    break;
                }

                case CalpontSystemCatalog::UTINYINT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                        fprintf(ci.filePtr, "%u%c", *((uint8_t*)buf), ci.delimiter);

                    buf += 1;
                    break;
                }

                case CalpontSystemCatalog::FLOAT:
                case CalpontSystemCatalog::UFLOAT:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                    {
                        float val = *((float*)buf);

                        if ((fabs(val) > (1.0 / IDB_pow[4])) && (fabs(val) < (float) IDB_pow[6]))
                        {
                            fprintf(ci.filePtr, "%.7f%c", val, ci.delimiter);
                        }
                        else
                        {
                            fprintf(ci.filePtr, "%e%c", val, ci.delimiter);
                        }


                        //fprintf(ci.filePtr, "%.7g|", *((float*)buf));
                        //printf("%.7f|", *((float*)buf));
                    }

                    buf += 4;
                    break;
                }

                case CalpontSystemCatalog::DOUBLE:
                case CalpontSystemCatalog::UDOUBLE:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                    {
                        fprintf(ci.filePtr, "%.15g%c", *((double*)buf), ci.delimiter);
                        //printf("%.15g|", *((double*)buf));
                    }

                    buf += 8;
                    break;
                }

                case CalpontSystemCatalog::LONGDOUBLE:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    else
                    {
                        fprintf(ci.filePtr, "%.15Lg%c", *((long double*)buf), ci.delimiter);
                        //printf("%.15g|", *((double*)buf));
                    }

                    buf += 8;
                    break;
                }

                case CalpontSystemCatalog::DECIMAL:
                case CalpontSystemCatalog::UDECIMAL:
                {
                    uint bytesBefore = 1;
                    uint totalBytes = 9;

                    switch (ci.columnTypes[colpos].precision)
                    {
                        case 18:
                        case 17:
                        case 16:
                        {
                            totalBytes = 8;
                            break;
                        }

                        case 15:
                        case 14:
                        {
                            totalBytes = 7;
                            break;
                        }

                        case 13:
                        case 12:
                        {
                            totalBytes =  6;
                            break;
                        }

                        case 11:
                        {
                            totalBytes =  5;
                            break;
                        }

                        case 10:
                        {
                            totalBytes =  5;
                            break;
                        }

                        case 9:
                        case 8:
                        case 7:
                        {
                            totalBytes =  4;
                            break;
                        }

                        case 6:
                        case 5:
                        {
                            totalBytes = 3;
                            break;
                        }

                        case 4:
                        case 3:
                        {
                            totalBytes =  2;
                            break;
                        }

                        case 2:
                        case 1:
                        {
                            totalBytes = 1;
                            break;
                        }

                        default:
                            break;
                    }

                    switch (ci.columnTypes[colpos].scale)
                    {
                        case 0:
                        {
                            bytesBefore = totalBytes;
                            break;
                        }

                        case 1: //1 byte for digits after decimal point
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14)
                                    && (ci.columnTypes[colpos].precision != 12) && (ci.columnTypes[colpos].precision != 10)
                                    && (ci.columnTypes[colpos].precision != 7) && (ci.columnTypes[colpos].precision != 5)
                                    && (ci.columnTypes[colpos].precision != 3) && (ci.columnTypes[colpos].precision != 1))
                                totalBytes++;

                            bytesBefore = totalBytes - 1;
                            break;
                        }

                        case 2: //1 byte for digits after decimal point
                        {
                            if ((ci.columnTypes[colpos].precision == 18) || (ci.columnTypes[colpos].precision == 9))
                                totalBytes++;

                            bytesBefore = totalBytes - 1;
                            break;
                        }

                        case 3: //2 bytes for digits after decimal point
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14)
                                    && (ci.columnTypes[colpos].precision != 12) && (ci.columnTypes[colpos].precision != 7)
                                    && (ci.columnTypes[colpos].precision != 5) && (ci.columnTypes[colpos].precision != 3))
                                totalBytes++;

                            bytesBefore = totalBytes - 2;
                            break;
                        }

                        case 4:
                        {
                            if ((ci.columnTypes[colpos].precision == 18) || (ci.columnTypes[colpos].precision == 11)
                                    || (ci.columnTypes[colpos].precision == 9))
                                totalBytes++;

                            bytesBefore = totalBytes - 2;
                            break;

                        }

                        case 5:
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14)
                                    && (ci.columnTypes[colpos].precision != 7) && (ci.columnTypes[colpos].precision != 5))
                                totalBytes++;

                            bytesBefore = totalBytes - 3;
                            break;
                        }

                        case 6:
                        {
                            if ((ci.columnTypes[colpos].precision == 18) || (ci.columnTypes[colpos].precision == 13)
                                    || (ci.columnTypes[colpos].precision == 11) || (ci.columnTypes[colpos].precision == 9))
                                totalBytes++;

                            bytesBefore = totalBytes - 3;
                            break;
                        }

                        case 7:
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 7))
                                totalBytes++;

                            bytesBefore = totalBytes - 4;
                            break;
                        }

                        case 8:
                        {
                            if ((ci.columnTypes[colpos].precision == 18) || (ci.columnTypes[colpos].precision == 15)
                                    || (ci.columnTypes[colpos].precision == 13) || (ci.columnTypes[colpos].precision == 11)
                                    || (ci.columnTypes[colpos].precision == 9))
                                totalBytes++;

                            bytesBefore = totalBytes - 4;;
                            break;
                        }

                        case 9:
                        {
                            bytesBefore = totalBytes - 4;;
                            break;
                        }

                        case 10:
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14)
                                    && (ci.columnTypes[colpos].precision != 12) && (ci.columnTypes[colpos].precision != 10))
                                totalBytes++;

                            bytesBefore = totalBytes - 5;;
                            break;
                        }

                        case 11:
                        {
                            if (ci.columnTypes[colpos].precision == 18)
                                totalBytes++;

                            bytesBefore = totalBytes - 5;
                            break;
                        }

                        case 12:
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14)
                                    && (ci.columnTypes[colpos].precision != 12))
                                totalBytes++;

                            bytesBefore = totalBytes - 6;
                            break;
                        }

                        case 13:
                        {
                            if (ci.columnTypes[colpos].precision == 18)
                                totalBytes++;

                            bytesBefore = totalBytes - 6;
                            break;
                        }

                        case 14:
                        {
                            if ((ci.columnTypes[colpos].precision != 16) && (ci.columnTypes[colpos].precision != 14))
                                totalBytes++;

                            bytesBefore = totalBytes - 7;
                            break;
                        }

                        case 15:
                        {
                            if (ci.columnTypes[colpos].precision == 18)
                                totalBytes++;

                            bytesBefore = totalBytes - 7;
                            break;
                        }

                        case 16:
                        {
                            if (ci.columnTypes[colpos].precision != 16)
                                totalBytes++;

                            bytesBefore = totalBytes - 8;
                            break;
                        }

                        case 17:
                        {
                            if (ci.columnTypes[colpos].precision == 18)
                                totalBytes++;

                            bytesBefore = totalBytes - 8;
                            break;
                        }

                        case 18:
                        {
                            bytesBefore = totalBytes - 8;
                            break;
                        }

                        default:
                            break;
                    }

                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);
                        //printf("|");
                    }
                    else
                    {
                        uint32_t mask [5] = {0, 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF};
                        char neg = '-';

                        if (ci.columnTypes[colpos].scale == 0)
                        {
                            const uchar* tmpBuf = buf;
                            //test flag bit for sign
                            bool posNum  = tmpBuf[0] & (0x80);
                            uchar tmpChr = tmpBuf[0];
                            tmpChr ^= 0x80; //flip the bit
                            int32_t tmp1 = tmpChr;

                            if (totalBytes > 4)
                            {
                                for (uint i = 1; i < (totalBytes - 4); i++)
                                {
                                    tmp1 = (tmp1 << 8) + tmpBuf[i];
                                }

                                if (( tmp1 != 0 ) && (tmp1 != -1))
                                {
                                    if (!posNum)
                                    {
                                        tmp1 = mask[totalBytes - 4] - tmp1;

                                        if (tmp1 != 0)
                                        {
                                            fprintf(ci.filePtr, "%c", neg);
                                            //printf("%c", neg);
                                        }
                                    }

                                    if (tmp1 != 0)
                                    {
                                        fprintf(ci.filePtr, "%d", tmp1);
                                        ////printf("%d", tmp1);
                                    }
                                }

                                int32_t tmp2 = tmpBuf[totalBytes - 4];

                                for (uint i = (totalBytes - 3); i < totalBytes; i++)
                                {
                                    tmp2 = (tmp2 << 8) + tmpBuf[i];
                                }

                                if ( tmp1 != 0 )
                                {
                                    if (!posNum)
                                    {
                                        tmp2 = mask[4] - tmp2;

                                        if (tmp1 == -1)
                                        {
                                            fprintf(ci.filePtr, "%c", neg);
                                            fprintf(ci.filePtr, "%d%c", tmp2, ci.delimiter);
                                            ////printf("%c", neg);
                                            //////printf( "%d|", tmp2);
                                        }
                                        else
                                        {
                                            fprintf(ci.filePtr, "%09u%c", tmp2, ci.delimiter);
                                            ////printf("%09u|", tmp2);
                                        }
                                    }
                                    else
                                    {
                                        fprintf(ci.filePtr, "%09u%c", tmp2, ci.delimiter);
                                        //printf("%09u|", tmp2);
                                    }
                                }
                                else
                                {
                                    if (!posNum)
                                    {
                                        tmp2 = mask[4] - tmp2;
                                        fprintf(ci.filePtr, "%c", neg);
                                        //printf("%c", neg);
                                    }

                                    fprintf(ci.filePtr, "%d%c", tmp2, ci.delimiter);
                                    //printf("%d|", tmp2);
                                }
                            }
                            else
                            {
                                for (uint i = 1; i < totalBytes; i++)
                                {
                                    tmp1 = (tmp1 << 8) + tmpBuf[i];
                                }

                                if (!posNum)
                                {
                                    tmp1 = mask[totalBytes] - tmp1;
                                    fprintf(ci.filePtr, "%c", neg);
                                    //printf("%c", neg);
                                }

                                fprintf(ci.filePtr, "%d%c", tmp1, ci.delimiter);
                                //printf("%d|", tmp1);
                            }
                        }
                        else
                        {
                            const uchar* tmpBuf = buf;
                            //test flag bit for sign
                            bool posNum  = tmpBuf[0] & (0x80);
                            uchar tmpChr = tmpBuf[0];
                            tmpChr ^= 0x80; //flip the bit
                            int32_t tmp1 = tmpChr;

                            //fetch the digits before decimal point
                            if (bytesBefore == 0)
                            {
                                if (!posNum)
                                {
                                    fprintf(ci.filePtr, "%c", neg);
                                    //printf("%c", neg);
                                }

                                fprintf(ci.filePtr, "0.");
                                //printf("0.");
                            }
                            else if (bytesBefore > 4)
                            {
                                for (uint i = 1; i < (bytesBefore - 4); i++)
                                {
                                    tmp1 = (tmp1 << 8) + tmpBuf[i];
                                }

                                if (!posNum)
                                {
                                    tmp1 = mask[bytesBefore - 4] - tmp1;
                                }

                                if (( tmp1 != 0 ) && (tmp1 != -1))
                                {
                                    if (!posNum)
                                    {
                                        fprintf(ci.filePtr, "%c", neg);
                                        //printf("%c", neg);
                                    }

                                    fprintf(ci.filePtr, "%d", tmp1);
                                    //printf("%d", tmp1);
                                }

                                tmpBuf += (bytesBefore - 4);
                                int32_t tmp2 = *((int32_t*)tmpBuf);
                                tmp2 = ntohl(tmp2);

                                if ( tmp1 != 0 )
                                {
                                    if (!posNum)
                                    {
                                        tmp2 = mask[4] - tmp2;
                                    }

                                    if (tmp1 == -1)
                                    {
                                        fprintf(ci.filePtr, "%c", neg);
                                        fprintf(ci.filePtr, "%d.", tmp2);
                                        //printf("%c", neg);
                                        //printf("%d.", tmp2);
                                    }
                                    else
                                    {
                                        fprintf(ci.filePtr, "%09u.", tmp2);
                                        //printf("%09u.", tmp2);
                                    }
                                }
                                else
                                {
                                    if (!posNum)
                                    {
                                        tmp2 = mask[4] - tmp2;
                                        fprintf(ci.filePtr, "%c", neg);
                                        //printf("%c", neg);
                                    }

                                    fprintf(ci.filePtr, "%d.", tmp2);
                                    //printf("%d.", tmp2);
                                }
                            }
                            else
                            {
                                for (uint i = 1; i < bytesBefore; i++)
                                {
                                    tmp1 = (tmp1 << 8) + tmpBuf[i];
                                }

                                if (!posNum)
                                {
                                    tmp1 = mask[bytesBefore] - tmp1;
                                    fprintf(ci.filePtr, "%c", neg);
                                    //printf("%c", neg);
                                }

                                fprintf(ci.filePtr, "%d.", tmp1);
                                //printf("%d.", tmp1);
                            }

                            //fetch the digits after decimal point
                            int32_t tmp2 = 0;

                            if (bytesBefore > 4)
                                tmpBuf += 4;
                            else
                                tmpBuf += bytesBefore;

                            tmp2 = tmpBuf[0];

                            if ((totalBytes - bytesBefore) < 5)
                            {
                                for (uint j = 1; j < (totalBytes - bytesBefore); j++)
                                {
                                    tmp2 = (tmp2 << 8) + tmpBuf[j];
                                }

                                int8_t digits = ci.columnTypes[colpos].scale - 9; //9 digits is a 4 bytes chunk

                                if ( digits <= 0 )
                                    digits = ci.columnTypes[colpos].scale;

                                if (!posNum)
                                {
                                    tmp2 = mask[totalBytes - bytesBefore] - tmp2;
                                }

                                fprintf(ci.filePtr, "%0*u%c", digits, tmp2, ci.delimiter);
                                //printf("%0*u|", digits, tmp2);
                            }
                            else
                            {
                                for (uint j = 1; j < 4; j++)
                                {
                                    tmp2 = (tmp2 << 8) +  tmpBuf[j];
                                }

                                if (!posNum)
                                {
                                    tmp2 = mask[4] - tmp2;
                                }

                                fprintf(ci.filePtr, "%09u", tmp2);
                                //printf("%09u", tmp2);

                                tmpBuf += 4;
                                int32_t tmp3 = tmpBuf[0];

                                for (uint j = 1; j < (totalBytes - bytesBefore - 4); j++)
                                {
                                    tmp3 = (tmp3 << 8) +  tmpBuf[j];
                                }

                                int8_t digits = ci.columnTypes[colpos].scale - 9; //9 digits is a 4 bytes chunk

                                if ( digits < 0 )
                                    digits = ci.columnTypes[colpos].scale;

                                if (!posNum)
                                {
                                    tmp3 = mask[totalBytes - bytesBefore - 4] - tmp3;
                                }

                                fprintf(ci.filePtr, "%0*u%c", digits, tmp3, ci.delimiter);
                                //printf("%0*u|", digits, tmp3);
                            }
                        }
                    }

                    buf += totalBytes;
                    break;
                }

                case CalpontSystemCatalog::VARBINARY:
                {
                    if (nullVal && (ci.columnTypes[colpos].constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
                    {
                        fprintf(ci.filePtr, "%c", ci.delimiter);

                        if (!ci.utf8)
                        {
                            if (ci.columnTypes[colpos].colWidth < 256)
                            {
                                buf++;
                            }
                            else
                            {
                                buf = buf + 2 ;
                            }
                        }
                        else //utf8
                        {
                            if (ci.columnTypes[colpos].colWidth < 86)
                            {
                                buf++;
                            }
                            else
                            {
                                buf = buf + 2 ;
                            }
                        }
                    }
                    else
                    {
                        int dataLength = 0;

                        if (!ci.utf8)
                        {
                            if (ci.columnTypes[colpos].colWidth < 256)
                            {
                                dataLength = *(int8_t*) buf;
                                buf++;
                            }
                            else
                            {
                                dataLength = *(int16_t*) buf;
                                buf = buf + 2 ;
                            }

                            const uchar* tmpBuf = buf;

                            for (int32_t i = 0; i < dataLength; i++)
                            {
                                fprintf(ci.filePtr, "%02x", *(uint8_t*)tmpBuf);
                                tmpBuf++;
                            }

                            fprintf(ci.filePtr, "%c", ci.delimiter);
                        }
                        else //utf8
                        {
                            if (ci.columnTypes[colpos].colWidth < 86)
                            {
                                dataLength = *(int8_t*) buf;
                                buf++;
                            }
                            else
                            {
                                dataLength = *(uint16_t*) buf;
                                buf = buf + 2 ;
                            }

                            if ( dataLength > ci.columnTypes[colpos].colWidth)
                                dataLength = ci.columnTypes[colpos].colWidth;

                            const uchar* tmpBuf = buf;

                            for (int32_t i = 0; i < dataLength; i++)
                            {
                                fprintf(ci.filePtr, "%02x", *(uint8_t*)tmpBuf);
                                tmpBuf++;
                            }

                            fprintf(ci.filePtr, "%c", ci.delimiter);
                        }
                    }

                    if (ci.utf8)
                        buf += (ci.columnTypes[colpos].colWidth * 3);
                    else
                        buf += ci.columnTypes[colpos].colWidth;

                    break;
                }

                case CalpontSystemCatalog::BLOB:
                case CalpontSystemCatalog::TEXT:
                {
                    // MCOL-4005 Note that we don't handle nulls as a special
                    // case here as we do for other datatypes, the below works
                    // as expected for nulls.
                    uint32_t dataLength = 0;
                    uintptr_t* dataptr;
                    uchar* ucharptr;
                    uint colWidthInBytes = (ci.utf8 ?
                        ci.columnTypes[colpos].colWidth * 3: ci.columnTypes[colpos].colWidth);

                    if (colWidthInBytes < 256)
                    {
                        dataLength = *(uint8_t*) buf;
                        buf++;
                    }
                    else if (colWidthInBytes < 65536)
                    {
                        dataLength = *(uint16_t*) buf;
                        buf += 2;
                    }
                    else if (colWidthInBytes < 16777216)
                    {
                        dataLength = *(uint16_t*) buf;
                        dataLength |= ((int) buf[2]) << 16;
                        buf += 3;
                    }
                    else
                    {
                        dataLength = *(uint32_t*) buf;
                        buf += 4;
                    }

                    // buf contains pointer to blob, for example:
                    // (gdb) p (char*)*(uintptr_t*)buf
                    // $43 = 0x7f68500c58f8 "hello world"

                    dataptr = (uintptr_t*)buf;
                    ucharptr = (uchar*)*dataptr;
                    buf += sizeof(uintptr_t);

                    if (ci.columnTypes[colpos].colDataType == CalpontSystemCatalog::BLOB)
                    {
                        for (uint32_t i = 0; i < dataLength; i++)
                        {
                            fprintf(ci.filePtr, "%02x", *(uint8_t*)ucharptr);
                            ucharptr++;
                        }

                        fprintf(ci.filePtr, "%c", ci.delimiter);
                    }
                    else
                    {
                        // TEXT Column
                        escape.assign((char*)ucharptr, dataLength);
                        boost::replace_all(escape, "\\", "\\\\");
                        fprintf(ci.filePtr, "%c%.*s%c%c", ci.enclosed_by, (int)escape.length(), escape.c_str(), ci.enclosed_by, ci.delimiter);
                    }

                    break;

                }

                default:	// treat as int64
                {
                    break;
                }
            }

            colpos++;
        }
    }

    rc = fprintf(ci.filePtr, "\n"); //@bug 6077 check whether thhe pipe is still open

    if ( rc < 0)
        rc = -1;
    else
        rc = 0;

    return rc;
}

std::string  ha_mcs_impl_viewtablelock( cal_impl_if::cal_connection_info& ci, execplan::CalpontSystemCatalog::TableName& tablename)
{
    THD* thd = current_thd;
    ulong sessionID = tid2sid(thd->thread_id);
    CalpontDMLPackage* pDMLPackage;
    std::string dmlStatement( "VIEWTABLELOCK" );
    VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
    pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
    pDMLPackage->set_SchemaName (tablename.schema);
    pDMLPackage->set_TableName (tablename.table);

    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);
    pDMLPackage->write(bytestream);
    delete pDMLPackage;

    ByteStream::byte b = 0;
    ByteStream::octbyte rows;
    std::string errorMsg;
    std::string tableLockInfo;
    //int dmlRowCount = 0;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();

        if ( bytestream.length() == 0 )
        {
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [5]");
        }
        else
        {
            bytestream >> b;
            bytestream >> rows;
            bytestream >> errorMsg;
            bytestream >> tableLockInfo;
        }

    }
    catch (runtime_error&)
    {
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [6]");
    }
    catch (...)
    {
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, "Caught unknown error");
    }

    if ( b != 0 )
        tableLockInfo = errorMsg;

    return tableLockInfo;

}

//------------------------------------------------------------------------------
// Clear the table lock associated with the specified table lock id.
// Any bulk rollback that is pending will be applied before the table
// lock is released.
//------------------------------------------------------------------------------
std::string  ha_mcs_impl_cleartablelock(
    cal_impl_if::cal_connection_info& ci,
    uint64_t tableLockID)
{
    execplan::CalpontSystemCatalog::TableName tblName;
    THD* thd        = current_thd;
    ulong sessionID = tid2sid(thd->thread_id);
    std::string tableLockInfo;
    BRM::TableLockInfo lockInfo;

    // Perform preliminary setup.  CalpontDMLPackage expects schema and table
    // name to be provided, so we get the table OID for the specified table
    // lock, and then get the table name for the applicable table OID.
    std::string prelimTask;

    try
    {
        BRM::DBRM brm;
        prelimTask = "getting table locks from BRM.";
        bool getLockInfo = brm.getTableLockInfo(tableLockID, &lockInfo);

        if (!getLockInfo)
        {
            tableLockInfo = "No table lock found for specified table lock ID";
            return tableLockInfo;
        }

        boost::shared_ptr<execplan::CalpontSystemCatalog> csc =
            execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
        csc->identity(execplan::CalpontSystemCatalog::FE);

        prelimTask = "getting table name from system catalog.";
        tblName    = csc->tableName( lockInfo.tableOID );
    }
    catch (std::exception& ex)
    {
        std::string eMsg(ex.what());
        eMsg += " Error ";
        eMsg += prelimTask;

        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, eMsg.c_str());
        return tableLockInfo;
    }
    catch (...)
    {
        std::string eMsg(" Error ");
        eMsg += prelimTask;

        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, eMsg.c_str());
        return tableLockInfo;
    }

    std::string dmlStatement( "CLEARTABLELOCK" );
    VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
    CalpontDMLPackage* pDMLPackage =
        CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer( cmdStmt );
    pDMLPackage->set_SchemaName(tblName.schema);
    pDMLPackage->set_TableName (tblName.table );

    // Table lock ID is passed in the SQL statement attribute
    std::ostringstream lockIDString;
    lockIDString << tableLockID;
    pDMLPackage->set_SQLStatement( lockIDString.str() );

    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);
    pDMLPackage->write(bytestream);
    delete pDMLPackage;

    ByteStream::byte b = 0;
    ByteStream::octbyte rows;
    std::string errorMsg;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();

        if ( bytestream.length() == 0 )
        {
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [7]");
        }
        else
        {
            bytestream >> b;
            bytestream >> rows;
            bytestream >> errorMsg;
            bytestream >> tableLockInfo;
        }

    }
    catch (runtime_error&)
    {
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DMLProc [8]");
    }
    catch (...)
    {
        thd->get_stmt_da()->set_overwrite_status(true);
        thd->raise_error_printf(ER_INTERNAL_ERROR, "Caught unknown error");
    }

    //@Bug 2606. Send error message back to sql session
    if ( b != 0 )
        tableLockInfo = errorMsg;

    return tableLockInfo;
}

int ha_mcs_impl_commit_ (handlerton* hton, THD* thd, bool all, cal_connection_info& ci )
{
    int rc = 0;

    if (thd->slave_thread && !get_replication_slave(thd))
        return 0;

    std::string command("COMMIT");
#ifdef INFINIDB_DEBUG
    cout << "COMMIT" << endl;
#endif
    rc = ProcessCommandStatement(thd, command, ci);
    return rc;
}

int ha_mcs_impl_rollback_ (handlerton* hton, THD* thd, bool all, cal_connection_info& ci)
{
    int rc = 0;
#ifdef INFINIDB_DEBUG
    cout << "ROLLBACK" << endl;
#endif

    if (useHdfs)
    {
        string msg = string("Some non-transactional changed tables couldn't be rolled back");
        //	cout << "Some non-transactional changed tables couldn't be rolled back" << endl;
        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 1196, msg.c_str());
        return rc;
    }

    std::string command("ROLLBACK");
    rc = ProcessCommandStatement(thd, command, ci);
    return rc;
}

int ha_mcs_impl_close_connection_ (handlerton* hton, THD* thd, cal_connection_info& ci )
{
    int rc = 0;
#ifdef INFINIDB_DEBUG
    cout << "Close connection session ID " << thd->thread_id << endl;
#endif

    if ( !ci.dmlProc )
    {
        return rc;
    }

    std::string command("CLEANUP");
    rc = ProcessCommandStatement(thd, command, ci);
    // @bug 1622. remove calpontsystemcatalog and close the socket when session quit.
    // @info when Calpont process a select query, an alter table phase is involved in
    // the vtable design, which will auto start a transaction. when autocommit on (by default), rollback is automically called
    // when session quit. rollback can also be called by user explicitly to rollback
    // a transaction. Under either situation, system catalog cache for this session should
    // be removed
    return rc;
}

// vim:ts=4 sw=4:

