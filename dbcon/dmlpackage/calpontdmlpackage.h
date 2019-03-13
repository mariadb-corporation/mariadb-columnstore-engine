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
 *   $Id: calpontdmlpackage.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef CALPONTDMLPACKAGE_H
#define CALPONTDMLPACKAGE_H
#include <string>
#include <boost/uuid/uuid.hpp>

#include "bytestream.h"
#include "dmlpackage.h"
#include "dmltable.h"
#include "calpontselectexecutionplan.h"
#include "we_chunkmanager.h"
#define DML_PACKAGE_DEBUG

namespace dmlpackage
{
/** @brief abstract class that defines the general interface and
 * implemetation of a CalpontDMLPackage
 */
class CalpontDMLPackage
{

public:
    /** @brief ctor
     */
    CalpontDMLPackage();

    /** @brief ctor
     *
     * @param schemaName the schema of the table being operated on
     * @param tableName the name of the table being operated on
     * @param dmlStatement the dml statement
     * @param sessionID the session id
     */
    CalpontDMLPackage( std::string schemaName, std::string tableName,
                       std::string dmlStatement, int sessionID );

    /** @brief dtor
     */
    virtual ~CalpontDMLPackage();

    /** @brief write a CalpontDMLPackage to a ByteStream
     *
     * @param bytestream the ByteStream to write to
     */
    virtual int write( messageqcpp::ByteStream& bytestream ) = 0;

    /** @brief read a CalpontDMLPackage from a ByteStream
     *
     * @param bytestream the ByteStream to read from
     */
    virtual int read( messageqcpp::ByteStream& bytestream ) = 0;

    /** @brief build a CalpontDMLPackage from a string buffer
     *
     * @param buffer the row buffer
     * @param columns the number of columns in the buffer
     * @param rows the number of rows in the buffer
     */
    virtual int buildFromBuffer( std::string& buffer, int columns, int rows ) = 0;

    /** @brief build a CalpontDMLPackage from a parsed SqlStatement
     *
     * @param sqlStatement the parsed SqlStatement
     */
    virtual int buildFromSqlStatement( SqlStatement& sqlStatement ) = 0;

    /** @brief build a CalpontDMLPackage from valuelist built from mysql table fields
     *
     * @param tableValuesMap  the value list for each column in the table
     * @param colNameList the column name for each column
     * @param columns number of columns in the table
     * @param rows  number of rows to be touched
     */
    virtual int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns, int rows, NullValuesBitset& nullValues) = 0;

    /** @brief get the table object
     */
    DMLTable* get_Table()
    {
        return fTable;
    }

    /** @brief set the DML statement (the parsed statement)
     *
     * @param statement the dml statement to set
     */
    void set_DMLStatement( const std::string& statement )
    {
        fDMLStatement = statement;
    }

    /** @brief get the DML statement (the parsed statement)
     */
    const std::string get_DMLStatement() const
    {
        return fDMLStatement;
    }

    /** @brief set the SQL statement (the original SQL statement)
     *
     * @param statement the SQL statement to set (the original SQL statement with quotes)
     */
    void set_SQLStatement( const std::string& statement )
    {
        fSQLStatement = statement;
    }

    /** @brief get the SQL statement (the original SQL statement)
     */
    const std::string get_SQLStatement() const
    {
        return fSQLStatement;
    }

    /** @brief get the logging flag
     */
    const bool get_Logging() const
    {
        return fLogging;
    }

    /** @brief set the logging flag
     *
     * @param logging the logging flag to set
     */
    void set_Logging( bool logging )
    {
        fLogging = logging;
    }

    /** @brief get the logending flag
     */
    const bool get_Logending() const
    {
        return fLogending;
    }

    /** @brief set the logending flag
     *
     * @param logending the logending flag to set
     */
    void set_Logending( bool logending )
    {
        fLogending = logending;
    }

    /** @brief get the isFromCol flag
    */
    const bool get_IsFromCol() const
    {
        return fIsFromCol;
    }

    /** @brief set the update column from column flag
     *
     * @param logging the logging flag to set
     */
    void set_IsFromCol ( bool isFromCol )
    {
        fIsFromCol = isFromCol;
    }
    /** @brief set the Table name
     *
     * @param tableName the name to set
     */
    void set_TableName( std::string& tableName )
    {
        fTableName = tableName;

        if (fTable != 0)
            fTable->set_TableName(tableName);
    }

    /** @brief get the Table name
     */
    const std::string get_TableName() const
    {
        return fTableName;
    }

    /** @brief set the Schema name
     *
     * @param the schema to set
     */
    void set_SchemaName( std::string& schemaName )
    {
        fSchemaName = schemaName;

        if (fTable != 0)
            fTable->set_SchemaName(schemaName);
    }

    /** @brief get the Schema name
     */
    const std::string get_SchemaName() const
    {
        return fSchemaName;
    }

    /** @brief does this dml statement have a filter
     */
    bool HasFilter() const
    {
        return fHasFilter;
    }
    void HasFilter( bool hasFilter)
    {
        fHasFilter = hasFilter;
    }

    /** @brief get the filter  statement
     */
    const std::string get_QueryString() const
    {
        return fQueryString;
    }

    /** @brief set the sessionID associated with this package
     */
    void set_SessionID( int sessionID )
    {
        fSessionID = sessionID;
    }

    /** @brief get the sessionID associated with this package
     */
    int get_SessionID() const
    {
        return fSessionID;
    }

    /** @brief set the transaction ID associated with this package
     */
    void set_TxnID( execplan::CalpontSystemCatalog::SCN txnID )
    {
        fTxnId = txnID;
    }

    /** @brief get the transaction ID associated with this package
     */
    execplan::CalpontSystemCatalog::SCN get_TxnID() const
    {
        return fTxnId;
    }
    /** @brief set the chunkmanager associated with this package
     */
    void set_ChunkManager( WriteEngine::ChunkManager* cm )
    {
        fCM = cm;
    }

    /** @brief get the chunkmanager associated with this package
     */
    WriteEngine::ChunkManager* get_ChunkManager() const
    {
        return fCM;
    }

    /** @brief get the ExecutionPlan associated with this package
     */
    boost::shared_ptr<messageqcpp::ByteStream> get_ExecutionPlan()
    {
        return fPlan;
    }

    bool get_isInsertSelect()
    {
        return fIsInsertSelect;
    }
    void set_isInsertSelect( const bool isInsertSelect )
    {
        fIsInsertSelect = isInsertSelect;
    }

    bool get_isBatchInsert()
    {
        return fIsBatchInsert;
    }
    void set_isBatchInsert( const bool isBatchInsert )
    {
        fIsBatchInsert = isBatchInsert;
    }

    bool get_isAutocommitOn()
    {
        return fIsAutocommitOn;
    }
    void set_isAutocommitOn( const bool isAutocommitOn )
    {
        fIsAutocommitOn = isAutocommitOn;
    }

    bool get_isWarnToError()
    {
        return fIsWarnToError;
    }
    void set_isWarnToError( const bool isWarnToError )
    {
        fIsWarnToError = isWarnToError;
    }

    uint32_t getTableOid()
    {
        return fTableOid;
    }
    void setTableOid( const uint32_t tableOid )
    {
        fTableOid = tableOid;
    }

    void uuid(const boost::uuids::uuid& uuid)
    {
        fUuid = uuid;
    }
    const boost::uuids::uuid& uuid() const
    {
        return fUuid;
    }

protected:

    void initializeTable();

    std::string fSchemaName;
    std::string fTableName;
    std::string fDMLStatement;
    std::string fSQLStatement;
    std::string fQueryString;
    int fSessionID;
    boost::uuids::uuid fUuid;
    execplan::CalpontSystemCatalog::SCN        fTxnId;
    boost::shared_ptr<messageqcpp::ByteStream> fPlan;
    DMLTable*    fTable;
    bool fHasFilter;
    bool fLogging;
    bool fLogending;
    bool fIsFromCol;
    std::string StripLeadingWhitespace( std::string value );
    bool fIsInsertSelect;
    bool fIsBatchInsert;
    bool fIsAutocommitOn;
    bool fIsWarnToError;
    uint32_t fTableOid;
    WriteEngine::ChunkManager* fCM;
};
}
#endif                                            //CALPONTDMLPACKAGE_H
