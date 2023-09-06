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

/***********************************************************************
 *   $Id: calpontsystemcatalog.h 9474 2013-05-02 15:28:09Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once

#include <unistd.h>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <boost/any.hpp>
#include <boost/thread.hpp>
#include <fstream>
#include <boost/shared_ptr.hpp>
#include <limits>
#include <iosfwd>
#include <limits>

#include "mcs_basic_types.h"
#include "../../writeengine/shared/we_typeext.h"
#include "columnresult.h"
#include "bytestream.h"
#include "joblisttypes.h"
#include "stdexcept"

#undef min
#undef max

#include "mcs_datatype.h"
#include "collation.h"  // CHARSET_INFO, class Charset
#include "nullstring.h"

class ExecPlanTest;
namespace messageqcpp
{
class MessageQueueClient;
}

// This is now set in the Columnstore.xml file
// Use, e.g., 0x500 for uid 500 so it is easy to spot in ipcs list
// const int32_t BRM_UID = 0x0;

namespace execplan
{
class CalpontSelectExecutionPlan;
class ClientRotator;
class SessionManager;

/** MySQL $VTABLE ID */
const int32_t CNX_VTABLE_ID = 100;
const int32_t IDB_VTABLE_ID = CNX_VTABLE_ID;

/** The CalpontSystemCatalog class
 *
 * This object encapsulates the system catalog stored in the engine
 */
class CalpontSystemCatalog : public datatypes::SystemCatalog
{
 public:
  /** static calpontsystemcatalog instance map. one instance per session
   *  TODO: should be one per transaction
   */
  typedef std::map<uint32_t, boost::shared_ptr<CalpontSystemCatalog> > CatalogMap;

  /** Server Identity
   *
   */
  enum Identity
  {
    EC = 0,
    FE
  };

  /** the set of column constraint types
   *
   */
  enum ConstraintType
  {
    NO_CONSTRAINT,
    UNIQUE_CONSTRAINT,
    CHECK_CONSTRAINT,
    NOTNULL_CONSTRAINT,
    PRIMARYKEY_CONSTRAINT,
    REFERENCE_CONSTRAINT,
    DEFAULT_CONSTRAINT
  };

  enum CompressionType
  {
    NO_COMPRESSION,
    COMPRESSION1,
    COMPRESSION2
  };

  enum AutoincrColumn
  {
    NO_AUTOINCRCOL,
    AUTOINCRCOL
  };

  /** the type of an object number
   *
   * @todo TODO: OIDs aren't really signed. This should be fixed.
   */
  typedef int32_t OID;

  /** @brief a structure to hold a dictionary's
   *  object ids
   */
  struct DictOID
  {
    DictOID() : dictOID(0), listOID(0), treeOID(0), compressionType(0)
    {
    }
    DictOID(OID dictOID_, OID listOID_, OID treeOID_, int compressionType_) :
      dictOID(dictOID_), listOID(listOID_), treeOID(treeOID_),
      compressionType(compressionType_)
    {
    }
    DictOID(const DictOID& rhs)
    : dictOID(rhs.dictOID), listOID(rhs.listOID), treeOID(rhs.treeOID), compressionType(rhs.compressionType)
    {
    }
    OID dictOID;
    OID listOID;
    OID treeOID;
    int compressionType;
    bool operator==(const DictOID& t) const
    {
      if (dictOID != t.dictOID)
        return false;

      if (listOID != t.listOID)
        return false;

      if (treeOID != t.treeOID)
        return false;

      if (compressionType != t.compressionType)
        return false;

      return true;
    }
    bool operator!=(const DictOID& t) const
    {
      return !(*this == t);
    }
  };

  /** the type of a list of ColumnResult as returned from getSysData
   */
  typedef std::vector<ColumnResult*> NJLSysDataVector;
  struct NJLSysDataList
  {
    // If we used an unorderedmap<OID, ColumnResult*>, we might improve performance.
    // Maybe.
    NJLSysDataVector sysDataVec;
    NJLSysDataList(){};
    ~NJLSysDataList();
    NJLSysDataVector::const_iterator begin() const
    {
      return sysDataVec.begin();
    }
    NJLSysDataVector::const_iterator end() const
    {
      return sysDataVec.end();
    }
    void push_back(ColumnResult* cr)
    {
      sysDataVec.push_back(cr);
    }
    NJLSysDataVector::size_type size() const
    {
      return sysDataVec.size();
    }
    NJLSysDataVector::size_type findColumn(const OID& columnOID) const
    {
      for (NJLSysDataVector::size_type i = 0; i < sysDataVec.size(); i++)
        if (sysDataVec[i]->ColumnOID() == columnOID)
          return i;

      return -1;
    }
  };

  /** the type of a list of dictionary OIDs
   */
  typedef std::vector<DictOID> DictOIDList;

  /** the structure returned by colType
   *
   * defaultValue is only meaningful when constraintType == DEFAULT_CONSTRAINT
   */
  struct ColType : public datatypes::SystemCatalog::TypeHolderStd
  {
    ConstraintType constraintType = NO_CONSTRAINT;
    DictOID ddn;
    NullString defaultValue;
    int32_t colPosition = -1;  // temporally put here. may need to have ColInfo struct later
    int32_t compressionType = NO_COMPRESSION;
    OID columnOID = 0;
    bool autoincrement = 0;  // set to true if  SYSCOLUMN autoincrement is �y�
    uint64_t nextvalue = 0;  // next autoincrement value
    uint32_t charsetNumber = default_charset_info->number;
    const CHARSET_INFO* cs = nullptr;

   private:
    long timeZone;

   public:
    ColType() = default;
    ColType(const ColType& rhs);
    ColType(int32_t colWidth_, int32_t scale_, int32_t precision_,
            const ConstraintType& constraintType_, const DictOID& ddn_, int32_t colPosition_,
            int32_t compressionType_, OID columnOID_, const ColDataType& colDataType_);
    ColType& operator=(const ColType& rhs);

    CHARSET_INFO* getCharset();

    long getTimeZone() const
    {
      return timeZone;
    }
    void setTimeZone(long timeZone_)
    {
      timeZone = timeZone_;
    }

    // for F&E use. only serialize necessary info for now
    void serialize(messageqcpp::ByteStream& b) const
    {
      b << (uint32_t)colDataType;
      b << (uint32_t)colWidth;
      b << (uint32_t)scale;
      b << (uint32_t)precision;
      b << (uint32_t)compressionType;
      b << charsetNumber;
    }

    void unserialize(messageqcpp::ByteStream& b)
    {
      uint32_t val;
      b >> (uint32_t&)val;
      colDataType = (ColDataType)val;
      b >> (uint32_t&)colWidth;
      b >> (uint32_t&)scale;
      b >> (uint32_t&)precision;
      b >> (uint32_t&)compressionType;
      b >> charsetNumber;
    }

    /**
     * @brief convert a columns data, represnted as a string,
     * to its native format
     * @param       data       - the string representation
     * @param [OUT] bSaturate  - the value was truncated/adjusted
     * @param       timeZone   - the time zone name, for TIMESTAMP conversion
     * @param       nullFlag   - SQL NULL flag
     * @param       nRoundtrip
     * @param       isUpdate
     */
    boost::any convertColumnData(const std::string& data, bool& bSaturate, long timeZone,
                                 bool nulFlag = false, bool noRoundup = false, bool isUpdate = false) const;

    /**
     * @brief convert a columns data, represnted as a string,
     * to its native format
     * @param       data       - the string representation, with special NULL value
     * @param [OUT] bSaturate  - the value was truncated/adjusted
     * @param       timeZone   - the time zone name, for TIMESTAMP conversion
     * @param       nRoundtrip
     * @param       isUpdate
     */
    boost::any convertColumnData(const NullString& data, bool& bSaturate, long timeZone,
                                 bool noRoundup = false, bool isUpdate = false) const;

    const std::string toString() const;

    // Put these here so udf doesn't need to link libexecplan
    bool operator==(const ColType& t) const
    {
      // order these with the most likely first
      if (columnOID != t.columnOID)
        return false;

      if (colPosition != t.colPosition)
        return false;

      if (ddn != t.ddn)
        return false;

      if (colWidth != t.colWidth)
        return false;

      if (scale != t.scale)
        return false;

      if (precision != t.precision)
        return false;

      if (constraintType != t.constraintType)
        return false;

      return true;
    }

    bool operator!=(const ColType& t) const
    {
      return !(*this == t);
    }

    static ColType convertUnionColType(std::vector<ColType>&, unsigned int&);
  };

  /** the structure of a table infomation
   *
   *  this structure holds table related info. More in the future
   */
  struct TableInfo
  {
    TableInfo() : numOfCols(0), tablewithautoincr(NO_AUTOINCRCOL)
    {
    }
    int numOfCols;
    int tablewithautoincr;
  };

  /** the type of a Row ID
   *
   * @todo TODO: RIDs aren't really signed. This should be fixed.
   */
  //@bug 1866 match RID to writeengine, uint64_t
  typedef WriteEngine::RID RID;

  /**
   *
   */

  /** the type of a <Row ID, object number> pair
   *
   * @todo TODO: the rid field in this structure does not make sense. the structure is equalivent
   * to OID and therefore will be deprecated soon.
   */
  //@bug 1866 changed rid default to 0; RID matches writeengine rid, uint64_t
  struct ROPair
  {
    ROPair() : rid(std::numeric_limits<RID>::max()), objnum(0)
    {
    }
    RID rid;
    OID objnum;
  };

  /** the type of a list of Object ID's
   *
   * @todo TODO: the rid field in this structure does not make sense. the structure is equalivent
   * to OID and therefore will be deprecated soon.
   */
  typedef std::vector<ROPair> RIDList;

  /** the type of a <indexTreeOID, indexListOID> pair
   *
   */
  struct IndexOID
  {
    OID objnum;
    OID listOID;
    bool multiColFlag;
  };

  /** the type of a list of Index OIDs
   */
  typedef std::vector<IndexOID> IndexOIDList;

  /** the structure for libcalmysql to use. Alias is taken account to the identity of tables.
   *
   */
  struct TableAliasName
  {
    TableAliasName() : fisColumnStore(true)
    {
    }
    TableAliasName(const std::string& sch, const std::string& tb, const std::string& al)
     : schema(sch), table(tb), alias(al), fisColumnStore(true)
    {
    }
    TableAliasName(const std::string& sch, const std::string& tb, const std::string& al, const std::string& v)
     : schema(sch), table(tb), alias(al), view(v), fisColumnStore(true)
    {
    }
    std::string schema;
    std::string table;
    std::string alias;
    std::string view;
    bool fisColumnStore;
    void clear();
    bool operator<(const TableAliasName& rhs) const;
    bool operator>=(const TableAliasName& rhs) const
    {
      return !(*this < rhs);
    }
    bool operator==(const TableAliasName& rhs) const
    {
      return (schema == rhs.schema && table == rhs.table && alias == rhs.alias && view == rhs.view &&
              fisColumnStore == rhs.fisColumnStore);
    }
    bool operator!=(const TableAliasName& rhs) const
    {
      return !(*this == rhs);
    }
    void serialize(messageqcpp::ByteStream&) const;
    void unserialize(messageqcpp::ByteStream&);
    friend std::ostream& operator<<(std::ostream& os, const TableAliasName& rhs);
  };

  /** the structure passed into various RID methods
   *
   */
  struct TableName
  {
    TableName()
    {
    }
    TableName(std::string sch, std::string tb) : schema(sch), table(tb)
    {
    }
    TableName(const TableAliasName& tan) : schema(tan.schema), table(tan.table)
    {
    }
    std::string schema;
    std::string table;
    int64_t create_date = 0;
    bool operator<(const TableName& rhs) const;
    bool operator>=(const TableName& rhs) const
    {
      return !(*this < rhs);
    }
    bool operator==(const TableName& rhs) const
    {
      return (schema == rhs.schema && table == rhs.table);
    }
    bool operator!=(const TableName& rhs) const
    {
      return !(*this == rhs);
    }
    const std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const TableName& rhs)
    {
      os << rhs.toString();
      return os;
    }
  };

  /** the structure passed into get RID for Index values
   *
   */
  struct IndexName
  {
    std::string schema;
    std::string table;
    std::string index;
    bool multiColFlag;
    bool operator<(const IndexName& rhs) const;
  };

  /** the structure of a constraint infomation */
  struct ConstraintInfo
  {
    ConstraintInfo() : constraintType(NO_CONSTRAINT)
    {
    }
    int constraintType;
    IndexName constraintName;
    std::string constraintText;
    std::string referenceSchema;  // for foreign key constraint
    std::string referenceTable;   // for foreign key constraint
    std::string referencePKName;  // for foreign key constraint
    std::string constraintStatus;
  };

  /** the structure passed into lookupOID
   *
   */
  struct TableColName
  {
    std::string schema;
    std::string table;
    std::string column;
    bool operator<(const TableColName& rhs) const;
    const std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const TableColName& rhs);
  };

  typedef std::vector<IndexName> IndexNameList;
  typedef std::vector<TableColName> TableColNameList;

  /** the type of a System Change Number
   *
   */
  typedef int SCN;

  /** the type of an index number
   *
   */
  typedef int INDNUM;

  /** the type returned by makeCalpontSystemCatalog()
   *
   */
  typedef boost::shared_ptr<CalpontSystemCatalog> SPCSC;

  /** looks up a table's OID in the System Catalog
   *
   * For a unique table_name return the internal OID
   */
  OID lookupTableOID(const TableName& tableName, int lower_case_table_names = 0);

  /** looks up a column's OID in the System Catalog
   *
   * For a unique table_name.column_name return the internal OID
   */
  OID lookupOID(const TableColName& tableColName, int lower_case_table_names = 0);

  /** returns the column type attribute(s) for a column
   *
   * return the various column attributes for a given OID
   */
  const ColType colType(const OID& oid);

  /** returns the column type attribute(s) for a column gived a dictionary OID
   *
   * return the same thing as colType does for the corresponding token OID
   */
  const ColType colTypeDct(const OID& dictOid);

  /** returns the table column name for a column
   *
   * return the table column name for a given OID:
   */
  const TableColName colName(const OID& oid);
  /** returns the table column name for a dictionary column
   *
   * return the table column name for a given OID:
   */
  const TableColName dictColName(const OID& oid);

  /** returns the next value of autoincrement for the table
   *
   * return the next value of autoincrement for a given table:
   * AUTOINCR_SATURATED: limit exceeded
   * 0: Autoincrement does not exist for this table
   * Throws runtime_error if no such table found
   */
  uint64_t nextAutoIncrValue(TableName tableName, int lower_case_table_names = 0);

  /** returns the rid of next autoincrement value for the table oid
   *
   * return the rid of next value of autoincrement for a given table:
   */
  const ROPair nextAutoIncrRid(const OID& oid);

  /** returns the oid of autoincrement column for the table
   *
   * return the oid of autoincrement column for a given table:
   */
  int32_t autoColumOid(TableName tableName, int lower_case_table_names = 0);

  /** returns the columns bitmap file object number
   *
   * return the bitmap file object number for a given OID:
   */
  OID colBitmap(const OID& oid) const;

  /** return the current SCN
   *
   * returns the current System Change Number (for versioning support)
   */
  SCN scn(void) const;

  /** return the RID's of the indexes for a table
   *
   * returns a list of the RID's of the indexes for a table
   */
  const RIDList indexRIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** return the total number of columns for a table
   *
   * returns the total number of columns for a table
   */
  int colNumbers(const TableName& tableName, int lower_case_table_names = 0);

  /** return the RID's of the colindexes for a table
   *
   * returns a list of the RID's of the colindexes for a table
   */
  const RIDList indexColRIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** return the RID's of the index columns (SYSINDEXCOL) for a index
   *
   * returns a list of the RID's of the colindexes for a index
   */
  const RIDList indexColRIDs(const IndexName& indexName, int lower_case_table_names = 0);

  /** return the RID's of the constraints for a table
   *
   * returns a list of the RID's of the constraints for a table
   */
  const RIDList constraintRIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** return the RID of the constraint for a ConstrainName
   *
   * returns a RID of the constraint for a ConstrainName fron table SYSCONSTRAINT
   */
  RID constraintRID(const std::string constraintName);

  /** return the list of IndexName for a given TableColName
   *
   * return the list of IndexName for a given TableColName from table SYSCONSTRAINTCOL
   */
  const IndexNameList colValueSysconstraint(const TableColName& colName, bool useCache = false);

  /** return the RID's of the colconstraints for a table
   *
   * returns a list of the RID's of the colconstraints for a table
   */
  const RIDList constraintColRIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** return the RID of the colconstraint for a column
   *
   * returns the RID of the colconstraints for a column
   */
  RID constraintColRID(const TableColName& tableColName, int lower_case_table_names = 0);

  /** return the value for the given RID and column name from table SYSCONSTRAINTCOL
   *
   * returns the column value for the given RID a column name fron table SYSCONSTRAINTCOL
   */
  const std::string colValueSysconstraintCol(const TableColName& colName, int lower_case_table_names = 0);

  /** return the RID of the constraint for a ConstrainName
   *
   * returns a RID of the constraint for a ConstrainName fron table SYSCONSTRAINTCOL
   */
  const RIDList constraintColRID(const std::string constraintName);

  /** return the ROPair of the column
   *
   * @note the RID field in ROPair does not make sense. the purpose of this function is to
   * return OID. Therefore it's duplicate to lookupOID function. This function is to be
   * deprecated.
   */
  const ROPair columnRID(const TableColName& tableColName, int lower_case_table_names = 0);

  /** return the RID's of the columns for a table
   *
   * returns a list of the RID's of the columns for a table
   */
  const RIDList columnRIDs(const TableName& tableName, bool useCache = false, int lower_case_table_names = 0);

  /** return the RID of the table
   *
   * returns the RID of the table
   */
  const ROPair tableRID(const TableName& tableName, int lower_case_table_names = 0);

  /** return the OID of the table's AUX column
   *
   * returns the OID of the table's AUX column
   */
  OID tableAUXColumnOID(const TableName& tableName, int lower_case_table_names = 0);

  /** returns the table OID if the input OID is the AUX
   *  column OID of the table.
   */
  CalpontSystemCatalog::OID isAUXColumnOID(const OID& oid);

  /** return the RID of the index for a table
   *
   * returns the RID of the indexes for a table
   */
  const ROPair indexRID(const IndexName& indexName);
  /** return the INDEX NAME list for a table
   *
   * returns the index name list for a table
   */
  const IndexNameList indexNames(const TableName& tableName, int lower_case_table_names = 0);
  /** return the column names for a index
   *
   * returns the column name list for a index
   */
  const TableColNameList indexColNames(const IndexName& indexName);

  /** return the column names for a index
   *
   * returns the column name list for a index
   */
  const TableColNameList constraintColNames(const std::string constraintName);

  /** return the value for the given RID and column name from table SYSINDEX
   *
   * returns the column value for the given RID a column name fron table SYSINDEX
   */
  const std::string colValueSysindex(const TableColName& colName, int lower_case_table_names = 0);

  /** return the RID of the colindexe for a table
   *
   * returns the RID's of the colindexe for a table
   */
  const RIDList indexColRID(const IndexName& indexName);

  /** return the RID of the colindexe for a table
   *
   * returns the RID's of the colindexe for a table
   */
  const ROPair indexColRID(const TableColName& tableColName, int lower_case_table_names = 0);

  /** return the value for the given RID and column name from table SYSINDEX
   *
   * returns the column value for the given RID a column name fron table SYSINDEX
   */
  const IndexNameList colValueSysindexCol(const TableColName& colName, int lower_case_table_names = 0);

  /** looks up a Table Name for a Index in the System Catalog
   *
   * For a unique Index return the Table Name Structure
   */
  const TableName lookupTableForIndex(const std::string indexName, const std::string schema);

  /** looks up a index oid for a Index in the System Catalog
   *
   * For an index name return index OID
   */
  const IndexOID lookupIndexNbr(const IndexName& indexName);

  /** look up an Index Number for the given table column name
   *
   * If an index exists for the table and column return its number
   * @note if one column belongs to multiple index, this function will get confused.
   * for now, assume one column belongs to just one index. In the future getPlan
   * should give index name therefore this function will be deprecated.
   */
  const IndexOID lookupIndexNbr(const TableColName& tableColName, int lower_case_table_names = 0);

  /** return the list of Index OIDs for the given table
   *
   * returns the list of Index OIDs for a table
   */
  const IndexOIDList indexOIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** return the list of Dictionary OIDs for the given table
   *
   * returns the list of Dictionary OIDs for a table
   */
  const DictOIDList dictOIDs(const TableName& tableName, int lower_case_table_names = 0);

  /** Update column OID. This is for testing DDL and DML only
   * and will go away once READ works
   */
  void storeColOID(void);

  /** Update dictionary OIDs. This is for testing DDL and DML only
   * and will go away once READ works
   */
  void storeDictOID(void);

  /** Update index OIDs. This is for testing DDL and DML only
   * and will go away once READ works
   */
  void storeIndexOID(void);

  /** Reload dictionary OIDs, index OIDs, column OID. This is for testing DDL and DML only
   * and will go away once READ works
   */
  void updateColInfo(void);

  /** returns a pointer to the System Catalog singleton per session
   *  TODO: may need to change to one instance per transaction
   *  @parm sessionID to map the key of catalog map
   */
  static SPCSC makeCalpontSystemCatalog(uint32_t sessionID = 0);

  /** remove and delete the instance map to the sessionid
   *  @param sessionID
   */
  static void removeCalpontSystemCatalog(uint32_t sessionID = 0);
  /** sessionid access and mutator methods
   *
   */
  uint32_t sessionID() const
  {
    return fSessionID;
  }
  void sessionID(uint32_t sessionID)
  {
    fSessionID = sessionID;
  }
  /** identity access and mutator methods
   *
   */
  int identity() const
  {
    return fIdentity;
  }
  void identity(int identity)
  {
    fIdentity = identity;
  }

  /** return the column position
   *
   *  return the column position for a given OID
   */
  int colPosition(const OID& oid);
  /** return primary key name for the given table */
  const std::string primaryKeyName(const TableName& tableName, int lower_case_table_names = 0);
  /** return the table info
   *
   *  return the table info for a given TableName
   */
  const TableInfo tableInfo(const TableName& tb, int lower_case_table_names = 0);
  /** return the table name for a give table oid */
  const TableName tableName(const OID& oid);
  /** return the list of tables for a given schema */
  const std::vector<std::pair<OID, TableName> > getTables(const std::string schema = "",
                                                          int lower_case_table_names = 0);
  /** return the number of tables in the whole database */
  int getTableCount();
  /** return the constraint info for a given constraint */
  const ConstraintInfo constraintInfo(const IndexName& constraintName);
  /** return the constraintName list for a given referencePKName */
  const IndexNameList referenceConstraints(const IndexName& referencePKName);

  // @bug 682
  void getSchemaInfo(const std::string& schema, int lower_case_table_names = 0);

  typedef std::map<uint32_t, long long> OIDNextvalMap;
  void updateColinfoCache(OIDNextvalMap& oidNextvalMap);

  void flushCache();

  /** Convert a MySQL thread id to an InfiniDB session id */
  static uint32_t idb_tid2sid(const uint32_t tid);

  friend class ::ExecPlanTest;

  /** Destructor */
  ~CalpontSystemCatalog();

  // This ctor must be used to produce non-shared CSC that exists for a short period,
  // e.g. in client UDFs like calshowpartitions().
  explicit CalpontSystemCatalog();

 private:
  /** Constuctors */
  explicit CalpontSystemCatalog(const CalpontSystemCatalog& rhs);

  CalpontSystemCatalog& operator=(const CalpontSystemCatalog& rhs);

  /** get system data */
  void getSysData(execplan::CalpontSelectExecutionPlan&, NJLSysDataList&, const std::string& sysTableName);
  /** get system data for Front End */
  void getSysData_FE(const execplan::CalpontSelectExecutionPlan&, NJLSysDataList&,
                     const std::string& sysTableName);
  /** get system data for Engine Controller */
  void getSysData_EC(execplan::CalpontSelectExecutionPlan&, NJLSysDataList&, const std::string& sysTableName);

  void buildSysColinfomap();
  void buildSysOIDmap();
  void buildSysTablemap();
  void buildSysDctmap();

  void checkSysCatVer();

  static boost::mutex map_mutex;
  static CatalogMap fCatalogMap;

  typedef std::map<TableColName, OID> OIDmap;
  OIDmap fOIDmap;
  boost::mutex fOIDmapLock;  // Also locks fColRIDmap

  typedef std::map<TableName, RID> Tablemap;
  Tablemap fTablemap;

  typedef std::map<TableName, OID> TableOIDmap;
  TableOIDmap fTableAUXColumnOIDMap;
  boost::mutex fTableAUXColumnOIDMapLock;

  typedef std::map<OID, OID> AUXColumnOIDTableOIDmap;
  AUXColumnOIDTableOIDmap fAUXColumnOIDToTableOIDMap;
  boost::mutex fAUXColumnOIDToTableOIDMapLock;

  typedef std::map<OID, ColType> Colinfomap;
  Colinfomap fColinfomap;
  boost::mutex fColinfomapLock;

  /** this structure is used by ddl only. it cache the rid for rows in syscolumn
      that match the tableColName */
  typedef std::map<TableColName, RID> ColRIDmap;
  ColRIDmap fColRIDmap;

  /** this structure is used by ddl only. it cache the rid for rows in systable
      that match the tableName */
  typedef std::map<TableName, RID> TableRIDmap;
  TableRIDmap fTableRIDmap;

  // this structure may combine with Tablemap, where RID is added to TalbeInfo struct
  typedef std::map<TableName, TableInfo> TableInfoMap;
  TableInfoMap fTableInfoMap;
  boost::mutex fTableInfoMapLock;

  typedef std::map<TableColName, IndexNameList> ColIndexListmap;
  ColIndexListmap fColIndexListmap;
  boost::mutex fColIndexListmapLock;

  typedef std::map<OID, OID> DctTokenMap;
  DctTokenMap fDctTokenMap;
  // MCOL-859: this can lock when already locked in the same thread
  boost::recursive_mutex fDctTokenMapLock;

  typedef std::map<OID, TableName> TableNameMap;
  TableNameMap fTableNameMap;
  boost::mutex fTableNameMapLock;

  ClientRotator* fExeMgr;
  uint32_t fSessionID;
  uint32_t fTxn;
  int fIdentity;
  std::set<std::string> fSchemaCache;
  boost::mutex fSchemaCacheLock;
  // Cache flush
  boost::mutex fSyscatSCNLock;
  SCN fSyscatSCN;

  static uint32_t fModuleID;
};

// MCOL-5021
const datatypes::SystemCatalog::ColDataType AUX_COL_DATATYPE = datatypes::SystemCatalog::UTINYINT;
const int32_t AUX_COL_WIDTH = 1;
// TODO MCOL-5021 compressionType is hardcoded to 2 (SNAPPY)
const CalpontSystemCatalog::CompressionType AUX_COL_COMPRESSION_TYPE = CalpontSystemCatalog::COMPRESSION2;
const std::string AUX_COL_DATATYPE_STRING = "unsigned-tinyint";
const uint64_t AUX_COL_MINVALUE = MIN_UTINYINT;
const uint64_t AUX_COL_MAXVALUE = MAX_UTINYINT;
constexpr uint8_t AUX_COL_EMPTYVALUE = joblist::UTINYINTEMPTYROW;

/** convenience function to make a TableColName from 3 strings
 */
const CalpontSystemCatalog::TableColName make_tcn(const std::string& s, const std::string& t,
                                                  const std::string& c, int lower_case_table_names = 0);

/** convenience function to make a TableName from 2 strings
 */
const CalpontSystemCatalog::TableName make_table(const std::string& s, const std::string& t,
                                                 int lower_case_table_names = 0);
const CalpontSystemCatalog::TableAliasName make_aliastable(const std::string& s, const std::string& t,
                                                           const std::string& a,
                                                           const bool fisColumnStore = true,
                                                           int lower_case_table_names = 0);
const CalpontSystemCatalog::TableAliasName make_aliasview(const std::string& s, const std::string& t,
                                                          const std::string& a, const std::string& v,
                                                          const bool fisColumnStore = true,
                                                          int lower_case_table_names = 0);

inline bool isNull(int128_t val, const execplan::CalpontSystemCatalog::ColType& ct)
{
  return datatypes::Decimal::isWideDecimalNullValue(val);
}

inline bool isNull(int64_t val, const execplan::CalpontSystemCatalog::ColType& ct)
{
  bool ret = false;

  switch (ct.colDataType)
  {
    case execplan::CalpontSystemCatalog::TINYINT:
    {
      if ((int8_t)joblist::TINYINTNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    {
      int colWidth = ct.colWidth;

      if (colWidth <= 8)
      {
        if ((colWidth == 1) && ((uint8_t)joblist::CHAR1NULL == (uint8_t)val))
          ret = true;
        else if ((colWidth == 2) && ((uint16_t)joblist::CHAR2NULL == (uint16_t)val))
          ret = true;
        else if ((colWidth < 5) && ((uint32_t)joblist::CHAR4NULL == (uint32_t)val))
          ret = true;
        else if ((int64_t)joblist::CHAR8NULL == val)
          ret = true;
      }
      else
      {
        throw std::logic_error("Not a int column.");
      }
      break;
    }

    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      if ((int16_t)joblist::SMALLINTNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      switch (ct.colWidth)
      {
        case 1:
        {
          if ((int8_t)joblist::TINYINTNULL == val)
            ret = true;

          break;
        }

        case 2:
        {
          if ((int16_t)joblist::SMALLINTNULL == val)
            ret = true;

          break;
        }

        case 4:
        {
          if ((int32_t)joblist::INTNULL == val)
            ret = true;

          break;
        }

        default:
        {
          if ((int64_t)joblist::BIGINTNULL == val)
            ret = true;

          break;
        }
      }
      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      if ((int64_t)joblist::DOUBLENULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    {
      if ((int32_t)joblist::INTNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      if ((int32_t)joblist::FLOATNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      if ((int32_t)joblist::DATENULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::BIGINT:
    {
      if ((int64_t)joblist::BIGINTNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      if ((int64_t)joblist::DATETIMENULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      if ((int64_t)joblist::TIMESTAMPNULL == val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::UTINYINT:
    {
      if (joblist::UTINYINTNULL == (uint8_t)val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      if (joblist::USMALLINTNULL == (uint16_t)val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
    {
      if (joblist::UINTNULL == (uint32_t)val)
        ret = true;

      break;
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    {
      if (joblist::UBIGINTNULL == (uint64_t)val)
        ret = true;

      break;
    }

    default: break;
  }

  return ret;
}

/** constants for system table names
 */
const std::string CALPONT_SCHEMA = "calpontsys";
const std::string SYSCOLUMN_TABLE = "syscolumn";
const std::string SYSTABLE_TABLE = "systable";
const std::string SYSCONSTRAINT_TABLE = "sysconstraint";
const std::string SYSCONSTRAINTCOL_TABLE = "sysconstraintcol";
const std::string SYSINDEX_TABLE = "sysindex";
const std::string SYSINDEXCOL_TABLE = "sysindexcol";
const std::string SYSSCHEMA_TABLE = "sysschema";
const std::string SYSDATATYPE_TABLE = "sysdatatype";

/** constants for system table column names
 */
const std::string SCHEMA_COL = "schema";
const std::string TABLENAME_COL = "tablename";
const std::string COLNAME_COL = "columnname";
const std::string OBJECTID_COL = "objectid";
const std::string DICTOID_COL = "dictobjectid";
const std::string LISTOBJID_COL = "listobjectid";
const std::string TREEOBJID_COL = "treeobjectid";
const std::string DATATYPE_COL = "datatype";
const std::string COLUMNTYPE_COL = "columntype";
const std::string COLUMNLEN_COL = "columnlength";
const std::string COLUMNPOS_COL = "columnposition";
const std::string CREATEDATE_COL = "createdate";
const std::string LASTUPDATE_COL = "lastupdate";
const std::string DEFAULTVAL_COL = "defaultvalue";
const std::string NULLABLE_COL = "nullable";
const std::string SCALE_COL = "scale";
const std::string PRECISION_COL = "prec";
const std::string MINVAL_COL = "minval";
const std::string MAXVAL_COL = "maxval";
const std::string AUTOINC_COL = "autoincrement";
const std::string INIT_COL = "init";
const std::string NEXT_COL = "next";
const std::string NUMOFROWS_COL = "numofrows";
const std::string AVGROWLEN_COL = "avgrowlen";
const std::string NUMOFBLOCKS_COL = "numofblocks";
const std::string DISTCOUNT_COL = "distcount";
const std::string NULLCOUNT_COL = "nullcount";
const std::string MINVALUE_COL = "minvalue";
const std::string MAXVALUE_COL = "maxvalue";
const std::string COMPRESSIONTYPE_COL = "compressiontype";
const std::string NEXTVALUE_COL = "nextvalue";
const std::string AUXCOLUMNOID_COL = "auxcolumnoid";
const std::string CHARSETNUM_COL = "charsetnum";

/*****************************************************
 * System tables OID definition
 ******************************************************/
const int SYSTABLE_BASE = 1000;       /** @brief SYSTABLE table base */
const int SYSTABLE_DICT_BASE = 2000;  /** @brief SYSTABLE table dictionary files base */
const int SYSCOLUMN_BASE = 1020;      /** @brief SYSCOLUMN table base */
const int SYSCOLUMN_DICT_BASE = 2060; /** @brief SYSCOLUMN table dictionary files base */

/*****************************************************
 * SYSTABLE columns OID definition
 ******************************************************/
const int OID_SYSTABLE_TABLENAME = SYSTABLE_BASE + 1;      /** @brief TABLENAME column */
const int OID_SYSTABLE_SCHEMA = SYSTABLE_BASE + 2;         /** @brief SCHEMA column */
const int OID_SYSTABLE_OBJECTID = SYSTABLE_BASE + 3;       /** @brief OBJECTID column */
const int OID_SYSTABLE_CREATEDATE = SYSTABLE_BASE + 4;     /** @brief CREATEDATE column */
const int OID_SYSTABLE_LASTUPDATE = SYSTABLE_BASE + 5;     /** @brief LASTUPDATE column */
const int OID_SYSTABLE_INIT = SYSTABLE_BASE + 6;           /** @brief INIT column */
const int OID_SYSTABLE_NEXT = SYSTABLE_BASE + 7;           /** @brief NEXT column */
const int OID_SYSTABLE_NUMOFROWS = SYSTABLE_BASE + 8;      /** @brief total num of rows column */
const int OID_SYSTABLE_AVGROWLEN = SYSTABLE_BASE + 9;      /** @brief avg. row length column */
const int OID_SYSTABLE_NUMOFBLOCKS = SYSTABLE_BASE + 10;   /** @brief num. of blocks column */
const int OID_SYSTABLE_AUTOINCREMENT = SYSTABLE_BASE + 11; /** @brief AUTOINCREMENT column */
const int OID_SYSTABLE_AUXCOLUMNOID = SYSTABLE_BASE + 12;  /** @brief AUXCOLUMNOID column */
const int SYSTABLE_MAX = SYSTABLE_BASE + 13;               // be sure this is one more than the highest #

/*****************************************************
 * SYSCOLUMN columns OID definition
 ******************************************************/
const int OID_SYSCOLUMN_SCHEMA = SYSCOLUMN_BASE + 1;           /** @brief SCHEMA column */
const int OID_SYSCOLUMN_TABLENAME = SYSCOLUMN_BASE + 2;        /** @brief TABLENAME column */
const int OID_SYSCOLUMN_COLNAME = SYSCOLUMN_BASE + 3;          /** @brief COLNAME column */
const int OID_SYSCOLUMN_OBJECTID = SYSCOLUMN_BASE + 4;         /** @brief OBJECTID column */
const int OID_SYSCOLUMN_DICTOID = SYSCOLUMN_BASE + 5;          /** @brief DICTOID column */
const int OID_SYSCOLUMN_LISTOBJID = SYSCOLUMN_BASE + 6;        /** @brief LISTOBJID column */
const int OID_SYSCOLUMN_TREEOBJID = SYSCOLUMN_BASE + 7;        /** @brief TREEOBJID column */
const int OID_SYSCOLUMN_DATATYPE = SYSCOLUMN_BASE + 8;         /** @brief DATATYPE column */
const int OID_SYSCOLUMN_COLUMNLEN = SYSCOLUMN_BASE + 9;        /** @brief COLUMNLEN column */
const int OID_SYSCOLUMN_COLUMNPOS = SYSCOLUMN_BASE + 10;       /** @brief COLUMNPOS column */
const int OID_SYSCOLUMN_LASTUPDATE = SYSCOLUMN_BASE + 11;      /** @brief LASTUPDATE column */
const int OID_SYSCOLUMN_DEFAULTVAL = SYSCOLUMN_BASE + 12;      /** @brief DEFAULTVAL column */
const int OID_SYSCOLUMN_NULLABLE = SYSCOLUMN_BASE + 13;        /** @brief NULLABLE column */
const int OID_SYSCOLUMN_SCALE = SYSCOLUMN_BASE + 14;           /** @brief SCALE column */
const int OID_SYSCOLUMN_PRECISION = SYSCOLUMN_BASE + 15;       /** @brief PRECISION column */
const int OID_SYSCOLUMN_AUTOINC = SYSCOLUMN_BASE + 16;         /** @brief AUTOINC column */
const int OID_SYSCOLUMN_DISTCOUNT = SYSCOLUMN_BASE + 17;       /** @brief Num of distinct value col */
const int OID_SYSCOLUMN_NULLCOUNT = SYSCOLUMN_BASE + 18;       /** @brief Num of null value col */
const int OID_SYSCOLUMN_MINVALUE = SYSCOLUMN_BASE + 19;        /** @brief min value col */
const int OID_SYSCOLUMN_MAXVALUE = SYSCOLUMN_BASE + 20;        /** @brief max value col */
const int OID_SYSCOLUMN_COMPRESSIONTYPE = SYSCOLUMN_BASE + 21; /** @brief compression type */
const int OID_SYSCOLUMN_NEXTVALUE = SYSCOLUMN_BASE + 22;       /** @brief next value */
const int OID_SYSCOLUMN_CHARSETNUM = SYSCOLUMN_BASE + 23;      /** @brief character set number for the column */
const int SYSCOLUMN_MAX = SYSCOLUMN_BASE + 24;                 // be sure this is one more than the highest #

/*****************************************************
 * SYSTABLE columns dictionary OID definition
 ******************************************************/
const int DICTOID_SYSTABLE_TABLENAME = SYSTABLE_DICT_BASE + 1; /** @brief TABLENAME column DICTOID*/
const int LISTOID_SYSTABLE_TABLENAME = SYSTABLE_DICT_BASE + 2; /** @brief TABLENAME column LISTOID*/
const int TREEOID_SYSTABLE_TABLENAME = SYSTABLE_DICT_BASE + 3; /** @brief TABLENAME column TREEOID*/
const int DICTOID_SYSTABLE_SCHEMA = SYSTABLE_DICT_BASE + 4;    /** @brief SCHEMA column DICTOID*/
const int LISTOID_SYSTABLE_SCHEMA = SYSTABLE_DICT_BASE + 5;    /** @brief SCHEMA column LISTOID*/
const int TREEOID_SYSTABLE_SCHEMA = SYSTABLE_DICT_BASE + 6;    /** @brief SCHEMA column TREEOID*/
const int SYSTABLE_DICT_MAX = SYSTABLE_DICT_BASE + 7;          // be sure this is one more than the highest #

/*****************************************************
 * SYSCOLUMN columns dictionary OID definition
 ******************************************************/
const int DICTOID_SYSCOLUMN_SCHEMA = SYSCOLUMN_DICT_BASE + 1;      /** @brief SCHEMA column DICTOID*/
const int LISTOID_SYSCOLUMN_SCHEMA = SYSCOLUMN_DICT_BASE + 2;      /** @brief SCHEMA column LISTOID*/
const int TREEOID_SYSCOLUMN_SCHEMA = SYSCOLUMN_DICT_BASE + 3;      /** @brief SCHEMA column TREEOID*/
const int DICTOID_SYSCOLUMN_TABLENAME = SYSCOLUMN_DICT_BASE + 4;   /** @brief TABLENAME column DICTOID*/
const int LISTOID_SYSCOLUMN_TABLENAME = SYSCOLUMN_DICT_BASE + 5;   /** @brief TABLENAME column LISTOID*/
const int TREEOID_SYSCOLUMN_TABLENAME = SYSCOLUMN_DICT_BASE + 6;   /** @brief TABLENAME column TREEOID*/
const int DICTOID_SYSCOLUMN_COLNAME = SYSCOLUMN_DICT_BASE + 7;     /** @brief COLNAME column DICTOID*/
const int LISTOID_SYSCOLUMN_COLNAME = SYSCOLUMN_DICT_BASE + 8;     /** @brief COLNAME column LISTOID*/
const int TREEOID_SYSCOLUMN_COLNAME = SYSCOLUMN_DICT_BASE + 9;     /** @brief COLNAME column TREEOID*/
const int DICTOID_SYSCOLUMN_DEFAULTVAL = SYSCOLUMN_DICT_BASE + 10; /** @brief DEFAULTVAL column DICTOID*/
const int LISTOID_SYSCOLUMN_DEFAULTVAL = SYSCOLUMN_DICT_BASE + 11; /** @brief DEFAULTVAL column LISTOID*/
const int TREEOID_SYSCOLUMN_DEFAULTVAL = SYSCOLUMN_DICT_BASE + 12; /** @brief DEFAULTVAL column TREEOID*/
const int DICTOID_SYSCOLUMN_MINVALUE = SYSCOLUMN_DICT_BASE + 13;   /** @brief MINVAL column DICTOID*/
const int LISTOID_SYSCOLUMN_MINVALUE = SYSCOLUMN_DICT_BASE + 14;   /** @brief MINVAL column LISTOID*/
const int TREEOID_SYSCOLUMN_MINVALUE = SYSCOLUMN_DICT_BASE + 15;   /** @brief MINVAL column TREEOID*/
const int DICTOID_SYSCOLUMN_MAXVALUE = SYSCOLUMN_DICT_BASE + 16;   /** @brief MAXVAL column DICTOID*/
const int LISTOID_SYSCOLUMN_MAXVALUE = SYSCOLUMN_DICT_BASE + 17;   /** @brief MAXVAL column LISTOID*/
const int TREEOID_SYSCOLUMN_MAXVALUE = SYSCOLUMN_DICT_BASE + 18;   /** @brief MAXVAL column TREEOID*/
const int SYSCOLUMN_DICT_MAX = SYSCOLUMN_DICT_BASE + 19;  // be sure this is one more than the highest #

std::vector<CalpontSystemCatalog::OID> getAllSysCatOIDs();

/**
 * stream operator
 */
std::ostream& operator<<(std::ostream& os, const CalpontSystemCatalog::ColType& rhs);

const std::string colDataTypeToString(CalpontSystemCatalog::ColDataType cdt);

bool ctListSort(const CalpontSystemCatalog::ColType& a, const CalpontSystemCatalog::ColType& b);

}  // namespace execplan
