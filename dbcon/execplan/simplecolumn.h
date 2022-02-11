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
 *   $Id: simplecolumn.h 9679 2013-07-11 22:32:03Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef SIMPLECOLUMN_H
#define SIMPLECOLUMN_H
#include <string>
#include <boost/shared_ptr.hpp>

#include "returnedcolumn.h"
#include "treenode.h"
#include "calpontsystemcatalog.h"
#include "dataconvert.h"
#include "calpontselectexecutionplan.h"
#include <boost/algorithm/string/case_conv.hpp>

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
class ParseTree;
/**
 * @brief A class to represent a simple returned column
 *
 * This class is a specialization of class ReturnedColumn that handles
 * a column name.
 */
class SimpleColumn : public ReturnedColumn
{
 public:
  /**
   * Constructors
   */
  SimpleColumn();
  SimpleColumn(const std::string& token, const uint32_t sessionID = 0);
  SimpleColumn(const std::string& schema, const std::string& table, const std::string& col,
               const uint32_t sessionID = 0, const int lower_case_table_names = 0);
  SimpleColumn(const std::string& schema, const std::string& table, const std::string& col,
               const bool isColumnStore, const uint32_t sessionID = 0, const int lower_case_table_names = 0);
  SimpleColumn(const SimpleColumn& rhs, const uint32_t sessionID = 0);

  /**
   * Destructor
   */
  virtual ~SimpleColumn();

  /**
   * Accessor Methods
   */
  inline const std::string& schemaName() const
  {
    return fSchemaName;
  }

  inline void schemaName(const std::string& schemaName, int lower_case_table_names = 0)
  {
    fSchemaName = schemaName;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fSchemaName);
  }

  inline const std::string& tableName() const
  {
    return fTableName;
  }

  inline void tableName(const std::string& tableName, int lower_case_table_names = 0)
  {
    fTableName = tableName;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fTableName);
  }

  inline const std::string& columnName() const
  {
    return fColumnName;
  }

  inline void columnName(const std::string& columnName)
  {
    fColumnName = columnName;
  }

  inline const CalpontSystemCatalog::OID& oid() const
  {
    return fOid;
  }

  inline void oid(const CalpontSystemCatalog::OID& oid)
  {
    fOid = oid;
  }

  virtual const std::string data() const;
  virtual void data(const std::string data)
  {
    fData = data;
  }

  inline const std::string& tableAlias() const
  {
    return fTableAlias;
  }
  inline void tableAlias(const std::string& tableAlias, int lower_case_table_names = 0)
  {
    fTableAlias = tableAlias;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fTableAlias);
  }
  inline const std::string& indexName() const
  {
    return fIndexName;
  }
  inline void indexName(const std::string& indexName)
  {
    fIndexName = indexName;
  }
  inline const std::string& viewName() const
  {
    return fViewName;
  }
  inline void viewName(const std::string& viewName, int lower_case_table_names = 0)
  {
    fViewName = viewName;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fViewName);
  }
  inline long timeZone() const
  {
    return fTimeZone;
  }
  inline void timeZone(const long timeZone)
  {
    fTimeZone = timeZone;
  }
  inline bool isColumnStore() const
  {
    return fisColumnStore;
  }
  inline void isColumnStore(const bool isColumnStore)
  {
    fisColumnStore = isColumnStore;
  }

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline virtual SimpleColumn* clone() const
  {
    return new SimpleColumn(*this);
  }
  /**
   * Overloaded assignment operator
   */
  SimpleColumn& operator=(const SimpleColumn& rhs);

  /**
   * The serialize interface
   */
  virtual void serialize(messageqcpp::ByteStream&) const;
  virtual void unserialize(messageqcpp::ByteStream&);

  virtual const std::string toString() const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  bool operator==(const SimpleColumn& t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  virtual bool operator!=(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  bool operator!=(const SimpleColumn& t) const;

  /** @brief check if this column is the same as the argument */
  virtual bool sameColumn(const ReturnedColumn* rc) const;

  /** @brief return column type of this column (could be of any engine type) */
  const CalpontSystemCatalog::ColType& colType() const
  {
    return fResultType;
  }

  /** @brief set the column's OID from the syscat */
  void setOID();

  virtual bool hasWindowFunc()
  {
    return false;
  }

  void setDerivedTable();

  /**
   * Return the tableAlias name of the table that the column arguments belong to.
   *
   * @param TableAliasName that will be set in the function
   * @return true, if all arguments belong to one table
   *         false, if multiple tables are involved in the function
   */
  virtual bool singleTable(CalpontSystemCatalog::TableAliasName& tan);

 protected:
  /**
   * Fields
   */
  std::string fSchemaName;         /// schema name
  std::string fTableName;          /// table name
  std::string fColumnName;         /// column name
  CalpontSystemCatalog::OID fOid;  /// column TCN number
  std::string fTableAlias;         /// table alias
  std::string fData;
  /// index name. for oracle use. deprecated
  std::string fIndexName;
  // if belong to view, view name is non-empty
  std::string fViewName;
  long fTimeZone;
  bool fisColumnStore;

  /** @brief parse SimpleColumn text
   *
   * parse the incomming sql text to construct a SimpleColumn object.
   * used by the constructor.
   */
  void parse(const std::string& sql);

  /***********************************************************
   *                  F&E framework                          *
   ***********************************************************/
 public:
  virtual void evaluate(rowgroup::Row& row, bool& isNull);
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getBoolVal();
  }
  virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getStrVal(fTimeZone);
  }

  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getIntVal();
  }

  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getUintVal();
  }

  virtual float getFloatVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getFloatVal();
  }

  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getDoubleVal();
  }

  virtual long double getLongDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getLongDoubleVal();
  }

  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);

    // @bug5736, double type with precision -1 indicates that this type is for decimal math,
    //      the original decimal scale is stored in scale field, which is no use for double.
    if (fResultType.precision == -1)
    {
      if (fResultType.colDataType == CalpontSystemCatalog::DOUBLE)
      {
        IDB_Decimal rv((int64_t)(TreeNode::getDoubleVal() * IDB_pow[fResultType.scale]), fResultType.scale,
                       15);
        return rv;
      }
      else if (fResultType.colDataType == CalpontSystemCatalog::LONGDOUBLE)
      {
        IDB_Decimal rv((int64_t)(TreeNode::getLongDoubleVal() * IDB_pow[fResultType.scale]),
                       fResultType.scale, fResultType.precision);
        return rv;
      }
    }

    return TreeNode::getDecimalVal();
  }

  inline int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getDateIntVal();
  }

  inline int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getDatetimeIntVal(fTimeZone);
  }

  inline int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getTimestampIntVal();
  }

  inline int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    evaluate(row, isNull);
    return TreeNode::getTimeIntVal();
  }
};

typedef boost::shared_ptr<SimpleColumn> SSC;

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const SimpleColumn& rhs);

/**
 * utility function to extract all simple columns from a parse tree
 */
void getSimpleCols(ParseTree* n, void* obj);
ParseTree* replaceRefCol(ParseTree*& n, CalpontSelectExecutionPlan::ReturnedColumnList&);

}  // namespace execplan
#endif  // SIMPLECOLUMN_H
