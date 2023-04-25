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
 *   $Id: simplecolumn.cpp 9576 2013-05-29 21:02:11Z zzhu $
 *
 *
 ***********************************************************************/

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sstream>

using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "objectreader.h"
#include "calpontselectexecutionplan.h"

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "dataconvert.h"

#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "constantfilter.h"
#include "../../utils/windowfunction/windowfunction.h"
#include "utils/common/branchpred.h"

namespace execplan
{
void getSimpleCols(execplan::ParseTree* n, void* obj)
{
  vector<SimpleColumn*>* list = reinterpret_cast<vector<SimpleColumn*>*>(obj);
  TreeNode* tn = n->data();
  SimpleColumn* sc = dynamic_cast<SimpleColumn*>(tn);
  FunctionColumn* fc = dynamic_cast<FunctionColumn*>(tn);
  ArithmeticColumn* ac = dynamic_cast<ArithmeticColumn*>(tn);
  SimpleFilter* sf = dynamic_cast<SimpleFilter*>(tn);
  ConstantFilter* cf = dynamic_cast<ConstantFilter*>(tn);

  if (sc)
  {
    list->push_back(sc);
  }
  else if (fc)
  {
    fc->setSimpleColumnList();
    list->insert(list->end(), fc->simpleColumnList().begin(), fc->simpleColumnList().end());
  }
  else if (ac)
  {
    ac->setSimpleColumnList();
    list->insert(list->end(), ac->simpleColumnList().begin(), ac->simpleColumnList().end());
  }
  else if (sf)
  {
    sf->setSimpleColumnList();
    list->insert(list->end(), sf->simpleColumnList().begin(), sf->simpleColumnList().end());
  }
  else if (cf)
  {
    cf->setSimpleColumnList();
    list->insert(list->end(), cf->simpleColumnList().begin(), cf->simpleColumnList().end());
  }
}

ParseTree* replaceRefCol(ParseTree*& n, CalpontSelectExecutionPlan::ReturnedColumnList& derivedColList)
{
  ParseTree* lhs = n->left();
  ParseTree* rhs = n->right();

  if (lhs)
    n->left(replaceRefCol(lhs, derivedColList));

  if (rhs)
    n->right(replaceRefCol(rhs, derivedColList));

  SimpleFilter* sf = dynamic_cast<SimpleFilter*>(n->data());
  ConstantFilter* cf = dynamic_cast<ConstantFilter*>(n->data());
  ReturnedColumn* rc = dynamic_cast<ReturnedColumn*>(n->data());

  if (sf)
  {
    sf->replaceRealCol(derivedColList);
  }
  else if (cf)
  {
    cf->replaceRealCol(derivedColList);
  }
  else if (rc)
  {
    SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

    if (sc && (sc->colPosition() > -1))
    {
      ReturnedColumn* tmp = derivedColList[sc->colPosition()]->clone();
      delete sc;
      n->data(tmp);
    }
    else
    {
      rc->replaceRealCol(derivedColList);
    }
  }

  return n;
}

/**
 * Constructors/Destructors
 */
SimpleColumn::SimpleColumn() : ReturnedColumn(), fOid(0), fisColumnStore(true)
{
  fDistinct = false;
}

SimpleColumn::SimpleColumn(const std::string& token, ForTestPurposeWithoutOID)
 : ReturnedColumn(0), fOid(0), fData(token), fisColumnStore(true)
{
  parse(token);
  fDistinct = false;
}
SimpleColumn::SimpleColumn(const string& token, const uint32_t sessionID)
 : ReturnedColumn(sessionID), fOid(0), fData(token), fisColumnStore(true)
{
  parse(token);
  setOID();
  fDistinct = false;
}

SimpleColumn::SimpleColumn(const string& schemaName, const string& tableName, const string& columnName,
                           const uint32_t sessionID, const int lower_case_table_names)
 : ReturnedColumn(sessionID)
 , fSchemaName(schemaName)
 , fTableName(tableName)
 , fColumnName(columnName)
 , fisColumnStore(true)
{
  setOID();
  fDistinct = false;
  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(fSchemaName);
    boost::algorithm::to_lower(fTableName);
  }
  boost::algorithm::to_lower(fColumnName);
}

SimpleColumn::SimpleColumn(const string& schemaName, const string& tableName, const string& columnName,
                           const bool isColumnStore, const uint32_t sessionID,
                           const int lower_case_table_names)
 : ReturnedColumn(sessionID)
 , fSchemaName(schemaName)
 , fTableName(tableName)
 , fColumnName(columnName)
 , fisColumnStore(isColumnStore)
{
  if (isColumnStore)
    setOID();
  fDistinct = false;
  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(fSchemaName);
    boost::algorithm::to_lower(fTableName);
  }
  boost::algorithm::to_lower(fColumnName);
}

SimpleColumn::SimpleColumn(const SimpleColumn& rhs, const uint32_t sessionID)
 : ReturnedColumn(rhs, sessionID)
 , fSchemaName(rhs.schemaName())
 , fTableName(rhs.tableName())
 , fColumnName(rhs.columnName())
 , fOid(rhs.oid())
 , fTableAlias(rhs.tableAlias())
 , fData(rhs.data())
 , fIndexName(rhs.indexName())
 , fViewName(rhs.viewName())
 , fTimeZone(rhs.timeZone())
 , fisColumnStore(rhs.isColumnStore())
{
}

SimpleColumn::~SimpleColumn()
{
}
/**
 * Methods
 */

const string SimpleColumn::data() const
{
  if (!fData.empty())
    return fData;
  else if (!fTableAlias.empty())
    return string("`" + fSchemaName + "`.`" + fTableAlias + "`.`" + fColumnName + "`");

  return string("`" + fSchemaName + "`.`" + fTableName + "`.`" + fColumnName + "`");
}

SimpleColumn& SimpleColumn::operator=(const SimpleColumn& rhs)
{
  if (this != &rhs)
  {
    fTableName = rhs.tableName();
    fColumnName = rhs.columnName();
    fOid = rhs.oid();
    fSchemaName = rhs.schemaName();
    fAlias = rhs.alias();
    fTableAlias = rhs.tableAlias();
    fAsc = rhs.asc();
    fIndexName = rhs.indexName();
    fViewName = rhs.viewName();
    fTimeZone = rhs.timeZone();
    fData = rhs.data();
    fSequence = rhs.sequence();
    fDistinct = rhs.distinct();
    fisColumnStore = rhs.isColumnStore();
  }

  return *this;
}

ostream& operator<<(ostream& output, const SimpleColumn& rhs)
{
  output << rhs.toString();

  return output;
}

const string SimpleColumn::toString() const
{
  static const char delim = '/';
  ostringstream output;

  output << "SimpleColumn " << data() << endl;
  // collations in both result and operations type are the same and
  // set in the plugin code.
  datatypes::Charset cs(fResultType.charsetNumber);
  output << "  s/t/c/v/o/ct/TA/CA/RA/#/card/join/source/engine/colPos/cs/coll: " << schemaName() << delim
         << tableName() << delim << columnName() << delim << viewName() << delim << oid() << delim
         << colDataTypeToString(fResultType.colDataType) << delim << tableAlias() << delim << alias() << delim
         << returnAll() << delim << sequence() << delim << cardinality() << delim << joinInfo() << delim
         << colSource() << delim << (isColumnStore() ? "ColumnStore" : "ForeignEngine") << delim
         << colPosition() << delim << cs.getCharset().cs_name.str << delim << cs.getCharset().coll_name.str
         << delim << endl;

  return output.str();
}

string SimpleColumn::toCppCode(IncludeSet& includes) const
{
  includes.insert("simplecolumn.h");
  stringstream ss;

  ss << "SimpleColumn(" << std::quoted(fData)  << ", SimpleColumn::ForTestPurposeWithoutOID{})";

  return ss.str();
}

void SimpleColumn::parse(const string& token)
{
  // get schema name, table name and column name for token.
  string::size_type pos = token.find_first_of(".", 0);

  // if no '.' in column name, consider it a function name;
  if (pos == string::npos)
  {
    fData = token;
    fColumnName = token;
    return;
  }

  fSchemaName = token.substr(0, pos);

  string::size_type newPos = token.find_first_of(".", pos + 1);

  if (newPos == string::npos)
  {
    // only one '.' in column name, consider table.col pattern
    fTableName = fSchemaName;
    fColumnName = token.substr(pos + 1, token.length());
    return;
  }

  fTableName = token.substr(pos + 1, newPos - pos - 1);
  fColumnName = token.substr(newPos + 1, token.length());
}

void SimpleColumn::setOID()
{
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
  CalpontSystemCatalog::TableColName tcn;
  tcn = make_tcn(fSchemaName, fTableName, fColumnName);

  fOid = csc->lookupOID(tcn);

  if (fOid == -1)
  {
    // get colMap from CalpontSelectExecutionPlan
    // and try to map the schema and table name
    CalpontSelectExecutionPlan::ColumnMap::iterator iter;
    CalpontSelectExecutionPlan::ColumnMap colMap = CalpontSelectExecutionPlan::colMap();

    // if this is the only column name exist in the map, return it ??
    for (iter = colMap.find(fColumnName); iter != colMap.end(); ++iter)
    {
      SRCP srcp = iter->second;
      SimpleColumn* scp = dynamic_cast<SimpleColumn*>(srcp.get());

      if (colMap.count(fColumnName) == 1 || scp->tableName().compare(fTableName) == 0)
      {
        fOid = scp->oid();
        //@bug 221 fix
        fTableName = scp->tableName();
        fSchemaName = scp->schemaName();
        //@info assign tableAlias also. watch for other conflict
        fTableAlias = scp->tableAlias();
        fResultType = scp->resultType();
        return;
      }
    }
  }

  fResultType = csc->colType(fOid);
}

void SimpleColumn::serialize(messageqcpp::ByteStream& b) const
{
  b << (ObjectReader::id_t)ObjectReader::SIMPLECOLUMN;
  ReturnedColumn::serialize(b);
  b << fSchemaName;
  b << fTableName;
  b << fColumnName;
  b << fIndexName;
  b << fViewName;
  messageqcpp::ByteStream::octbyte timeZone = fTimeZone;
  b << timeZone;
  b << (uint32_t)fOid;
  b << fData;
  b << fTableAlias;
  b << (uint32_t)fSequence;
  b << static_cast<ByteStream::doublebyte>(fisColumnStore);
}

void SimpleColumn::unserialize(messageqcpp::ByteStream& b)
{
  ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN);
  ReturnedColumn::unserialize(b);
  b >> fSchemaName;
  b >> fTableName;
  b >> fColumnName;
  b >> fIndexName;
  b >> fViewName;
  messageqcpp::ByteStream::octbyte timeZone;
  b >> timeZone;
  fTimeZone = timeZone;
  b >> (uint32_t&)fOid;
  b >> fData;
  b >> fTableAlias;
  b >> (uint32_t&)fSequence;
  b >> reinterpret_cast<ByteStream::doublebyte&>(fisColumnStore);
}

bool SimpleColumn::operator==(const SimpleColumn& t) const
{
  const ReturnedColumn *rc1, *rc2;

  rc1 = static_cast<const ReturnedColumn*>(this);
  rc2 = static_cast<const ReturnedColumn*>(&t);

  if (*rc1 != *rc2)
    return false;

  if (fSchemaName != t.fSchemaName)
    return false;

  if (fTableName != t.fTableName)
    return false;

  if (fColumnName != t.fColumnName)
    return false;

  if (fViewName != t.fViewName)
    return false;

  if (fTimeZone != t.fTimeZone)
    return false;

  if (fOid != t.fOid)
    return false;

  if (data() != t.data())
    return false;

  if (fTableAlias != t.fTableAlias)
    return false;

  if (fAsc != t.fAsc)
    return false;

  if (fReturnAll != t.fReturnAll)
    return false;

  if (fisColumnStore != t.fisColumnStore)
    return false;

  return true;
}

bool SimpleColumn::operator==(const TreeNode* t) const
{
  const SimpleColumn* sc;

  sc = dynamic_cast<const SimpleColumn*>(t);

  if (sc == NULL)
    return false;

  return *this == *sc;
}

bool SimpleColumn::operator!=(const SimpleColumn& t) const
{
  return !(*this == t);
}

bool SimpleColumn::operator!=(const TreeNode* t) const
{
  return !(*this == t);
}

bool SimpleColumn::sameColumn(const ReturnedColumn* rc) const
{
  /** NOTE: Operations can still be merged on different table alias */
  const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(rc);

  if (!sc)
    return false;

  return (fSchemaName.compare(sc->schemaName()) == 0 && fTableName.compare(sc->tableName()) == 0 &&
          fColumnName.compare(sc->columnName()) == 0 && fTableAlias.compare(sc->tableAlias()) == 0 &&
          fViewName.compare(sc->viewName()) == 0 && fisColumnStore == sc->isColumnStore());
}

void SimpleColumn::setDerivedTable()
{
  if (hasAggregate() || hasWindowFunc())
  {
    fDerivedTable = "";
    return;
  }
  ReturnedColumn* rc = dynamic_cast<ReturnedColumn*>(fDerivedRefCol);
  // @todo make aggregate filter to having clause. not optimize it for now
  if (rc)
  {
    if (rc->hasAggregate() || rc->hasWindowFunc())
    {
      fDerivedTable = "";
      return;
    }
  }

  // fDerivedTable is set at the parsing phase
  if (!fSchemaName.empty())
    fDerivedTable = "";
}

bool SimpleColumn::singleTable(CalpontSystemCatalog::TableAliasName& tan)
{
  tan.table = fTableName;
  tan.schema = fSchemaName;
  tan.view = fViewName;
  tan.alias = fTableAlias;
  return true;
}

// @todo move to inline
void SimpleColumn::evaluate(Row& row, bool& isNull)
{
  bool isNull2 = row.isNullValue(fInputIndex);

  if (isNull2)
  {
    isNull = true;
    return;
  }

  switch (fResultType.colDataType)
  {
    case CalpontSystemCatalog::DATE:
    {
      fResult.intVal = row.getUintField<4>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME:
    {
      fResult.intVal = row.getUintField<8>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::STRINT:
    {
      switch (row.getColumnWidth(fInputIndex))
      {
        case 1:
        {
          fResult.origIntVal = row.getUintField<1>(fInputIndex);
          break;
        }

        case 2:
        {
          fResult.origIntVal = row.getUintField<2>(fInputIndex);
          break;
        }

        case 3:
        case 4:
        {
          fResult.origIntVal = row.getUintField<4>(fInputIndex);
          break;
        }

        case 5:
        case 6:
        case 7:
        case 8:
        {
          fResult.origIntVal = row.getUintField<8>(fInputIndex);
          break;
        }

        default:
        {
          fResult.strVal = row.getStringField(fInputIndex);
          break;
        }
      }

      if (fResultType.colDataType == CalpontSystemCatalog::STRINT)
        fResult.intVal = uint64ToStr(fResult.origIntVal);
      else
        fResult.intVal = atoll((char*)&fResult.origIntVal);

      // MCOL-4580 - related, probably can be marked with XXX.
      // This does not fail in any tests, but it is considered wrong.
      // The reasonin behind that is that we changed signedness if characters to unsigned
      // and it might be a case with short strings that they were copied as is using
      // uint64ToStr encoding into int64_t values. So, potentially, unsuspecting code
      // may use getUintVal instead of getIntVal to process short char column, getting
      // unitialized value and give floating behavior.
      // None of our tests failed, though.
      fResult.uintVal = fResult.intVal;

      break;
    }

    case CalpontSystemCatalog::BIGINT:
    {
      fResult.intVal = row.getIntField<8>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::MEDINT:
    {
      fResult.intVal = row.getIntField<4>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::SMALLINT:
    {
      fResult.intVal = row.getIntField<2>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::TINYINT:
    {
      fResult.intVal = row.getIntField<1>(fInputIndex);
      break;
    }

    // In this case, we're trying to load a double output column with float data. This is the
    // case when you do sum(floatcol), e.g.
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      fResult.floatVal = row.getFloatField(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      fResult.doubleVal = row.getDoubleField(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::LONGDOUBLE:
    {
      fResult.longDoubleVal = row.getLongDoubleField(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      switch (fResultType.colWidth)
      {
        case 16:
        {
          fResult.decimalVal.s128Value = row.getTSInt128Field(fInputIndex).getValue();
          break;
        }
        case 1:
        {
          fResult.decimalVal.value = row.getIntField<1>(fInputIndex);
          break;
        }

        case 2:
        {
          fResult.decimalVal.value = row.getIntField<2>(fInputIndex);
          break;
        }

        case 4:
        {
          fResult.decimalVal.value = row.getIntField<4>(fInputIndex);
          break;
        }

        default:
        {
          fResult.decimalVal.value = (int64_t)row.getUintField<8>(fInputIndex);
          break;
        }
      }

      fResult.decimalVal.scale = (unsigned)fResultType.scale;
      fResult.decimalVal.precision = (unsigned)fResultType.precision;

      break;
    }

    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    {
      fResult.strVal.assign(row.getVarBinaryField(fInputIndex), row.getVarBinaryLength(fInputIndex));
      break;
    }

    case CalpontSystemCatalog::UBIGINT:
    {
      fResult.uintVal = row.getUintField<8>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UMEDINT:
    {
      fResult.uintVal = row.getUintField<4>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::USMALLINT:
    {
      fResult.uintVal = row.getUintField<2>(fInputIndex);
      break;
    }

    case CalpontSystemCatalog::UTINYINT:
    {
      fResult.uintVal = row.getUintField<1>(fInputIndex);
      break;
    }

    default:  // treat as int64
    {
      fResult.intVal = row.getUintField<8>(fInputIndex);
      break;
    }
  }
}

}  // namespace execplan
