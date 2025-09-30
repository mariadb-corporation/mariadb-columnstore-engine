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
 *   $Id: calpontselectexecutionplan.cpp 9576 2013-05-29 21:02:11Z zzhu $
 *
 *
 ***********************************************************************/
#include <iostream>
#include <algorithm>
#include <sstream>
using namespace std;

#include <boost/uuid/uuid_io.hpp>
#include <utility>

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "objectreader.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"
#include "querystats.h"
#include "simplefilter.h"
#include "operator.h"
#include <boost/core/demangle.hpp>

#include "querytele.h"
#include "utils/pron/pron.h"

using namespace querytele;

namespace
{
template <class T>
struct deleter
{
  void operator()(T& x)
  {
    delete x;
    x = 0;
  }
};

}  // namespace

namespace execplan
{
/** Static */
CalpontSelectExecutionPlan::ColumnMap CalpontSelectExecutionPlan::fColMap;

/**
 * Constructors/Destructors
 */
CalpontSelectExecutionPlan::CalpontSelectExecutionPlan(int location)
 : fLocalQuery(GLOBAL_QUERY)
 , fFilters(nullptr)
 , fHaving(nullptr)
 , fLocation(location)
 , fDependent(false)
 , fTxnID(-1)
 , fTraceFlags(TRACE_NONE)
 , fStatementID(0)
 , fDistinct(false)
 , fOverrideLargeSideEstimate(false)
 , fDistinctUnionNum(0)
 , fSubType(MAIN_SELECT)
 , fLimitStart(0)
 , fLimitNum(-1)
 , fHasOrderBy(false)
 , fStringScanThreshold(ULONG_MAX)
 , fQueryType(IDBQueryType::SELECT)
 , fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL)
 , fStringTableThreshold(20)
 , fOrderByThreads(1)
 , fDJSSmallSideLimit(0)
 , fDJSLargeSideLimit(0)
 , fDJSPartitionSize(100 * 1024 * 1024)
 , fDJSMaxPartitionTreeDepth(8)
 , fDJSForceRun(false)
 , fMaxPmJoinResultCount(1048576)
 ,  // 100MB mem usage for disk based join,
 fUMMemLimit(numeric_limits<int64_t>::max())
 , fIsDML(false)
 , fWithRollup(false)
{
  fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::CalpontSelectExecutionPlan(ReturnedColumnList returnedCols, ParseTree* filters,
                                                       SelectList subSelects, GroupByColumnList groupByCols,
                                                       ParseTree* having, OrderByColumnList orderByCols,
                                                       string alias, int location, bool dependent,
                                                       bool withRollup)
 : fReturnedCols(std::move(returnedCols))
 , fFilters(filters)
 , fSubSelects(std::move(subSelects))
 , fGroupByCols(std::move(groupByCols))
 , fHaving(having)
 , fOrderByCols(std::move(orderByCols))
 , fTableAlias(std::move(alias))
 , fLocation(location)
 , fDependent(dependent)
 , fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL)
 , fWithRollup(withRollup)
{
  fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::CalpontSelectExecutionPlan(string data)
 : fData(std::move(data)), fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL), fWithRollup(false)
{
  fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::~CalpontSelectExecutionPlan()
{
  delete fFilters;
  delete fHaving;

  fFilters = nullptr;
  fHaving = nullptr;

  if (!fDynamicParseTreeVec.empty())
  {
    for (auto& parseTree : fDynamicParseTreeVec)
    {
      if (parseTree)
      {
        // 'delete fFilters;' above has already deleted objects pointed
        // to by parseTree->left()/right()/data(), so we set the
        // pointers to NULL here before calling 'delete parseTree;'
        parseTree->left((ParseTree*)(nullptr));
        parseTree->right((ParseTree*)(nullptr));
        parseTree->data((TreeNode*)(nullptr));
        delete parseTree;
        parseTree = nullptr;
      }
    }

    fDynamicParseTreeVec.clear();
  }
}

/**
 * Methods
 */

void CalpontSelectExecutionPlan::filterTokenList(FilterTokenList& filterTokenList)
{
  fFilterTokenList = filterTokenList;

  Parser parser;
  std::vector<Token> tokens;
  Token t;

  for (unsigned int i = 0; i < filterTokenList.size(); i++)
  {
    t.value = filterTokenList[i];
    tokens.push_back(t);
  }

  if (tokens.size() > 0)
    filters(parser.parse(tokens.begin(), tokens.end()));
}

void CalpontSelectExecutionPlan::havingTokenList(const FilterTokenList& havingTokenList)
{
  fHavingTokenList = havingTokenList;

  Parser parser;
  std::vector<Token> tokens;
  Token t;

  for (unsigned int i = 0; i < havingTokenList.size(); i++)
  {
    t.value = havingTokenList[i];
    tokens.push_back(t);
  }

  if (tokens.size() > 0)
    having(parser.parse(tokens.begin(), tokens.end()));
}

std::string endlWithIndent(const size_t ident)
{
  ostringstream output;
  output << endl;

  for (size_t i = 0; i < ident; i++)
    output << " ";

  return output.str();
}

// Iterative tree printer that preserves vertical branches for multi-line nodes
static void printIndentedFilterTreeImpl(const ParseTree* root, ostringstream& output, size_t indent,
                                        const std::vector<bool>& rootAncestors, bool rootIsLast)
{
  // Stack holds frames: (node, ancestorHasNext, isLastAtLevel)
  struct Frame
  {
    const ParseTree* node;
    std::vector<bool> ancestors;
    bool isLast;
  };

  std::vector<Frame> stack;
  stack.push_back(Frame{root, rootAncestors, rootIsLast});

  while (!stack.empty())
  {
    Frame frame = std::move(stack.back());
    stack.pop_back();

    const ParseTree* node = frame.node;
    const std::vector<bool>& ancestorHasNext = frame.ancestors;
    const bool isLastAtLevel = frame.isLast;

    if (!node)
    {
      // Build prefix for a null placeholder
      std::string base;
      for (bool hasNext : ancestorHasNext)
        base += hasNext ? "│   " : "    ";
      std::string nodePrefix = base + (isLastAtLevel ? "└── " : "├── ");
      output << endlWithIndent(indent) << nodePrefix << "(null)";
      continue;
    }

    // Gather children in left-to-right order
    std::vector<const ParseTree*> children;
    if (node->left())
      children.push_back(node->left());
    if (node->right())
      children.push_back(node->right());

    // Helper to build prefixes
    auto makePrefixes = [&](bool isLast)
    {
      std::string base;
      for (bool hasNext : ancestorHasNext)
        base += hasNext ? "│   " : "    ";
      std::string first = base + (isLast ? "└── " : "├── ");
      std::string cont = base + (isLast ? "    " : "│   ");
      return std::pair<std::string, std::string>(first, cont);
    };

    // Build node content string
    TreeNode* data = node->data();
    std::string nodeContent;
    if (data)
    {
      if (auto sf = dynamic_cast<SimpleFilter*>(data))
      {
        nodeContent = sf->toString(true);
      }
      else if (auto op = dynamic_cast<Operator*>(data))
      {
        nodeContent = op->toString();
      }
      else
      {
        nodeContent = boost::core::demangle(typeid(*data).name()) + ": " + data->toString();
      }
    }
    else
    {
      nodeContent = "(null data)";
    }

    // Print current node content
    if (ancestorHasNext.empty())
    {
      // Root: print without branch glyphs
      std::istringstream contentStream(nodeContent);
      std::string line;
      while (std::getline(contentStream, line))
        output << endlWithIndent(indent) << line;
    }
    else
    {
      auto prefixes = makePrefixes(isLastAtLevel);
      std::istringstream contentStream(nodeContent);
      std::string line;
      bool firstLine = true;
      while (std::getline(contentStream, line))
      {
        if (firstLine)
        {
          output << endlWithIndent(indent) << prefixes.first << line;
          firstLine = false;
        }
        else
        {
          output << endlWithIndent(indent) << prefixes.second << line;
        }
      }
    }

    // Push children onto stack in reverse order to process left child first
    for (size_t i = children.size(); i-- > 0;)
    {
      bool childIsLast = (i == children.size() - 1);
      std::vector<bool> nextAncestors = ancestorHasNext;
      // For children, push whether THIS node has a next sibling; this keeps the vertical bar under this node
      // if we are not the last at our level.
      nextAncestors.push_back(!isLastAtLevel);
      stack.push_back(Frame{children[i], std::move(nextAncestors), childIsLast});
    }
  }
}

void printIndentedFilterTree(const ParseTree* tree, ostringstream& output, size_t indent)
{
  // Start with empty ancestor vector and indicate root is last at its (virtual) level
  std::vector<bool> ancestors;  // empty => root
  printIndentedFilterTreeImpl(tree, output, indent, ancestors, true);
}

void CalpontSelectExecutionPlan::printSubCSEP(const size_t& ident, ostringstream& output,
                                              CalpontSelectExecutionPlan*& plan) const
{
  if (plan)
  {
    output << endlWithIndent(ident) << "{";
    output << plan->toString(ident + 2);
    output << endlWithIndent(ident) << "}";
  }
}
string CalpontSelectExecutionPlan::toString(const size_t ident) const
{
  ostringstream output;

  output << endlWithIndent(ident) << "SELECT ";

  if (distinct())
  {
    output << endlWithIndent(ident) << "DISTINCT ";
  }

  output << endlWithIndent(ident) << "limit: " << limitStart() << " - " << limitNum();

  switch (location())
  {
    case CalpontSelectExecutionPlan::MAIN: output << endlWithIndent(ident) << "MAIN"; break;

    case CalpontSelectExecutionPlan::FROM: output << endlWithIndent(ident) << "FROM"; break;

    case CalpontSelectExecutionPlan::WHERE: output << endlWithIndent(ident) << "WHERE"; break;

    case CalpontSelectExecutionPlan::HAVING: output << "HAVING" << endlWithIndent(ident); break;
  }

  // Returned Column
  CalpontSelectExecutionPlan::ReturnedColumnList retCols = returnedCols();
  output << endlWithIndent(ident) << ">>Returned Columns";

  uint32_t seq = 0;

  for (unsigned int i = 0; i < retCols.size(); i++)
  {
    output << endlWithIndent(ident + 2) << *retCols[i];  // WIP replace with constant

    if (retCols[i]->colSource() & SELECT_SUB)
    {
      output << endlWithIndent(ident + 2) << "select sub -- ";
      CalpontSelectExecutionPlan* plan =
          dynamic_cast<CalpontSelectExecutionPlan*>(fSelectSubList[seq++].get());

      printSubCSEP(ident + 2, output, plan);
    }
  }

  // From Clause
  CalpontSelectExecutionPlan::TableList tables = tableList();
  output << endlWithIndent(ident) << ">>From Tables";
  seq = 0;

  for (unsigned int i = 0; i < tables.size(); i++)
  {
    // derived table
    if (tables[i].schema.length() == 0 && tables[i].table.length() == 0)
    {
      output << endlWithIndent(ident + 2) << "derived table - " << tables[i].alias;
      CalpontSelectExecutionPlan* plan =
          dynamic_cast<CalpontSelectExecutionPlan*>(fDerivedTableList[seq++].get());

      printSubCSEP(ident + 2, output, plan);
    }
    else
    {
      output << endlWithIndent(ident + 2) << tables[i];
    }
  }

  // Filters
  output << endlWithIndent(ident) << ">>Filters";

  if (filters() != nullptr)
  {
    output << endlWithIndent(ident + 2) << "Filter Tree:";
    printIndentedFilterTree(filters(), output, ident + 4);
  }
  else
  {
    output << endlWithIndent(ident + 2) << "empty filter tree";
  }

  // Group by columns
  const CalpontSelectExecutionPlan::GroupByColumnList& gbc = groupByCols();

  if (gbc.size() > 0)
  {
    output << endlWithIndent(ident) << ">>Group By Columns";
    output << std::string(ident, ' ');

    for (unsigned int i = 0; i < gbc.size(); i++)
    {
      output << endlWithIndent(ident + 2) << *gbc[i];
    }
    output << std::string(ident, ' ');
  }

  // Having
  if (having() != nullptr)
  {
    output << endlWithIndent(ident) << ">>Having";
    output << endlWithIndent(ident + 2) << "Having Tree:";
    printIndentedFilterTree(having(), output, ident + 4);
  }

  // Order by columns
  const CalpontSelectExecutionPlan::OrderByColumnList& obc = orderByCols();

  if (obc.size() > 0)
  {
    output << endlWithIndent(ident) << ">>Order By Columns";

    for (unsigned int i = 0; i < obc.size(); i++)
      output << endlWithIndent(ident + 2) << *obc[i];
  }

  output << endlWithIndent(ident) << "SessionID: " << fSessionID;
  output << endlWithIndent(ident) << "TxnID: " << fTxnID;
  output << endlWithIndent(ident) << "VerID: " << fVerID;
  output << endlWithIndent(ident) << "TraceFlags: " << fTraceFlags;
  output << endlWithIndent(ident) << "StatementID: " << fStatementID;
  output << endlWithIndent(ident) << "DistUnionNum: " << (int)fDistinctUnionNum;
  output << endlWithIndent(ident) << "Limit: " << fLimitStart << " - " << fLimitNum;
  output << endlWithIndent(ident) << "String table threshold: " << fStringTableThreshold;

  output << endlWithIndent(ident) << "--- Column Map ---";
  CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;

  for (iter = columnMap().begin(); iter != columnMap().end(); iter++)
  {
    output << endlWithIndent(ident + 2) << (*iter).first << " : " << (*iter).second;
  }

  output << endlWithIndent(ident) << "UUID: " << fUuid;
  output << endlWithIndent(ident) << "QueryType: " << queryType();

  if (!unionVec().empty())
  {
    output << endlWithIndent(ident) << "--- Union Unit ---";
  }

  for (unsigned i = 0; i < unionVec().size(); i++)
  {
    CalpontSelectExecutionPlan* plan = dynamic_cast<CalpontSelectExecutionPlan*>(unionVec()[i].get());

    printSubCSEP(ident, output, plan);
  }

  return output.str();
}

string CalpontSelectExecutionPlan::queryTypeToString(const IDBQueryType queryType)
{
  switch (queryType)
  {
    case IDBQueryType::SELECT: return "SELECT";
    case IDBQueryType::UPDATE: return "UPDATE";
    case IDBQueryType::DELETE: return "DELETE";
    case IDBQueryType::INSERT_SELECT: return "INSERT_SELECT";
    case IDBQueryType::CREATE_TABLE: return "CREATE_TABLE";
    case IDBQueryType::DROP_TABLE: return "DROP_TABLE";
    case IDBQueryType::ALTER_TABLE: return "ALTER_TABLE";
    case IDBQueryType::INSERT: return "INSERT";
    case IDBQueryType::LOAD_DATA_INFILE: return "LOAD_DATA_INFILE";
    case IDBQueryType::UNION: return "UNION";
  }

  return "UNKNOWN";
}

void CalpontSelectExecutionPlan::serialize(messageqcpp::ByteStream& b) const
{
  ReturnedColumnList::const_iterator rcit;
  ColumnMap::const_iterator mapiter;
  TableList::const_iterator tit;

  b << static_cast<ObjectReader::id_t>(ObjectReader::CALPONTSELECTEXECUTIONPLAN);

  b << static_cast<uint32_t>(fReturnedCols.size());

  for (rcit = fReturnedCols.begin(); rcit != fReturnedCols.end(); ++rcit)
    (*rcit)->serialize(b);

  b << static_cast<uint32_t>(fTableList.size());

  for (tit = fTableList.begin(); tit != fTableList.end(); ++tit)
  {
    (*tit).serialize(b);
  }

  ObjectReader::writeParseTree(fFilters, b);

  b << static_cast<uint32_t>(fSubSelects.size());

  for (uint32_t i = 0; i < fSubSelects.size(); i++)
    fSubSelects[i]->serialize(b);

  b << static_cast<uint32_t>(fGroupByCols.size());

  for (rcit = fGroupByCols.begin(); rcit != fGroupByCols.end(); ++rcit)
    (*rcit)->serialize(b);

  ObjectReader::writeParseTree(fHaving, b);

  b << static_cast<uint32_t>(fOrderByCols.size());

  for (rcit = fOrderByCols.begin(); rcit != fOrderByCols.end(); ++rcit)
    (*rcit)->serialize(b);

  b << static_cast<uint32_t>(fColumnMap.size());

  for (mapiter = fColumnMap.begin(); mapiter != fColumnMap.end(); ++mapiter)
  {
    b << (*mapiter).first;
    (*mapiter).second->serialize(b);
  }

  b << static_cast<uint32_t>(frmParms.size());

  for (RMParmVec::const_iterator it = frmParms.begin(); it != frmParms.end(); ++it)
  {
    b << it->sessionId;
    b << it->id;
    b << it->value;
  }

  b << fTableAlias;
  b << static_cast<uint32_t>(fLocation);

  b << static_cast<ByteStream::byte>(fDependent);

  // ? not sure if this needs to be added
  b << fData;
  b << static_cast<uint32_t>(fSessionID);
  b << static_cast<uint32_t>(fTxnID);
  b << fVerID;
  b << fTraceFlags;
  b << fStatementID;
  b << static_cast<ByteStream::byte>(fDistinct);
  b << static_cast<uint8_t>(fOverrideLargeSideEstimate);

  // for union
  b << (uint8_t)fDistinctUnionNum;
  b << (uint32_t)fUnionVec.size();

  for (uint32_t i = 0; i < fUnionVec.size(); i++)
    fUnionVec[i]->serialize(b);

  b << (uint64_t)fSubType;

  // for FROM subquery
  b << static_cast<uint32_t>(fDerivedTableList.size());

  for (uint32_t i = 0; i < fDerivedTableList.size(); i++)
    fDerivedTableList[i]->serialize(b);

  b << (uint64_t)fLimitStart;
  b << (uint64_t)fLimitNum;
  b << static_cast<ByteStream::byte>(fHasOrderBy);
  b << static_cast<ByteStream::byte>(fSpecHandlerProcessed);
  b << reinterpret_cast<uint32_t>(fOrderByThreads);

  b << static_cast<uint32_t>(fSelectSubList.size());

  for (uint32_t i = 0; i < fSelectSubList.size(); i++)
    fSelectSubList[i]->serialize(b);

  b << (uint64_t)fStringScanThreshold;
  b << (uint32_t)fQueryType;
  b << fPriority;
  b << fStringTableThreshold;
  b << fSchemaName;
  b << fLocalQuery;
  b << fUuid;
  b << fDJSSmallSideLimit;
  b << fDJSLargeSideLimit;
  b << fDJSPartitionSize;
  b << fDJSMaxPartitionTreeDepth;
  b << (uint8_t)fDJSForceRun;
  b << (uint32_t)fMaxPmJoinResultCount;
  b << fUMMemLimit;
  b << (uint8_t)fIsDML;
  messageqcpp::ByteStream::octbyte timeZone = fTimeZone;
  b << timeZone;
  b << fPron;
  b << (uint8_t)fWithRollup;
}

void CalpontSelectExecutionPlan::unserialize(messageqcpp::ByteStream& b)
{
  ReturnedColumn* rc;
  CalpontExecutionPlan* cep;
  string colName;
  uint8_t tmp8;

  ObjectReader::checkType(b, ObjectReader::CALPONTSELECTEXECUTIONPLAN);

  // erase elements, otherwise vectors contain null pointers
  fReturnedCols.clear();
  fSubSelects.clear();
  fGroupByCols.clear();
  fOrderByCols.clear();
  fTableList.clear();
  fColumnMap.clear();
  fUnionVec.clear();
  frmParms.clear();
  fDerivedTableList.clear();
  fSelectSubList.clear();

  if (fFilters != nullptr)
  {
    delete fFilters;
    fFilters = nullptr;
  }

  if (fHaving != nullptr)
  {
    delete fHaving;
    fHaving = nullptr;
  }

  if (!fDynamicParseTreeVec.empty())
  {
    for (auto& parseTree : fDynamicParseTreeVec)
    {
      if (parseTree)
      {
        // 'delete fFilters;' above has already deleted objects pointed
        // to by parseTree->left()/right()/data(), so we set the
        // pointers to NULL here before calling 'delete parseTree;'
        parseTree->left((ParseTree*)(nullptr));
        parseTree->right((ParseTree*)(nullptr));
        parseTree->data((TreeNode*)(nullptr));
        delete parseTree;
        parseTree = nullptr;
      }
    }

    fDynamicParseTreeVec.clear();
  }

  messageqcpp::ByteStream::quadbyte size;
  messageqcpp::ByteStream::quadbyte i;

  b >> size;

  for (i = 0; i < size; i++)
  {
    rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
    SRCP srcp(rc);
    fReturnedCols.push_back(srcp);
  }

  b >> size;
  CalpontSystemCatalog::TableAliasName tan;

  for (i = 0; i < size; i++)
  {
    tan.unserialize(b);
    fTableList.push_back(tan);
  }

  fFilters = ObjectReader::createParseTree(b);

  b >> size;

  for (i = 0; i < size; i++)
  {
    cep = ObjectReader::createExecutionPlan(b);
    fSubSelects.push_back(SCEP(cep));
  }

  b >> size;

  for (i = 0; i < size; i++)
  {
    rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
    SRCP srcp(rc);
    fGroupByCols.push_back(srcp);
  }

  fHaving = ObjectReader::createParseTree(b);

  b >> size;

  for (i = 0; i < size; i++)
  {
    rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
    SRCP srcp(rc);
    fOrderByCols.push_back(srcp);
  }

  b >> size;

  for (i = 0; i < size; i++)
  {
    b >> colName;
    rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
    SRCP srcp(rc);
    fColumnMap.insert(ColumnMap::value_type(colName, srcp));
  }

  b >> size;
  messageqcpp::ByteStream::doublebyte id;
  messageqcpp::ByteStream::quadbyte sessionId;
  messageqcpp::ByteStream::octbyte memory;

  for (i = 0; i < size; i++)
  {
    b >> sessionId;
    b >> id;
    b >> memory;
    frmParms.push_back(RMParam(sessionId, id, memory));
  }

  b >> fTableAlias;
  b >> reinterpret_cast<uint32_t&>(fLocation);
  b >> reinterpret_cast<ByteStream::byte&>(fDependent);

  // ? not sure if this needs to be added
  b >> fData;
  b >> reinterpret_cast<uint32_t&>(fSessionID);
  b >> reinterpret_cast<uint32_t&>(fTxnID);
  b >> fVerID;
  b >> fTraceFlags;
  b >> fStatementID;
  b >> reinterpret_cast<ByteStream::byte&>(fDistinct);
  uint8_t val;
  b >> reinterpret_cast<uint8_t&>(val);
  fOverrideLargeSideEstimate = (val != 0);

  // for union
  b >> (uint8_t&)(fDistinctUnionNum);
  b >> size;

  for (i = 0; i < size; i++)
  {
    cep = ObjectReader::createExecutionPlan(b);
    fUnionVec.push_back(SCEP(cep));
  }

  b >> (uint64_t&)fSubType;

  // for FROM subquery
  b >> size;

  for (i = 0; i < size; i++)
  {
    cep = ObjectReader::createExecutionPlan(b);
    fDerivedTableList.push_back(SCEP(cep));
  }

  b >> (uint64_t&)fLimitStart;
  b >> (uint64_t&)fLimitNum;
  b >> reinterpret_cast<ByteStream::byte&>(fHasOrderBy);
  b >> reinterpret_cast<ByteStream::byte&>(fSpecHandlerProcessed);
  b >> reinterpret_cast<uint32_t&>(fOrderByThreads);

  // for SELECT subquery
  b >> size;

  for (i = 0; i < size; i++)
  {
    cep = ObjectReader::createExecutionPlan(b);
    fSelectSubList.push_back(SCEP(cep));
  }

  b >> (uint64_t&)fStringScanThreshold;
  b >> (uint32_t&)fQueryType;
  b >> fPriority;
  b >> fStringTableThreshold;
  b >> fSchemaName;
  b >> fLocalQuery;
  b >> fUuid;
  b >> fDJSSmallSideLimit;
  b >> fDJSLargeSideLimit;
  b >> fDJSPartitionSize;
  b >> fDJSMaxPartitionTreeDepth;
  b >> (uint8_t&)fDJSForceRun;
  b >> (uint32_t&)fMaxPmJoinResultCount;
  b >> fUMMemLimit;
  b >> tmp8;
  fIsDML = tmp8;
  messageqcpp::ByteStream::octbyte timeZone;
  b >> timeZone;
  fTimeZone = timeZone;
  b >> fPron;
  utils::Pron::instance().pron(fPron);
  b >> tmp8;
  fWithRollup = tmp8;
}

bool CalpontSelectExecutionPlan::operator==(const CalpontSelectExecutionPlan& t) const
{
  // If we use this outside the serialization tests, we should
  // reorder these comparisons to speed up the common case

  ReturnedColumnList::const_iterator rcit;
  ReturnedColumnList::const_iterator rcit2;
  SelectList::const_iterator sit, sit2;
  ColumnMap::const_iterator map_it, map_it2;

  // fReturnedCols
  if (fReturnedCols.size() != t.fReturnedCols.size())
    return false;

  for (rcit = fReturnedCols.begin(), rcit2 = t.fReturnedCols.begin(); rcit != fReturnedCols.end();
       ++rcit, ++rcit2)
    if (**rcit != **rcit2)
      return false;

  // fFilters
  if (fFilters != nullptr && t.fFilters != nullptr)
  {
    if (*fFilters != *t.fFilters)
      return false;
  }
  else if (fFilters != nullptr || t.fFilters != nullptr)
    return false;

  // fSubSelects
  if (fSubSelects.size() != t.fSubSelects.size())
    return false;

  for (sit = fSubSelects.begin(), sit2 = t.fSubSelects.begin(); sit != fSubSelects.end(); ++sit, ++sit2)
    if (*((*sit).get()) != (*sit2).get())
      return false;

  // fGroupByCols
  if (fGroupByCols.size() != t.fGroupByCols.size())
    return false;

  for (rcit = fGroupByCols.begin(), rcit2 = t.fGroupByCols.begin(); rcit != fGroupByCols.end();
       ++rcit, ++rcit2)
    if (**rcit != **rcit2)
      return false;

  // fHaving
  if (fHaving != nullptr && t.fHaving != nullptr)
  {
    if (*fHaving != *t.fHaving)
      return false;
  }
  else if (fHaving != nullptr || t.fHaving != nullptr)
    return false;

  // fOrderByCols
  if (fOrderByCols.size() != t.fOrderByCols.size())
    return false;

  for (rcit = fOrderByCols.begin(), rcit2 = t.fOrderByCols.begin(); rcit != fOrderByCols.end();
       ++rcit, ++rcit2)
    if (**rcit != **rcit2)
      return false;

  // fColumnMap
  if (fColumnMap.size() != t.fColumnMap.size())
    return false;

  for (map_it = fColumnMap.begin(), map_it2 = t.fColumnMap.begin(); map_it != fColumnMap.end();
       ++map_it, ++map_it2)
    if (*(map_it->second) != *(map_it2->second))
      return false;

  if (fTableAlias != t.fTableAlias)
    return false;

  if (fLocation != t.fLocation)
    return false;

  if (fDependent != t.fDependent)
    return false;

  // Trace flags don't affect equivalency?
  // if (fTraceFlags != t.fTraceFlags) return false;
  if (fStatementID != t.fStatementID)
    return false;

  if (fSubType != t.fSubType)
    return false;

  if (fPriority != t.fPriority)
    return false;

  if (fStringTableThreshold != t.fStringTableThreshold)
    return false;

  if (fDJSSmallSideLimit != t.fDJSSmallSideLimit)
    return false;

  if (fDJSLargeSideLimit != t.fDJSLargeSideLimit)
    return false;

  if (fDJSPartitionSize != t.fDJSPartitionSize)
    return false;

  if (fUMMemLimit != t.fUMMemLimit)
    return false;

  return true;
}

bool CalpontSelectExecutionPlan::operator==(const CalpontExecutionPlan* t) const
{
  const CalpontSelectExecutionPlan* ac;

  ac = dynamic_cast<const CalpontSelectExecutionPlan*>(t);

  if (ac == nullptr)
    return false;

  return *this == *ac;
}

bool CalpontSelectExecutionPlan::operator!=(const CalpontSelectExecutionPlan& t) const
{
  return !(*this == t);
}

bool CalpontSelectExecutionPlan::operator!=(const CalpontExecutionPlan* t) const
{
  return !(*this == t);
}

void CalpontSelectExecutionPlan::columnMap(const ColumnMap& columnMap)
{
  ColumnMap::const_iterator map_it1, map_it2;
  fColumnMap.erase(fColumnMap.begin(), fColumnMap.end());

  SRCP srcp;

  for (map_it2 = columnMap.begin(); map_it2 != columnMap.end(); ++map_it2)
  {
    srcp.reset(map_it2->second->clone());
    fColumnMap.insert(ColumnMap::value_type(map_it2->first, srcp));
  }
}

void CalpontSelectExecutionPlan::rmParms(const RMParmVec& parms)
{
  frmParms.clear();
  frmParms.assign(parms.begin(), parms.end());
}

void CalpontSelectExecutionPlan::pron(std::string&& pron)
{
  fPron = pron;
}

// This routine doesn't copy derived table list, union vector, select subqueries, subquery list, and
// subselects.
execplan::SCSEP CalpontSelectExecutionPlan::cloneWORecursiveSelects()
{
  execplan::SCSEP newPlan(new CalpontSelectExecutionPlan(fLocation));

  // Copy simple members
  newPlan->fLocalQuery = fLocalQuery;
  newPlan->fTableAlias = fTableAlias;
  newPlan->fLocation = fLocation;
  newPlan->fDependent = fDependent;
  newPlan->fData = fData;
  newPlan->fSessionID = fSessionID;
  newPlan->fTxnID = fTxnID;
  newPlan->fVerID = fVerID;
  newPlan->fSchemaName = fSchemaName;
  newPlan->fTableName = fTableName;
  newPlan->fTraceFlags = fTraceFlags;
  newPlan->fStatementID = fStatementID;
  newPlan->fDistinct = fDistinct;
  newPlan->fOverrideLargeSideEstimate = fOverrideLargeSideEstimate;
  newPlan->fDistinctUnionNum = fDistinctUnionNum;
  newPlan->fSubType = fSubType;
  newPlan->fDerivedTbAlias = fDerivedTbAlias;
  newPlan->fDerivedTbView = fDerivedTbView;
  newPlan->fLimitStart = fLimitStart;
  newPlan->fLimitNum = fLimitNum;
  newPlan->fHasOrderBy = fHasOrderBy;
  newPlan->fStringScanThreshold = fStringScanThreshold;
  newPlan->fQueryType = fQueryType;
  newPlan->fPriority = fPriority;
  newPlan->fStringTableThreshold = fStringTableThreshold;
  newPlan->fSpecHandlerProcessed = fSpecHandlerProcessed;
  newPlan->fOrderByThreads = fOrderByThreads;
  newPlan->fUuid = fUuid;
  newPlan->fDJSSmallSideLimit = fDJSSmallSideLimit;
  newPlan->fDJSLargeSideLimit = fDJSLargeSideLimit;
  newPlan->fDJSPartitionSize = fDJSPartitionSize;
  newPlan->fDJSMaxPartitionTreeDepth = fDJSMaxPartitionTreeDepth;
  newPlan->fDJSForceRun = fDJSForceRun;
  newPlan->fMaxPmJoinResultCount = fMaxPmJoinResultCount;
  newPlan->fUMMemLimit = fUMMemLimit;
  newPlan->fIsDML = fIsDML;
  newPlan->fTimeZone = fTimeZone;
  newPlan->fPron = fPron;
  newPlan->fWithRollup = fWithRollup;

  // Deep copy of ReturnedColumnList
  ReturnedColumnList newReturnedCols;
  for (const auto& col : fReturnedCols)
  {
    if (col)
      newReturnedCols.push_back(SRCP(col->clone()));
  }
  newPlan->returnedCols(newReturnedCols);

  // Deep copy of filters
  if (fFilters)
    newPlan->filters(new ParseTree(*fFilters));

  // Deep copy of filter token list
  newPlan->filterTokenList(fFilterTokenList);
  newPlan->havingTokenList(fHavingTokenList);

  // Deep copy of group by columns
  GroupByColumnList newGroupByCols;
  for (const auto& col : fGroupByCols)
  {
    if (col)
      newGroupByCols.push_back(SRCP(col->clone()));
  }
  newPlan->groupByCols(newGroupByCols);

  // Deep copy of having clause
  if (fHaving)
    newPlan->having(new ParseTree(*fHaving));

  // Deep copy of order by columns
  OrderByColumnList newOrderByCols;
  for (const auto& col : fOrderByCols)
  {
    if (col)
      newOrderByCols.push_back(SRCP(col->clone()));
  }
  newPlan->orderByCols(newOrderByCols);

  // Deep copy of column map
  ColumnMap newColumnMap;
  for (const auto& entry : fColumnMap)
  {
    if (entry.second)
      newColumnMap.insert(ColumnMap::value_type(entry.first, SRCP(entry.second->clone())));
  }
  newPlan->columnMap(newColumnMap);

  // Copy RM parameters
  newPlan->rmParms(frmParms);

  // Deep copy of table list
  newPlan->tableList(fTableList);

  return newPlan;
}

// This clone must return CSEP w/o sub-selects, group by, order by, having limit.
execplan::SCSEP CalpontSelectExecutionPlan::cloneForTableWORecursiveSelectsGbObHaving(
    const execplan::CalpontSystemCatalog::TableAliasName& targetTableAlias, const bool withFilters)
{
  execplan::SCSEP newPlan(new CalpontSelectExecutionPlan(fLocation));

  // Copy simple members
  newPlan->fLocalQuery = fLocalQuery;
  newPlan->fTableAlias = fTableAlias;
  newPlan->fLocation = fLocation;
  newPlan->fDependent = fDependent;
  newPlan->fData = fData;
  newPlan->fSessionID = fSessionID;
  newPlan->fTxnID = fTxnID;
  newPlan->fVerID = fVerID;
  newPlan->fSchemaName = fSchemaName;
  newPlan->fTableName = fTableName;
  newPlan->fTraceFlags = fTraceFlags;
  newPlan->fStatementID = fStatementID;
  newPlan->fDistinct = fDistinct;
  newPlan->fOverrideLargeSideEstimate = fOverrideLargeSideEstimate;
  newPlan->fDistinctUnionNum = fDistinctUnionNum;
  newPlan->fSubType = fSubType;
  newPlan->fDerivedTbAlias = fDerivedTbAlias;
  newPlan->fDerivedTbView = fDerivedTbView;
  newPlan->fStringScanThreshold = fStringScanThreshold;
  newPlan->fQueryType = fQueryType;
  newPlan->fPriority = fPriority;
  newPlan->fStringTableThreshold = fStringTableThreshold;
  newPlan->fSpecHandlerProcessed = fSpecHandlerProcessed;
  newPlan->fOrderByThreads = fOrderByThreads;
  newPlan->fUuid = fUuid;
  newPlan->fDJSSmallSideLimit = fDJSSmallSideLimit;
  newPlan->fDJSLargeSideLimit = fDJSLargeSideLimit;
  newPlan->fDJSPartitionSize = fDJSPartitionSize;
  newPlan->fDJSMaxPartitionTreeDepth = fDJSMaxPartitionTreeDepth;
  newPlan->fDJSForceRun = fDJSForceRun;
  newPlan->fMaxPmJoinResultCount = fMaxPmJoinResultCount;
  newPlan->fUMMemLimit = fUMMemLimit;
  newPlan->fIsDML = fIsDML;
  newPlan->fTimeZone = fTimeZone;
  newPlan->fPron = fPron;
  newPlan->fWithRollup = fWithRollup;

  // Deep copy of ReturnedColumnList
  ReturnedColumnList newReturnedCols;

  for (const auto& rc : fReturnedCols)
  {
    rc->setSimpleColumnList();
    for (auto* simpleColumn : rc->simpleColumnList())
    {
      auto tableAlias = simpleColumn->singleTable();
      if (tableAlias && targetTableAlias.weakerEq(*tableAlias))
      {
        // TODO We insert multiple times if there are multiple SCs for the same RC.
        newReturnedCols.push_back(SRCP(rc->clone()));
      }
    }
  }

  newPlan->returnedCols(newReturnedCols);

  // Deep copy of filters
  // WIP only filters that apply to the target table must be left intact
  // replace all irrelevant branches with true
  if (fFilters && withFilters)
  {
    newPlan->filters(new ParseTree(*fFilters));
  }

  // Deep copy of filter token list
  newPlan->filterTokenList(fFilterTokenList);

  ColumnMap newColumnMap;
  // Deep copy of column map
  for (const auto& entry : fColumnMap)
  {
    auto tableAlias = entry.second->singleTable();
    // TODO We insert multiple times if there are multiple SCs for the same RC.
    if (tableAlias && targetTableAlias.weakerEq(*tableAlias))
    {
      auto it = fColumnMap.find(entry.first);
      if (it == fColumnMap.end())
      {
        newColumnMap.insert({entry.first, SRCP(entry.second->clone())});
      }
    }
  }

  newPlan->columnMap(newColumnMap);

  // Copy RM parameters
  newPlan->rmParms(frmParms);

  // Deep copy of table list
  // INV the target table must be contained in the original TableList in this CSEP
  TableList newTableList{targetTableAlias};

  newPlan->tableList(newTableList);

  return newPlan;
}

SCSEP CalpontSelectExecutionPlan::clone()
{
  auto newPlan = cloneWORecursiveSelects();

  newPlan->fSelectSubList.clear();
  for (const auto& subPlan : fSubSelects)
  {
    auto* subCSEP = dynamic_cast<CalpontSelectExecutionPlan*>(subPlan.get());
    idbassert_s(subCSEP != nullptr, "subPlan is not a CalpontSelectExecutionPlan");
    newPlan->fSubSelects.push_back(subCSEP->clone());
  }

  newPlan->fDerivedTableList.clear();
  for (const auto& drvTable: fDerivedTableList)
  {
    auto* drvCSEP = dynamic_cast<CalpontSelectExecutionPlan*>(drvTable.get());
    idbassert_s(drvCSEP != nullptr, "derivedTable is not a CalpontSelectExecutionPlan");
    newPlan->fDerivedTableList.push_back(drvCSEP->clone());
  }

  newPlan->fUnionVec.clear();
  for (const auto& subPlan : fUnionVec)
  {
    auto* subCSEP = dynamic_cast<CalpontSelectExecutionPlan*>(subPlan.get());
    idbassert_s(subCSEP != nullptr, "unionVec is not a CalpontSelectExecutionPlan");
    newPlan->fUnionVec.push_back(subCSEP->clone());
  }

  newPlan->fSelectSubList.clear();
  for (const auto& subPlan : fSelectSubList)
  {
    auto* subCSEP = dynamic_cast<CalpontSelectExecutionPlan*>(subPlan.get());
    idbassert_s(subCSEP != nullptr, "subPlan is not a CalpontSelectExecutionPlan");
    newPlan->fSelectSubList.push_back(subCSEP->clone());
  }

  newPlan->fSubSelectList.clear();
  for (const auto& subPlan : fSubSelectList)
  {
    newPlan->fSubSelectList.push_back(subPlan->clone());
  }

  return newPlan;
}

}  // namespace execplan
