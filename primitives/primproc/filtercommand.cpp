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
 *   $Id: filtercommand.cpp 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *
 ***********************************************************************/

#include <sstream>
#include "primitiveserver.h"
#include "columncommand.h"
#include "dictstep.h"
#include "filtercommand.h"
#include "dataconvert.h"
#include "mcs_decimal.h"
#include "mcs_string.h"

using namespace std;
using namespace messageqcpp;

namespace
{
uint32_t cc = primitiveprocessor::Command::COLUMN_COMMAND;
uint32_t ds = primitiveprocessor::Command::DICT_STEP;
const uint32_t CC = (cc << 8) | cc;
const uint32_t DCC = (ds << 16) | (cc << 8) | cc;
const uint32_t CDC = (cc << 16) | (ds << 8) | cc;
const uint32_t DCDC = (ds << 24) | (cc << 16) | (ds << 8) | cc;

};  // namespace

namespace primitiveprocessor
{
Command* FilterCommand::makeFilterCommand(ByteStream& bs, vector<SCommand>& cmds)
{
  bs.advance(1);
  // find out the # of commands in the cmds vector,
  // vector::size() will not work, because cmds is resize() to filterCount
  uint64_t nc = 0;

  while (cmds[nc].get() != NULL)
    nc++;

  // figure out the feeding commands
  // must have 2 columncommands, may have 1 or 2 dictsteps.
  uint64_t cols = 0;  // # of columncommands
  uint32_t columns = 0;
  uint64_t i = nc;

  while (i > 0 && cols < 2)
  {
    Command::CommandType cmdType = cmds[i - 1]->getCommandType();

    if (cmdType != Command::COLUMN_COMMAND && cmdType != Command::DICT_STEP)
    {
      stringstream msg;
      msg << "FilterCommand: feeded by " << cmdType << " is not supported.";
      throw logic_error(msg.str());
    }

    columns = (columns << 8) + cmdType;

    if (cmdType == Command::COLUMN_COMMAND)
      cols++;

    i--;
  }

  // should not happen
  if (cols < 2)
    throw logic_error("FilterCommand: not enough feeding ColumnCommands.");

  FilterCommand* fc = NULL;

  // the order setting left/right feeder is important, left is the smaller index.
  // because the doFilter relies on the rids of right-feeder is a subset of left.
  if (columns == CC)
  {
    cmds[nc - 2]->filterFeeder(LEFT_FEEDER);
    cmds[nc - 1]->filterFeeder(RIGHT_FEEDER);

    ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc - 2].get());
    ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc - 1].get());
    uint32_t scale0 = cmd0->getScale();
    uint32_t scale1 = cmd1->getScale();

    // char[] is stored as int, but cannot directly compare if length is different
    // due to endian issue
    if (cmd0->getColType().colDataType == execplan::CalpontSystemCatalog::CHAR ||
        cmd0->getColType().colDataType == execplan::CalpontSystemCatalog::VARCHAR ||
        cmd0->getColType().colDataType == execplan::CalpontSystemCatalog::TEXT)
    {
      StrFilterCmd* sc = new StrFilterCmd();
      sc->setCompareFunc(CC);
      fc = sc;
    }
    else if (scale0 == scale1)
    {
      fc = new FilterCommand();
    }
    else
    {
      ScaledFilterCmd* sc = new ScaledFilterCmd();
      sc->setFactor(datatypes::scaleDivisor<double>(scale1) / datatypes::scaleDivisor<double>(scale0));
      fc = sc;
    }

    fc->setColTypes(cmd0->getColType(), cmd1->getColType());
  }
  else if (columns == DCDC)  // both string
  {
    StrFilterCmd* sc = new StrFilterCmd();
    cmds[nc - 4]->filterFeeder(FILT_FEEDER);
    cmds[nc - 3]->filterFeeder(LEFT_FEEDER);
    cmds[nc - 2]->filterFeeder(FILT_FEEDER);
    cmds[nc - 1]->filterFeeder(RIGHT_FEEDER);
    sc->setCompareFunc(DCDC);
    fc = sc;

    ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc - 4].get());
    ColumnCommand* cmd2 = dynamic_cast<ColumnCommand*>(cmds[nc - 2].get());
    fc->setColTypes(cmd0->getColType(), cmd2->getColType());
  }
  else if (columns == DCC)  // lhs: char[]; rhs: string
  {
    StrFilterCmd* sc = new StrFilterCmd();
    cmds[nc - 3]->filterFeeder(LEFT_FEEDER);
    cmds[nc - 2]->filterFeeder(FILT_FEEDER);
    cmds[nc - 1]->filterFeeder(RIGHT_FEEDER);
    ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc - 3].get());
    ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc - 2].get());
    size_t cl = cmd0->getWidth();  // char[] column
    sc->setCharLength(cl);
    sc->setCompareFunc(DCC);
    fc = sc;
    fc->setColTypes(cmd0->getColType(), cmd1->getColType());
  }
  else if (columns == CDC)  // lhs: string; rhs: char[]
  {
    StrFilterCmd* sc = new StrFilterCmd();
    cmds[nc - 3]->filterFeeder(FILT_FEEDER);
    cmds[nc - 2]->filterFeeder(LEFT_FEEDER);
    cmds[nc - 1]->filterFeeder(RIGHT_FEEDER);
    ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc - 3].get());
    ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc - 1].get());
    size_t cl = cmd1->getWidth();  // char[] column
    sc->setCharLength(cl);
    sc->setCompareFunc(CDC);
    fc = sc;
    fc->setColTypes(cmd0->getColType(), cmd1->getColType());
  }
  else
  {
    stringstream msg;
    msg << "FilterCommand does not handle this column code: " << hex << columns << dec;
    throw logic_error(msg.str());
  }

  return fc;
}

FilterCommand::FilterCommand() : Command(FILTER_COMMAND), fBOP(0), hasWideColumns(false)
{
}

FilterCommand::~FilterCommand()
{
}

void FilterCommand::execute()
{
  doFilter();
}

void FilterCommand::createCommand(ByteStream& bs)
{
  bs >> fBOP;
  Command::createCommand(bs);
}

void FilterCommand::resetCommand(ByteStream& bs)
{
}

void FilterCommand::prep(int8_t outputType, bool absRids)
{
}

void FilterCommand::project()
{
}

void FilterCommand::projectIntoRowGroup(rowgroup::RowGroup& rg, uint32_t col)
{
}

uint64_t FilterCommand::getLBID()
{
  return 0;
}

void FilterCommand::nextLBID()
{
}

SCommand FilterCommand::duplicate()
{
  return SCommand(new FilterCommand(*this));
}

void FilterCommand::setColTypes(const execplan::CalpontSystemCatalog::ColType& left,
                                const execplan::CalpontSystemCatalog::ColType& right)
{
  leftColType = left;
  rightColType = right;

  if (left.isWideDecimalType() || right.isWideDecimalType())
    hasWideColumns = true;
}

void FilterCommand::doFilter()
{
  bpp->ridMap = 0;
  bpp->ridCount = 0;

  bool (FilterCommand::*compareFunc)(uint64_t, uint64_t);

  if (hasWideColumns)
    compareFunc = &FilterCommand::binaryCompare;
  else
    compareFunc = &FilterCommand::compare;

  // rids in [0] is used for scan [1], so [1] is a subset of [0], and same order.
  // -- see makeFilterCommand() above.
  for (uint64_t i = 0, j = 0; j < bpp->fFiltRidCount[1];)
  {
    if (bpp->fFiltCmdRids[0][i] != bpp->fFiltCmdRids[1][j])
    {
      i++;
    }
    else
    {
      if ((this->*compareFunc)(i, j) == true)
      {
        bpp->relRids[bpp->ridCount] = bpp->fFiltCmdRids[0][i];
        if (leftColType.isWideDecimalType())
          bpp->wide128Values[bpp->ridCount] = bpp->fFiltCmdBinaryValues[0][i];
        else
          bpp->values[bpp->ridCount] = bpp->fFiltCmdValues[0][i];
        bpp->ridMap |= 1 << (bpp->relRids[bpp->ridCount] >> 9);
        bpp->ridCount++;
      }

      i++;
      j++;
    }
  }

  // bug 1247 -- reset the rid count
  bpp->fFiltRidCount[0] = bpp->fFiltRidCount[1] = 0;
}

bool FilterCommand::compare(uint64_t i, uint64_t j)
{
  if (execplan::isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
      execplan::isNull(bpp->fFiltCmdValues[1][j], rightColType))
    return false;

  switch (fBOP)
  {
    case COMPARE_GT: return bpp->fFiltCmdValues[0][i] > bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_LT: return bpp->fFiltCmdValues[0][i] < bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_EQ: return bpp->fFiltCmdValues[0][i] == bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_GE: return bpp->fFiltCmdValues[0][i] >= bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_LE: return bpp->fFiltCmdValues[0][i] <= bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_NE: return bpp->fFiltCmdValues[0][i] != bpp->fFiltCmdValues[1][j]; break;

    default: return false; break;
  }
}

bool FilterCommand::binaryCompare(uint64_t i, uint64_t j)
{
  // We type-promote to int128_t if either of the columns are
  // not int128_t
  int128_t leftVal, rightVal;

  if (leftColType.isWideDecimalType())
  {
    if (execplan::isNull(bpp->fFiltCmdBinaryValues[0][i], leftColType))
      return false;
    leftVal = bpp->fFiltCmdBinaryValues[0][i];
  }
  else
  {
    if (execplan::isNull(bpp->fFiltCmdValues[0][i], leftColType))
      return false;
    leftVal = bpp->fFiltCmdValues[0][i];
  }

  if (rightColType.isWideDecimalType())
  {
    if (execplan::isNull(bpp->fFiltCmdBinaryValues[1][j], rightColType))
      return false;
    rightVal = bpp->fFiltCmdBinaryValues[1][j];
  }
  else
  {
    if (execplan::isNull(bpp->fFiltCmdValues[1][j], rightColType))
      return false;
    rightVal = bpp->fFiltCmdValues[1][j];
  }

  switch (fBOP)
  {
    case COMPARE_GT: return leftVal > rightVal; break;

    case COMPARE_LT: return leftVal < rightVal; break;

    case COMPARE_EQ: return leftVal == rightVal; break;

    case COMPARE_GE: return leftVal >= rightVal; break;

    case COMPARE_LE: return leftVal <= rightVal; break;

    case COMPARE_NE: return leftVal != rightVal; break;

    default: return false; break;
  }
}

bool FilterCommand::operator==(const FilterCommand& c) const
{
  return (fBOP == c.fBOP);
}

bool FilterCommand::operator!=(const FilterCommand& c) const
{
  return !(*this == c);
}

// == ScaledFilterCmd ==
ScaledFilterCmd::ScaledFilterCmd() : fFactor(1)
{
}

ScaledFilterCmd::~ScaledFilterCmd()
{
}

SCommand ScaledFilterCmd::duplicate()
{
  SCommand ret;
  ScaledFilterCmd* filterCmd;

  ret.reset(new ScaledFilterCmd());
  filterCmd = (ScaledFilterCmd*)ret.get();
  filterCmd->fBOP = fBOP;
  filterCmd->fFactor = fFactor;

  return ret;
}

bool ScaledFilterCmd::compare(uint64_t i, uint64_t j)
{
  if (execplan::isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
      execplan::isNull(bpp->fFiltCmdValues[1][j], rightColType))
    return false;

  switch (fBOP)
  {
    case COMPARE_GT: return bpp->fFiltCmdValues[0][i] * fFactor > bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_LT: return bpp->fFiltCmdValues[0][i] * fFactor < bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_EQ: return bpp->fFiltCmdValues[0][i] * fFactor == bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_GE: return bpp->fFiltCmdValues[0][i] * fFactor >= bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_LE: return bpp->fFiltCmdValues[0][i] * fFactor <= bpp->fFiltCmdValues[1][j]; break;

    case COMPARE_NE: return bpp->fFiltCmdValues[0][i] * fFactor != bpp->fFiltCmdValues[1][j]; break;

    default: return false; break;
  }
}

void ScaledFilterCmd::setFactor(double f)
{
  fFactor = f;
}

double ScaledFilterCmd::factor()
{
  return fFactor;
}

bool ScaledFilterCmd::operator==(const ScaledFilterCmd& c) const
{
  return ((fBOP == c.fBOP) && (fFactor == c.fFactor));
}

bool ScaledFilterCmd::operator!=(const ScaledFilterCmd& c) const
{
  return !(*this == c);
}

// == StrFilterCmd ==
StrFilterCmd::StrFilterCmd() : fCompare(NULL), fCharLength(8)
{
}

StrFilterCmd::~StrFilterCmd()
{
}

SCommand StrFilterCmd::duplicate()
{
  return SCommand(new StrFilterCmd(*this));
}

void StrFilterCmd::execute()
{
  doFilter();
}

bool StrFilterCmd::compare(uint64_t i, uint64_t j)
{
  return (this->*fCompare)(i, j);
}

void StrFilterCmd::setCompareFunc(uint32_t columns)
{
  if (columns == CC)  // char[] : char
  {
    fCompare = &StrFilterCmd::compare_cc;
  }
  else if (columns == DCDC)  // string : string
  {
    fCompare = &StrFilterCmd::compare_ss;
  }
  else if (columns == DCC)  // char[] : string
  {
    fCompare = &StrFilterCmd::compare_cs;
  }
  else if (columns == CDC)  // string : char[]
  {
    fCompare = &StrFilterCmd::compare_sc;
  }
  else
  {
    stringstream msg;
    msg << "StrFilterCmd: unhandled column combination " << hex << columns << dec;
    throw logic_error(msg.str());
  }
}

// TODO:
// Move this function as a method to class Charset
// and reuse it in:
// - colCompareStr() in primitives/linux-port/column.cpp
// - compareStr()    in dbcon/joblist/lbidlist.cpp
//
// Note, the COMPARE_XXX constant should be put into
// a globally visible  enum first, e.g. utils/common/mcs_basic_types.h
// and all "fBOP" members in all classes should be changed to this enum.

static inline bool compareString(const datatypes::Charset& cs, const utils::ConstString& s0,
                                 const utils::ConstString& s1, uint8_t fBOP)
{
  int cmp = cs.strnncollsp(s0, s1);
  switch (fBOP)
  {
    case COMPARE_GT: return cmp > 0;

    case COMPARE_LT: return cmp < 0;

    case COMPARE_EQ: return cmp == 0;

    case COMPARE_GE: return cmp >= 0;

    case COMPARE_LE: return cmp <= 0;

    case COMPARE_NE: return cmp != 0;

    default: break;
  }
  return false;
}

bool StrFilterCmd::compare_cc(uint64_t i, uint64_t j)
{
  if (execplan::isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
      execplan::isNull(bpp->fFiltCmdValues[1][j], rightColType))
    return false;

  datatypes::Charset cs(leftColType.getCharset());
  datatypes::TCharShort s0(bpp->fFiltCmdValues[0][i]);
  datatypes::TCharShort s1(bpp->fFiltCmdValues[1][j]);
  return compareString(cs, s0.toConstString(leftColType.colWidth), s1.toConstString(rightColType.colWidth),
                       fBOP);
}

bool StrFilterCmd::compare_ss(uint64_t i, uint64_t j)
{
  if (bpp->fFiltStrValues[0][i] == "" || bpp->fFiltStrValues[1][j] == "" ||
      bpp->fFiltStrValues[0][i] == joblist::CPNULLSTRMARK ||
      bpp->fFiltStrValues[1][j] == joblist::CPNULLSTRMARK)
    return false;

  datatypes::Charset cs(leftColType.getCharset());
  utils::ConstString s0(utils::ConstString(bpp->fFiltStrValues[0][i]));
  utils::ConstString s1(utils::ConstString(bpp->fFiltStrValues[1][j]));
  return compareString(cs, s0, s1, fBOP);
}

bool StrFilterCmd::compare_cs(uint64_t i, uint64_t j)
{
  if (execplan::isNull(bpp->fFiltCmdValues[0][i], leftColType) || bpp->fFiltStrValues[1][j] == "" ||
      bpp->fFiltStrValues[1][j] == joblist::CPNULLSTRMARK)
    return false;

  datatypes::Charset cs(leftColType.getCharset());
  datatypes::TCharShort s0(bpp->fFiltCmdValues[0][i]);
  utils::ConstString s1(bpp->fFiltStrValues[1][j]);
  return compareString(cs, s0.toConstString(leftColType.colWidth), s1, fBOP);
}

bool StrFilterCmd::compare_sc(uint64_t i, uint64_t j)
{
  if (bpp->fFiltStrValues[0][i] == "" || bpp->fFiltStrValues[0][i] == joblist::CPNULLSTRMARK ||
      execplan::isNull(bpp->fFiltCmdValues[1][j], rightColType))
    return false;

  datatypes::Charset cs(leftColType.getCharset());
  utils::ConstString s0(bpp->fFiltStrValues[0][i]);
  datatypes::TCharShort s1(bpp->fFiltCmdValues[1][j]);
  return compareString(cs, s0, s1.toConstString(rightColType.colWidth), fBOP);
}

void StrFilterCmd::setCharLength(size_t l)
{
  fCharLength = l;
}

size_t StrFilterCmd::charLength()
{
  return fCharLength;
}

bool StrFilterCmd::operator==(const StrFilterCmd& c) const
{
  return ((fBOP == c.fBOP) && fCharLength == c.fCharLength);
}

bool StrFilterCmd::operator!=(const StrFilterCmd& c) const
{
  return !(*this == c);
}

};  // namespace primitiveprocessor
