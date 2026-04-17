/* Copyright (C) 2026 MariaDB Corporation

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

#include "filter_builders.h"

#include <boost/make_shared.hpp>

#include "execplan/predicateoperator.h"
#include "execplan/returnedcolumn.h"
#include "execplan/simplefilter.h"

namespace optimizer
{
namespace lib
{

execplan::SOP makePredicateOp(const std::string& opSym,
                              const execplan::CalpontSystemCatalog::ColType& lhsType,
                              const execplan::CalpontSystemCatalog::ColType& rhsType)
{
  execplan::SOP op = boost::make_shared<execplan::PredicateOperator>(opSym);
  execplan::CalpontSystemCatalog::ColType l = lhsType;
  execplan::CalpontSystemCatalog::ColType r = rhsType;
  op->setOpType(l, r);
  op->resultType(op->operationType());
  return op;
}

execplan::ParseTree* makeCmpFilter(execplan::ReturnedColumn* lhs, const std::string& opSym,
                                   execplan::ReturnedColumn* rhs, long timeZone)
{
  execplan::SOP op = makePredicateOp(opSym, lhs->resultType(), rhs->resultType());
  auto* sf = new execplan::SimpleFilter(op, lhs, rhs, timeZone);
  return new execplan::ParseTree(sf);
}

execplan::ParseTree* makeIsNullFilter(execplan::ReturnedColumn* col)
{
  auto* nullConst = new execplan::ConstantColumnNull();
  nullConst->resultType(col->resultType());
  return makeCmpFilter(col, "isnull", nullConst, 0);
}

execplan::ParseTree* makeIsNotNullFilter(execplan::ReturnedColumn* col)
{
  auto* nullConst = new execplan::ConstantColumnNull();
  nullConst->resultType(col->resultType());
  return makeCmpFilter(col, "isnotnull", nullConst, 0);
}

execplan::ConstantColumn* makeConstUInt(uint64_t value, int8_t scale, uint8_t precision)
{
  return new execplan::ConstantColumnUInt(value, scale, precision);
}

execplan::ConstantColumn* makeConstNull()
{
  return new execplan::ConstantColumnNull();
}

}  // namespace lib
}  // namespace optimizer
