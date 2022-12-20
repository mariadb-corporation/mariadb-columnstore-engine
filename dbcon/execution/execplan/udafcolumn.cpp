/*
   Copyright (c) 2017, MariaDB

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
   MA 02110-1301, USA.
 */

#include <sstream>
#include <cstring>

using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "simplefilter.h"
#include "constantfilter.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "objectreader.h"
#include "groupconcatcolumn.h"
#include "udafcolumn.h"

namespace execplan
{
/**
 * Constructors/Destructors
 */
UDAFColumn::UDAFColumn() : AggregateColumn()
{
}

UDAFColumn::UDAFColumn(const uint32_t sessionID) : AggregateColumn(sessionID)
{
}

UDAFColumn::UDAFColumn(const UDAFColumn& rhs, const uint32_t sessionID)
 : AggregateColumn(dynamic_cast<const AggregateColumn&>(rhs), sessionID), context(rhs.context)
{
}

UDAFColumn::~UDAFColumn()
{
}

/**
 * Methods
 */

const string UDAFColumn::toString() const
{
  ostringstream output;
  output << "UDAFColumn " << endl;
  output << AggregateColumn::toString() << endl;
  output << context.toString() << endl;
  return output.str();
}

ostream& operator<<(ostream& output, const UDAFColumn& rhs)
{
  output << rhs.toString();
  return output;
}

void UDAFColumn::serialize(messageqcpp::ByteStream& b) const
{
  b << (uint8_t)ObjectReader::UDAFCOLUMN;
  AggregateColumn::serialize(b);
  context.serialize(b);
}

void UDAFColumn::unserialize(messageqcpp::ByteStream& b)
{
  ObjectReader::checkType(b, ObjectReader::UDAFCOLUMN);
  AggregateColumn::unserialize(b);
  context.unserialize(b);
}

bool UDAFColumn::operator==(const UDAFColumn& t) const
{
  const AggregateColumn *rc1, *rc2;

  rc1 = static_cast<const AggregateColumn*>(this);
  rc2 = static_cast<const AggregateColumn*>(&t);

  if (*rc1 != *rc2)
    return false;

  if (context != t.context)
    return false;

  return true;
}

bool UDAFColumn::operator==(const TreeNode* t) const
{
  const UDAFColumn* ac;

  ac = dynamic_cast<const UDAFColumn*>(t);

  if (ac == NULL)
    return false;

  return *this == *ac;
}

bool UDAFColumn::operator!=(const UDAFColumn& t) const
{
  return !(*this == t);
}

bool UDAFColumn::operator!=(const TreeNode* t) const
{
  return !(*this == t);
}

}  // namespace execplan
