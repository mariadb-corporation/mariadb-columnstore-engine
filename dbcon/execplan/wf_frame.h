/*
   Copyright (c) 2017, MariaDB
   Copyright (C) 2014 InfiniDB, Inc.

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

#ifndef WINDOW_FUNCTION_FRAME_H
#define WINDOW_FUNCTION_FRAME_H
#include <string>
#include <iosfwd>
#include <vector>

#include "returnedcolumn.h"

/**
 * Namespace
 */
namespace execplan
{
// This enum is made consistant with mysql Item_window_func
enum WF_FRAME
{
  WF_PRECEDING = 0,
  WF_FOLLOWING,
  WF_UNBOUNDED_PRECEDING,
  WF_UNBOUNDED_FOLLOWING,
  WF_CURRENT_ROW,
  WF_UNKNOWN
};

struct WF_Boundary
{
  WF_Boundary()
  {
  }
  WF_Boundary(WF_FRAME frame) : fFrame(frame)
  {
  }
  ~WF_Boundary()
  {
  }
  const std::string toString() const;
  void serialize(messageqcpp::ByteStream&) const;
  void unserialize(messageqcpp::ByteStream&);
  SRCP fVal;    /// has to evaluate to unsigned value
  SRCP fBound;  /// order by col +, -, date_add or date_sub for RANGE window
  enum WF_FRAME fFrame;
};

struct WF_Frame
{
  WF_Frame() : fIsRange(true)
  {
    fStart.fFrame = WF_UNBOUNDED_PRECEDING;
    fEnd.fFrame = WF_UNBOUNDED_FOLLOWING;
  }
  ~WF_Frame()
  {
  }
  const std::string toString() const;
  void serialize(messageqcpp::ByteStream&) const;
  void unserialize(messageqcpp::ByteStream&);
  WF_Boundary fStart;
  WF_Boundary fEnd;
  bool fIsRange;  /// RANGE or ROWS
};

/**
 * @brief A class to represent the order by clause of window function
 */
struct WF_OrderBy
{
  WF_OrderBy()
  {
  }
  WF_OrderBy(std::vector<SRCP> orders) : fOrders(orders)
  {
  }
  ~WF_OrderBy(){};
  const std::string toString() const;
  void serialize(messageqcpp::ByteStream&) const;
  void unserialize(messageqcpp::ByteStream&);
  std::vector<SRCP> fOrders;
  WF_Frame fFrame;
};

}  // namespace execplan
#endif
