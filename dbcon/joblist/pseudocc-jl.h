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

#pragma once

#include "columncommand-jl.h"

namespace joblist
{
class PseudoCCJL : public ColumnCommandJL
{
 public:
  explicit PseudoCCJL(const PseudoColStep&);
  ~PseudoCCJL() override;

  void createCommand(messageqcpp::ByteStream&) const override;
  void runCommand(messageqcpp::ByteStream&) const override;
  std::string toString() override;
  uint32_t getFunction() const
  {
    return function;
  }

 protected:
 private:
  uint32_t function;
};

}  // namespace joblist
