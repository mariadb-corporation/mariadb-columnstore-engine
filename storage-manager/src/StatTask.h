/* Copyright (C) 2019 MariaDB Corporation

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

#ifndef STATTASK_H_
#define STATTASK_H_

#include "PosixTask.h"

namespace storagemanager
{
class StatTask : public PosixTask
{
 public:
  StatTask(int sock, uint length);
  virtual ~StatTask();

  bool run();

 private:
  StatTask();
};

}  // namespace storagemanager
#endif
