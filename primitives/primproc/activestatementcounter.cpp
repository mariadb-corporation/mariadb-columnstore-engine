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

// $Id: activestatementcounter.cpp 940 2013-01-21 14:11:31Z rdempsey $
//

#include <unistd.h>
#include <map>
#include <mutex>

#include "activestatementcounter.h"

void ActiveStatementCounter::incr(bool& counted)
{
  if (counted)
    return;

  counted = true;
  std::unique_lock lk(fMutex);

  if (upperLimit > 0)
    while (fStatementCount >= upperLimit)
    {
      fStatementsWaiting++;
      condvar.wait(lk);
      --fStatementsWaiting;
    }

  fStatementCount++;
}

void ActiveStatementCounter::decr(bool& counted)
{
  if (!counted)
    return;

  counted = false;
  std::unique_lock lk(fMutex);

  if (fStatementCount == 0)
    return;

  --fStatementCount;
  condvar.notify_one();
}
