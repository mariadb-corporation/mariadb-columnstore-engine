/* Copyright (C) 2022 MariaDB Corporation

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

#include <thread>

#include "rssmonfcn.h"
#include "serviceexemgr.h"

namespace exemgr
{
  void RssMonFcn::operator()() const
  {
    logging::Logger& msgLog = globServiceExeMgr->getLogger();
    auto* statementsRunningCount = globServiceExeMgr->getStatementsRunningCount();
    for (;;)
    {
      size_t rssMb = rss();
      size_t pct = rssMb * 100 / fMemTotal;

      if (pct > fMaxPct)
      {
        if (fMaxPct >= 95)
        {
          std::cerr << "Too much memory allocated!" << std::endl;
          logging::Message::Args args;
          args.add((int)pct);
          args.add((int)fMaxPct);
          msgLog.logMessage(logging::LOG_TYPE_CRITICAL, ServiceExeMgr::logRssTooBig, args, logging::LoggingID(16));
          exit(1);
        }

        if (statementsRunningCount->cur() == 0)
        {
          std::cerr << "Too much memory allocated!" << std::endl;
          logging::Message::Args args;
          args.add((int)pct);
          args.add((int)fMaxPct);
          msgLog.logMessage(logging::LOG_TYPE_WARNING, ServiceExeMgr::logRssTooBig, args, logging::LoggingID(16));
          exit(1);
        }

        std::cerr << "Too much memory allocated, but stmts running" << std::endl;
      }

      // Update sessionMemMap entries lower than current mem % use
      globServiceExeMgr->updateSessionMap(pct);

      pause_();
    }
  }
  void startRssMon(size_t maxPct, int pauseSeconds)
  {
    new std::thread(RssMonFcn(maxPct, pauseSeconds));
  }
} // namespace