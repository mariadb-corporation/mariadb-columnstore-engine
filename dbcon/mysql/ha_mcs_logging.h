/* Copyright (C) 2021 MariaDB Corporation
 *
 * This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

#ifndef HA_MCS_LOGGING_H__
#define HA_MCS_LOGGING_H__

#include "messagelog.h"

/**
  @brief
  Wrapper around logging facility.

  @details
  Reduces the boiler plate code.

  Called from number of places(mostly DML) in
  ha_mcs_impl.cpp().
*/
namespace ha_mcs_impl
{
inline void log_this(THD* thd, const char* message, logging::LOG_TYPE log_type, unsigned sid)
{
  // corresponds with dbcon in SubsystemID vector
  // in messagelog.cpp
  unsigned int subSystemId = 24;
  logging::LoggingID logid(subSystemId, sid, 0);
  logging::Message::Args args1;
  logging::Message msg(1);
  args1.add(message);
  msg.format(args1);
  logging::Logger logger(logid.fSubsysID);
  logger.logMessage(log_type, msg, logid);
}

};  // namespace ha_mcs_impl

#endif
