/* Copyright (C) 2016 MariaDB Corporaton

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

#ifndef ALARMGLOBAL_H
#define ALARMGLOBAL_H

#ifdef __linux__
#include <sys/file.h>
#include <linux/unistd.h>
#endif
#include <string>
#include <stdint.h>

namespace alarmmanager {


/** @brief constant define
 *
 */
const int SET = 1;
const int CLEAR = 0;

const std::string ACTIVE_ALARM_FILE = "/var/log/mariadb/columnstore/activeAlarms";
const std::string ALARM_FILE = "/var/log/mariadb/columnstore/alarm.log";
const std::string ALARM_ARCHIVE_FILE = "/var/log/mariadb/columnstore/archive";

const bool ALARM_DEBUG = false;
const uint16_t INVALID_ALARM_ID = 0;
}

#endif
