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

#include <iostream>

#ifdef __aarch64__
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif

namespace archcheck
{
#if defined(__x86_64__)
__attribute__((target("default")))
int checkArchitecture()
{
  // The default version.
  return 1;
}

__attribute__ ((target ("sse4.2")))
int checkArchitecture ()
{
  // version for SSE4.2
  return 2;
}
#elif defined(__aarch64__)
int checkArchitecture()
{
  // version for arm
  if ((getauxval(AT_HWCAP) & HWCAP_ASIMD) != 0)
    return 2;
  return 1;
}
#else
int checkArchitecture()
{
  return -1;
}
#endif
}
