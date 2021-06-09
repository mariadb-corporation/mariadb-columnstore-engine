/*
   Copyright (C) 2021 MariaDB Corporation

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

/*
  Restore all conflicting definitions
  (between my_config.h and mcsconfig.h)
  previously remembered by mcsconfig_conflicting_defs_remember.h
*/

#pragma pop_macro("PACKAGE")
#pragma pop_macro("PACKAGE_BUGREPORT")
#pragma pop_macro("PACKAGE_NAME")
#pragma pop_macro("PACKAGE_STRING")
#pragma pop_macro("PACKAGE_TARNAME")
#pragma pop_macro("PACKAGE_VERSION")
#pragma pop_macro("VERSION")
#pragma pop_macro("HAVE_STRTOLL")
