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
  Remember all conflicting definitions
  (between my_config.h and mcsconfig.h)
  so that we can restore them later.
*/

#pragma push_macro("PACKAGE")
#pragma push_macro("PACKAGE_BUGREPORT")
#pragma push_macro("PACKAGE_NAME")
#pragma push_macro("PACKAGE_STRING")
#pragma push_macro("PACKAGE_TARNAME")
#pragma push_macro("PACKAGE_VERSION")
#pragma push_macro("VERSION")
#pragma push_macro("HAVE_STRTOLL")
