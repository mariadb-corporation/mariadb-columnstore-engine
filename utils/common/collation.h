/*
   Copyright (C) 2020 MariaDB Corporation

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
#ifndef COLLATION_H_INCLUDED
#define COLLATION_H_INCLUDED


/*
  Redefine definitions used by MariaDB m_ctype.h.
  This is needed to avoid including <mariadb.h> and <my_sys.h>,
  which conflict with many MCS and boost headers.
*/

#ifndef FALSE
#define FALSE (0)
#endif

#ifndef TRUE
#define TRUE  (1)
#endif

#ifndef DBUG_ASSERT
#define DBUG_ASSERT(x)  idbassert(x)
#define DBUG_ASSERT_TEMPORARILY_DEFINED
#endif

#ifndef MYSQL_PLUGIN_IMPORT
#if (defined(_WIN32) && defined(MYSQL_DYNAMIC_PLUGIN))
#define MYSQL_PLUGIN_IMPORT __declspec(dllimport)
#else
#define MYSQL_PLUGIN_IMPORT
#endif
#endif

typedef long long int longlong;
typedef unsigned long long int ulonglong;
typedef uint32_t uint32;
typedef uint16_t uint16;
typedef char my_bool;
typedef unsigned char uchar;

#if defined(__GNUC__) && !defined(_lint)
typedef char	pchar;		/* Mixed prototypes can take char */
typedef char	puchar;		/* Mixed prototypes can take char */
typedef char	pbool;		/* Mixed prototypes can take char */
typedef short	pshort;		/* Mixed prototypes can take short int */
typedef float	pfloat;		/* Mixed prototypes can take float */
#else
typedef int	pchar;		/* Mixed prototypes can't take char */
typedef uint	puchar;		/* Mixed prototypes can't take char */
typedef int	pbool;		/* Mixed prototypes can't take char */
typedef int	pshort;		/* Mixed prototypes can't take short int */
typedef double	pfloat;		/* Mixed prototypes can't take float */
#endif

typedef const struct charset_info_st CHARSET_INFO;
extern "C" MYSQL_PLUGIN_IMPORT CHARSET_INFO *default_charset_info;

#include "m_ctype.h"

#undef FALSE
#undef TRUE

#ifdef DBUG_ASSERT_TEMPORARILY_DEFINED
#undef DBUG_ASSERT
#endif


namespace datatypes
{

// A reference to MariaDB CHARSET_INFO.

class Charset
{
protected:
    const struct charset_info_st & mCharset;
public:
    Charset(CHARSET_INFO & cs) :mCharset(cs) { }
    Charset(uint32_t charsetNumber);
    CHARSET_INFO & getCharset() const { return mCharset; }
};


} // end of namespace datatypes

#endif
