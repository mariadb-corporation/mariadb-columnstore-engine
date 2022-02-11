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

#if defined(PREFER_MY_CONFIG_H)

#if !defined(MY_CONFIG_H)
#error my_config.h was not included (but PREFER_MY_CONFIG_H was set)
#endif

#include "mcsconfig_conflicting_defs_remember.h"
#include "mcsconfig_conflicting_defs_undef.h"

#else
#if defined(MY_CONFIG_H)
#error my_config.h was included before mcsconfig.h (and PREFER_MY_CONFIG_H was not set)
#endif
#endif  // PREFER_MY_CONFIG_H

#include "mcsconfig.h"

#include "exceptclasses.h"
#include "conststring.h"

/*
  Redefine definitions used by MariaDB m_ctype.h.
  This is needed to avoid including <mariadb.h> and <my_sys.h>,
  which conflict with many MCS and boost headers.
*/

#ifndef FALSE
#define FALSE (0)
#endif

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef DBUG_ASSERT
#define DBUG_ASSERT(x) idbassert(x)
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
typedef char pchar;   /* Mixed prototypes can take char */
typedef char puchar;  /* Mixed prototypes can take char */
typedef char pbool;   /* Mixed prototypes can take char */
typedef short pshort; /* Mixed prototypes can take short int */
typedef float pfloat; /* Mixed prototypes can take float */
#else
typedef int pchar;     /* Mixed prototypes can't take char */
typedef uint puchar;   /* Mixed prototypes can't take char */
typedef int pbool;     /* Mixed prototypes can't take char */
typedef int pshort;    /* Mixed prototypes can't take short int */
typedef double pfloat; /* Mixed prototypes can't take float */
#endif

typedef const struct charset_info_st CHARSET_INFO;
extern "C" MYSQL_PLUGIN_IMPORT CHARSET_INFO* default_charset_info;

#include "m_ctype.h"

#undef FALSE
#undef TRUE

#ifdef DBUG_ASSERT_TEMPORARILY_DEFINED
#undef DBUG_ASSERT
#endif

#if defined(PREFER_MY_CONFIG_H)
#include "mcsconfig_conflicting_defs_restore.h"
#endif

namespace datatypes
{
class MariaDBHasher
{
  ulong mPart1;
  ulong mPart2;

 public:
  MariaDBHasher() : mPart1(1), mPart2(4)
  {
  }
  MariaDBHasher& add(CHARSET_INFO* cs, const char* str, size_t length)
  {
    cs->hash_sort((const uchar*)str, length, &mPart1, &mPart2);
    return *this;
  }
  MariaDBHasher& add(CHARSET_INFO* cs, const utils::ConstString& str)
  {
    return add(cs, str.str(), str.length());
  }
  uint32_t finalize() const
  {
    return (uint32_t)mPart1;
  }
};

// A reference to MariaDB CHARSET_INFO.

class Charset
{
 protected:
  const struct charset_info_st* mCharset;

 public:
  Charset(CHARSET_INFO& cs) : mCharset(&cs)
  {
  }
  Charset(CHARSET_INFO* cs) : mCharset(cs ? cs : &my_charset_bin)
  {
  }
  Charset(uint32_t charsetNumber);
  CHARSET_INFO& getCharset() const
  {
    return *mCharset;
  }
  uint32_t hash(const char* data, uint64_t len) const
  {
    return MariaDBHasher().add(mCharset, data, len).finalize();
  }
  bool eq(const std::string& str1, const std::string& str2) const
  {
    return mCharset->strnncollsp(str1.data(), str1.length(), str2.data(), str2.length()) == 0;
  }
  int strnncollsp(const utils::ConstString& str1, const utils::ConstString& str2) const
  {
    return mCharset->strnncollsp(str1.str(), str1.length(), str2.str(), str2.length());
  }
  int strnncollsp(const char* str1, size_t length1, const char* str2, size_t length2) const
  {
    return mCharset->strnncollsp(str1, length1, str2, length2);
  }
  int strnncollsp(const unsigned char* str1, size_t length1, const unsigned char* str2, size_t length2) const
  {
    return mCharset->strnncollsp((const char*)str1, length1, (const char*)str2, length2);
  }
  bool test_if_important_data(const char* str, const char* end) const
  {
    if (mCharset->state & MY_CS_NOPAD)
      return str < end;
    return str + mCharset->scan(str, end, MY_SEQ_SPACES) < end;
  }
  bool like(bool neg, const utils::ConstString& subject, const utils::ConstString& pattern) const
  {
    bool res = !mCharset->wildcmp(subject.str(), subject.end(), pattern.str(), pattern.end(), '\\', '_', '%');
    return neg ? !res : res;
  }
};

class CollationAwareHasher : public Charset
{
 public:
  CollationAwareHasher(const Charset& cs) : Charset(cs)
  {
  }
  inline uint32_t operator()(const std::string& s) const
  {
    return operator()(s.data(), s.length());
  }
  inline uint32_t operator()(const char* data, uint64_t len) const
  {
    return Charset::hash(data, len);
  }
};

class CollationAwareComparator : public Charset
{
 public:
  CollationAwareComparator(const Charset& cs) : Charset(cs)
  {
  }
  bool operator()(const std::string& str1, const std::string& str2) const
  {
    return Charset::eq(str1, str2);
  }
};

}  // end of namespace datatypes

#endif
