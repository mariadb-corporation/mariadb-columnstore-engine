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
#ifndef MCS_CHARSET_H_INCLUDED
#define MCS_CHARSET_H_INCLUDED

// Because including my_sys.h in a Columnstore header causes too many conflicts
struct charset_info_st;
extern "C" struct charset_info_st my_charset_latin1;
typedef const struct charset_info_st CHARSET_INFO;


namespace datatypes
{


class Charset
{
protected:
   CHARSET_INFO *mCharset;

public:
   Charset()
    :mCharset(&my_charset_latin1)
   { }
   Charset(CHARSET_INFO *cs)
    :mCharset(cs)
   { }
   void setCharset(const Charset &rhs)
   {
     mCharset = rhs.getCharset();
   }
   void setCharset(CHARSET_INFO *cs)
   {
     mCharset = cs;
   }
   CHARSET_INFO *getCharset() const
   {
     return mCharset;
   }
};


} // end of namespace datatypes

#endif // MCS_CHARSET_H_INCLUDED
