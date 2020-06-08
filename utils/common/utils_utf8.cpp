/* Copyright (C) 2020 MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

#include "utils_utf8.h"
#include "collation.h"

namespace utf8
{

/*
 * mcs_strcoll
*/
int mcs_strcoll(const char* str1, const char* str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncoll(str1, strlen(str1), str2, strlen(str2));
}

int mcs_strcoll(const char* str1, const uint32_t l1, const char* str2, const uint32_t l2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncoll(str1, l1, str2, l2);
}

int mcs_strcoll(const std::string* str1, const std::string* str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncoll(str1->c_str(), str1->length(), str2->c_str(), str2->length());
}

int mcs_strcoll(const std::string& str1, const std::string& str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncoll(str1.c_str(), str1.length(), str2.c_str(), str2.length());
}

/*
 * mcs_strcollsp
*/
int mcs_strcollsp(const char* str1, const char* str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncollsp(str1, strlen(str1), str2, strlen(str2));
}

int mcs_strcollsp(const char* str1, uint32_t l1, const char* str2, const uint32_t l2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncollsp(str1, l1, str2, l2);
}

int mcs_strcollsp(const std::string* str1, const std::string* str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncollsp(str1->c_str(), str1->length(), str2->c_str(), str2->length());
}
    
int mcs_strcollsp(const std::string& str1, const std::string& str2, const uint32_t charsetNumber)
{
    const CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
    return cs->strnncollsp(str1.c_str(), str1.length(), str2.c_str(), str2.length());
}

} //namespace utf8

