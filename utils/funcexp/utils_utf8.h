/* Copyright (C) 2014 InfiniDB, Inc.
 * Copyright (C) 2016 MariaDB Corporation.

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

//  $Id$


#ifndef _UTILS_UTF8_H_
#define _UTILS_UTF8_H_



#include <string>
#if defined(_MSC_VER)
#include <malloc.h>
#include <windows.h>
#elif defined(__FreeBSD__)
//#include <cstdlib>
#else
#include <alloca.h>
#endif
#include <cstdlib>

#include <clocale>
#include "liboamcpp.h"

/** @file */

namespace funcexp
{
namespace utf8
{
extern bool JPcodePoint;		// code point ordering (Japanese UTF) flag, used in idb_strcoll

const int MAX_UTF8_BYTES_PER_CHAR = 4;

// A global loc object so we don't construct one at every compare
extern std::locale loc;
// Is there a way to construct a global reference to a facet?
// const std::collate<char>& coll = std::use_facet<std::collate<char> >(loc);

//Infinidb version of strlocale  BUG 5362
//set System Locale "C" by default
//return the system Locale currently set in from Columnstore.xml
inline
std::string idb_setlocale()
{
    // get and set locale language
    std::string systemLang("C");
    oam::Oam oam;
    static bool loggedMsg = false;

    try
    {
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch (...)
    {
        systemLang = "C";
    }

    char* pLoc = setlocale(LC_ALL, systemLang.c_str());

    if (pLoc == NULL)
    {
        try
        {
            if (!loggedMsg)
            {
                //send alarm
                alarmmanager::ALARMManager alarmMgr;
                std::string alarmItem = "system";
                alarmMgr.sendAlarmReport(alarmItem.c_str(), oam::INVALID_LOCALE, alarmmanager::SET);
                
                // Log one line
                logging::LoggingID lid(17);  // ProcessManager -- probably the only one to find this for now
                logging::MessageLog ml(lid);
                logging::Message msg(1);
                logging::Message::Args args;
                args.add("Failed to set locale ");
                args.add(systemLang.c_str());
                args.add(": Setting to 'C'. Critical alarm generated");
                msg.format( args );
                ml.logErrorMessage(msg);
                
                loggedMsg = true;
            }
            systemLang = "C";
        }
        catch (...)
        {
            // Ignoring for time being.
        }
    }
    else
    {
        try
        {
            //send alarm
            alarmmanager::ALARMManager alarmMgr;
            std::string alarmItem = "system";
            alarmMgr.sendAlarmReport(alarmItem.c_str(), oam::INVALID_LOCALE, alarmmanager::CLEAR);
        }
        catch (...)
        {
            // Ignoring for time being.
        }

    }

    printf ("Locale is : %s\n", systemLang.c_str() );

    //BUG 2991
    setlocale(LC_NUMERIC, "C");

    if (systemLang.find("ja_JP") != std::string::npos)
        JPcodePoint = true;

    // MCOL-1559 Save off the locale to save runtime cpus
    std::locale localloc(systemLang.c_str());
    loc = localloc;

    return systemLang;
}

// Infinidb version of strcoll.  BUG 5362
// strcoll() comparison while ja_JP.utf8 does not give correct results.
// For correct results strcmp() can be used.
inline
int idb_strcoll(const char* str1, const char* str2)
{
    if (JPcodePoint)
        return strcmp(str1, str2);
    else
        return strcoll(str1, str2);
}

// MCOL-1559 Add a trimmed version of strcoll
// The intent here is to make no copy of the original strings and
// not modify them, so we can't use trim to deal with the spaces.
inline
int idb_strtrimcoll(const std::string& str1, const std::string& str2)
{
    static const std::string whitespaces (" ");
    const char* s1 = str1.c_str();
    const char* s2 = str2.c_str();

    // Set found1 to the last non-whitespace char in str1
    std::size_t found1 = str1.find_last_not_of(whitespaces);
    // Set found2 to the first whitespace char in str2
    std::size_t found2 = str2.find_last_not_of(whitespaces);

     // Are both strings empty or all whitespace?
    if (found1 == std::string::npos && found2 == std::string::npos)
    {
        return 0; // they match
    }
    // If str1 is empty or all spaces
    if (found1 == std::string::npos)
    {
        return -1;
    }
    // If str2 is empty or all spaces
    if (found2 == std::string::npos)
    {
        return 1;
    }

    // found1 and found2 point to the character that is not a space. 
    // compare wants it to point to one past.
    found1 += 1;
    found2 += 1;
    // If no trimming needs doing, then strcoll is faster
    if (found1 == str1.size() && found2 == str2.size())
    {
        return idb_strcoll(s1, s2);
    }
    // Compare the (trimmed) strings
    const std::collate<char>& coll = std::use_facet<std::collate<char> >(loc);
    int rtn = coll.compare(s1, s1+found1, s2, s2+found2);
    return rtn;
}

// BUG 5241
// Infinidb specific mbstowcs(). This will handle both windows and unix platforms
// Params dest and max should have enough length to accomodate NULL
inline
size_t idb_mbstowcs(wchar_t* dest, const char* src, size_t max)
{
#ifdef _MSC_VER
    // 4th param (-1) denotes to convert till hit NULL char
    // if 6th param max = 0, will return the required buffer size
    size_t strwclen = MultiByteToWideChar(CP_UTF8, 0, src, -1, dest, (int)max);
    // decrement the count of NULL; will become -1 on failure
    return --strwclen;

#else
    return mbstowcs(dest, src, max);
#endif
}

// BUG 5241
// Infinidb specific wcstombs(). This will handle both windows and unix platforms
// Params dest and max should have enough length to accomodate NULL
inline
size_t idb_wcstombs(char* dest, const wchar_t* src, size_t max)
{
#ifdef _MSC_VER
    // 4th param (-1) denotes to convert till hit NULL char
    //if 6th param max = 0, will return the required buffer size
    size_t strmblen = WideCharToMultiByte( CP_UTF8, 0, src, -1, dest, (int)max, NULL, NULL);
    // decrement the count of NULL; will become -1 on failure
    return --strmblen;
#else
    return wcstombs(dest, src, max);
#endif
}

// convert UTF-8 string to wstring
inline
std::wstring utf8_to_wstring (const std::string& str)
{
    size_t bufsize = str.length() + 1;

    // Convert to wide characters. Do all further work in wide characters
    wchar_t* wcbuf = new wchar_t[bufsize];
    // Passing +1 so that windows is happy to see extra position to place NULL
    size_t strwclen = idb_mbstowcs(wcbuf, str.c_str(), str.length() + 1);

    // if result is -1 it means bad characters which may happen if locale is wrong.
    // return an empty string
    if ( strwclen == static_cast<size_t>(-1) )
        strwclen = 0;

    std::wstring ret(wcbuf, strwclen);

    delete [] wcbuf;
    return ret;
}


// convert wstring to UTF-8 string
inline
std::string wstring_to_utf8 (const std::wstring& str)
{
    char* outbuf = new char[(str.length() * MAX_UTF8_BYTES_PER_CHAR) + 1];
    // Passing +1 so that windows is happy to see extra position to place NULL
    size_t strmblen = idb_wcstombs(outbuf, str.c_str(), str.length() * MAX_UTF8_BYTES_PER_CHAR + 1);

    // if result is -1 it means bad characters which may happen if locale is wrong.
    // return an empty string
    if ( strmblen == static_cast<size_t>(-1) )
        strmblen = 0;

    std::string ret(outbuf, strmblen);

    delete [] outbuf;
    return ret;
}

inline
uint8_t utf8_truncate_point(const char* input, size_t length)
{
    // Find the beginning of a multibyte char to truncate at and return the
    // number of bytes to truncate
    if (length < 3)
    {
        return 0;
    }

    const unsigned char* b = (const unsigned char*)(input) + length - 3;

    if (b[2] & 0x80)
    {
        // First byte in a new multi-byte sequence
        if (b[2] & 0x40) return 1;
        // 3 byte sequence
        else if ((b[1] & 0xe0) == 0xe0) return 2;
        // 4 byte sequence
        else if ((b[0] & 0xf0) == 0xf0) return 3;
    }

    return 0;
}

} //namespace utf8
} //namespace funcexp

#endif
