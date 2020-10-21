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

/* Implementing common regular expression pattern matching across platforms. */

/** @file */

#ifndef REGEX_PORT_H_
#define REGEX_PORT_H_

#if defined(WITH_LIBRE2)
#   include <re2/re2.h>
#else
#   ifdef __linux__
#       define WITH_POSIX_REGEX
#   endif

// these regex libraries for fallback if RE2 fails.
#   ifdef WITH_POSIX_REGEX
#       include <regex.h>
#   else
#       include <boost/regex.hpp>
#   endif
#endif


namespace utils {


// There are three versions of same interface for different variants
// of underlying regex engines.
//
// This way they are clear to read, write and follow. Otherwise they were morphing
// into unwieldy mess of code and conditional compilation.

#if defined(WITH_LIBRE2)
struct mcs_regex_t
{
private:

    re2::RE2* regex; // we have no assignment operators and need to construct object dynamically.
public:
    mcs_regex_t() : regex(0) { }
    ~mcs_regex_t()
    {
        if (regex)
	{
            delete regex;
        }
    }

    bool isLike(const char* str) const
    {
	return re2::RE2::FullMatch(str, *regex);
    }
    bool isLikeWithLength(const unsigned char* str, size_t len) const
    {
        re2::StringPiece wrapped_str(reinterpret_cast<const char*>(str), len);
	return re2::RE2::FullMatch(wrapped_str, *regex);
    }
    bool matchSubstring(const char* str) const
    {
	return re2::RE2::PartialMatch(str, *regex);
    }

    void compile(const char* str)
    {
        regex = new re2::RE2(str);
    }

    bool notInitialized() const
    {
        return !regex;
    }

    void reset()
    {
        if (regex) {
            delete regex;
	    regex = 0;
	}
    }
};
#elif defined(WITH_POSIX_REGEX)
struct mcs_regex_t
{
private:

    bool used; // this member is associated with POSIX regex field @regex@.
    regex_t regex;
public:
    mcs_regex_t() : used(false) { }
    ~mcs_regex_t()
    {
        if (used)
            regfree(&regex);
    }

    bool isLike(const char* str) const
    {
        return (regexec(&regex, str, 0, NULL, 0) == 0);
    }
    bool isLikeWithLength(const unsigned char* str, size_t len) const
    {
        std::string strbuf(reinterpret_cast<const char*>(str), len);

        return (regexec(&regex, strbuf.c_str(), 0, NULL, 0) == 0);
    }
    bool matchSubstring(const char* str) const
    {
	return regexec(&regex, str, 0, NULL, 0) == 0;
    }

    void compile(const char* str)
    {
	used = true;
        regcomp(&regex, str, REG_NOSUB | REG_EXTENDED);
    }

    bool notInitialized() const
    {
        return !used;
    }

    void reset()
    {
        if (used) {
            regfree(&regex);
	}
        used = false;
    }
};
#else // Boost::regex.
struct mcs_regex_t
{
private:

    bool used; // this member is associated with POSIX regex field @regex@.
    boost::regex regex;
public:
    mcs_regex_t() : used(false) { }
    ~mcs_regex_t() { }

    bool isLike(const char* str) const
    {
        return regex_match(str, regex);
    }
    bool isLikeWithLength(const unsigned char* str, size_t len) const
    {
        /* Note, the passed-in pointers are effectively begin() and end() iterators */
        return regex_match(str, str + len, regex);
    }
    bool matchSubstring(const char* str) const
    {
	return regex_search(str, regex);
    }

    void compile(const char* str)
    {
	used = true;
        regex = str;
    }

    bool notInitialized() const
    {
        return !used;
    }

    void reset()
    {
        used = false;
    }
};
#endif

} // namespace utils

#endif /* REGEX_PORT_H_ */

