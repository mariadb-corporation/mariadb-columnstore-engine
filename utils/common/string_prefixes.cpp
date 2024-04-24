/*
   Copyright (C) 2021, 2022 MariaDB Corporation

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

/* handling of the conversion of string prefixes to int64_t for quick range checking */

#include "collation.h"
#include "joblisttypes.h"

#include "string_prefixes.h"

#if 01
#define idblog(x)                                                                       \
  do                                                                                       \
  {                                                                                        \
    {                                                                                      \
      std::ostringstream os;                                                               \
                                                                                           \
      os << __FILE__ << "@" << __LINE__ << ": \'" << x << "\'"; \
      std::cerr << os.str() << std::endl;                                                  \
      logging::MessageLog logger((logging::LoggingID()));                                  \
      logging::Message message;                                                            \
      logging::Message::Args args;                                                         \
                                                                                           \
      args.add(os.str());                                                                  \
      message.format(args);                                                                \
      logger.logErrorMessage(message);                                                     \
    }                                                                                      \
  } while (0)
#else
#define idblog(_)
#endif

// XXX: string (or, actually, a BLOB) with all NUL chars will be encoded into zero. Which corresponds to
//      encoding of empty string.
int64_t encodeStringPrefix(const uint8_t* str, size_t len, datatypes::Charset& cset)
{
  CHARSET_INFO& ci = cset.getCharset();
  bool csHasPad = (ci.mstate & MY_CS_NOPAD) == 0;
  size_t i;
  std::string s((const char*)str, len);
  idblog("cset #" << ci.number << " encoding <<" << s << ">>, " << (csHasPad ? "padded" : "not padded"));
  size_t 
  uint8_t fixedLenPrefix[16];
  memset(fixedLenPrefix, 0, sizeof(fixedLenPrefix));
  if (csHasPad)
  {
    // we have to pad.
    // we pad to 16 bytes just to be safe. I am worrying that we can
    // cross symbol boundaries, so I choose to overshoot.
    const size_t tempN = 16;
    uint8_t tempBuf[tempN*2];
    size_t minLen = len < tempN ? len : tempN;
    for (i = 0; i < minLen; i ++)
    {
      tempBuf[i] = str[i];
    }
    for (i = 0; i < tempN; i++)
    {
      tempBuf[i] = ' '; // XXX: it appears that it is good enough even for binary.
    }
    cset.strnxfrm(fixedLenPrefix, sizeof(fixedLenPrefix), 8, tempBuf, tempN, 0);
  }
  else
  {
    cset.strnxfrm(fixedLenPrefix, sizeof(fixedLenPrefix), 8, str, len, 0);
  }
  int64_t acc = 0;
  for (i = 0; i < 8; i++)
  {
    uint8_t byte = fixedLenPrefix[i];
    acc = (acc << 8) + byte;
  }
  return acc;
}

int64_t encodeStringPrefix_check_null(const uint8_t* str, size_t len, datatypes::Charset& cset)
{
  if (len < 1)
  {
    return joblist::UBIGINTNULL;
  }
  return encodeStringPrefix(str, len, cset);
}
