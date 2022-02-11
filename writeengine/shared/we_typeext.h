/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2021 MariaDB Corporation

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

// $Id$

/** @file
 *
 * File contains basic typedefs shared externally with other parts of the
 * system.  These types are placed here rather than in we_type.h in order
 * to decouple we_type.h and we_define.h from external components, so that
 * the other libraries won't have to recompile every time we change something
 * in we_type.h and/or we_define.h.
 */

#ifndef _WE_TYPEEXT_H_
#define _WE_TYPEEXT_H_
#include <stdint.h>
#include <sys/types.h>
#include <pwd.h>
#include <sstream>
#include "IDBFileSystem.h"

/** Namespace WriteEngine */
namespace WriteEngine
{
/************************************************************************
 * Type definitions
 ************************************************************************/
typedef uint64_t RID;  // Row ID

/************************************************************************
 * Dictionary related structure
 ************************************************************************/
struct Token
{
  uint64_t op : 10;   // ordinal position within a block
  uint64_t fbo : 36;  // file block number
  uint64_t bc : 18;   // block count
  Token()             // constructor, set to null value
  {
    op = 0x3FE;
    fbo = 0xFFFFFFFFFLL;
    bc = 0x3FFFF;
  }
};

constexpr uid_t UID_NONE = (uid_t)-1;
constexpr gid_t GID_NONE = (gid_t)-1;

class WeUIDGID
{
 public:
  WeUIDGID() : uid(UID_NONE), gid(GID_NONE)
  {
  }
  virtual ~WeUIDGID(){};
  virtual void setUIDGID(const uid_t uid, const gid_t gid);
  void setUIDGID(const WeUIDGID* id);
  bool chownPath(std::ostringstream& error, const std::string& fileName,
                 const idbdatafile::IDBFileSystem& fs) const;
  ;

 private:
  uid_t uid;
  gid_t gid;
};

inline void WeUIDGID::setUIDGID(const uid_t p_uid, const gid_t p_gid)
{
  uid = p_uid;
  gid = p_gid;
}

inline void WeUIDGID::setUIDGID(const WeUIDGID* id)
{
  if (id->uid != UID_NONE)
    *this = *id;
}

inline bool WeUIDGID::chownPath(std::ostringstream& error, const std::string& fileName,
                                const idbdatafile::IDBFileSystem& fs) const
{
  if (uid != UID_NONE)
  {
    int funcErrno = 0;
    if (fs.chown(fileName.c_str(), uid, gid, funcErrno) == -1)
    {
      error << "Error calling chown() with uid " << uid << " and gid " << gid << " with the file " << fileName
            << " with errno " << funcErrno;
      return true;
    }
  }
  return false;
}

}  // namespace WriteEngine

#endif  // _WE_TYPEEXT_H_
