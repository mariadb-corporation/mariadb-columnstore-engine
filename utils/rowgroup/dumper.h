/* Copyright (C) 2025 MariaDB Corporation

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

#pragma once

#include <string>
#include <memory>
#include "idbcompress.h"
#include "memmanager.h"
#include "rowgroup.h"

namespace common
{
  std::string errorString(int errNo);
}

namespace rowgroup
{

class Dumper
{
 public:
  Dumper(const compress::CompressInterface* comp, MemManager* mm);

  int write(const std::string& fname, const char* buf, size_t sz);
  int read(const std::string& fname, std::vector<char>& buf);
  size_t size() const;

 private:
  void checkBuffer(size_t len);

 protected:
  const compress::CompressInterface* fCompressor;
  std::unique_ptr<MemManager> fMM;
  std::vector<char> fTmpBuf;
};

class RGDumper: protected Dumper
{
 public:
  RGDumper(const compress::CompressInterface* comp, MemManager* mm, const std::string& tmpDir, const uint64_t uniqId) : Dumper(comp, mm), fTmpDir(tmpDir) { }
  void loadRG(uint64_t rgid, const uint16_t generation, RowGroup& fRowGroupOut, std::unique_ptr<RGData>& rgdata, bool unlinkDump = false);
  void saveRG(uint64_t rgid, const uint16_t generation, RowGroup& fRowGroupOut, RGData* rgdata);

  std::string makeRGFilename(uint64_t rgid, const uint16_t generation) const;
private:
  std::string fTmpDir;
  uint64_t fUniqId;
};

}