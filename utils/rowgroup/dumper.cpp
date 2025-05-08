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

#include <fcntl.h>
#include <sys/stat.h>
#include <cassert>
#include <errno.h>

#include "dumper.h"

namespace common
{
std::string errorString(int errNo)
{
  char tmp[1024];
  auto* buf = strerror_r(errNo, tmp, sizeof(tmp));
  return {buf};
}
}  // namespace common

namespace rowgroup
{
int Dumper::write(const std::string& fname, const char* buf, size_t sz)
{
  if (sz == 0)
    return 0;

  int fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (UNLIKELY(fd < 0))
    return errno;

  const char* tmpbuf;
  if (fCompressor)
  {
    auto len = fCompressor->maxCompressedSize(sz);
    checkBuffer(len);
    fCompressor->compress(buf, sz, fTmpBuf.data(), &len);
    tmpbuf = fTmpBuf.data();
    sz = len;
  }
  else
  {
    tmpbuf = buf;
  }

  auto to_write = sz;
  int ret = 0;
  while (to_write > 0)
  {
    auto r = ::write(fd, tmpbuf + sz - to_write, to_write);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      ret = errno;
      close(fd);
      return ret;
    }
    assert(size_t(r) <= to_write);
    to_write -= r;
  }

  close(fd);
  return ret;
}

int Dumper::read(const std::string& fname, std::vector<char>& buf)
{
  int fd = open(fname.c_str(), O_RDONLY);
  if (UNLIKELY(fd < 0))
    return errno;

  struct stat st{};
  fstat(fd, &st);
  size_t sz = st.st_size;
  std::vector<char>* tmpbuf;
  if (fCompressor)
  {
    tmpbuf = &fTmpBuf;
    checkBuffer(sz);
  }
  else
  {
    tmpbuf = &buf;
    buf.resize(sz);
  }

  auto to_read = sz;
  int ret = 0;
  while (to_read > 0)
  {
    auto r = ::read(fd, tmpbuf->data() + sz - to_read, to_read);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      ret = errno;
      close(fd);
      return ret;
    }

    assert(size_t(r) <= to_read);
    to_read -= r;
  }

  if (fCompressor)
  {
    size_t len;
    if (!fCompressor->getUncompressedSize(tmpbuf->data(), sz, &len))
    {
      ret = EPROTO;
      close(fd);
      return ret;
    }

    buf.resize(len);
    fCompressor->uncompress(tmpbuf->data(), sz, buf.data(), &len);
  }

  close(fd);
  return ret;
}

size_t Dumper::size() const
{
  return fTmpBuf.size();
}

void Dumper::checkBuffer(size_t len)
{
  if (fTmpBuf.size() < len)
  {
    size_t newtmpsz = (len + 8191) / 8192 * 8192;
    std::vector<char> tmpvec(newtmpsz);
    // WIP needs OOM check
    fMM->acquire(newtmpsz - fTmpBuf.size());
    fTmpBuf.swap(tmpvec);
  }
}

std::string RGDumper::makeRGFilename(uint64_t rgid, const uint16_t generation) const
{
  char buf[PATH_MAX];
  snprintf(buf, sizeof(buf), "%s/p%u-t%ld-g%u-rg%lu", fTmpDir.c_str(), getpid(),
           fUniqId, generation, rgid);
  return buf;
}

void RGDumper::loadRG(uint64_t rgid, const uint16_t generation, RowGroup& fRowGroupOut,
                      std::unique_ptr<RGData>& rgdata, bool unlinkDump)
{
  auto fname = makeRGFilename(rgid, generation);

  std::vector<char> data;
  int errNo;
  if ((errNo = read(fname, data)) != 0)
  {
    unlink(fname.c_str());
    // WIP replace errorcodes
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR,
                                                                         common::errorString(errNo)),
                             logging::ERR_DISKAGG_FILEIO_ERROR);
  }

  messageqcpp::ByteStream bs(reinterpret_cast<uint8_t*>(data.data()), data.size());

  if (unlinkDump)
    unlink(fname.c_str());
  rgdata.reset(new RGData());
  rgdata->deserialize(bs, rowgroup::rgCommonSize);
  assert(bs.length() == 0);

  fRowGroupOut.setData(rgdata.get());
  auto memSz = fRowGroupOut.getSizeWithStrings();

  if (!fMM->acquire(memSz))
  {
    // WIP replace errorcodes
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
}

void RGDumper::saveRG(uint64_t rgid, const uint16_t generation, RowGroup& fRowGroupOut, RGData* rgdata)
{
  messageqcpp::ByteStream bs;
  fRowGroupOut.setData(rgdata);
  rgdata->serialize(bs, fRowGroupOut.getDataSize());

  int errNo;
  auto name = makeRGFilename(rgid, generation);
  std::cout << "RGDumper::saveRG  " << name << std::endl;
  if ((errNo = write(makeRGFilename(rgid, generation), (char*)bs.buf(), bs.length())) != 0)
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR,
                                                                         common::errorString(errNo)),
                             logging::ERR_DISKAGG_FILEIO_ERROR);
  }
}

}  // namespace rowgroup
