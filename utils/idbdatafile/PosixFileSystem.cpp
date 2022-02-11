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

#include "PosixFileSystem.h"
#include "IDBLogger.h"
#include "idbcompress.h"

#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

using namespace std;

#ifdef _MSC_VER
#include "utils_utf8.h"
#endif

namespace idbdatafile
{
PosixFileSystem::PosixFileSystem() : IDBFileSystem(IDBFileSystem::POSIX)
{
}

PosixFileSystem::~PosixFileSystem()
{
}

int PosixFileSystem::mkdir(const char* pathname)
{
  int ret = 0;
  boost::filesystem::path pathDir(pathname);

  try
  {
    boost::filesystem::create_directories(pathDir);
  }
  catch (std::exception& ex)
  {
    std::ostringstream oss;
    oss << "Failed to create directories: " << pathDir << ", exception: " << ex.what() << endl;
    IDBLogger::syslog(oss.str(), logging::LOG_TYPE_ERROR);
    ret = -1;
  }

  if (IDBLogger::isEnabled())
    IDBLogger::logFSop(POSIX, "mkdir", pathname, this, ret);

  return ret;
}

int PosixFileSystem::remove(const char* pathname)
{
  int ret = 0;
  boost::filesystem::path dirPath(pathname);

  try
  {
    boost::filesystem::remove_all(dirPath);
  }

#ifdef _MSC_VER
  catch (std::exception& ex)
  {
    // FIXME: alas, Windows cannot delete a file that is in use :-(
    std::string reason(ex.what());
    std::string ignore("The directory is not empty");

    if (reason.find(ignore) != std::string::npos)
      (void)0;
    else
      ret = -1;
  }

#endif
  catch (...)
  {
    // TODO Log this
    ret = -1;
  }

  if (IDBLogger::isEnabled())
    IDBLogger::logFSop(POSIX, "remove", pathname, this, ret);

  return ret;
}

int PosixFileSystem::rename(const char* oldpath, const char* newpath)
{
  // should this use Boost??
  int ret = ::rename(oldpath, newpath);
  int savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logFSop2(POSIX, "rename", oldpath, newpath, this, ret);

  errno = savedErrno;
  return ret;
}

off64_t PosixFileSystem::size(const char* path) const
{
  // should this use Boost??
  struct stat statBuf;
  int rc = ::stat(path, &statBuf);
  int savedErrno = errno;
  off64_t ret = ((rc == 0) ? statBuf.st_size : -1);

  if (IDBLogger::isEnabled())
    IDBLogger::logFSop(POSIX, "fs:size", path, this, ret);

  errno = savedErrno;
  return ret;
}

size_t readFillBuffer(idbdatafile::IDBDataFile* pFile, char* buffer, size_t bytesReq)
{
  char* pBuf = buffer;
  ssize_t nBytes;
  size_t bytesToRead = bytesReq;
  size_t totalBytesRead = 0;

  while (1)
  {
    nBytes = pFile->read(pBuf, bytesToRead);

    if (nBytes > 0)
      totalBytesRead += nBytes;
    else
      break;

    if ((size_t)nBytes == bytesToRead)
      break;

    pBuf += nBytes;
    bytesToRead = bytesToRead - (size_t)nBytes;
  }

  return totalBytesRead;
}

off64_t PosixFileSystem::compressedSize(const char* path) const
{
  IDBDataFile* pFile = NULL;
  size_t nBytes;
  off64_t dataSize = 0;

  try
  {
    pFile = IDBDataFile::open(IDBDataFile::BUFFERED, path, "r", 0);

    if (!pFile)
    {
      return -1;
    }

    char hdr1[compress::CompressInterface::HDR_BUF_LEN];
    nBytes = readFillBuffer(pFile, hdr1, compress::CompressInterface::HDR_BUF_LEN);

    if (nBytes != compress::CompressInterface::HDR_BUF_LEN)
    {
      delete pFile;
      return -1;
    }

    // Verify we are a compressed file
    if (compress::CompressInterface::verifyHdr(hdr1) < 0)
    {
      delete pFile;
      return -1;
    }

    int64_t ptrSecSize =
        compress::CompressInterface::getHdrSize(hdr1) - compress::CompressInterface::HDR_BUF_LEN;
    char* hdr2 = new char[ptrSecSize];
    nBytes = readFillBuffer(pFile, hdr2, ptrSecSize);

    if ((int64_t)nBytes != ptrSecSize)
    {
      delete[] hdr2;
      delete pFile;
      return -1;
    }

    compress::CompChunkPtrList chunkPtrs;
    int rc = compress::CompressInterface::getPtrList(hdr2, ptrSecSize, chunkPtrs);
    delete[] hdr2;

    if (rc != 0)
    {
      delete pFile;
      return -1;
    }

    unsigned k = chunkPtrs.size();

    // last header's offset + length will be the data bytes
    if (k < 1)
    {
      delete pFile;
      return -1;
    }

    dataSize = chunkPtrs[k - 1].first + chunkPtrs[k - 1].second;
    delete pFile;
    return dataSize;
  }
  catch (...)
  {
    delete pFile;
    return -1;
  }
}

bool PosixFileSystem::exists(const char* pathname) const
{
  boost::filesystem::path dirPath(pathname);
  return boost::filesystem::exists(dirPath);
}

int PosixFileSystem::listDirectory(const char* pathname, std::list<std::string>& contents) const
{
  int ret = 0;

  // clear the return list
  contents.erase(contents.begin(), contents.end());

  try
  {
    boost::filesystem::path dirPath(pathname);
    boost::filesystem::directory_iterator end_itr;  // create EOD marker

    // Loop through all the files in the specified directory
    for (boost::filesystem::directory_iterator itr(dirPath); itr != end_itr; ++itr)
    {
      contents.push_back(itr->path().filename().generic_string());
    }
  }
  catch (std::exception&)
  {
    ret = -1;
  }

  return ret;
}

bool PosixFileSystem::isDir(const char* pathname) const
{
  boost::filesystem::path dirPath(pathname);
  return boost::filesystem::is_directory(dirPath);
}

int PosixFileSystem::copyFile(const char* srcPath, const char* destPath) const
{
  int ret = 0;

  try
  {
    boost::filesystem::path inPath(srcPath);
    boost::filesystem::path outPath(destPath);

    boost::filesystem::copy_file(inPath, outPath);
  }

#if BOOST_VERSION >= 105200
  catch (boost::filesystem::filesystem_error& ex)
#else
  catch (boost::filesystem::basic_filesystem_error<boost::filesystem::path>& ex)
#endif
  {
    std::ostringstream oss;
    oss << "Failed to copy file: " << srcPath << " to " << destPath << ", exception: " << ex.what() << endl;
    IDBLogger::syslog(oss.str(), logging::LOG_TYPE_ERROR);
    ret = -1;
  }

  if (IDBLogger::isEnabled())
    IDBLogger::logFSop2(POSIX, "copyFile", srcPath, destPath, this, ret);

  return ret;
}

int PosixFileSystem::chown(const char* objectName, const uid_t p_uid, const gid_t p_gid, int& funcErrno) const
{
  int ret = 0;
  errno = 0;
  if ((ret = ::chown(objectName, p_uid, p_gid)))
    funcErrno = errno;
  return ret;
}

}  // namespace idbdatafile
