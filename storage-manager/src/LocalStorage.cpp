/* Copyright (C) 2019 MariaDB Corporation

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

#include <boost/filesystem.hpp>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include "LocalStorage.h"
#include "Config.h"

using namespace std;
namespace bf = boost::filesystem;

namespace storagemanager
{
LocalStorage::LocalStorage()
{
  prefix = Config::get()->getValue("LocalStorage", "path");
  // cout << "LS: got prefix " << prefix << endl;
  if (!bf::is_directory(prefix))
  {
    try
    {
      bf::create_directories(prefix);
    }
    catch (exception& e)
    {
      logger->log(LOG_CRIT, "Failed to create %s, got: %s", prefix.string().c_str(), e.what());
      throw e;
    }
  }
  string stmp = Config::get()->getValue("LocalStorage", "fake_latency");
  if (!stmp.empty() && (stmp[0] == 'Y' || stmp[0] == 'y'))
  {
    fakeLatency = true;
    stmp = Config::get()->getValue("LocalStorage", "max_latency");
    usecLatencyCap = strtoull(stmp.c_str(), NULL, 10);
    if (usecLatencyCap == 0)
    {
      logger->log(LOG_CRIT, "LocalStorage:  bad value for max_latency");
      throw runtime_error("LocalStorage:  bad value for max_latency");
    }
    r_seed = (uint)::time(NULL);
    logger->log(LOG_DEBUG, "LocalStorage:  Will simulate cloud latency of max %llu us", usecLatencyCap);
  }
  else
    fakeLatency = false;

  bytesRead = bytesWritten = 0;
}

LocalStorage::~LocalStorage()
{
}

void LocalStorage::printKPIs() const
{
  cout << "LocalStorage" << endl;
  cout << "\tbytesRead = " << bytesRead << endl;
  cout << "\tbytesWritten = " << bytesWritten << endl;
  CloudStorage::printKPIs();
}

const bf::path& LocalStorage::getPrefix() const
{
  return prefix;
}

inline void LocalStorage::addLatency()
{
  if (fakeLatency)
  {
    uint64_t usec_delay = ((double)rand_r(&r_seed) / (double)RAND_MAX) * usecLatencyCap;
    ::usleep(usec_delay);
  }
}

int LocalStorage::copy(const bf::path& source, const bf::path& dest)
{
  boost::system::error_code err;
  bf::copy_file(source, dest, bf::copy_option::fail_if_exists, err);
  if (err)
  {
    errno = err.value();
    ::unlink(dest.string().c_str());
    return -1;
  }
  return 0;
}

bf::path operator+(const bf::path& p1, const bf::path& p2)
{
  bf::path ret(p1);
  ret /= p2;
  return ret;
}

int LocalStorage::getObject(const string& source, const string& dest, size_t* size)
{
  addLatency();

  int ret = copy(prefix / source, dest);
  if (ret)
    return ret;
  size_t _size = bf::file_size(dest);
  if (size)
    *size = _size;
  bytesRead += _size;
  bytesWritten += _size;
  ++objectsGotten;
  return ret;
}

int LocalStorage::getObject(const std::string& sourceKey, boost::shared_array<uint8_t>* data, size_t* size)
{
  addLatency();

  bf::path source = prefix / sourceKey;
  const char* c_source = source.string().c_str();
  // char buf[80];
  int l_errno;

  int fd = ::open(c_source, O_RDONLY);
  if (fd < 0)
  {
    l_errno = errno;
    // logger->log(LOG_WARNING, "LocalStorage::getObject() failed to open %s, got '%s'", c_source,
    // strerror_r(errno, buf, 80));
    errno = l_errno;
    return fd;
  }

  size_t l_size = bf::file_size(source);
  data->reset(new uint8_t[l_size]);
  size_t count = 0;
  while (count < l_size)
  {
    int err = ::read(fd, &(*data)[count], l_size - count);
    if (err < 0)
    {
      l_errno = errno;
      // logger->log(LOG_WARNING, "LocalStorage::getObject() failed to read %s, got '%s'", c_source,
      // strerror_r(errno, buf, 80));
      close(fd);
      bytesRead += count;
      errno = l_errno;
      return err;
    }
    count += err;
  }
  if (size)
    *size = l_size;
  close(fd);
  bytesRead += l_size;
  ++objectsGotten;
  return 0;
}

int LocalStorage::putObject(const string& source, const string& dest)
{
  addLatency();

  int ret = copy(source, prefix / dest);

  if (ret == 0)
  {
    size_t _size = bf::file_size(source);
    bytesRead += _size;
    bytesWritten += _size;
    ++objectsPut;
  }
  return ret;
}

int LocalStorage::putObject(boost::shared_array<uint8_t> data, size_t len, const string& dest)
{
  addLatency();

  bf::path destPath = prefix / dest;
  const char* c_dest = destPath.string().c_str();
  // char buf[80];
  int l_errno;

  int fd = ::open(c_dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
  {
    l_errno = errno;
    // logger->log(LOG_CRIT, "LocalStorage::putObject(): Failed to open %s, got '%s'", c_dest,
    // strerror_r(errno, buf, 80));
    errno = l_errno;
    return fd;
  }

  size_t count = 0;
  int err;
  while (count < len)
  {
    err = ::write(fd, &data[count], len - count);
    if (err < 0)
    {
      l_errno = errno;
      // logger->log(LOG_CRIT, "LocalStorage::putObject(): Failed to write to %s, got '%s'", c_dest,
      // strerror_r(errno, buf, 80));
      close(fd);
      ::unlink(c_dest);
      errno = l_errno;
      bytesWritten += count;
      return err;
    }
    count += err;
  }
  close(fd);
  bytesWritten += count;
  ++objectsPut;
  return 0;
}

int LocalStorage::copyObject(const string& source, const string& dest)
{
  addLatency();

  int ret = copy(prefix / source, prefix / dest);

  if (ret == 0)
  {
    ++objectsCopied;
    size_t _size = bf::file_size(prefix / source);
    bytesRead += _size;
    bytesWritten += _size;
  }
  return ret;
}

int LocalStorage::deleteObject(const string& key)
{
  addLatency();

  ++objectsDeleted;
  boost::system::error_code err;
  bf::remove(prefix / key, err);
  return 0;
}

int LocalStorage::exists(const std::string& key, bool* out)
{
  addLatency();

  ++existenceChecks;
  *out = bf::exists(prefix / key);
  return 0;
}

}  // namespace storagemanager
