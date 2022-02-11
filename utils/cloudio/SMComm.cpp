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

#include "SMComm.h"
#include "messageFormat.h"

using namespace std;
using namespace messageqcpp;

namespace
{
idbdatafile::SMComm* instance = NULL;
boost::mutex m;
};  // namespace

namespace idbdatafile
{
SMComm* SMComm::get()
{
  if (instance)
    return instance;

  boost::mutex::scoped_lock sl(m);

  if (instance)
    return instance;
  instance = new SMComm();
  return instance;
}

// timesavers
#define common_exit(bs1, bs2, retCode) \
  {                                    \
    int l_errno = errno;               \
    buffers.returnByteStream(bs1);     \
    buffers.returnByteStream(bs2);     \
    errno = l_errno;                   \
    return retCode;                    \
  }

// bs1 is the bytestream ptr with the command to SMComm.
// bs2 is the bytestream pointer with the response from SMComm.
// retCode is the var to store the return code in from the msg.
// returns with the output pointer at the fcn-specific data
#define check_for_error(bs1, bs2, retCode) \
  {                                        \
    int l_errno;                           \
    *bs2 >> retCode;                       \
    if (retCode < 0)                       \
    {                                      \
      *bs2 >> l_errno;                     \
      errno = l_errno;                     \
      common_exit(bs1, bs2, retCode);      \
    }                                      \
    else                                   \
      errno = 0;                           \
  }

SMComm::SMComm()
{
  char buf[4096];
  cwd = ::getcwd(buf, 4096);
}

SMComm::~SMComm()
{
}

string SMComm::getAbsFilename(const string& filename)
{
  if (filename[0] == '/')
    return filename;
  else
    return cwd + '/' + filename;
}

int SMComm::open(const string& filename, const int mode, struct stat* statbuf)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::OPEN << mode << absfilename;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);

  check_for_error(command, response, err);
  memcpy(statbuf, response->buf(), sizeof(*statbuf));
  common_exit(command, response, err);
}

ssize_t SMComm::pread(const string& filename, void* buf, const size_t count, const off_t offset)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::READ << count << offset << absfilename;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);

  memcpy(buf, response->buf(), err);
  common_exit(command, response, err);
}

ssize_t SMComm::pwrite(const string& filename, const void* buf, const size_t count, const off_t offset)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::WRITE << count << offset << absfilename;
  command->needAtLeast(count);
  uint8_t* cmdBuf = command->getInputPtr();
  memcpy(cmdBuf, buf, count);
  command->advanceInputPtr(count);
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

ssize_t SMComm::append(const string& filename, const void* buf, const size_t count)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::APPEND << count << absfilename;
  command->needAtLeast(count);
  uint8_t* cmdBuf = command->getInputPtr();
  memcpy(cmdBuf, buf, count);
  command->advanceInputPtr(count);
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

int SMComm::unlink(const string& filename)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::UNLINK << absfilename;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

int SMComm::stat(const string& filename, struct stat* statbuf)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::STAT << absfilename;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);

  memcpy(statbuf, response->buf(), sizeof(*statbuf));
  common_exit(command, response, err);
}

int SMComm::truncate(const string& filename, const off64_t length)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename(getAbsFilename(filename));

  *command << (uint8_t)storagemanager::TRUNCATE << length << absfilename;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

int SMComm::listDirectory(const string& path, list<string>* entries)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string abspath(getAbsFilename(path));

  *command << (uint8_t)storagemanager::LIST_DIRECTORY << abspath;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);

  uint32_t numElements;
  string stmp;
  entries->clear();
  *response >> numElements;
  while (numElements > 0)
  {
    *response >> stmp;
    entries->push_back(stmp);
    numElements--;
  }
  common_exit(command, response, err);
}

int SMComm::ping()
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;

  *command << (uint8_t)storagemanager::PING;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

int SMComm::sync()
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;

  *command << (uint8_t)storagemanager::SYNC;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

int SMComm::copyFile(const string& file1, const string& file2)
{
  ByteStream* command = buffers.getByteStream();
  ByteStream* response = buffers.getByteStream();
  ssize_t err;
  string absfilename1(getAbsFilename(file1));
  string absfilename2(getAbsFilename(file2));

  *command << (uint8_t)storagemanager::COPY << absfilename1 << absfilename2;
  err = sockets.send_recv(*command, response);
  if (err)
    common_exit(command, response, err);
  check_for_error(command, response, err);
  common_exit(command, response, err);
}

}  // namespace idbdatafile
