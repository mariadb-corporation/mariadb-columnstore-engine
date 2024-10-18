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

/***********************************************************************
 *   $Id$
 *
 *
 ***********************************************************************/
#include "bytestream.h"
#include "mcsconfig.h"

#include <stdexcept>
#include <string>
#include <sstream>
#if __FreeBSD__
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#endif
#include <sys/socket.h>
#include <poll.h>
#include <netinet/in.h>
#include <netinet/ip_icmp.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <fcntl.h>

#include "compressed_iss.h"
#include "iosocket.h"
#include "configcpp.h"

using namespace std;
using namespace boost;
using namespace compress;

namespace messageqcpp
{
CompressedInetStreamSocket::CompressedInetStreamSocket()
{
  config::Config* config = config::Config::makeConfig();
  string val;
  string compressionType;

  try
  {
    val = config->getConfig("NetworkCompression", "Enabled");
  }
  catch (...)
  {
  }

  if (val == "" || val == "Y")
    useCompression = true;
  else
    useCompression = false;

  try
  {
    compressionType = config->getConfig("NetworkCompression", "NetworkCompression");
  }
  catch (...)
  {
  }

  auto* compressInterface = compress::getCompressInterfaceByName(compressionType);
  if (!compressInterface)
    compressInterface = new compress::CompressInterfaceSnappy();

  alg.reset(compressInterface);
}

Socket* CompressedInetStreamSocket::clone() const
{
  return new CompressedInetStreamSocket(*this);
}

const SBS CompressedInetStreamSocket::read(const struct timespec* timeout, bool* isTimeOut,
                                           Stats* stats) const
{
  SBS readBS, ret;
  size_t uncompressedSize;

  readBS = InetStreamSocket::read(timeout, isTimeOut, stats);

  if (readBS->length() == 0 || fMagicBuffer == BYTESTREAM_MAGIC)
    return readBS;

  // Read stored len, first 4 bytes.
  uint32_t storedLen = *(uint32_t*)readBS->buf();

  if (!storedLen)
    return SBS(new ByteStream(0));

  uncompressedSize = storedLen;
  ret.reset(new ByteStream(uncompressedSize));

  alg->uncompress((char*)readBS->buf() + HEADER_SIZE, readBS->length() - HEADER_SIZE,
                  (char*)ret->getInputPtr(), &uncompressedSize);

  ret->advanceInputPtr(uncompressedSize);

  return ret;
}

void CompressedInetStreamSocket::write(const ByteStream& msg, Stats* stats)
{
  BSSizeType len = msg.length();

  if (useCompression && (len > 512))
  {
    size_t outLen = alg->maxCompressedSize(len) + HEADER_SIZE;
    ByteStream smsg(outLen);

    alg->compress((char*)msg.buf(), len, (char*)smsg.getInputPtr() + HEADER_SIZE, &outLen);
    // Save original len.
    // !!!
    // !!! Reducing BS size type from 64bit down to 32 and potentially loosing data.
    // !!!
    *(uint32_t*)smsg.getInputPtr() = len;
    smsg.advanceInputPtr(outLen + HEADER_SIZE);

    if (outLen < len)
      do_write(smsg, COMPRESSED_BYTESTREAM_MAGIC, stats);
    else
      InetStreamSocket::write(msg, stats);
  }
  else
    InetStreamSocket::write(msg, stats);
}

void CompressedInetStreamSocket::write(SBS msg, Stats* stats)
{
  write(*msg, stats);
}

/* this was cut & pasted from InetStreamSocket;
 * is there a clean way to wrap ISS::accept()?
 */
const IOSocket CompressedInetStreamSocket::accept(const struct timespec* timeout)
{
  int clientfd;
  long msecs = 0;

  struct pollfd pfd[1];
  pfd[0].fd = socketParms().sd();
  pfd[0].events = POLLIN;

  if (timeout != 0)
  {
    msecs = timeout->tv_sec * 1000 + timeout->tv_nsec / 1000000;

    if (poll(pfd, 1, msecs) != 1 || (pfd[0].revents & POLLIN) == 0 ||
        pfd[0].revents & (POLLERR | POLLHUP | POLLNVAL))
      return IOSocket(new CompressedInetStreamSocket());
  }

  struct sockaddr sa;

  socklen_t sl = sizeof(sa);

  int e;

  do
  {
    clientfd = ::accept(socketParms().sd(), &sa, &sl);
    e = errno;
  } while (clientfd < 0 && (e == EINTR ||
#ifdef ERESTART
                            e == ERESTART ||
#endif
#ifdef ECONNABORTED
                            e == ECONNABORTED ||
#endif
                            false));

  if (clientfd < 0)
  {
    string msg = "CompressedInetStreamSocket::accept: accept() error: ";
    scoped_array<char> buf(new char[80]);
#if STRERROR_R_CHAR_P
    const char* p;

    if ((p = strerror_r(e, buf.get(), 80)) != 0)
      msg += p;

#else
    int p;

    if ((p = strerror_r(e, buf.get(), 80)) == 0)
      msg += buf.get();

#endif
    throw runtime_error(msg);
  }

  if (fSyncProto)
  {
    /* send a byte to artificially synchronize with connect() on the remote */
    char b = 'A';
    int ret;

    ret = ::send(clientfd, &b, 1, 0);
    e = errno;

    if (ret < 0)
    {
      ostringstream os;
      char blah[80];
#if STRERROR_R_CHAR_P
      const char* p;

      if ((p = strerror_r(e, blah, 80)) != 0)
        os << "CompressedInetStreamSocket::accept sync: " << p;

#else
      int p;

      if ((p = strerror_r(e, blah, 80)) == 0)
        os << "CompressedInetStreamSocket::accept sync: " << blah;

#endif
      ::close(clientfd);
      throw runtime_error(os.str());
    }
    else if (ret == 0)
    {
      ::close(clientfd);
      throw runtime_error("CompressedInetStreamSocket::accept sync: got unexpected error code");
    }
  }

  CompressedInetStreamSocket* ciss = new CompressedInetStreamSocket();
  IOSocket ios;
  sockaddr_in* sin = (sockaddr_in*)&sa;

  if ((sin->sin_addr.s_addr == fSa.sin_addr.s_addr) || sin->sin_addr.s_addr == inet_addr("127.0.0.1"))
    ciss->useCompression = false;

  ios.setSocketImpl(ciss);
  SocketParms sp;
  sp = ios.socketParms();
  sp.sd(clientfd);
  ios.socketParms(sp);
  ios.sa(&sa);
  return ios;
}

void CompressedInetStreamSocket::connect(const sockaddr* serv_addr)
{
  sockaddr_in* sin = (sockaddr_in*)serv_addr;

  if (sin->sin_addr.s_addr == fSa.sin_addr.s_addr || sin->sin_addr.s_addr == inet_addr("127.0.0.1"))
    useCompression = false;

  InetStreamSocket::connect(serv_addr);
}

}  // namespace messageqcpp
