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
 *   $Id: inetstreamsocket.h 3632 2013-03-13 18:08:46Z pleblanc $
 *
 *
 ***********************************************************************/
/** @file */
#pragma once

#include <ctime>
#include <unistd.h>
#include <netinet/in.h>
#include <cstring>

#include "socket.h"
#include "socketparms.h"
#include "bytestream.h"

#define EXPORT

class MessageQTestSuite;

namespace messageqcpp
{
class IOSocket;

/// random # marking the beginning of a ByteStream in the stream
const uint32_t BYTESTREAM_MAGIC = 0x14fbc137;
const uint32_t COMPRESSED_BYTESTREAM_MAGIC = 0x14fbc138;

/** An Inet Stream Socket
 *
 */
class InetStreamSocket : public Socket
{
 public:
  /** ctor
   *
   */
  explicit InetStreamSocket(size_t blocksize = ByteStream::BlockSize);

  /** dtor
   *
   */
  ~InetStreamSocket() override;

  /** copy ctor
   *
   */
  InetStreamSocket(const InetStreamSocket& rhs);

  /** assign op
   *
   */
  InetStreamSocket& operator=(const InetStreamSocket& rhs);

  /** fSocket mutator
   *
   */
  inline void socketParms(const SocketParms& socket) override;

  /** fSocket accessor
   *
   */
  inline const SocketParms socketParms() const override;

  /** sockaddr mutator
   *
   */
  inline void sa(const sockaddr* sa) override;

  /** call socket() to get a sd
   *
   */
  void open() override;

  /** close the sd
   *
   */
  void close() override;

  /** test if this socket is open
   *
   */
  inline bool isOpen() const override;

  /** read a message from the socket
   *
   * wait for and return a message from the socket. The deafult timeout waits forever. Note that
   * eventhough struct timespec has nanosecond resolution, this method only has milisecond resolution.
   * @warning If you specify a timeout, the stream can be corrupted in certain
   * extreme circumstances.  The circumstance: receiving a portion of the message
   * followed by a timeout.  If the rest of the message is ever received, it
   * will be misinterpreted by the following read().  Symptom:  The caller will
   * receive an incomplete ByteStream
   * (do try-catch around all ">>" operations to detect underflow).  Mitigation:
   * the caller should not perform another read().  Caller should close the connection.
   * The behavior will be unpredictable and possibly fatal.
   * @note A fix is being reviewed but this is low-priority.
   */
  const SBS read(const struct timespec* timeout = nullptr, bool* isTimeOut = nullptr,
                 Stats* stats = nullptr) const override;

  /** write a message to the socket
   *
   * write a message to the socket
   */
  void write(const ByteStream& msg, Stats* stats = nullptr) override;
  void write_raw(const ByteStream& msg, Stats* stats = nullptr) const override;

  /** this version of write takes ownership of the bytestream
   */
  void write(SBS msg, Stats* stats = nullptr) override;

  /** bind to a port
   *
   */
  void bind(const sockaddr* serv_addr) override;

  /** listen for connections
   *
   */
  void listen(int backlog = 5) override;

  /** return an (accepted) IOSocket ready for I/O
   *
   */
  const IOSocket accept(const struct timespec* timeout = nullptr) override;

  /** connect to a server socket
   *
   */
  void connect(const sockaddr* serv_addr) override;

  /** dynamically allocate a copy of this object
   *
   */
  Socket* clone() const override;

  /** get a string rep of the object
   *
   */
  virtual const std::string toString() const;

  /** set the connection timeout (in ms)
   *
   */
  void connectionTimeout(const struct ::timespec* timeout) override
  {
    if (timeout)
      fConnectionTimeout = *timeout;
  }

  /** set the connection protocol to be synchronous
   *
   */
  void syncProto(bool use) override
  {
    fSyncProto = use;
  }

  int getConnectionNum() const override
  {
    return fSocketParms.sd();
  }

  /* The caller needs to know when/if the remote closes the connection or sends data.
   * Returns 0 on timeout, 1 if there is data to read, 2 if the connection was dropped.
   * On error 3 is returned.
   */
  static int pollConnection(int connectionNum, long msecs);

  /** return the address as a string
   *
   */
  const std::string addr2String() const override;

  /** compare 2 addresses
   *
   */
  bool isSameAddr(const Socket* rhs) const override;
  bool isSameAddr(const struct in_addr& ipv4Addr) const override;

  /** ping an ip address
   *
   */
  EXPORT static int ping(const std::string& ipaddr, const struct timespec* timeout = nullptr);

  // Check if we are still connected
  bool isConnected() const override;

  // Check if the socket still has data pending

  bool hasData() const override;

  /*
   * allow test suite access to private data for OOB test
   */
  friend class ::MessageQTestSuite;

 protected:
  static const int KERR_ERESTARTSYS = 512;

  void logIoError(const char* errMsg, int errNum) const;

  /** Empty the stream up to the beginning of the next ByteStream.
   *
   * Reads until the beginning of the next ByteStream is found.
   * @param msecs An optional timeout value.
   * @param residual Pass in an array of at least 8 bytes, on return it will contain
   * the first bytes of the stream.
   * @param reslen On return, it will contain the # of bytes in residual.
   * @return true if the next byte in the stream is the beginning of a ByteStream,
   * false otherwise.
   */
  virtual bool readToMagic(long msecs, bool* isTimeOut, Stats* stats) const;

  void do_write(const ByteStream& msg, uint32_t magic, Stats* stats = nullptr) const;
  ssize_t written(int fd, const uint8_t* ptr, size_t nbytes) const;
  bool readFixedSizeData(struct pollfd* pfd, uint8_t* buffer, size_t numberOfBytes,
                         const struct ::timespec* timeout, bool* isTimeOut, Stats* stats, int64_t msec) const;

  SocketParms fSocketParms;  /// The socket parms
  size_t fBlocksize;
  sockaddr_in fSa;

  // how long to wait for a connect() call to complete (in ms)
  struct ::timespec fConnectionTimeout;

  // use sync proto
  bool fSyncProto;

  /// The buffer used to scan for the ByteStream magic in the stream.
  mutable uint32_t fMagicBuffer;

 private:
  void doCopy(const InetStreamSocket& rhs);
};

inline bool InetStreamSocket::isOpen() const
{
  return (fSocketParms.sd() >= 0);
}
inline const SocketParms InetStreamSocket::socketParms() const
{
  return fSocketParms;
}
inline void InetStreamSocket::socketParms(const SocketParms& socketParms)
{
  fSocketParms = socketParms;
}
inline void InetStreamSocket::sa(const sockaddr* sa)
{
  memcpy(&fSa, sa, sizeof(sockaddr_in));
}

/**
 * stream an InetStreamSocket rep to any ostream
 */
inline std::ostream& operator<<(std::ostream& os, const InetStreamSocket& rhs)
{
  os << rhs.toString();
  return os;
}

}  // namespace messageqcpp

#undef EXPORT
