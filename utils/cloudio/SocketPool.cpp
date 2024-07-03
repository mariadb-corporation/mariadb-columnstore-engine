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

#include "SocketPool.h"
#include "configcpp.h"
#include "logger.h"
#include "messageFormat.h"

#include <sys/socket.h>
#include <sys/un.h>

using namespace std;

namespace
{
void log(logging::LOG_TYPE whichLogFile, const string& msg)
{
  logging::Logger logger(12);  // 12 = configcpp
  logger.logMessage(whichLogFile, msg, logging::LoggingID(12));
}

}  // namespace

namespace idbdatafile
{
SocketPool::SocketPool()
{
  config::Config* config =
      config::Config::makeConfig();  // the most 'config' ever put into a single line of code?
  string stmp;
  int64_t itmp = 0;

  try
  {
    stmp = config->getConfig("StorageManager", "MaxSockets");
    itmp = strtol(stmp.c_str(), NULL, 10);
    if (itmp > 500 || itmp < 1)
    {
      string errmsg =
          "SocketPool(): Got a bad value '" + stmp + "' for StorageManager/MaxSockets.  Range is 1-500.";
      log(logging::LOG_TYPE_CRITICAL, errmsg);
      throw runtime_error(errmsg);
    }
    maxSockets = itmp;
  }
  catch (exception& e)
  {
    ostringstream os;
    os << "SocketPool(): Using default of " << defaultSockets << ".";
    log(logging::LOG_TYPE_CRITICAL, os.str());
    maxSockets = defaultSockets;
  }
}

SocketPool::~SocketPool()
{
  boost::mutex::scoped_lock lock(mutex);

  for (uint i = 0; i < allSockets.size(); i++)
    ::close(allSockets[i]);
}

#define sm_check_error                                                                  \
  if (err < 0)                                                                          \
  {                                                                                     \
    char _smbuf[80];                                                                    \
    int l_errno = errno;                                                                \
    log(logging::LOG_TYPE_ERROR,                                                        \
        string("SocketPool: got a network error: ") + strerror_r(l_errno, _smbuf, 80)); \
    remoteClosed(sock);                                                                 \
    return -1;                                                                          \
  }

int SocketPool::send_recv(messageqcpp::ByteStream& in, messageqcpp::ByteStream* out)
{
  uint count = 0;
  uint length = in.length();
  int sock = -1;
  const uint8_t* inbuf = in.buf();
  ssize_t err = 0;

retry:
  int retries = 0;
  while (sock < 0)
  {
    sock = getSocket();
    if (sock < 0)
    {
      if (++retries < 10)
      {
        // log(logging::LOG_TYPE_ERROR, "SocketPool::send_recv(): retrying in 5 sec...");
        sleep(1);
      }
      else
      {
        errno = ECONNREFUSED;
        return -1;
      }
    }
  }

  storagemanager::sm_msg_header hdr;
  hdr.type = storagemanager::SM_MSG_START;
  hdr.payloadLen = length;
  hdr.flags = 0;
  // cout << "SP sending msg on sock " << sock << " with length = " << length << endl;
  err = ::write(sock, &hdr, sizeof(hdr));
  if (err < 0 && errno == EPIPE)
  {
    log(logging::LOG_TYPE_WARNING, "SocketPool: remote connection is closed, getting a new one");
    remoteClosed(sock);
    sock = -1;
    goto retry;
  }
  sm_check_error;
  while (count < length)
  {
    err = ::write(sock, &inbuf[count], length - count);
    sm_check_error;
    count += err;
    in.advance(err);
  }
  // cout << "SP sent msg with length = " << length << endl;

  out->restart();
  uint8_t* outbuf;
  uint8_t window[8192];
  uint remainingBytes = 0;
  uint i;
  storagemanager::sm_msg_header* resp = NULL;

  while (1)
  {
    // cout << "SP receiving msg on sock " << sock << endl;
    // here remainingBytes means the # of bytes from the previous message
    err = ::read(sock, &window[remainingBytes], 8192 - remainingBytes);
    sm_check_error;
    if (err == 0)
    {
      remoteClosed(sock);
      // TODO, a retry loop
      return -1;
    }
    uint endOfData = remainingBytes + err;  // for clarity

    // scan for the 8-byte header.  If it is fragmented, move the fragment to the front of the buffer
    // for the next iteration to handle.

    if (endOfData < storagemanager::SM_HEADER_LEN)
    {
      remainingBytes = endOfData;
      continue;
    }

    for (i = 0; i <= endOfData - storagemanager::SM_HEADER_LEN; i++)
    {
      if (*((uint*)&window[i]) == storagemanager::SM_MSG_START)
      {
        resp = (storagemanager::sm_msg_header*)&window[i];
        break;
      }
    }

    if (resp == NULL)  // didn't find the header yet
    {
      remainingBytes = endOfData - i;
      memmove(window, &window[i], remainingBytes);
    }
    else
    {
      // i == first byte of the header here
      // copy the payload fragment we got into the output bytestream
      uint startOfPayload = i + storagemanager::SM_HEADER_LEN;  // for clarity
      out->needAtLeast(resp->payloadLen);
      outbuf = out->getInputPtr();
      if (resp->payloadLen < endOfData - startOfPayload)
        cout << "SocketPool: warning!  Probably got a bad length field!  payload length = "
             << resp->payloadLen << " endOfData = " << endOfData << " startOfPayload = " << startOfPayload
             << endl;
      memcpy(outbuf, &window[startOfPayload], endOfData - startOfPayload);
      remainingBytes = resp->payloadLen -
                       (endOfData - startOfPayload);  // remainingBytes is now the # of bytes left to read
      out->advanceInputPtr(endOfData - startOfPayload);
      break;  // done looking for the header, can fill the output buffer directly now.
    }
  }

  // read the rest of the payload directly into the output bytestream
  while (remainingBytes > 0)
  {
    err = ::read(sock, &outbuf[resp->payloadLen - remainingBytes], remainingBytes);
    sm_check_error;
    remainingBytes -= err;
    out->advanceInputPtr(err);
  }
  returnSocket(sock);
  return 0;
}

int SocketPool::getSocket()
{
  boost::mutex::scoped_lock lock(mutex);
  int clientSocket;

  if (freeSockets.size() == 0 && allSockets.size() < maxSockets)
  {
    // make a new connection
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(&addr.sun_path[1], &storagemanager::socket_name[1]);  // first char is null...
    clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
    int err = ::connect(clientSocket, (const struct sockaddr*)&addr, sizeof(addr));
    if (err >= 0)
      allSockets.push_back(clientSocket);
    else
    {
      int saved_errno = errno;
      ostringstream os;
      char buf[80];
      os << "SocketPool::getSocket() failed to connect; got '" << strerror_r(saved_errno, buf, 80) << "'";
      cout << os.str() << endl;
      log(logging::LOG_TYPE_ERROR, os.str());
      close(clientSocket);
      errno = saved_errno;
      return -1;
    }
    return clientSocket;
  }

  // wait for a socket to become free
  while (freeSockets.size() == 0)
    socketAvailable.wait(lock);

  clientSocket = freeSockets.front();
  freeSockets.pop_front();
  return clientSocket;
}

void SocketPool::returnSocket(const int sock)
{
  boost::mutex::scoped_lock lock(mutex);
  // cout << "returning socket " << sock << endl;
  freeSockets.push_back(sock);
  socketAvailable.notify_one();
}

void SocketPool::remoteClosed(const int sock)
{
  boost::mutex::scoped_lock lock(mutex);
  // cout << "closing socket " << sock << endl;
  ::close(sock);
  for (vector<int>::iterator i = allSockets.begin(); i != allSockets.end(); ++i)
    if (*i == sock)
    {
      allSockets.erase(i, i + 1);
      break;
    }
}

}  // namespace idbdatafile
