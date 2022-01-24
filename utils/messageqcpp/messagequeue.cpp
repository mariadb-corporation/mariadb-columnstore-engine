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
 *   $Id: messagequeue.cpp 3632 2013-03-13 18:08:46Z pleblanc $
 *
 *
 ***********************************************************************/
#include <string>
#include <stdexcept>
#include <sstream>
#include <cstring>
using namespace std;

#include <unistd.h>
#include <sys/types.h>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#include "configcpp.h"
using namespace config;

#include "bytestream.h"
#include "inetstreamsocket.h"
#ifndef SKIP_IDB_COMPRESSION
#include "compressed_iss.h"
#endif
#include "socketclosed.h"

#define MESSAGEQUEUE_DLLEXPORT
#include "messagequeue.h"
#undef MESSAGEQUEUE_DLLEXPORT

namespace messageqcpp
{
// Aux function to try to resolve supplied identifier to fill struct addrinfo.
struct sockaddr* hostnameResolver(const std::string& dnOrIp, const uint16_t port, logging::Logger& aLogger,
                                  struct sockaddr* sockAddrPtr)
{
  struct addrinfo hints;
  struct addrinfo* servinfo;
  int rc = 0;

  memset(&hints, 0, sizeof hints);
  // ATM We support IPv4 only.
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if (!(rc = getaddrinfo(dnOrIp.c_str(), nullptr, &hints, &servinfo)))
  {
    memset(sockAddrPtr, 0, sizeof(*sockAddrPtr));
    sockaddr_in* sinp = reinterpret_cast<sockaddr_in*>(sockAddrPtr);
    *sinp = *reinterpret_cast<sockaddr_in*>(servinfo->ai_addr);
    sinp->sin_port = htons(port);
    freeaddrinfo(servinfo);
  }
  else
  {
    string msg = "messageqcpp::hostnameResolver ";
    msg.append(gai_strerror(rc));
    logging::Message::Args args;
    logging::LoggingID li(31);
    args.add(msg);
    aLogger.logMessage(logging::LOG_TYPE_ERROR, logging::M0000, args, li);
  }
  return sockAddrPtr;
}

void MessageQueueServer::setup(size_t blocksize, int backlog, bool syncProto)
{
  string thisEndPortStr;

  thisEndPortStr = fConfig->getConfig(fThisEnd, "Port");
  uint16_t port;

  if (thisEndPortStr.length() == 0 ||
      (port = static_cast<uint16_t>(strtol(thisEndPortStr.c_str(), 0, 0))) == 0)
  {
    string msg =
        "MessageQueueServer::MessageQueueServer: config error: Invalid/Missing Port "
        "attribute for " +
        fThisEnd;
    throw runtime_error(msg);
  }

  in_addr listenAddr;
  listenAddr.s_addr = INADDR_ANY;
  string listenAddrStr = fConfig->getConfig(fThisEnd, "ListenAddr");

  if (listenAddrStr.length() > 0)
  {
    struct in_addr la;

    if (inet_aton(listenAddrStr.c_str(), &la) != 0)
      listenAddr = la;
  }

  memset(&fServ_addr, 0, sizeof(fServ_addr));
  sockaddr_in* sinp = reinterpret_cast<sockaddr_in*>(&fServ_addr);
  sinp->sin_family = AF_INET;
  sinp->sin_addr.s_addr = listenAddr.s_addr;
  sinp->sin_port = htons(port);

#ifdef SKIP_IDB_COMPRESSION
  fListenSock.setSocketImpl(new InetStreamSocket(blocksize));
#else
  fListenSock.setSocketImpl(new CompressedInetStreamSocket());
#endif
  fListenSock.syncProto(syncProto);
  fListenSock.open();
  fListenSock.bind(&fServ_addr);
  fListenSock.listen(backlog);

#ifdef SKIP_IDB_COMPRESSION
  fClientSock.setSocketImpl(new InetStreamSocket(blocksize));
#else
  fClientSock.setSocketImpl(new CompressedInetStreamSocket());
#endif
  fClientSock.syncProto(syncProto);
}

MessageQueueServer::MessageQueueServer(const string& thisEnd, const string& config, size_t blocksize,
                                       int backlog, bool syncProto)
 : fThisEnd(thisEnd), fConfig(Config::makeConfig(config)), fLogger(31)
{
  setup(blocksize, backlog, syncProto);
}

MessageQueueServer::MessageQueueServer(const string& thisEnd, Config* config, size_t blocksize, int backlog,
                                       bool syncProto)
 : fThisEnd(thisEnd), fConfig(config), fLogger(31)
{
  if (fConfig == 0)
    fConfig = Config::makeConfig();

  setup(blocksize, backlog, syncProto);
}

MessageQueueServer::~MessageQueueServer()
{
  fClientSock.close();
  fListenSock.close();
}

const IOSocket MessageQueueServer::accept(const struct timespec* timeout) const
{
  return fListenSock.accept(timeout);
}

void MessageQueueServer::syncProto(bool use)
{
  fListenSock.syncProto(use);
  fClientSock.syncProto(use);
}

MessageQueueClient::~MessageQueueClient()
{
  fClientSock.close();
}

void MessageQueueClient::shutdown()
{
  fClientSock.close();
}

std::pair<std::string, uint16_t> getAddressAndPort(config::Config* config, const std::string& fOtherEnd)
{
  std::string otherEndDnOrIPStr = config->getConfig(fOtherEnd, "IPAddr");
  std::string otherEndPortStr = config->getConfig(fOtherEnd, "Port");
  uint16_t port =
      otherEndPortStr.length() > 0 ? static_cast<uint16_t>(strtol(otherEndPortStr.c_str(), 0, 0)) : 0;

  if (otherEndDnOrIPStr == "unassigned")
    return {"0.0.0.0", port};

  if (otherEndDnOrIPStr.empty())
    return {"127.0.0.1", port};

  return {otherEndDnOrIPStr, port};
}

void MessageQueueClient::setup(bool syncProto)
{
  auto addressAndPort = getAddressAndPort(fConfig, fOtherEnd);
  if (!addressAndPort.second)
  {
    string msg = "MessageQueueClient::setup(): config error: Invalid/Missing Port attribute";
    throw runtime_error(msg);
  }
#ifdef SKIP_IDB_COMPRESSION
  fClientSock.setSocketImpl(new InetStreamSocket());
#else
  fClientSock.setSocketImpl(new CompressedInetStreamSocket());
#endif
  fClientSock.syncProto(syncProto);
  fClientSock.sa(hostnameResolver(addressAndPort.first, addressAndPort.second, fLogger, &fServ_addr));
}

MessageQueueClient::MessageQueueClient(const string& otherEnd, const string& config, bool syncProto)
 : fOtherEnd(otherEnd), fConfig(Config::makeConfig(config)), fLogger(31), fIsAvailable(true)
{
  setup(syncProto);
}

MessageQueueClient::MessageQueueClient(const string& otherEnd, Config* config, bool syncProto)
 : fOtherEnd(otherEnd), fConfig(config), fLogger(31), fIsAvailable(true)
{
  if (fConfig == 0)
    fConfig = Config::makeConfig();

  setup(syncProto);
}

MessageQueueClient::MessageQueueClient(const string& dnOrIp, uint16_t port, bool syncProto)
 : fLogger(31), fIsAvailable(true)
{
#ifdef SKIP_IDB_COMPRESSION
  fClientSock.setSocketImpl(new InetStreamSocket());
#else
  fClientSock.setSocketImpl(new CompressedInetStreamSocket());
#endif
  fClientSock.syncProto(syncProto);
  fClientSock.sa(hostnameResolver(dnOrIp, port, fLogger, &fServ_addr));
}

const SBS MessageQueueClient::read(const struct timespec* timeout, bool* isTimeOut, Stats* stats) const
{
  if (!fClientSock.isOpen())
  {
    fClientSock.open();

    try
    {
      fClientSock.connect(&fServ_addr);
    }
    catch (...)
    {
      fClientSock.close();
      throw;
    }
  }

  SBS res;

  try
  {
    res = fClientSock.read(timeout, isTimeOut, stats);
  }
  catch (runtime_error& re)
  {
    // This is an I/O error from IOSocket::read()
    //		cerr << "MessageQueueClient::read: close socket for " << re.what() << endl;
    logging::Message::Args args;
    logging::LoggingID li(31);
    args.add("Client read close socket for");
    args.add(re.what());
    fLogger.logMessage(logging::LOG_TYPE_WARNING, logging::M0000, args, li);
    fClientSock.close();
    throw;
  }
  catch (SocketClosed& e)
  {
    //		cerr << "MessageQueueClient::read: close socket for " << e.what() << endl;
    logging::Message::Args args;
    logging::LoggingID li(31);
    args.add("Client read close socket for");
    args.add(e.what());
    fLogger.logMessage(logging::LOG_TYPE_WARNING, logging::M0000, args, li);
    fClientSock.close();
    throw;
  }

  return res;
}

void MessageQueueClient::write(const ByteStream& msg, const struct timespec* timeout, Stats* stats) const
{
  if (!fClientSock.isOpen())
  {
    fClientSock.open();

    try
    {
      fClientSock.connectionTimeout(timeout);
      fClientSock.connect(&fServ_addr);
    }
    catch (...)
    {
      fClientSock.close();
      throw;
    }
  }

  try
  {
    fClientSock.write(msg, stats);
  }
  catch (runtime_error& e)
  {
    try
    {
      ostringstream oss;
      oss << "MessageQueueClient::write: error writing " << msg.length() << " bytes to " << fClientSock
          << ". Socket error was " << e.what() << endl;
      //			cerr << oss.str() << endl;
      logging::Message::Args args;
      logging::LoggingID li(31);
      args.add(oss.str());
      fLogger.logMessage(logging::LOG_TYPE_WARNING, logging::M0000, args, li);
    }
    catch (...)
    {
    }

    fClientSock.close();
    throw;
  }
}

bool MessageQueueClient::connect() const
{
  if (!fClientSock.isOpen())
  {
    fClientSock.open();

    try
    {
      fClientSock.connect(&fServ_addr);
    }
    catch (runtime_error& re)
    {
      string what = re.what();

      if (what.find("Connection refused") != string::npos)
      {
        try
        {
          fClientSock.close();
        }
        catch (...)
        {
        }
      }
      else
        throw;
    }
    catch (...)
    {
      throw;
    }
  }

  return fClientSock.isOpen();
}

}  // namespace messageqcpp
