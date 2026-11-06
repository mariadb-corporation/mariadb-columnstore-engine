/* Copyright (C) 2026 MariaDB Corporation

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

/* MCOL-6613: the PrimProc connections ExeMgr/DMLProc keep are reconnected
   before a query, so the socket layer gained two controls:
   - autoReconnect(false): read()/write() on a closed MessageQueueClient throw
     SocketClosed instead of quietly opening a new connection; only connect()
     opens it again.
   - tcpConnectTimeout(ms): connect() gives up on the TCP handshake after ms,
     instead of waiting for the kernel's SYN retries (about two minutes).

   Everything runs on loopback. A peer that never answers the handshake is a
   listener whose accept queue is full: Linux drops the SYN, so connect()
   waits exactly as it would for a dead host.
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "bytestream.h"
#include "compressed_iss.h"
#include "inetstreamsocket.h"
#include "iosocket.h"
#include "messagequeue.h"
#include "socketclosed.h"

using namespace messageqcpp;

namespace
{
using Clock = std::chrono::steady_clock;

Socket* newSocketImpl()
{
#ifdef SKIP_IDB_COMPRESSION
  return new InetStreamSocket();
#else
  return new CompressedInetStreamSocket();
#endif
}

sockaddr_in loopback(uint16_t port)
{
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  return sa;
}

uint16_t portOf(int sd)
{
  sockaddr_in sa{};
  socklen_t len = sizeof(sa);
  if (getsockname(sd, reinterpret_cast<sockaddr*>(&sa), &len) != 0)
    return 0;
  return ntohs(sa.sin_port);
}

// A ColumnStore-protocol listener on 127.0.0.1 (port 0 = any free port).
struct Listener
{
  std::unique_ptr<Socket> sock;
  uint16_t port = 0;

  explicit Listener(uint16_t wanted = 0, Socket* impl = newSocketImpl()) : sock(impl)
  {
    sock->open();
    sockaddr_in sa = loopback(wanted);
    sock->bind(reinterpret_cast<sockaddr*>(&sa));
    sock->listen(5);
    port = portOf(sock->socketParms().sd());
  }
  ~Listener()
  {
    sock->close();
  }
  // With the sync protocol connect() waits for the byte accept() sends, so
  // accept on another thread while the client connects.
  std::future<IOSocket> acceptAsync()
  {
    return std::async(std::launch::async,
                      [this]
                      {
                        timespec t{5, 0};
                        return sock->accept(&t);
                      });
  }
};

// Runs fn on its own thread. If it hasn't returned within the limit, the test
// can't finish (the thread is stuck in connect()), so fail and exit rather
// than hang ctest.
template <typename Fn>
void returnsWithin(std::chrono::seconds limit, const char* what, Fn fn)
{
  auto done = std::make_shared<std::promise<void>>();
  std::future<void> f = done->get_future();
  std::thread t(
      [done, fn]() mutable
      {
        try
        {
          fn();
          done->set_value();
        }
        catch (...)
        {
          done->set_exception(std::current_exception());
        }
      });
  if (f.wait_for(limit) != std::future_status::ready)
  {
    fprintf(stderr, "%s did not return within %lld s\n", what, static_cast<long long>(limit.count()));
    fflush(stderr);
    _exit(1);
  }
  t.join();
  f.get();  // rethrows what fn threw
}
// MessageQueueClient(ip, port) always makes a CompressedInetStreamSocket, and
// its constructor reads NetworkCompression from the default config file.
class SocketReconnectTest : public testing::Test
{
 protected:
  void SetUp() override
  {
#ifndef SKIP_IDB_COMPRESSION
    if (access("/etc/columnstore/Columnstore.xml", R_OK) != 0)
      GTEST_SKIP() << "needs /etc/columnstore/Columnstore.xml: CompressedInetStreamSocket() reads it";
#endif
  }
};
}  // namespace

TEST_F(SocketReconnectTest, PeerCloseThenNoAutoReconnect)
{
  auto listener = std::make_unique<Listener>();
  const uint16_t port = listener->port;
  ASSERT_NE(port, 0);
  MessageQueueClient client("127.0.0.1", port);
  client.autoReconnect(false);
  auto accepted = listener->acceptAsync();
  ASSERT_TRUE(client.connect());

  // The peer goes away: read() with no timeout sees EOF as a zero-length
  // message (with a timeout it throws SocketClosed instead).
  IOSocket server = accepted.get();
  server.close();
  SBS got;
  returnsWithin(std::chrono::seconds(10), "read() after the peer closed", [&] { got = client.read(); });
  timespec t{2, 0};
  ASSERT_TRUE(got);
  EXPECT_EQ(got->length(), 0U);

  // Closed, and no quiet reconnect: the listener is still there, so a
  // reconnect would succeed and hide the dead connection from the caller.
  client.shutdown();
  ByteStream msg;
  msg << static_cast<uint32_t>(6613);
  EXPECT_THROW(client.read(&t), SocketClosed);
  EXPECT_THROW(client.write(msg), SocketClosed);

  // connect() still opens it again, to a new listener on the same port.
  listener.reset();
  listener = std::make_unique<Listener>(port);
  auto accepted2 = listener->acceptAsync();
  ASSERT_TRUE(client.connect());
  IOSocket server2 = accepted2.get();
  client.write(msg);
  SBS in = server2.read(&t);
  ASSERT_TRUE(in);
  uint32_t v = 0;
  *in >> v;
  EXPECT_EQ(v, 6613U);

  ByteStream reply;
  reply << static_cast<uint32_t>(v + 1);
  server2.write(reply);
  SBS back = client.read(&t);
  ASSERT_TRUE(back);
  ASSERT_EQ(back->length(), sizeof(uint32_t));
  *back >> v;
  EXPECT_EQ(v, 6614U);
}

// A listener that has stopped answering handshakes: its accept queue is full,
// so the kernel drops further SYNs. Skips (with a message) if the queue never
// fills, rather than letting the test hang.
TEST_F(SocketReconnectTest, ConnectTimeoutIsBounded)
{
  int lsd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(lsd, 0);
  sockaddr_in sa = loopback(0);
  ASSERT_EQ(bind(lsd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)), 0);
  ASSERT_EQ(listen(lsd, 0), 0);
  const uint16_t port = portOf(lsd);

  std::vector<int> fillers;
  bool full = false;
  for (int i = 0; i < 16 && !full; ++i)
  {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    ASSERT_GE(fd, 0);
    fillers.push_back(fd);
    sockaddr_in to = loopback(port);
    if (connect(fd, reinterpret_cast<sockaddr*>(&to), sizeof(to)) == 0)
      continue;
    ASSERT_EQ(errno, EINPROGRESS);
    pollfd pfd{fd, POLLOUT, 0};
    full = poll(&pfd, 1, 200) == 0;
  }
  if (!full)
  {
    for (int fd : fillers)
      close(fd);
    close(lsd);
    GTEST_SKIP() << "the listener's accept queue never filled, so there is no unanswered handshake to time";
  }

  MessageQueueClient client("127.0.0.1", port);
  client.tcpConnectTimeout(300);
  std::string what;
  const auto start = Clock::now();
  returnsWithin(std::chrono::seconds(10), "connect() with a 300 ms TCP connect timeout",
                [&]
                {
                  try
                  {
                    client.connect();
                  }
                  catch (const std::runtime_error& e)
                  {
                    what = e.what();
                  }
                });
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();

  // strerror(ETIMEDOUT) from the TCP handshake, not the sync-protocol wait
  EXPECT_NE(what.find("Connection timed out"), std::string::npos) << "connect() said: '" << what << "'";
  EXPECT_GE(ms, 250);
  EXPECT_LT(ms, 1000);

  for (int fd : fillers)
    close(fd);
  close(lsd);
}

TEST_F(SocketReconnectTest, RefusedConnectFailsFast)
{
  // A port nothing listens on: bind it, read it back, close it.
  int sd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sd, 0);
  sockaddr_in sa = loopback(0);
  ASSERT_EQ(bind(sd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)), 0);
  const uint16_t port = portOf(sd);
  close(sd);

  MessageQueueClient client("127.0.0.1", port);
  client.tcpConnectTimeout(300);
  bool connected = true;
  const auto start = Clock::now();
  returnsWithin(std::chrono::seconds(10), "connect() to a refused port",
                [&] { connected = client.connect(); });
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();

  EXPECT_FALSE(connected);
  EXPECT_LT(ms, 250);
}

// Plain InetStreamSocket on both ends: needs no config, so it runs anywhere.
TEST(SocketBlockingTest, BoundedConnectLeavesSocketBlocking)
{
  Listener listener(0, new InetStreamSocket());
  IOSocket sock;
  sock.setSocketImpl(new InetStreamSocket());
  sock.open();
  sock.tcpConnectTimeout(300);
  sockaddr_in sa = loopback(listener.port);
  auto accepted = listener.acceptAsync();
  sock.connect(reinterpret_cast<sockaddr*>(&sa));
  IOSocket server = accepted.get();

  const int flags = fcntl(sock.socketParms().sd(), F_GETFL, 0);
  ASSERT_NE(flags, -1);
  EXPECT_EQ(flags & O_NONBLOCK, 0) << "the rest of messageqcpp expects a blocking socket";
  sock.close();
  server.close();
}
