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
/** @file */
#pragma once

#include <unistd.h>
#include <netinet/in.h>

#include "socket.h"
#include "iosocket.h"
#include "bytestream.h"
#include "inetstreamsocket.h"
#include "idbcompress.h"

namespace messageqcpp
{
class CompressedInetStreamSocket : public InetStreamSocket
{
 public:
  CompressedInetStreamSocket();
  CompressedInetStreamSocket(const CompressedInetStreamSocket&) = default;
  ~CompressedInetStreamSocket() override = default;

  using InetStreamSocket::operator=;
  Socket* clone() const override;
  const SBS read(const struct timespec* timeout = nullptr, bool* isTimeOut = nullptr,
                 Stats* stats = nullptr) const override;
  void write(const ByteStream& msg, Stats* stats = nullptr) override;
  void write(SBS msg, Stats* stats = nullptr) override;
  const IOSocket accept(const struct timespec* timeout) override;
  void connect(const sockaddr* addr) override;

 private:
  std::shared_ptr<compress::CompressInterface> alg;
  bool useCompression;
  static const uint32_t HEADER_SIZE = 4;
};

}  // namespace messageqcpp

#undef EXPORT
