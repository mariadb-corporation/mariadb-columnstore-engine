/* Copyright (C) 2024 MariaDB Corp.

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

#include <string>

#include "samenodepseudosocket.h"
#include "iosocket.h"

namespace messageqcpp
{
SameNodePseudoSocket::SameNodePseudoSocket(joblist::DistributedEngineComm* exeMgrDecPtr) : dec_(exeMgrDecPtr)
{
  assert(dec_);
}

SameNodePseudoSocket::~SameNodePseudoSocket()
{
}

void SameNodePseudoSocket::open()
{
}

void SameNodePseudoSocket::close()
{
}

Socket* SameNodePseudoSocket::clone() const
{
  return nullptr;
}

SameNodePseudoSocket::SameNodePseudoSocket(const SameNodePseudoSocket& /*rhs*/)
{
}

SameNodePseudoSocket& SameNodePseudoSocket::operator=(const SameNodePseudoSocket& /*rhs*/)
{
  return *this;
}

const SBS SameNodePseudoSocket::read(const struct ::timespec* /*timeout*/, bool* /*isTimeOut*/, Stats* /*stats*/) const
{
  return nullptr;
}

// This is the only working method of this class. It puts SBS directly into DEC queue.
void SameNodePseudoSocket::write(SBS msg, Stats* /*stats*/)
{
  dec_->addDataToOutput(msg);
}

void SameNodePseudoSocket::write(const ByteStream& /*msg*/, Stats* /*stats*/)
{
}

void SameNodePseudoSocket::write_raw(const ByteStream& /*msg*/, Stats* /*stats*/) const
{
}

void SameNodePseudoSocket::connect(const sockaddr* /*serv_addr*/)
{
}

void SameNodePseudoSocket::bind(const sockaddr* /*serv_addr*/)
{
}

const IOSocket SameNodePseudoSocket::accept(const struct timespec* /*timeout*/)
{
  return IOSocket();
}

void SameNodePseudoSocket::listen(int /*backlog*/)
{
}

const std::string SameNodePseudoSocket::toString() const
{
  return "";
}

const std::string SameNodePseudoSocket::addr2String() const
{
  return "";
}

bool SameNodePseudoSocket::isSameAddr(const Socket* /*rhs*/) const
{
  return false;
}

bool SameNodePseudoSocket::isSameAddr(const struct in_addr& /*ipv4Addr*/) const
{
  return false;
}

int SameNodePseudoSocket::ping(const std::string& /*ipaddr*/, const struct timespec* /*timeout*/)
{
  return 0;
}

bool SameNodePseudoSocket::isConnected() const
{
  return true;
}

bool SameNodePseudoSocket::hasData() const
{
  return false;
}

}  // namespace messageqcpp
