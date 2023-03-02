#pragma once

#include <stdint.h>
#include <unistd.h>
#include <string>

#include "socktype.h"

namespace qfe
{
namespace socketio
{
void readn(int fd, void* buf, const size_t wanted);
size_t writen(int fd, const void* data, const size_t nbytes);
uint32_t readNumber32(SockType);
std::string readString(SockType);
void writeString(SockType, const std::string&);

}  // namespace socketio
}  // namespace qfe
