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

// Defines stuff client code will use to construct messages to StorageManager.

#ifndef MESSAGEFORMAT_H_
#define MESSAGEFORMAT_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>

namespace storagemanager
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"

#pragma pack(push, 1)
struct sm_msg_header
{
  uint32_t type;        // SM_MSG_{START,CONT,END}
  uint32_t payloadLen;  // refers to the length of what follows the header
  uint8_t flags;        // see below for valid values
};

// current values for the flags field in sm_msg_header
static const uint8_t CONT =
    0x1;  // this means another message will follow as part of this request or response

struct sm_request
{
  sm_msg_header header;
  uint8_t payload[];
};

struct sm_response
{
  sm_msg_header header;
  ssize_t returnCode;  // if < 0 it indicates an error, and payload contains a 4-byte errno value
  uint8_t payload[];
};

// all msgs to and from StorageManager begin with this magic and a payload length
static const uint32_t SM_MSG_START = 0xbf65a7e1;

// for read/write/append, which may break a message into chunks, messages not the
// beginning or end of the larger message will preface a chunk with this magic
static const uint32_t SM_MSG_CONT = 0xfa371bd2;

// for read/write/append, the last chunk of a message should begin with this magic
static const uint32_t SM_MSG_END = 0x9d5bc31b;

static const uint32_t SM_HEADER_LEN = sizeof(sm_msg_header);

// the unix socket StorageManager is listening on
__attribute__((unused)) static const char* socket_name = "\0storagemanager";

#pragma GCC diagnostic pop

// opcodes understood by StorageManager.  Cast these to
// a uint8_t to use them.
enum Opcodes
{
  OPEN,
  READ,
  WRITE,
  STAT,
  UNLINK,
  APPEND,
  TRUNCATE,
  LIST_DIRECTORY,
  PING,
  COPY,
  SYNC
};

/*
    All commands sent to and from StorageManager begin with
    SM_MSG_START, and a uint32_t for the length of the payload.

    In the payload, all responses from StorageManager begin with
    a return code, which is the same as the corresponding syscall.
    If the return code is an error (usually < 0), then a 4-byte
    value for errno follows.  On success, no errno is sent.

    On success, what follows is any output parameters from the call.

    Note: filenames and pathnames in the following parameters should
    be absolute rather than relative.
*/

/*
    OPEN
    ----
    command format:
    1-byte opcode|4-byte openmode|4-byte filename length|filename

    response format:
    struct stat
*/
struct open_cmd
{
  uint8_t opcode;  // == OPEN
  int32_t openmode;
  uint32_t flen;
  char filename[];
};

struct open_resp
{
  struct stat statbuf;
};

/*
    READ
    ----
    command format:
    1-byte opcode|size_t count|off_t offset|4-byte filename length|filename

    response format:
    data (size is stored in the return code)
*/
struct read_cmd
{
  uint8_t opcode;  // == READ
  size_t count;
  off_t offset;
  uint32_t flen;
  char filename[];
};

typedef uint8_t read_resp;

/*
    WRITE
    -----
    command format:
    1-byte opcode|size_t count|off_t offset|4-byte filename length|filename|data

    response format:
*/
struct write_cmd
{
  uint8_t opcode;  // == WRITE
  ssize_t count;
  off_t offset;
  uint32_t flen;
  char filename[];
  // after this is the data to be written, ie at &filename[flen]
};

/*
    APPEND
    ------
    command format:
    1-byte opcode|size_t count|4-byte filename length|filename|data

    response format:
*/
struct append_cmd
{
  uint8_t opcode;  // == APPEND
  ssize_t count;
  uint32_t flen;
  char filename[];
  // after this is the data to be written, ie at &filename[flen]
};

/*
    UNLINK
    ------
    command format:
    1-byte opcode|4-byte filename length|filename

    response format:
*/
struct unlink_cmd
{
  uint8_t opcode;  // == UNLINK
  uint32_t flen;
  char filename[];
};

/*

    STAT
    ----
    command format:
    1-byte opcode|4-byte filename length|filename

    response format:
    struct stat
*/
struct stat_cmd
{
  uint8_t opcode;  // == STAT
  uint32_t flen;
  char filename[];
};

struct stat_resp
{
  struct stat statbuf;
};

/*

    TRUNCATE
    --------
    command format:
    1-byte opcode|off_t length|4-byte filename length|filename

    response format:
*/
struct truncate_cmd
{
  uint8_t opcode;  // == TRUNCATE
  off_t length;
  uint32_t flen;
  char filename[];
};

/*
    LIST_DIRECTORY
    --------------
    command format:
    1-byte opcode|4-byte path length|pathname

    response format:
    4-byte num elements|
    (4-byte filename length|filename) * num elements
*/

struct listdir_cmd
{
  uint8_t opcode;  // == LIST_DIRECTORY
  uint32_t plen;
  char path[];
};

struct listdir_resp_entry
{
  uint32_t flen;
  char filename[];
};

struct listdir_resp
{
  uint32_t elements;
  listdir_resp_entry entries[];
  // followed by (elements * listdir_resp_entry)
};

/*
    PING
    ----
    command format:
    1-byte opcode

    response format:
    nothing yet
*/
struct ping_cmd
{
  uint8_t opcode;
};

/*
    COPY
    ----
    command format:
    1-byte opcode|4-byte filename1 length|filename2|4-byte filename2 length|filename2

    response format:
*/

struct f_name
{
  uint32_t flen;
  char filename[];
};

struct copy_cmd
{
  uint8_t opcode;
  f_name file1;
  // use f_name as an overlay at the end of file1 to get file2.
};

#pragma pack(pop)

}  // namespace storagemanager

#endif
