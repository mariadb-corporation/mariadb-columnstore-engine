// Copyright (C) 2019 MariaDB Corporaton


// Eventually this will define stuff client code will use to
// construct messages to StorageManager.

#ifndef MESSAGEFORMAT_H_
#define MESSAGEFORMAT_H_

namespace storagemanager
{

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"

// all msgs to and from StorageManager begin with this magic and a payload length
static const uint SM_MSG_START=0xbf65a7e1;

// for read/write/append, which may break a message into chunks, messages not the 
// beginning or end of the larger message will preface a chunk with this magic 
static const uint SM_MSG_CONT=0xfa371bd2;

// for read/write/append, the last chunk of a message should begin with this magic
static const uint SM_MSG_END=0x9d5bc31b;

static const uint SM_HEADER_LEN = 8;

// the unix socket StorageManager is listening on
static const char *socket_name = "\0storagemanager";

#pragma GCC diagnostic pop


// opcodes understood by StorageManager.  Cast these to
// a uint8_t to use them.
enum Opcodes {
    OPEN,
    READ,
    WRITE,
    STAT,
    UNLINK,
    APPEND,
    TRUNCATE,
    LIST_DIRECTORY,
    PING
};

/*
    All commands sent to and from StorageManager begin with
    SM_MSG_START, and a uint32_t for the length of the payload.
    
    In the payload, all responses from StorageManager begin with
    a return code, which is the same as the corresponding syscall.
    If the return code is an error (usually < 0), then a 4-byte
    value for errno follows.  On success, no errno is sent.

    On success, what follows is any output parameters from the call.
    
    TBD: Require filenames to be NULL-terminated.  Currently they are not.

    OPEN
    ----
    command format:
    1-byte opcode|4-byte filename length|filename|4-byte openmode

    response format:
    struct stat

    READ
    ----
    command format:
    1-byte opcode|4-byte filename length|filename|size_t count|off_t offset

    response format:
    data (size is stored in the return code)

    WRITE
    -----
    command format:
    1-byte opcode|size_t count|off_t offset|4-byte filename length|filename|data

    response format:

    APPEND
    ------
    command format:
    1-byte opcode|4-byte filename length|filename|size_t count|data

    response format:

    UNLINK
    ------
    command format:
    1-byte opcode|4-byte filename length|filename

    response format:
    
    STAT
    ----
    command format:
    1-byte opcode|4-byte filename length|filename

    response format:
    struct stat

    TRUNCATE
    --------
    command format:
    1-byte opcode|4-byte filename length|filename|off64_t length
   
    response format:
    
    LIST_DIRECTORY
    --------------
    command format:
    1-byte opcode|4-byte path length|pathname

    response format:
    4-byte num elements|
    (4-byte filename length|filename) * num elements
    
    PING
    ----
    command format:
    1-byte opcode

    reponse format:

*/

}

#endif

