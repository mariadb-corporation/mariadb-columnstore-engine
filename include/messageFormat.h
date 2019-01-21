// Eventually this will define stuff client code will use to
// construct messages to StorageManager.

#ifndef MESSAGEFORMAT_H_
#define MESSAGEFORMAT_H_

namespace storagemanager
{

static const uint SM_MSG_START=0xbf65a7e1;

enum Opcodes {
    OPEN,
    READ,
    WRITE,
    STAT,
    UNLINK
};

}

#endif

