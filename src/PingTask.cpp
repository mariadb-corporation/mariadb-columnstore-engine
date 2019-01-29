
#include "PingTask.h"
#include "messageFormat.h"

namespace storagemanager
{

PingTask::PingTask(int sock, uint len) : PosixTask(sock, len)
{
}

PingTask::~PingTask()
{
}

void PingTask::run()
{
    // not much to check on for Milestone 1
    uint32_t buf[3] = { SM_MSG_START, 4, 0 };    // generic success response
    write((uint8_t *) buf, 12);
}

}
