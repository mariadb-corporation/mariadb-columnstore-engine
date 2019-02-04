
#include "ListDirectoryTask.h"
#include "messageFormat.h"
#include <errno.h>
#include <string.h>
#include <iostream>

using namespace std;

namespace storagemanager
{

ListDirectoryTask::ListDirectoryTask(int sock, uint len) : PosixTask(sock, len)
{
}

ListDirectoryTask::~ListDirectoryTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }
    
#define check_error_2(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return false; \
    }
    
#define min(x, y) (x < y ? x : y)

bool ListDirectoryTask::writeString(uint8_t *buf, int *offset, int size, const string &str)
{
    bool success;
    if (size - *offset < 4)   // eh, let's not frag 4 bytes.
    {
        success = write(buf, *offset);
        check_error_2("ListDirectoryTask::writeString()");
        *offset = 0;
    }
    uint count = 0, len = str.length();
    *((uint32_t *) &buf[*offset]) = len;
    *offset += 4;
    while (count < len)
    {
        int toWrite = min(size - *offset, len);
        memcpy(&buf[*offset], &str.data()[count], toWrite);
        count += toWrite;
        *offset += toWrite;
        if (*offset == size)
        {
            success = write(buf, *offset);
            check_error_2("ListDirectoryTask::writeString()");
            *offset = 0;
        }
    }
    return true;
}
    
void ListDirectoryTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    int err;
    
    if (getLength() > 1023) {
        handleError("ListDirectoryTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("ListDirectoryTask read");
    listdir_cmd *cmd = (listdir_cmd *) buf;
    
    vector<string> listing;
    err = ioc->listDirectory(cmd->path, &listing);
    if (err)
    {
        handleError("ListDirectory", errno);
        return;
    }
    
    // be careful modifying the listdir return types...
    uint payloadLen = sizeof(listdir_resp_entry) * listing.size();
    for (uint i = 0; i < listing.size(); i++)
        payloadLen += listing.size();
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = payloadLen + sizeof(listdir_resp);
    resp->returnCode = 0;
    listdir_resp *r = (listdir_resp *) resp->payload;
    r->elements = listing.size();
    
    int offset = (uint64_t) r->entries - (uint64_t) buf;
    for (uint i = 0; i < listing.size(); i++)
    {
        success = writeString(buf, &offset, 1024, listing[i]);
        check_error("ListDirectoryTask write");
    }
        
    if (offset != 0)
    {
        success = write(buf, offset);
        check_error("ListDirectoryTask write");
    }
}
    
}
