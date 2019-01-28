
#include "ListDirectoryTask.h"

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

bool ListDirectoryTask::writeString(uint8_t buf, int *offset, int size, const string &str)
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
    uint8_t buf[1024];
    
    if (getLength() > 1024) {
        handleError("ListDirectoryTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("ListDirectoryTask read");
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    vector<string> listing;
    //  IOC->listDirectory(path, &listing)
    
    // bogus response
    listing.push_back("dummy1");
    listing.push_back("dummy2");
    
    uint payloadLen = 4 * listing.size();
    for (uint i = 0; i < listing.size(); i++)
        payloadLen += listing.size();
    
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = payloadLen;
    
    int offset = 8;
    for (uint i = 0; i < listing.size(); i++)
    {
        success = writeString(buf, &offset, 1024, listing);
        check_error("ListDirectoryTask write");
    }
        
    if (offset != 0)
    {
        success = write(buf, offset);
        check_error("ListDirectoryTask write");
    }
}
    
    
    
    
}
