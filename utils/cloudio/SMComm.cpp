// copy licensing stuff here



#include "SMComm.h"
#include "bytestream.h"
#include <boost/thread/mutex.hpp>

using namespace std;
using namespace messageqcpp;

namespace
{
SMComm *instance = NULL;
boost::mutex m;
}

namespace idbdatafile
{

static SMComm * SMComm::get()
{
    if (instance)
        return instance;
        
    boost::mutex::scoped_lock sl(m);
    
    if (instance)
        return instance;
    instance = new SMComm();
    return instance;
}

// timesavers
#define common_exit(bs1, bs2, retCode) \
    { \
        buffers.returnByteStream(bs1); \
        buffers.returnByteStream(bs2); \
        return retCode; \
    }

// bs1 is the bytestream ptr with the command to SMComm.
// bs2 is the bytestream pointer with the response from SMComm.
// retCode is the var to store the return code in from the msg.
// returns with the output pointer at the fcn-specific data
#define check_for_error(bs1, bs2, retCode) \
    { \
        int l_errno; \
        *bs2 >> retCode; \
        if (retCode < 0) \
        { \
            *bs2 >> l_errno; \
            errno = l_errno; \
            common_exit(bs1, bs2, retCode); \
        } \
    }
    
int SMComm::open(const string &filename, int mode, struct stat *statbuf)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::OPEN << filename << mode;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    
    check_for_error(command, response, err);
    memcpy(statbuf, response->buf(), sizeof(*statbuf));
    common_exit(command, response, err);
}

ssize_t SMComm::pread(const string &filename, const void *buf, size_t count, off_t offset)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;

    command << storagemanager::READ << filename << count << offset;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    
    memcpy(buf, response->buf(), err);
    common_exit(command, response, err);
}

ssize_t SMComm::pwrite(const string &filename, const void *buf, size_t count, off_t offset)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::WRITE << filename << count << offset;
    command.needAtLeast(count);
    uint8_t *cmdBuf = command->getInputPtr();
    memcpy(cmdBuf, buf, count);
    command->advanceInputPtr(count);
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    common_exit(command, response, err);
}

ssize_t SMComm::append(const string &filename, const void *buf, size_t count)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::APPEND << filename << count;
    command.needAtLeast(count);
    uint8_t *cmdBuf = command->getInputPtr();
    memcpy(cmdBuf, buf, count);
    command->advanceInputPtr(count);
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    common_exit(command, response, err);
}

int SMComm::unlink(const string &filename)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::UNLINK << filename;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    common_exit(command, response, err);
}

int SMComm::stat(const string &filename, struct stat *statbuf)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storageManager::STAT << filename;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    
    memcpy(statbuf, response->buf(), sizeof(*statbuf));
    common_exit(command, response, err);
}

int SMComm::truncate(const string &filename, off64_t length)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::TRUNCATE << filename << length;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    common_exit(command, response, err);
}

int SMComm::listDirectory(const string &path, list<string> *entries)
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::LIST_DIRECTORY << path;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    
    uint32_t numElements;
    string stmp;
    entries->clear();
    *response >> numElements;
    while (numElements > 0) {
        *response >> stmp;
        entries->push_back(stmp);
    }
    common_exit(command, response, err);
}

int SMComm::ping()
{
    ByteStream *command = buffers.getByteStream();
    ByteStream *response = buffers.getByteStream();
    int err;
    
    command << storagemanager::PING << filename << length;
    err = sockets.send_recv(*command, response);
    if (err)
        common_exit(command, response, err);
    check_for_error(command, response, err);
    common_exit(command, response, err);
}

}
