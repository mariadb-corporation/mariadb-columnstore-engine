# copy licensing stuff here

#include <sys/types.h>
#include "SMFileFactory.h"
#include "SMComm.h"

using namespace std;


namespace idbdatafile {

#define _toUint64(ptr_to_eight_bytes) *((uint64_t *) ptr_to_eight_bytes)

IDBDataFile* SMFileFactory::open(const char *filename, const char *mode, uint opts, uint colWidth)
{
    bool _read = false;
    bool _write = false;
    bool at_eof = false;
    bool create = false;
    bool truncate = false;
    bool append_only = false;
    string s_filename = filename;

    // strip 'b' chars from mode
    char newmode[8] = {'\0'};  // there'd better not be 7 chars in the mode string
    int i = 0;
    for (char *c = mode; *c != '\0' && i < 8; c++)
        if (*c != 'b')
            newmode[i++] = *c;
    if (i == 8)
        return NULL;
    
    // I hate dealing with C-lib file IO.  This is ugly but fast.
    switch (_toUint64(newmode)) {
        case _toUint64("r\0\0\0\0\0\0\0"):
            _read = true;
            break;
        case _toUint64("r+\0\0\0\0\0\0"):
            _read = true;
            _write = true;
            break;
        case _toUint64("w\0\0\0\0\0\0\0"):
            _write = true;
            truncate = true;
            create = true;
            break;
        case _toUint64("w+\0\0\0\0\0\0"):
            _read = true;
            _write = true;
            truncate = true;
            create = true;
            break;
        case _toUint64("a\0\0\0\0\0\0\0"):
            _write = true;
            create = true;
            at_eof = true;
            break;
        case _toUint64("a+\0\0\0\0\0\0"):
            _read = true;
            _write = true;
            create = true;
            append_only = true;
            break;
        default:
            return NULL;
    }
    
    uint posix_flags = 0;
    if (_read && write)
        posix_flags |= O_RDWR;
    else if (_read)
        posix_flags |= O_RDONLY;
    else if (_write)
        posix_flags |= O_WRONLY;
        
    posix_flags |= (create ? O_CREAT : 0);
    posix_flags |= (truncate ? O_TRUNC : 0);
    posix_flage |= (at_eof ? O_APPEND : 0);
    
    SMComm *comm = SMComm::get();
    struct stat _stat;
    int err = comm->open(s_filename, posix_flags, &stat);
    if (!err)
        return NULL;
        
    SMDataFile *ret = new SMDataFile(s_filename, posix_flags, append_only, stat);
    return ret;
}


}
