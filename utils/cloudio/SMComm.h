<<<<<<< HEAD
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
=======
# copy some licensing stuff here
>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.

#ifndef SMCOMM_H_
#define SMCOMM_H_

<<<<<<< HEAD
#include <sys/stat.h>
#include <string>
#include "SocketPool.h"
#include "bytestream.h"
#include "bytestreampool.h"

namespace idbdatafile 
{
=======
namespace idbdatafile {
>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.

class SMComm : public boost::noncopyable
{
    public:
<<<<<<< HEAD
        // This is a singleton.  Get it with get()
        static SMComm *get();
        
        /* Open currently returns a stat struct so SMDataFile can set its initial position, otherwise
           behaves how you'd think. */
        int open(const std::string &filename, const int mode, struct stat *statbuf);
        
        ssize_t pread(const std::string &filename, void *buf, const size_t count, const off_t offset);
        
        ssize_t pwrite(const std::string &filename, const void *buf, const size_t count, const off_t offset);
        
        /* append exists for cases where the file is open in append mode.  A normal write won't work
        because the file position/size may be out of date if there are multiple writers. */
        ssize_t append(const std::string &filename, const void *buf, const size_t count);
=======
        SMComm *get();
        
        /* Open currently returns a stat struct so SMDataFile can set its initial position, otherwise
           behaves how you'd think. */
        int open(const std::string &filename, int mode, struct stat *statbuf);
        
        ssize_t pread(const std::string &filename, const void *buf, size_t count, off_t offset);
        
        ssize_t pwrite(const std::string &filename, const void *buf, size_t count, off_t offset);
        
        /* append exists for cases where the file is open in append mode.  A normal write won't work
        because the file position may be out of date if there are multiple writers. */
        ssize_t append(const std::string &filename, const void *buf, size_t count);
>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.
        
        int unlink(const std::string &filename);
        
        int stat(const std::string &filename, struct stat *statbuf);
        
        // added this one because it should be trivial to implement in SM, and prevents a large
        // operation in SMDataFile.
<<<<<<< HEAD
        int truncate(const std::string &filename, const off64_t length);
=======
        int truncate(const std::string &filename, off64_t length);
>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.
        
        int listDirectory(const std::string &path, std::list<std::string> *entries);
        
        // health indicator.  0 = processes are talking to each other and SM has read/write access to 
        // the specified S3 bucket.  Need to define specific error codes.
        int ping();
        
<<<<<<< HEAD
        int copyFile(const std::string &file1, const std::string &file2);
        
=======
>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.
        virtual ~SMComm();
        
    private:
        SMComm();
        
<<<<<<< HEAD
        std::string getAbsFilename(const std::string &filename);
        
        SocketPool sockets;
        messageqcpp::ByteStreamPool buffers;
        std::string cwd;
};

}

=======
        SocketPool sockets;


}


}



>>>>>>> d53471fc... Checkpointing some stuff.  No way it'll build yet.
#endif
