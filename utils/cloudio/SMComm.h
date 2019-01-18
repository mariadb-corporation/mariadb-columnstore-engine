# copy some licensing stuff here

#ifndef SMCOMM_H_
#define SMCOMM_H_

namespace idbdatafile {

class SMComm : public boost::noncopyable
{
    public:
        SMComm *get();
        
        /* Open currently returns a stat struct so SMDataFile can set its initial position, otherwise
           behaves how you'd think. */
        int open(const std::string &filename, int mode, struct stat *statbuf);
        
        ssize_t pread(const std::string &filename, const void *buf, size_t count, off_t offset);
        
        ssize_t pwrite(const std::string &filename, const void *buf, size_t count, off_t offset);
        
        /* append exists for cases where the file is open in append mode.  A normal write won't work
        because the file position may be out of date if there are multiple writers. */
        ssize_t append(const std::string &filename, const void *buf, size_t count);
        
        int unlink(const std::string &filename);
        
        int stat(const std::string &filename, struct stat *statbuf);
        
        // added this one because it should be trivial to implement in SM, and prevents a large
        // operation in SMDataFile.
        int truncate(const std::string &filename, off64_t length);
        
        int listDirectory(const std::string &path, std::list<std::string> *entries);
        
        // health indicator.  0 = processes are talking to each other and SM has read/write access to 
        // the specified S3 bucket.  Need to define specific error codes.
        int ping();
        
        virtual ~SMComm();
        
    private:
        SMComm();
        
        SocketPool sockets;


}


}



#endif
