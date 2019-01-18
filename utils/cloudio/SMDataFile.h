# copy some licensing stuff here


#ifndef SMDATAFILE_H_
#define SMDATAFILE_H_

#include <string>
#include <boost/utility.hpp>
#include "IDBDataFile.h"
#include "SMComm.h"

namespace idbdatafile 
{

class SMDataFile : public IDBDataFile
{
    public:
        virtual ~SMDataFile();

        ssize_t pread(void* ptr, off64_t offset, size_t count);
        ssize_t read(void* ptr, size_t count);
        ssize_t write(const void* ptr, size_t count);
        int seek(off64_t offset, int whence);
        int truncate(off64_t length);
        off64_t size();
        off64_t tell();
        int flush();
        time_t mtime();
    
    private:
        SMDataFile();
        SMDataFile(const char *fname, int openmode, const struct stat &);
        off64_t position;
        int openmode;
        SMComm *comm;
        
        friend class SMFileFactory;
};

}

#endif
