# copy licensing stuff here


#ifndef SMFILESYSTEM_H_
#define SMFILESYSTEM_H_

#include <list>
#include <string>
#include <boost/utility.hpp>
#include "IDBFileSystem.h"

namespace idbdatafile
{

class SMFileSystem : public IDBFileSystem, boost::noncopyable
{
    public:
        SMFileSystem();
        virtual ~SMFileSystem();

        int mkdir(const char* pathname);
        off64_t size(const char* path) const;
        off64_t compressedSize(const char* path) const;
        int remove(const char* pathname);
        int rename(const char* oldpath, const char* newpath);
        bool exists(const char* pathname) const;
        int listDirectory(const char* pathname, std::list<std::string>& contents) const;
        bool isDir(const char* pathname) const;
        int copyFile(const char* srcPath, const char* destPath) const;
        bool filesystemIsUp() const;
};

}

#endif
