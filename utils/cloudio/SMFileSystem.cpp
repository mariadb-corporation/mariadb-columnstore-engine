# copy licensing stuff here

#include <sys/types.h>
#include "SMFileSystem.h"
#include "SMComm.h"
#include "sm_exceptions.h"

using namespace std;

namespace idbdatafile
{

SMFileSystem::SMFileSystem() : IDBFileSystem(IDBFileSystem::CLOUD)
{
    SMComm::getSMComm();   // get SMComm running
}

int SMFileSystem::mkdir(const char *path)
{
    return 0;
}

int SMFileSystem::size(const char *filename) const
{
    struct stat _stat;
    
    SMComm *smComm = SMComm::get();
    int err = smComm->stat(filename, &_stat);
    if (!err)
        return err;
    
    return _stat.st_size;
}

off64_t SMFileSystem::compressedSize(const char *filename) const
{
    // Yikes, punting on this one.
    throw NotImplementedYet(__func__);
}

int SMFileSystem::remove(const char *filename) const
{
    SMComm *comm = SMComm::get();
    return comm->unlink(filename);
}

int SMFileSystem::rename(const char *oldFile, const char *newFile) const
{
    // This will actually be pretty expensive to do b/c we store the filename in 
    // the key in cloud.  If we do this a lot, we'll have to implement copy() in the SM.
    throw NotImplementedYet(__func__);
}

bool SMFileSystem::exists(const char *filename) const
{
    struct stat _stat;
    SMComm *comm = SMComm::get();
    
    int err = comm->stat(filename, &_stat);
    return (err == 0);
}

int SMFileSystem::listDirectory(const char* pathname, std::list<std::string>& contents) const
{
    SMComm *comm = SMComm::get();
    return comm->listDirectory(pathname, &contents);
}

bool SMFileSystem::isDir(const char *path) const
{
    SMComm *comm = SMComm::get();
    struct stat _stat;
    
    int err = comm->stat(path, &stat);
    if (err != 0)
        return false;   // reasonable to throw here?  todo, look at what the other classes do.
    return (stat.st_mode & S_IFDIR);
}

int SMFileSystem::copyFile(const char *src, const char *dest) const
{
    throw NotImplementedYet(__func__);
}
    
bool SMFileSystem::filesystemIsUp() const
{
    SMComm *comm = SMComm::get();
    return (comm->ping() == 0);
}

}
