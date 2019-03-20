
#include "Replicator.h"
#include "IOCoordinator.h"
#include "SMLogging.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/shared_array.hpp>
#include <boost/format.hpp>
#include <iostream>

using namespace std;

namespace
{
    storagemanager::Replicator *rep = NULL;
    boost::mutex m;
}
namespace storagemanager
{

Replicator::Replicator()
{
}

Replicator::~Replicator()
{
}

Replicator * Replicator::get()
{
    if (rep)
        return rep;
    boost::mutex::scoped_lock s(m);
    if (rep)
        return rep;
    rep = new Replicator();
    return rep;
}

struct scoped_closer {
    scoped_closer(int f) : fd(f) { }
    ~scoped_closer() {
        int s_errno = errno;
        ::close(fd);
        errno = s_errno;
    }
    int fd;
};

#define OPEN(name, mode) \
    fd = ::open(name, mode, 0600); \
    if (fd < 0) \
        return fd; \
    scoped_closer sc(fd);

int Replicator::newObject(const char *filename, const uint8_t *data, size_t length )
{
    int fd, err;

    OPEN(filename, O_WRONLY | O_CREAT);
    size_t count = 0;
    while (count < length) {
        err = ::write(fd, &data[count], length - count);
        if (err <= 0)
            if (count > 0)   // return what was successfully written
                return count;
            else
                return err;
        count += err;
    }

    return count;
}

int Replicator::addJournalEntry(const char *filename, const uint8_t *data, off_t offset, size_t length)
{
    int fd, err;
    uint64_t offlen[] = {offset,length};
    size_t count = 0;
    int version = 1;
    string journalFilename = string(filename) + ".journal";
    uint64_t thisEntryMaxOffset = (offset + length - 1);
    if (!boost::filesystem::exists(journalFilename))
    {
        // create new journal file with header
        OPEN(journalFilename.c_str(), O_WRONLY | O_CREAT);
        string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version % thisEntryMaxOffset).str();
        err = ::write(fd, header.c_str(), header.length() + 1);
        if (err <= 0)
            return err;
    }
    else
    {
        // read the existing header and check if max_offset needs to be updated
        OPEN(journalFilename.c_str(), O_RDWR);
        boost::shared_array<char> headertxt = seekToEndOfHeader1(fd);
        stringstream ss;
        ss << headertxt.get();
        boost::property_tree::ptree header;
        boost::property_tree::json_parser::read_json(ss, header);
        assert(header.get<int>("version") == 1);
        uint64_t currentMaxOffset = header.get<uint64_t>("max_offset");
        if (thisEntryMaxOffset > currentMaxOffset)
        {
            string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version % thisEntryMaxOffset).str();
            err = ::pwrite(fd, header.c_str(), header.length() + 1,0);
            if (err <= 0)
                return err;
        }
    }

    OPEN(journalFilename.c_str(), O_WRONLY | O_APPEND);

    err = ::write(fd, offlen, 16);
    if (err <= 0)
        return err;

    while (count < length) {
        err = ::write(fd, &data[count], length - count);
        if (err <= 0)
            if (count > 0)   // return what was successfully written
                return count;
            else
                return err;
        count += err;
    }

    return count;
}

int Replicator::remove(const char *filename, uint8_t flags)
{
    int ret = 0;
    boost::filesystem::path p(filename);

    try
    {
        boost::filesystem::remove_all(filename);
    }
    catch(boost::filesystem::filesystem_error &e)
    {
        errno = e.code().value();
        ret = -1;
    }
    return ret;
}

}
