

#include <boost/filesystem.hpp>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "LocalStorage.h"
#include "Config.h"

using namespace std;
namespace bf = boost::filesystem;

namespace storagemanager
{

LocalStorage::LocalStorage()
{
    prefix = Config::get()->getValue("LocalStorage", "path");
    //cout << "LS: got prefix " << prefix << endl;
    if (!bf::is_directory(prefix))
    {
        try 
        {
            bf::create_directories(prefix);
        }
        catch (exception &e)
        {
            logger->log(LOG_CRIT, "Failed to create %s, got: %s", prefix.string().c_str(), e.what());
            throw e;
        }
    }
}

LocalStorage::~LocalStorage()
{
}

const bf::path & LocalStorage::getPrefix() const 
{
    return prefix;
}

int LocalStorage::copy(const bf::path &source, const bf::path &dest)
{
    boost::system::error_code err;
    bf::copy_file(source, dest, bf::copy_option::fail_if_exists, err);
    if (err)
    {
        errno = err.value();
        return -1;
    }
    return 0;
}

bf::path operator+(const bf::path &p1, const bf::path &p2)
{
    bf::path ret(p1);
    ret /= p2;
    return ret;
}

int LocalStorage::getObject(const string &source, const string &dest, size_t *size)
{
    int ret = copy(prefix / source, dest);
    if (ret)
        return ret;
    if (size)
        *size = bf::file_size(dest);
    return ret;
}

int LocalStorage::getObject(const std::string &sourceKey, boost::shared_array<uint8_t> *data, size_t *size)
{
    bf::path source = prefix / sourceKey;
    const char *c_source = source.string().c_str();
    //char buf[80];
    int l_errno;
    
    int fd = ::open(c_source, O_RDONLY);
    if (fd < 0)
    {
        l_errno = errno;
        //logger->log(LOG_WARNING, "LocalStorage::getObject() failed to open %s, got '%s'", c_source, strerror_r(errno, buf, 80));
        errno = l_errno;
        return fd;
    }
    
    size_t l_size = bf::file_size(source);
    data->reset(new uint8_t[l_size]);
    size_t count = 0;
    while (count < l_size)
    {
        int err = ::read(fd, &(*data)[count], l_size - count);
        if (err < 0)
        {
            l_errno = errno;
            //logger->log(LOG_WARNING, "LocalStorage::getObject() failed to read %s, got '%s'", c_source, strerror_r(errno, buf, 80));
            close(fd);
            errno = l_errno;
            return err;
        }
        count += err;
    }
    if (size)
        *size = l_size;
    close(fd);
    return 0;
}

int LocalStorage::putObject(const string &source, const string &dest)
{
    return copy(source, prefix / dest);
}

int LocalStorage::putObject(boost::shared_array<uint8_t> data, size_t len, const string &dest)
{
    bf::path destPath = prefix / dest;
    const char *c_dest = destPath.string().c_str();
    //char buf[80];
    int l_errno;
    
    int fd = ::open(c_dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        l_errno = errno;
        //logger->log(LOG_CRIT, "LocalStorage::putObject(): Failed to open %s, got '%s'", c_dest, strerror_r(errno, buf, 80));
        errno = l_errno;
        return fd;
    }

    size_t count = 0;
    int err;
    while (count < len)
    {
        err = ::write(fd, &data[count], len - count);
        if (err < 0)
        {
            l_errno = errno;
            //logger->log(LOG_CRIT, "LocalStorage::putObject(): Failed to write to %s, got '%s'", c_dest, strerror_r(errno, buf, 80));
            close(fd);
            errno = l_errno;
            return err;
        }
        count += err;
    }
    close(fd);
    return 0;
}

int LocalStorage::copyObject(const string &source, const string &dest)
{
    return copy(prefix / source, prefix / dest);
}

int LocalStorage::deleteObject(const string &key)
{
    boost::system::error_code err;
    
    bf::remove(prefix / key, err);
    return 0;
}

int LocalStorage::exists(const std::string &key, bool *out)
{
    *out = bf::exists(prefix / key);
    return 0;
}

}
