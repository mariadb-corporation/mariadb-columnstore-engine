

#include <boost/filesystem.hpp>
#include <iostream>
#include <syslog.h>
#include "LocalStorage.h"
#include "Config.h"

using namespace std;
using namespace boost::filesystem;

namespace storagemanager
{

LocalStorage::LocalStorage()
{
    prefix = Config::get()->getValue("LocalStorage", "path");
    //cout << "LS: got prefix " << prefix << endl;
    if (!is_directory(prefix))
    {
        try 
        {
            create_directories(prefix);
        }
        catch (exception &e)
        {
            syslog(LOG_CRIT, "Failed to create %s, got: %s", prefix.string().c_str(), e.what());
            throw e;
        }
    }
}

LocalStorage::~LocalStorage()
{
}

const boost::filesystem::path & LocalStorage::getPrefix() const 
{
    return prefix;
}

int LocalStorage::copy(const path &source, const path &dest)
{
    boost::system::error_code err;
    copy_file(source, dest, copy_option::fail_if_exists, err);
    if (err)
    {
        errno = err.value();
        return -1;
    }
    return 0;
}

path operator+(const path &p1, const path &p2)
{
    path ret(p1);
    ret /= p2;
    return ret;
}

int LocalStorage::getObject(const string &source, const string &dest, size_t *size)
{
    int ret = copy(prefix / source, dest);
    if (ret)
        return ret;
    if (size)
        *size = boost::filesystem::file_size(dest);
    return ret;
}

int LocalStorage::putObject(const string &source, const string &dest)
{
    return copy(source, prefix / dest);
}

int LocalStorage::copyObject(const string &source, const string &dest)
{
    return copy(prefix / source, prefix / dest);
}

void LocalStorage::deleteObject(const string &key)
{
    boost::system::error_code err;
    
    boost::filesystem::remove(prefix / key, err);
}

int LocalStorage::exists(const std::string &key, bool *out)
{
    *out = boost::filesystem::exists(prefix / key);
    return 0;
}

}
