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


#include "S3Storage.h"
#include "Config.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <boost/filesystem.hpp>
#include <iostream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include "Utilities.h"

using namespace std;

namespace storagemanager
{

inline bool retryable_error(uint8_t s3err)
{
    return (
        s3err == MS3_ERR_RESPONSE_PARSE || 
        s3err == MS3_ERR_REQUEST_ERROR ||
        s3err == MS3_ERR_OOM ||
        s3err == MS3_ERR_IMPOSSIBLE ||
        s3err == MS3_ERR_SERVER
    );
}

// Best effort to map the errors returned by the ms3 API to linux errnos
// Can and should be changed if we find better mappings.  Some of these are
// nonsensical or misleading, so we should allow non-errno values to be propagated upward.
const int s3err_to_errno[] = {
    0,
    EINVAL,     // 1 MS3_ERR_PARAMETER.  Best effort.  D'oh.
    ENODATA,    // 2 MS3_ERR_NO_DATA
    ENAMETOOLONG,  // 3 MS3_ERR_URI_TOO_LONG
    EBADMSG,    // 4 MS3_ERR_RESPONSE_PARSE
    ECOMM,      // 5 MS3_ERR_REQUEST_ERROR
    ENOMEM,     // 6 MS3_ERR_OOM
    EINVAL,     // 7 MS3_ERR_IMPOSSIBLE.  Will have to look through the code to find out what this is exactly.
    EKEYREJECTED,  // 8 MS3_ERR_AUTH
    ENOENT,     // 9 MS3_ERR_NOT_FOUND
    EPROTO,     // 10 MS3_ERR_SERVER
    EMSGSIZE    // 11 MS3_ERR_TOO_BIG
};

const char *s3err_msgs[] = {
    "All is well",
    "A required function parameter is missing",
    "No data is supplied to a function that requires data",
    "The generated URI for the request is too long",
    "The API could not parse the response",
    "The API could not send the request",
    "Could not allocate required memory",
    "An impossible condition occurred, how unlucky are you?",
    "Authentication failed",
    "Object not found",
    "Unknown error code in response",
    "Data to PUT is too large; 4GB maximum length"
};


S3Storage::ScopedConnection::ScopedConnection(S3Storage *s, ms3_st *m) : s3(s), conn(m)
{
    assert(conn);
}

S3Storage::ScopedConnection::~ScopedConnection()
{
    s3->returnConnection(conn);
}

S3Storage::S3Storage()
{
    /*  Check creds from envvars
        Get necessary vars from config
        Init an ms3_st object
    */
    Config *config = Config::get();
    
    const char *keyerr = "S3 access requires setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars, "
        " or setting aws_access_key_id and aws_secret_access_key in storagemanager.cnf";
    key = config->getValue("S3", "aws_access_key_id");
    secret = config->getValue("S3", "aws_secret_access_key");
    if (key.empty())
    {
        char *_key_id = getenv("AWS_ACCESS_KEY_ID");
        if (!_key_id)
        {
            logger->log(LOG_ERR, keyerr);
            throw runtime_error(keyerr);
        }
        key = _key_id;
    }
    if (secret.empty())
    {
        char *_secret_id = getenv("AWS_SECRET_ACCESS_KEY");
        if (!_secret_id)
        {
            logger->log(LOG_ERR, keyerr);
            throw runtime_error(keyerr);
        }
        secret = _secret_id;
    }
    
    region = config->getValue("S3", "region");
    bucket = config->getValue("S3", "bucket");
    prefix = config->getValue("S3", "prefix");
    if (bucket.empty())
    {
        const char *msg = "S3 access requires setting S3/bucket in storagemanager.cnf";
        logger->log(LOG_ERR, msg);
        throw runtime_error(msg);
    }
    
    endpoint = config->getValue("S3", "endpoint");

    ms3_library_init();
    //ms3_debug(true);
    testConnectivityAndPerms();
}

S3Storage::~S3Storage()
{
    for (auto &conn : freeConns)
        ms3_deinit(conn.conn);
    ms3_library_deinit();
}

// convenience macro for testConnectivityAndPerms()
#define FAIL(OP) { \
        const char *msg = "S3Storage: failed to " #OP ", check log files for specific error"; \
        logger->log(LOG_ERR, msg); \
        throw runtime_error(msg); \
}

void S3Storage::testConnectivityAndPerms()
{
    boost::shared_array<uint8_t> testObj(new uint8_t[1]);
    testObj[0] = 0;
    boost::uuids::uuid u = boost::uuids::random_generator()();
    ostringstream oss;
    oss << u << "connectivity_test";
    
    string testObjKey = oss.str();
    
    int err = putObject(testObj, 1, testObjKey);
    if (err)
        FAIL(PUT)
    bool _exists;
    err = exists(testObjKey, &_exists);
    if (err)
        FAIL(HEAD)
    size_t len;
    err = getObject(testObjKey, &testObj, &len);
    if (err)
        FAIL(GET)
    err = deleteObject(testObjKey);
    if (err)
        FAIL(DELETE)
    logger->log(LOG_INFO, "S3Storage: S3 connectivity & permissions are OK");
}

int S3Storage::getObject(const string &sourceKey, const string &destFile, size_t *size)
{
    int fd, err;
    boost::shared_array<uint8_t> data;
    size_t len, count = 0;
    char buf[80];
    
    err = getObject(sourceKey, &data, &len);
    if (err)
        return err;
    
    fd = ::open(destFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        int l_errno = errno;
        logger->log(LOG_ERR, "S3Storage::getObject(): Failed to open %s, got %s", destFile.c_str(), 
            strerror_r(l_errno, buf, 80));
        errno = l_errno;
        return err;
    }
    ScopedCloser s(fd);
    while (count < len)
    {
        err = ::write(fd, &data[count], len - count);
        if (err < 0)
        {
            int l_errno = errno;
            logger->log(LOG_ERR, "S3Storage::getObject(): Failed to write to %s, got %s", destFile.c_str(), 
              strerror_r(errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        count += err;
    }
    if (size)
        *size = len;
    return 0;
}

int S3Storage::getObject(const string &_sourceKey, boost::shared_array<uint8_t> *data, size_t *size)
{
    uint8_t err;
    size_t len = 0;
    uint8_t *_data = NULL;
    string sourceKey = prefix + _sourceKey;
    
    ms3_st *creds = getConnection();
    ScopedConnection sc(this, creds);
    
    do {
        err = ms3_get(creds, bucket.c_str(), sourceKey.c_str(), &_data, &len);
        if (err && retryable_error(err))
        { 
            if (ms3_server_error(creds))
                logger->log(LOG_ERR, "S3Storage::getObject(): failed to GET, server says '%s'.  bucket = %s, key = %s."
                    "  Retrying...", ms3_server_error(creds), bucket.c_str(), sourceKey.c_str());
            else 
                logger->log(LOG_ERR, "S3Storage::getObject(): failed to GET, got '%s'.  bucket = %s, key = %s.  Retrying...",
                    s3err_msgs[err], bucket.c_str(), sourceKey.c_str());
            sleep(5);
        }
    } while (err && retryable_error(err));
    if (err)
    {
        if (ms3_server_error(creds))
            logger->log(LOG_ERR, "S3Storage::getObject(): failed to GET, server says '%s'.  bucket = %s, key = %s.",
                ms3_server_error(creds), bucket.c_str(), sourceKey.c_str());
        else
            logger->log(LOG_ERR, "S3Storage::getObject(): failed to GET, got '%s'.  bucket = %s, key = %s.",
                s3err_msgs[err], bucket.c_str(), sourceKey.c_str());
        data->reset();
        errno = s3err_to_errno[err];
        return -1;
    }

    data->reset(_data, free);
    if (size)
        *size = len;
    return 0;
}


int S3Storage::putObject(const string &sourceFile, const string &destKey)
{
    boost::shared_array<uint8_t> data;
    int err, fd;
    size_t len, count = 0;
    char buf[80];
    boost::system::error_code boost_err;
    
    len = boost::filesystem::file_size(sourceFile, boost_err);
    if (boost_err)
    {
        errno = boost_err.value();
        return -1;
    }
    data.reset(new uint8_t[len]);
    fd = ::open(sourceFile.c_str(), O_RDONLY);
    if (fd < 0)
    {
        int l_errno = errno;
        logger->log(LOG_ERR, "S3Storage::putObject(): Failed to open %s, got %s", sourceFile.c_str(), 
            strerror_r(l_errno, buf, 80));
        errno = l_errno;
        return -1;
    }
    ScopedCloser s(fd);
    while (count < len)
    {
        err = ::read(fd, &data[count], len - count);
        if (err < 0)
        {
            int l_errno = errno;
            logger->log(LOG_ERR, "S3Storage::putObject(): Failed to read %s @ position %ld, got %s", sourceFile.c_str(),
                count, strerror_r(l_errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        else if (err == 0)
        {
            // this shouldn't happen, we just checked the size
            logger->log(LOG_ERR, "S3Storage::putObject(): Got early EOF reading %s @ position %ld", sourceFile.c_str(),
                count);
            errno = ENODATA;   // is there a better errno for early eof?
            return -1;
        }
        count += err;
    }
    
    return putObject(data, len, destKey);
}

int S3Storage::putObject(const boost::shared_array<uint8_t> data, size_t len, const string &_destKey)
{
    string destKey = prefix + _destKey;
    uint8_t s3err;
    ms3_st *creds = getConnection();
    ScopedConnection sc(this, creds);
    
    do {
        s3err = ms3_put(creds, bucket.c_str(), destKey.c_str(), data.get(), len);
        if (s3err && retryable_error(s3err))
        {
            if (ms3_server_error(creds))
                logger->log(LOG_ERR, "S3Storage::putObject(): failed to PUT, server says '%s'.  bucket = %s, key = %s."
                    "  Retrying...", ms3_server_error(creds), bucket.c_str(), destKey.c_str());
            else
                logger->log(LOG_ERR, "S3Storage::putObject(): failed to PUT, got '%s'.  bucket = %s, key = %s."
                    "  Retrying...", s3err_msgs[s3err], bucket.c_str(), destKey.c_str());
            sleep(5);
        }
    } while (s3err && retryable_error(s3err));
    if (s3err)
    {
        if (ms3_server_error(creds))
            logger->log(LOG_ERR, "S3Storage::putObject(): failed to PUT, server says '%s'.  bucket = %s, key = %s.",
                ms3_server_error(creds), bucket.c_str(), destKey.c_str());
        else
            logger->log(LOG_ERR, "S3Storage::putObject(): failed to PUT, got '%s'.  bucket = %s, key = %s.",
                s3err_msgs[s3err], bucket.c_str(), destKey.c_str());
        errno = s3err_to_errno[s3err];
        return -1;
    }
    return 0;
}

int S3Storage::deleteObject(const string &_key)
{
    uint8_t s3err;
    string key = prefix + _key;
    ms3_st *creds = getConnection();
    ScopedConnection sc(this, creds);
        
    do {
        s3err = ms3_delete(creds, bucket.c_str(), key.c_str());
        if (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err))
        {
            if (ms3_server_error(creds))
                logger->log(LOG_ERR, "S3Storage::deleteObject(): failed to DELETE, server says '%s'.  bucket = %s, key = %s."
                    "  Retrying...", ms3_server_error(creds), bucket.c_str(), key.c_str());
            else
                logger->log(LOG_ERR, "S3Storage::deleteObject(): failed to DELETE, got '%s'.  bucket = %s, key = %s.  Retrying...",
                    s3err_msgs[s3err], bucket.c_str(), key.c_str());
            sleep(5);
        }
    } while (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err));
    
    if (s3err != 0 && s3err != MS3_ERR_NOT_FOUND)
    {
        if (ms3_server_error(creds))
            logger->log(LOG_ERR, "S3Storage::deleteObject(): failed to DELETE, server says '%s'.  bucket = %s, key = %s.",
                ms3_server_error(creds), bucket.c_str(), key.c_str());
        else
            logger->log(LOG_ERR, "S3Storage::deleteObject(): failed to DELETE, got '%s'.  bucket = %s, key = %s.",
                s3err_msgs[s3err], bucket.c_str(), key.c_str());
        return -1;        
    }
    return 0;
}

int S3Storage::copyObject(const string &_sourceKey, const string &_destKey)
{
    string sourceKey = prefix + _sourceKey, destKey = prefix + _destKey;
    uint8_t s3err;
    ms3_st *creds = getConnection();
    ScopedConnection sc(this, creds);
    
    do 
    {
        s3err = ms3_copy(creds, bucket.c_str(), sourceKey.c_str(), bucket.c_str(), destKey.c_str());
        if (s3err && retryable_error(s3err))
        {
            if (ms3_server_error(creds))
                logger->log(LOG_ERR, "S3Storage::copyObject(): failed to copy, server says '%s'.  bucket = %s, srckey = %s, "
                    "destkey = %s.  Retrying...", ms3_server_error(creds), bucket.c_str(), sourceKey.c_str(), destKey.c_str());
            else
                logger->log(LOG_ERR, "S3Storage::copyObject(): failed to copy, got '%s'.  bucket = %s, srckey = %s, "
                    " destkey = %s.  Retrying...", s3err_msgs[s3err], bucket.c_str(), sourceKey.c_str(), destKey.c_str());
            sleep(5);
        }
    } while (s3err && retryable_error(s3err));
    
    if (s3err) 
    {
        // added the add'l check MS3_ERR_NOT_FOUND to suppress error msgs for a legitimate case in IOC::copyFile()
        if (ms3_server_error(creds) && s3err != MS3_ERR_NOT_FOUND)
            logger->log(LOG_ERR, "S3Storage::copyObject(): failed to copy, server says '%s'.  bucket = %s, srckey = %s, "
                "destkey = %s.", ms3_server_error(creds), bucket.c_str(), sourceKey.c_str(), destKey.c_str());
        else if (s3err != MS3_ERR_NOT_FOUND)
            logger->log(LOG_ERR, "S3Storage::copyObject(): failed to copy, got '%s'.  bucket = %s, srckey = %s, "
                "destkey = %s.", s3err_msgs[s3err], bucket.c_str(), sourceKey.c_str(), destKey.c_str());
        errno = s3err_to_errno[s3err];
        return -1;
    }
    return 0;
    
#if 0
    // no s3-s3 copy yet.  get & put for now.
    
    int err;
    boost::shared_array<uint8_t> data;
    size_t len;
    err = getObject(sourceKey, &data, &len);
    if (err)
        return err;
    return putObject(data, len, destKey);
#endif
}

int S3Storage::exists(const string &_key, bool *out)
{
    string key = prefix + _key;
    uint8_t s3err;
    ms3_status_st status;
    ms3_st *creds = getConnection();
    ScopedConnection sc(this, creds);
    
    do {
        s3err = ms3_status(creds, bucket.c_str(), key.c_str(), &status);
        if (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err))
        {
            if (ms3_server_error(creds))
                logger->log(LOG_ERR, "S3Storage::exists(): failed to HEAD, server says '%s'.  bucket = %s, key = %s."
                    "  Retrying...", ms3_server_error(creds), bucket.c_str(), key.c_str());
            else
                logger->log(LOG_ERR, "S3Storage::exists(): failed to HEAD, got '%s'.  bucket = %s, key = %s.  Retrying...",
                    s3err_msgs[s3err], bucket.c_str(), key.c_str());
            sleep(5);
        }
    } while (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err));
    
    if (s3err != 0 && s3err != MS3_ERR_NOT_FOUND)
    {
        if (ms3_server_error(creds))
            logger->log(LOG_ERR, "S3Storage::exists(): failed to HEAD, server says '%s'.  bucket = %s, key = %s.",
                ms3_server_error(creds), bucket.c_str(), key.c_str());
        else    
            logger->log(LOG_ERR, "S3Storage::exists(): failed to HEAD, got '%s'.  bucket = %s, key = %s.",
                s3err_msgs[s3err], bucket.c_str(), key.c_str());
        errno = s3err_to_errno[s3err];
        return -1;
    }
        
    *out = (s3err == 0);
    return 0;
}

ms3_st * S3Storage::getConnection()
{
    boost::unique_lock<boost::mutex> s(connMutex);
    
    // prune the list.  Most-idle connections are at the back.
    timespec now;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now);
    while (!freeConns.empty())
    {
        Connection &back = freeConns.back();
        if (back.idleSince.tv_sec + maxIdleSecs <= now.tv_sec)
        {
            ms3_deinit(back.conn);
            //connMutexes.erase(back.conn);
            back.conn = NULL;
            freeConns.pop_back();
        }
        else
            break;  // everything in front of this entry will not have been idle long enough
    }
            
    // get a connection
    ms3_st *ret = NULL;
    if (freeConns.empty())
    {
        ret = ms3_init(key.c_str(), secret.c_str(), region.c_str(), (endpoint.empty() ? NULL : endpoint.c_str()));
        if (ret == NULL)
            logger->log(LOG_ERR, "S3Storage::getConnection(): ms3_init returned NULL, no specific info to report");
        //assert(connMutexes[ret].try_lock());
        s.unlock();
        return ret;
    }
    assert(freeConns.front().idleSince.tv_sec + maxIdleSecs > now.tv_sec);
    ret = freeConns.front().conn;
    freeConns.pop_front();
    //assert(connMutexes[ret].try_lock());
    return ret;
}

void S3Storage::returnConnection(ms3_st *ms3)
{
    assert(ms3);
    Connection conn;
    conn.conn = ms3;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &conn.idleSince);
    
    boost::unique_lock<boost::mutex> s(connMutex);
    freeConns.push_front(conn);
    //connMutexes[ms3].unlock();
}

}
