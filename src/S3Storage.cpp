
#include "S3Storage.h"
#include "libmarias3/marias3.h"
#include "Config.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <boost/filesystem.hpp>

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

S3Storage::S3Storage() : creds(NULL)
{
    /*  Check creds from envvars
        Get necessary vars from config
        Init an ms3_st object
    */
    char *key_id = getenv("AWS_ACCESS_KEY_ID");
    char *secret = getenv("AWS_SECRET_ACCESS_KEY");
    if (!key_id || !secret)
    {
        const char *msg = "S3 access requires setting the env vars AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY";
        logger->log(LOG_ERR, msg);
        throw runtime_error(msg);
    }
    
    Config *config = Config::get();
    string region = config->getValue("S3", "region");
    bucket = config->getValue("S3", "bucket");
    if (region.empty() || bucket.empty())
    {
        const char *msg = "S3 access requires setting S3/region and S3/bucket in storagemanager.cnf";
        logger->log(LOG_ERR, msg);
        throw runtime_error(msg);
    }
    
    string endpoint = config->getValue("S3", "endpoint");
    
    creds = ms3_init(key_id, secret, region.c_str(), (endpoint.empty() ? NULL : endpoint.c_str()));
    if (!creds)
    {
        const char *msg = "S3Storage: Failed to init s3 creds";
        logger->log(LOG_ERR, msg);
        throw runtime_error(msg);
    }
}

S3Storage::~S3Storage()
{
    ms3_deinit(creds);
}

int S3Storage::getObject(const string &sourceKey, const string &destFile, size_t *size)
{
    int fd, err;
    boost::shared_array<uint8_t> data;
    size_t len, count = 0;
    
    err = getObject(sourceKey, &data, &len);
    if (err)
        return err;
    
    fd = ::open(destFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
        return err;
    while (count < len)
    {
        err = ::write(fd, &data[count], len - count);
        if (err < 0)
        {
            char buf[80];
            int l_errno = errno;
            logger->log(LOG_ERR, "S3Storage::getObject(): Failed to write to %s, got %s", destFile.c_str(), 
              strerror_r(errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        count += err;
    }
    return 0;
}

int S3Storage::getObject(const string &sourceKey, boost::shared_array<uint8_t> *data, size_t *size)
{
    uint8_t err;
    size_t len = 0;
    uint8_t *_data = NULL;
    
    do {
        err = ms3_get(creds, bucket.c_str(), sourceKey.c_str(), &_data, &len);
        if (err)
        {
            logger->log(LOG_CRIT, "S3Storage::getObject(): failed to GET, got '%s'.  bucket = %s, key = %s.  Retrying...", 
                s3err_msgs[err], bucket.c_str(), sourceKey.c_str());
            sleep(5);
        }
    } while (err && retryable_error(err));
    if (err)
    {
        data->reset();
        errno = s3err_to_errno[err];
        return -1;
    }

    data->reset(_data);
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
    while (count < len)
    {
        err = ::read(fd, &data[count], len - count);
        if (err < 0)
        {
            int l_errno = errno;
            logger->log(LOG_ERR, "S3Storage::putObject(): Failed to read %s @ position %s, got %s", sourceFile.c_str(),
                count, strerror_r(l_errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        else if (err == 0)
        {
            // this shouldn't happen, we just checked the size
            logger->log(LOG_ERR, "S3Storage::putObject(): Got early EOF reading %s @ position %s", sourceFile.c_str(),
                count);
            errno = ENODATA;   // is there a better errno for early eof?
            return -1;
        }
        count += err;
    }
    
    return putObject(data, len, destKey);
}

int S3Storage::putObject(const boost::shared_array<uint8_t> data, size_t len, const string &destKey)
{
    uint8_t s3err;
    
    do {
        s3err = ms3_put(creds, bucket.c_str(), destKey.c_str(), data.get(), len);
        if (s3err)
        {
            logger->log(LOG_CRIT, "S3Storage::putObject(): failed to PUT, got '%s'.  bucket = %s, key = %s."
                "  Retrying...", s3err_msgs[s3err], bucket.c_str(), destKey.c_str());
            sleep(5);
        }
    } while (s3err && retryable_error(s3err));
    if (s3err)
    {
        errno = s3err_to_errno[s3err];
        return -1;
    }
    return 0;
}

void S3Storage::deleteObject(const string &key)
{
    uint8_t s3err;
    
    do {
        s3err = ms3_delete(creds, bucket.c_str(), key.c_str());
        if (s3err && s3err != MS3_ERR_NOT_FOUND)
        {
            logger->log(LOG_CRIT, "S3Storage::deleteObject(): failed to DELETE, got '%s'.  bucket = %s, key = %s.  Retrying...", 
                s3err_msgs[s3err], bucket.c_str(), key.c_str());
            sleep(5);
        }
    } while (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err));
}

int S3Storage::copyObject(const string &sourceKey, const string &destKey)
{
    // no s3-s3 copy yet.  get & put for now.
    
    int err;
    boost::shared_array<uint8_t> data;
    size_t len;
    err = getObject(sourceKey, &data, &len);
    if (err)
        return err;
    err = putObject(data, len, destKey);
    if (err)
        return err;
}

int S3Storage::exists(const string &key, bool *out)
{
    uint8_t s3err;
    ms3_status_st status;
    
    do {
        s3err = ms3_status(creds, bucket.c_str(), key.c_str(), &status);
        if (s3err && s3err != MS3_ERR_NOT_FOUND)
        {
            logger->log(LOG_CRIT, "S3Storage::exists(): failed to HEAD, got '%s'.  bucket = %s, key = %s.  Retrying...",
                s3err_msgs[s3err], bucket.c_str(), key.c_str());
            sleep(5);
        }
    } while (s3err && s3err != MS3_ERR_NOT_FOUND && retryable_error(s3err));
    
    if (s3err)
    {
        errno = s3err_to_errno[s3err];
        return -1;
    }
        
    *out = (s3err == 0);
    return 0;
}

}
