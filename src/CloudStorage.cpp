
#include "CloudStorage.h"
#include "Config.h"
#include "S3Storage.h"
#include "LocalStorage.h"
#include "SMLogging.h"
#include <boost/thread/mutex.hpp>
#include <string>
#include <ctype.h>

using namespace std;

namespace
{
    boost::mutex m;
    storagemanager::CloudStorage *inst = NULL;
    
string tolower(const string &s)
{
    string ret(s);
    for (uint i = 0; i < ret.length(); i++)
        ret[i] = ::tolower(ret[i]);
    return ret;
}

}

namespace storagemanager
{

CloudStorage * CloudStorage::get()
{
    if (inst)
        return inst;
    
    SMLogging* logger = SMLogging::get();
    Config *conf = Config::get();
    string type = tolower(conf->getValue("ObjectStorage", "service"));
    boost::mutex::scoped_lock s(m);
    if (inst)
        return inst;
    if (type == "s3")
        inst = new S3Storage();
    else if (type == "local" || type == "localstorage")
        inst = new LocalStorage();
    else {
        logger->log(LOG_CRIT,"CloudStorage: got unknown service provider: %s", type.c_str());
        throw runtime_error("CloudStorage: got unknown service provider");
    }
 
    return inst;
}

CloudStorage::CloudStorage()
{
    logger = SMLogging::get();
}

}
