
#include "CloudStorage.h"
#include "Config.h"
#include "S3Storage.h"
#include <boost/thread/mutex.hpp>
#include <syslog.h>
#include <string>

using namespace std;

namespace
{
    boost::mutex m;
    storagemanager::CloudStorage *inst;
}

namespace storagemanager
{
CloudStorage * CloudStorage::get()
{
    if (inst)
        return inst;
        
    Config *conf = Config::get();
    string type = conf->getValue("ObjectStorage", "service");
    boost::mutex::scoped_lock s(m);
    if (inst)
        return inst;
    if (type == "s3" || type == "S3")
        inst = new S3Storage();
    else {
        syslog(LOG_CRIT, "CloudStorage: got unknown service provider: %s", type.c_str());
        return NULL;
    }
 
    return inst;
}

}
