
#include "Synchronizer.h"
#include <boost/thread/mutex.hpp>

using namespace std;

namespace
{
    storagemanager::Synchronizer *instance = NULL;
    boost::mutex inst_mutex;
}


namespace storagemanager
{

Synchronizer * Synchronizer::get()
{
    if (instance)
        return instance;
    boost::unique_lock<boost::mutex> lock(inst_mutex);
    if (instance)
        return instance;
    instance = new Synchronizer();
    return instance;
}

Synchronizer::Synchronizer()
{
}

Synchronizer::~Synchronizer()
{
}

void Synchronizer::flushObject(const string &key)
{
}

}
