
#include <stdarg.h>
#include <unistd.h>
#include "SMLogging.h"

using namespace std;

namespace
{
storagemanager::SMLogging *smLog = NULL;
boost::mutex m;
};

namespace storagemanager
{

SMLogging::SMLogging()
{
    //TODO: make this configurable
    setlogmask (LOG_UPTO (LOG_DEBUG));
    openlog ("StorageManager", LOG_PID | LOG_NDELAY, LOG_LOCAL2);
}

SMLogging::~SMLogging()
{
    syslog(LOG_INFO, "CloseLog");
    closelog();
}

SMLogging * SMLogging::get()
{
    if (smLog)
        return smLog;
    boost::mutex::scoped_lock s(m);
    if (smLog)
        return smLog;
    smLog = new SMLogging();
    return smLog;
}

void SMLogging::log(int priority,const char *format, ...)
{
    va_list args;
    va_start(args, format);
    
    #ifdef DEBUG
    va_list args2;
    va_copy(args2, args);
    vfprintf(stderr, format, args2);
    fprintf(stderr, "\n");
    #endif
    vsyslog(priority, format, args);

    va_end(args);
}

}
