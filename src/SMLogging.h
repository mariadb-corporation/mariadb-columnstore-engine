
#ifndef SM_LOGGING_H_
#define SM_LOGGING_H_

#include <syslog.h>
#include <boost/thread.hpp>

namespace storagemanager
{

class SMLogging
{
    public:

        static SMLogging *get();
        ~SMLogging();

        void log(int priority, const char *format, ...);

    private:
        SMLogging();
        //SMConfig&  config;

};


}

#endif
