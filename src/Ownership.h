#ifndef OWNERSHIP_H_
#define OWNERSHIP_H_

#include <boost/filesystem/path.hpp>
#include <boost/thread.hpp>
#include <map>
#include "SMLogging.h"

/* This class tracks the ownership of each prefix and manages ownership transfer. 
   Could we come up with a better name btw? */

namespace storagemanager
{

class Ownership : public boost::noncopyable
{
    public:
        Ownership();
        ~Ownership();

        bool sharedFS();
        // returns the path "right shifted" by prefixDepth, and with ownership of that path.
        // on error it returns an empty path.
        boost::filesystem::path get(const boost::filesystem::path &);
        
        
    private:
        uint prefixDepth;
        boost::filesystem::path metadataPrefix;
        SMLogging *logger;
        
        void touchFlushing(const boost::filesystem::path &, volatile bool *) const;
        void takeOwnership(const boost::filesystem::path &);
        void releaseOwnership(const boost::filesystem::path &, bool isDtor = false);
        void _takeOwnership(const boost::filesystem::path &);
        
        struct Monitor
        {
            Monitor(Ownership *);
            ~Monitor();
            boost::thread thread;
            Ownership *owner;
            volatile bool stop;
            void watchForInterlopers();
        };
        
        // maps a prefix to a state.  ownedPrefixes[p] == false means it's being init'd, == true means it's ready for use.
        std::map<boost::filesystem::path, bool> ownedPrefixes;
        Monitor *monitor;
        boost::mutex mutex;
};

inline bool Ownership::sharedFS()
{
    return prefixDepth >= 0;
}

}

#endif
