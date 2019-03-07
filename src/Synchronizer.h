
#ifndef SYNCHRONIZER_H_
#define SYNCHRONIZER_H_

#include <string>
#include <boost/utility.hpp>

namespace storagemanager
{

class Synchronizer : public boost::noncopyable
{
    public:
        static Synchronizer *get();
        virtual ~Synchronizer();
        
        void flushObject(const std::string &key);

    private:
        Synchronizer();
};

}
#endif
