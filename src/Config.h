
#ifndef CONFIG_H_
#define CONFIG_H_

#include <boost/property_tree/ptree.hpp>
//#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>
#include <sys/types.h>

#include <string>


namespace storagemanager
{
class Config : public boost::noncopyable
{
    public:
        static Config *get();
        virtual ~Config();
        
        std::string getValue(const std::string &section, const std::string &key) const;
        
    private:
        Config();
        
        void reload();
        void reloadThreadFcn();
        struct ::timespec last_mtime;
        mutable boost::mutex mutex;
        boost::thread reloader;
        boost::posix_time::time_duration reloadInterval;
        
        std::string filename;
        boost::property_tree::ptree contents;
        bool die;
};

}

#endif
