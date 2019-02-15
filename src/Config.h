
#ifndef CONFIG_H_
#define CONFIG_H_

#include <boost/property_tree/ptree.hpp>
#include <string>


namespace storagemanager
{
class Config : public boost::noncopyable
{
    public:
        static Config *get();
        
        std::string getValue(const std::string &section, const std::string &key) const;
        
    private:
        Config();
        
        std::string filename;
        boost::property_tree::ptree contents;
};

}

#endif
