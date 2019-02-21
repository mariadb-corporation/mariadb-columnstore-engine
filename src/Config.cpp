
#include "Config.h"
#include <boost/thread/mutex.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#include <stdlib.h>
#include <vector>

using namespace std;

namespace
{
    boost::mutex m;
    storagemanager::Config *inst = NULL;
}
    
namespace storagemanager
{

Config * Config::get()
{
    if (inst)
        return inst;
    boost::mutex::scoped_lock s(m);
    if (inst)
        return inst;
    inst = new Config();
    return inst;
}

Config::Config()
{
    /* This will search the current directory,
       then $COLUMNSTORE_INSTALL_DIR/etc,
       then /etc
       looking for storagemanager.cnf
       
       We can change this however we need later.
    */
    char *cs_install_dir = getenv("COLUMNSTORE_INSTALL_DIR");
    
    vector<string> paths;
    
    // the paths to search in order
    paths.push_back(".");
    if (cs_install_dir)
        paths.push_back(cs_install_dir);
    paths.push_back("/etc");
    
    for (int i = 0; i < paths.size(); i++)
    {
        if (boost::filesystem::exists(paths[i] + "/storagemanager.cnf"))
        {
            filename = paths[i] + "/storagemanager.cnf";
            break;
        }
    }
    if (filename.empty())
        throw runtime_error("Config: Could not find the config file for StorageManager");
        
    boost::property_tree::ini_parser::read_ini(filename, contents);
}

string use_envvar(boost::smatch envvar)
{
    char *env = getenv(envvar[1].str().c_str());
    return (env ? env : "");
}

string Config::getValue(const string &section, const string &key) const
{
    // if we care, move this envvar substition stuff to where the file is loaded
    string ret = contents.get<string>(section + "." + key);
    boost::regex re("\\$\\{(.+)\\}");
    
    ret = boost::regex_replace(ret, re, use_envvar);
    return ret;
}

}
