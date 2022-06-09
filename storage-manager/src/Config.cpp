/* Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "Config.h"

// This one is the build system config
#include "mcsconfig.h"

#include <boost/thread/mutex.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#include <stdlib.h>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>

#include "SMLogging.h"

namespace bf = boost::filesystem;
using namespace std;

namespace
{
boost::mutex m;
storagemanager::Config* inst = NULL;
}  // namespace

namespace storagemanager
{
Config* Config::get()
{
  if (inst)
    return inst;
  boost::mutex::scoped_lock s(m);
  if (inst)
    return inst;
  inst = new Config();
  return inst;
}

Config* Config::get(const string& configFile)
{
  if (inst)
    return inst;
  boost::mutex::scoped_lock s(m);
  if (inst)
    return inst;
  inst = new Config(configFile);
  return inst;
}

Config::Config() : die(false)
{
  /* This will search the current directory,
     then $COLUMNSTORE_INSTALL_DIR/etc,
     then /etc
     looking for storagemanager.cnf

     We can change this however we need later.
  */
  // char *cs_install_dir = getenv("COLUMNSTORE_INSTALL_DIR");

  vector<string> paths;

  // the paths to search in order
  paths.push_back(".");
  paths.push_back(string(MCSSYSCONFDIR) + "/columnstore");
  paths.push_back("/etc");

  for (uint i = 0; i < paths.size(); i++)
  {
    // cout << "Config: searching '" << paths[i] << "'" << endl;
    if (boost::filesystem::exists(paths[i] + "/storagemanager.cnf"))
    {
      filename = paths[i] + "/storagemanager.cnf";
      SMLogging::get()->log(LOG_DEBUG, "Using the config file found at %s", filename.c_str());
      break;
    }
  }
  if (filename.empty())
    throw runtime_error("Config: Could not find the config file for StorageManager");

  reloadInterval = boost::posix_time::seconds(60);
  last_mtime = {0, 0};
  reload();
  reloader = boost::thread([this] { this->reloadThreadFcn(); });
}

Config::Config(const string& configFile) : filename(configFile), die(false)
{
  if (!bf::is_regular_file(configFile))
    throw runtime_error("Config: Could not find the config file for StorageManager");

  reloadInterval = boost::posix_time::seconds(60);
  last_mtime = {0, 0};
  reload();
  reloader = boost::thread([this] { this->reloadThreadFcn(); });
}

Config::~Config()
{
  die = true;
  reloader.interrupt();
  reloader.join();
}

void Config::reloadThreadFcn()
{
  while (!die)
  {
    try
    {
      if (reload())
      {
        for (auto& listener : configListeners)
          listener->configListener();
      }
      boost::this_thread::sleep(reloadInterval);
    }
    catch (boost::property_tree::ini_parser_error& e)
    {
      // log the error.  how to log things now?
    }
    catch (boost::thread_interrupted)
    {
    }
  }
}

bool Config::reload()
{
  bool rtn = false;
  struct stat statbuf;
  int err = stat(filename.c_str(), &statbuf);
  if (err)
  {
    SMLogging::get()->log(LOG_ERR, "Config::reload error %s", filename.c_str());
    return rtn;
  }
  if ((statbuf.st_mtim.tv_sec == last_mtime.tv_sec) && (statbuf.st_mtim.tv_nsec == last_mtime.tv_nsec))
    return rtn;
  last_mtime = statbuf.st_mtim;
  rtn = true;
  boost::unique_lock<boost::mutex> s(mutex);
  contents.clear();
  boost::property_tree::ini_parser::read_ini(filename, contents);
  return rtn;
}

string use_envvar(const boost::smatch& envvar)
{
  char* env = getenv(envvar[1].str().c_str());
  return (env ? env : "");
}

string expand_numbers(const boost::smatch& match)
{
  long long num = stol(match[1].str());
  char suffix = (char)::tolower(match[2].str()[0]);

  if (suffix == 't')
    num <<= 40;
  else if (suffix == 'g')
    num <<= 30;
  else if (suffix == 'm')
    num <<= 20;
  else if (suffix == 'k')
    num <<= 10;
  return ::to_string(num);
}

string Config::getValue(const string& section, const string& key) const
{
  // if we care, move this envvar substition stuff to where the file is loaded
  string ret;
  boost::unique_lock<boost::mutex> s(mutex);
  try
  {
    ret = contents.get<string>(section + "." + key);
  }
  catch (...)
  {
    return "";  // debating whether it's necessary to tell the caller there was no entry.
  }
  s.unlock();

  boost::regex re("\\$\\{(.+)\\}");

  ret = boost::regex_replace(ret, re, use_envvar);

  // do the numeric substitutions.  ex, the suffixes m, k, g
  // ehhhhh.  going to end up turning a string to a number, to a string, and then to a number again
  // don't like that.  OTOH who cares.
  boost::regex num_re("^([[:digit:]]+)([mMkKgG])$", boost::regex::extended);
  ret = boost::regex_replace(ret, num_re, expand_numbers);

  return ret;
}

void Config::addConfigListener(ConfigListener* listener)
{
  configListeners.push_back(listener);
}

void Config::removeConfigListener(ConfigListener* listener)
{
  auto iterator = std::find(configListeners.begin(), configListeners.end(), listener);

  if (iterator != configListeners.end())
    configListeners.erase(iterator);
}

}  // namespace storagemanager
