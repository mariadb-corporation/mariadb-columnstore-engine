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

#ifndef CONFIG_H_
#define CONFIG_H_

#include <boost/property_tree/ptree.hpp>
#include <boost/thread.hpp>
#include <sys/types.h>
#include <functional>
#include <vector>
#include <string>

/* TODO.  Need a config change listener impl. */

namespace storagemanager
{
class ConfigListener
{
 public:
  virtual ~ConfigListener(){};
  virtual void configListener() = 0;
};

class Config : public boost::noncopyable
{
 public:
  static Config* get();
  virtual ~Config();

  std::string getValue(const std::string& section, const std::string& key) const;

  // for testing, lets caller specify a config file to use
  static Config* get(const std::string&);

  void addConfigListener(ConfigListener* listener);
  void removeConfigListener(ConfigListener* listener);

 private:
  Config();
  Config(const std::string&);

  bool reload();
  void reloadThreadFcn();
  std::vector<ConfigListener*> configListeners;
  struct ::timespec last_mtime;
  mutable boost::mutex mutex;
  boost::thread reloader;
  boost::posix_time::time_duration reloadInterval;

  std::string filename;
  boost::property_tree::ptree contents;
  bool die;
};

}  // namespace storagemanager

#endif
