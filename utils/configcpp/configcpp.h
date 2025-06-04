/* Copyright (C) 2014 InfiniDB, Inc.

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

/******************************************************************************************
 * $Id: configcpp.h 3495 2013-01-21 14:09:51Z rdempsey $
 *
 ******************************************************************************************/
/**
 * @file
 */
#pragma once

#include <string>
#include <map>
#include <vector>
#include <stdint.h>
#include <boost/thread.hpp>

#include <sys/types.h>
#include <libxml/parser.h>

#include "xmlparser.h"

namespace messageqcpp

{
class ByteStream;

}

namespace config
{
/** @brief a config file I/F class
 *
 * This is a singleton pattern: you must use the makeConfig() factory method to get a
 * pointer to the config file. You can have as many config files open at the same time as you like,
 * but you need different pointers to each. This class handles locking internally using an NFS-compatible
 * fcntl() call.
 */
class Config
{
 public:
  /** @brief Config factory method
   *
   * Creates a singleton Config object
   */
  static Config* makeConfig(const char* cf = 0);

  /** @brief Config factory method
   *
   * Creates a singleton Config object
   */
  static Config* makeConfig(const std::string& cf);

  /** @brief dtor
   */
  virtual ~Config();

  /** @brief get name's value from section
   *
   * get name's value from section in the current config file.
   * @param section the name of the config file section to search
   * @param name the param name whose value is to be returned
   */
  const std::string getConfig(const std::string& section, const std::string& name);

  /** @brief get name's value from section
   *
   * get name's value from section in the current config file re-reading the
   * config file if it was updated.
   * @param section the name of the config file section to search
   * @param name the param name whose value is to be returned
   */
  const std::string getFromActualConfig(const std::string& section, const std::string& name);

  /** @brief get all name's values from a section
   *
   * get name's values from section in the current config file.
   * @param section the name of the config file section to search
   * @param name the param name whose value is to be returned
   * @param values the values in the section are returned in this vector
   */
  void getConfig(const std::string& section, const std::string& name, std::vector<std::string>& values);

  /** @brief set name's value in section
   *
   * set name's value in section in the current config file.
   * @param section the name of the config file section to update
   * @param name the param name whose value is to be updated
   * @param value the param value
   */
  // !!!Don't ever ever use this in the engine code b/c it might result in a race
  // b/w getConfig and setConfig methods.!!!
  void setConfig(const std::string& section, const std::string& name, const std::string& value);

  /** @brief delete name from section
   *
   * delete name from section in the current config file.
   * @param section the name of the config file section to search
   * @param name the param name whose entry is to be deleted
   * @note if you delete the last param from a section, the section will still remain
   */
  void delConfig(const std::string& section, const std::string& name);

  /** @brief write the config file back out to disk
   *
   * write the config file back out to disk using the current filename.
   */
  void write(void) const;

  /** @brief write the config file back out to disk as fileName
   *
   * write the config file out to disk as a new file fileName. Does not affect the current
   * config filename.
   */
  void write(const std::string& fileName) const;

  /** @brief return the name of this config file
   *
   * return the name of this config file.
   */

  static std::string configDefaultFileName()
  {
    static std::string defaultConfigName = "Columnstore.xml";
    return defaultConfigName;
  }

  inline const std::string& configFile() const
  {
    return fConfigFile;
  }

  /** @brief delete all config file instances
   *
   * deletes \b all config file maps
   */
  static void deleteInstanceMap();

  /** @brief parse config file numerics
   *
   * Convert human-friendly number formats to machine-friendly. Handle suffixes 'K', 'M', 'G'.
   * Handle decimal, hex and octal notation in the same way as the C compiler.
   * Ignore any 'B' following [KMG]. Ignore case in suffixes.
   * An empty string or an unparseable string returns 0.
   * Return a signed numeric value.
   */
  static int64_t fromText(const std::string& text);

  /** @brief parse config file numerics
   *
   * Return an unsigned numeric value.
   */
  static inline uint64_t uFromText(const std::string& text)
  {
    return static_cast<uint64_t>(fromText(text));
  }

  /** @brief Used externally to check whether there has been a config change without loading everything
   *
   */
  inline time_t getLastMTime() const
  {
    return fMtime;
  }

  /** @brief Used externally to check whether there has been a config change without loading everything
   *
   */
  time_t getCurrentMTime();

  /** @brief Enumerate all the sections in the config file
   *
   */
  const std::vector<std::string> enumConfig();

  /** @brief Enumerate all the names in a section in the config file
   *
   */
  const std::vector<std::string> enumSection(const std::string& section);

  enum class TempDirPurpose
  {
    Joins,      ///< disk joins
    Aggregates  ///< disk-based aggregation
  };
  /** @brief Return temporaru directory path for the specified purpose */
  std::string getTempFileDir(TempDirPurpose what);

 protected:
  /** @brief parse the XML file
   *
   */
  void parseDoc(void);

  /** @brief write the XML tree to disk
   *
   */
  void writeConfig(const std::string& fileName) const;

  /** @brief stop processing this XML file
   *
   */
  void closeConfig(void);

 private:
  typedef std::map<std::string, std::unique_ptr<Config>> configMap_t;

  static configMap_t& instanceMap()
  {
    static configMap_t instanceMap;
    return instanceMap;
  }

  static boost::mutex& instanceMapMutex()
  {
    static boost::mutex instanceMapMutex;
    return instanceMapMutex;
  }

  static boost::mutex& xmlMutex()
  {
    static boost::mutex xmlMutex;
    return xmlMutex;
  }

  static boost::mutex& writeXmlMutex()
  {
    static boost::mutex writeXmlMutex;
    return writeXmlMutex;
  }

  static Config& globConfigInstance();

  Config(const Config& rhs);
  Config& operator=(const Config& rhs);
  Config(const std::string& configFile);

  xmlDocPtr fDoc;
  const std::string fConfigFile;
  time_t fMtime;
  mutable boost::recursive_mutex fLock;
  XMLParser fParser;

  /** @brief Re-read the config file if it was updated
   *
   */
  void checkAndReloadConfig();
};

}  // namespace config

#undef EXPORT
