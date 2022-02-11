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

#ifndef OWNERSHIP_H_
#define OWNERSHIP_H_

#include <boost/filesystem/path.hpp>
#include <boost/noncopyable.hpp>
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
  // on error it throws a runtime exception
  // setting getOwnership to false will return the modified path but not also take ownership
  // of the returned prefix.
  boost::filesystem::path get(const boost::filesystem::path&, bool getOwnership = true);

 private:
  int prefixDepth;
  boost::filesystem::path metadataPrefix;
  SMLogging* logger;

  void touchFlushing(const boost::filesystem::path&, volatile bool*) const;
  void takeOwnership(const boost::filesystem::path&);
  void releaseOwnership(const boost::filesystem::path&, bool isDtor = false);
  void _takeOwnership(const boost::filesystem::path&);

  struct Monitor
  {
    Monitor(Ownership*);
    ~Monitor();
    boost::thread thread;
    Ownership* owner;
    volatile bool stop;
    void watchForInterlopers();
  };

  // maps a prefix to a state.  ownedPrefixes[p] == false means it's being init'd, == true means it's ready
  // for use.
  std::map<boost::filesystem::path, bool> ownedPrefixes;
  Monitor* monitor;
  boost::mutex mutex;
};

inline bool Ownership::sharedFS()
{
  return prefixDepth >= 0;
}

}  // namespace storagemanager

#endif
