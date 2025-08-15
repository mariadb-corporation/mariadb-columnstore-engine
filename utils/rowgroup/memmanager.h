/* Copyright (C) 2021-2025 MariaDB Corporation

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

#pragma once

#include <cstdint>
#include <bits/types.h>

#include "resourcemanager.h"

// TODO change namespace
namespace rowgroup
{
/** @brief Some service wrapping around ResourceManager (or NoOP) */

class MemManager
{
 public:
  MemManager()
  {
  }
  virtual ~MemManager()
  {
    release(fMemUsed);
  }

  bool acquire(std::size_t amount)
  {
    return acquireImpl(amount);
  }
  void release(ssize_t amount = 0)
  {
    // in some cases it tries to release more memory than acquired, ie create
    // new rowgroup, acquire maximum size (w/o strings), add some rows with
    // strings and finally release the actual size of RG with strings
    if (amount == 0 || amount > fMemUsed)
      amount = fMemUsed;
    releaseImpl(amount);
  }

  ssize_t getUsed() const
  {
    return fMemUsed;
  }
  virtual int64_t getFree() const
  {
    return std::numeric_limits<int64_t>::max();
  }

  virtual int64_t getConfigured() const
  {
    return std::numeric_limits<int64_t>::max();
  }

  virtual bool isStrict() const
  {
    return false;
  }

  virtual MemManager* clone() const
  {
    return new MemManager();
  }

  virtual joblist::ResourceManager* getResourceManaged()
  {
    return nullptr;
  }
  virtual boost::shared_ptr<int64_t> getSessionLimit()
  {
    return {};
  }

 protected:
  virtual bool acquireImpl(std::size_t amount)
  {
    fMemUsed += amount;
    return true;
  }
  virtual void releaseImpl(std::size_t amount)
  {
    fMemUsed -= amount;
  }
  ssize_t fMemUsed = 0;
};

class RMMemManager : public MemManager
{
 public:
  RMMemManager(joblist::ResourceManager* rm, boost::shared_ptr<int64_t> sl, bool wait = true,
               bool strict = true)
   : fRm(rm), fSessLimit(std::move(sl)), fWait(wait), fStrict(strict)
  {
  }

  ~RMMemManager() override
  {
    release(fMemUsed);
    fMemUsed = 0;
  }

  int64_t getConfigured() const final
  {
    return fRm->getConfiguredUMMemLimit();
  }

  int64_t getFree() const final
  {
    return std::min(fRm->availableMemory(), *fSessLimit);
  }

  bool isStrict() const final
  {
    return fStrict;
  }

  MemManager* clone() const final
  {
    return new RMMemManager(fRm, fSessLimit, fWait, fStrict);
  }

  joblist::ResourceManager* getResourceManaged() override
  {
    return fRm;
  }
  boost::shared_ptr<int64_t> getSessionLimit() override
  {
    return fSessLimit;
  }

 protected:
  bool acquireImpl(size_t amount) final
  {
    if (amount)
    {
      if (!fRm->getMemory(amount, fSessLimit, fWait) && fStrict)
      {
        return false;
      }
      MemManager::acquireImpl(amount);
    }
    return true;
  }

  void releaseImpl(size_t amount) override
  {
    if (amount)
    {
      MemManager::releaseImpl(amount);
      fRm->returnMemory(amount, fSessLimit);
    }
  }

 private:
  joblist::ResourceManager* fRm = nullptr;
  boost::shared_ptr<int64_t> fSessLimit;
  const bool fWait;
  const bool fStrict;
};

}