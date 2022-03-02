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

#pragma once

#include <iostream>
#include <vector>
#include <boost/shared_ptr.hpp>

#include <tr1/unordered_map>

#include "../common/simpleallocator.h"
#include "../joblist/elementtype.h"

namespace joiner
{
/* There has to be a better name for this.  Not used ATM. */
struct MatchedET
{
  MatchedET()
  {
  }
  MatchedET(const joblist::ElementType& et) : e(et)
  {
  }
  joblist::ElementType e;
  //	bool matched;    // Might need this, might not

  inline bool operator<(const MatchedET& c) const
  {
    return e.second < c.e.second;
  }
};

class Joiner
{
 public:
  //		typedef std::tr1::unordered_multimap<uint64_t, uint64_t> hash_t;
  typedef std::tr1::unordered_multimap<uint64_t, uint64_t, std::tr1::hash<uint64_t>, std::equal_to<uint64_t>,
                                       utils::SimpleAllocator<std::pair<uint64_t const, uint64_t> > >
      hash_t;

  typedef hash_t::iterator iterator;

  Joiner(bool bIncludeAll);
  virtual ~Joiner();

  // elements are stored as <value, rid>
  inline iterator begin()
  {
    return h->begin();
  }
  inline iterator end()
  {
    return h->end();
  }
  inline size_t size()
  {
    return h->size();
  }
  inline void insert(const joblist::ElementType& e)
  {
    h->insert(std::pair<uint64_t, uint64_t>(e.second, e.first));
  }
  void doneInserting();
  boost::shared_ptr<std::vector<joblist::ElementType> > getSmallSide();
  boost::shared_ptr<std::vector<joblist::ElementType> > getSortedMatches();

  /* Used by the UM */
  inline bool match(const joblist::ElementType& large)
  {
    std::pair<iterator, iterator> range;
    iterator it = h->find(large.second);

    if (it == h->end())
      return _includeAll;
    else if (it->second & MSB)
      return true;
    else
    {
      range = h->equal_range(large.second);

      for (; range.first != range.second; ++range.first)
        range.first->second |= MSB;

      return true;
    }
  }

  inline void mark(const joblist::ElementType& large)
  {
    std::pair<iterator, iterator> range;

    range = h->equal_range(large.second);

    for (; range.first != range.second; ++range.first)
      range.first->second |= MSB;
  }

  /* Used by the PM */
  inline bool getNewMatches(const uint64_t value, std::vector<joblist::ElementType>* newMatches)
  {
    std::pair<iterator, iterator> range;
    iterator it = h->find(value);

    if (it == h->end())
      return _includeAll;
    else if (it->second & MSB)
      return true;
    else
    {
      newMatches->push_back(joblist::ElementType(it->second | MSB, value));
      range = h->equal_range(value);

      for (; range.first != range.second; ++range.first)
        range.first->second |= MSB;

      return true;
    }
  }

  inline bool inPM()
  {
    return _inPM;
  }
  void inPM(bool b)
  {
    _inPM = b;
  }
  inline bool inUM()
  {
    return !_inPM;
  }
  void inUM(bool b)
  {
    _inPM = !b;
  }
  bool includeAll()
  {
    return _includeAll;
  }

  uint64_t getMemUsage()
  {
    return (_pool ? _pool->getMemUsage() : 0);
  }

  static const uint64_t MSB = 0x8000000000000000ULL;

 protected:
  Joiner();
  Joiner(const Joiner&);
  Joiner& operator=(const Joiner&);

 private:
  boost::shared_ptr<hash_t> h;
  bool _includeAll;
  bool _inPM;                                  // true -> should execute on the PM, false -> UM
  boost::shared_ptr<utils::SimplePool> _pool;  // pool for the table and nodes
};

}  // namespace joiner
