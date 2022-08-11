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

/***********************************************************************
 *   $Id$
 *
 *   moda.h
 ***********************************************************************/

/**
 * Columnstore interface for the moda User Defined Aggregate
 * Functions (UDAF) and User Defined Analytic Functions (UDAnF).
 *
 * To notify mysqld about the new function:
 *
 *    CREATE AGGREGATE FUNCTION moda returns STRING soname 'libregr_mysql.so';
 *
 * moda returns the value with the greatest number of occurances in
 * the dataset with ties being broken by:
 * 1) closest to AVG
 * 2) smallest value
 */
#pragma once

#include <cstdlib>
#include <string>
#include <vector>
#include <unordered_map>

#include "mcsv1_udaf.h"
#include "calpontsystemcatalog.h"
#include "windowfunctioncolumn.h"
#include "hasher.h"
#include "collation.h"

#if defined(_MSC_VER) && defined(xxxRGNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace mcsv1sdk
{
// A hasher that handles int128_t
template <class T>
struct hasher
{
  hasher(uint32_t cs_num){}

  inline size_t operator()(T val) const
  {
    return fHasher((char*)&val, sizeof(T));
  }

 private:
  utils::Hasher fHasher;
};

// A special hasher for double that may only have 10 bytes
template <>
struct hasher<long double>
{
  hasher(uint32_t cs_num){}
  inline size_t operator()(long double val) const
  {
#ifdef MASK_LONGDOUBLE
    // For Linux x86_64, long double is stored in 128 bits, but only 80 are significant
    return fHasher((char*)&val, 10);
#else
    return fHasher((char*)&val, sizeof(long double));
#endif
  }

 private:
  utils::Hasher fHasher;
};

// A collation aware hasher for strings
template<>
struct hasher<string>
{
  hasher(uint32_t cs_num) : fHasher(cs_num){}
  inline size_t operator()(string val) const
  {
    return fHasher(val.c_str(), val.size());
  }

private:
  datatypes::CollationAwareHasher fHasher; 
};

template<class T>
struct comparator
{
  comparator(uint32_t cs_num){}

  bool operator()(const T& lhs, const T& rhs) const
  {
    return lhs == rhs;
  }
};
// A collation aware string comparator
template <>
struct comparator<std::string>
{
  comparator(uint32_t cs_num) : fCs(cs_num) {}

  bool operator()(const std::string lhs, const std::string rhs) const
  {
    return fCs.eq(lhs, rhs);
  }
 private:
  datatypes::Charset fCs;
};



// Override UserData for data storage
struct ModaData : public UserData
{
  ModaData(uint32_t cs_num = 8)
   : fMap(NULL)
   , fReturnType((uint32_t)execplan::CalpontSystemCatalog::UNDEFINED)
   , fColWidth(0)
   , modaImpl(NULL)
   , fCs_num(cs_num){}

  virtual ~ModaData()
  {
    cleanup();
  }

  virtual void serialize(messageqcpp::ByteStream& bs) const;
  virtual void unserialize(messageqcpp::ByteStream& bs);

  template <class T>
  std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >* getMap()
  {
    if (!fMap)
    {
      // Just in time creation
      fMap = new std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >(
        10, hasher<T>(fCs_num), comparator<T>(fCs_num));
    }
    return (std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >*)fMap;
  }

  // The const version is only called by serialize()
  // It shouldn't (and can't) create a new map.
  template <class T>
  std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >* getMap() const
  {
    return (std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >*)fMap;
  }

  template <class T>
  void deleteMap()
  {
    if (fMap)
    {
      delete (std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >*)fMap;
      fMap = NULL;
    }
  }

  template <class T>
  void clear()
  {
    fSum = 0.0;
    fCount = 0;
    if (fMap)
      getMap<T>()->clear();
  }

  long double fSum;
  uint64_t fCount;
  void* fMap;  // Will be of type unordered_map<>
  uint32_t fReturnType;
  uint32_t fColWidth;
  mcsv1_UDAF* modaImpl;  // A pointer to one of the Moda_impl_T concrete classes
  uint32_t fCs_num;

 private:
  // For now, copy construction is unwanted
  ModaData(UserData&);

  void cleanup();

  // Templated map streamers
  template <class T>
  void serializeMap(messageqcpp::ByteStream& bs) const
  {
    bs << fCs_num;
    std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >* map = getMap<T>();
    if (map)
    {
      typename std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >::const_iterator iter;
      bs << (uint64_t)map->size();
      for (iter = map->begin(); iter != map->end(); ++iter)
      {
        bs << iter->first;
        bs << iter->second;
      }
    }
    else
    {
      bs << (uint64_t)0;
    }
  }

  template <class T>
  void unserializeMap(messageqcpp::ByteStream& bs)
  {
    bs >> fCs_num;

    uint32_t cnt;
    T num;
    uint64_t sz;
    bs >> sz;
    std::unordered_map<T, uint32_t, hasher<T>, comparator<T> >* map = getMap<T>();
    map->clear();
    for (uint64_t i = 0; i < sz; ++i)
    {
      bs >> num;
      bs >> cnt;
      (*map)[num] = cnt;
    }
  }
};

template <class T>
class Moda_impl_T : public mcsv1_UDAF
{
 public:
  // Defaults OK
  Moda_impl_T(){};
  virtual ~Moda_impl_T(){};

  virtual mcsv1_UDAF::ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

  virtual mcsv1_UDAF::ReturnCode reset(mcsv1Context* context);
  virtual mcsv1_UDAF::ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn);
  virtual mcsv1_UDAF::ReturnCode subEvaluate(mcsv1Context* context, const UserData* valIn);
  virtual mcsv1_UDAF::ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);
  virtual mcsv1_UDAF::ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped);

  // Dummy: not used
  virtual mcsv1_UDAF::ReturnCode createUserData(UserData*& userData, int32_t& length)
  {
    return mcsv1_UDAF::SUCCESS;
  }
};

template<> // string specialization
class Moda_impl_T<string> : public mcsv1_UDAF
{
 public:
  // Defaults OK
  Moda_impl_T() : cs(8) {};
  virtual ~Moda_impl_T() {};

  virtual mcsv1_UDAF::ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

  virtual mcsv1_UDAF::ReturnCode reset(mcsv1Context* context);
  virtual mcsv1_UDAF::ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn);
  virtual mcsv1_UDAF::ReturnCode subEvaluate(mcsv1Context* context, const UserData* valIn);
  virtual mcsv1_UDAF::ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);
  virtual mcsv1_UDAF::ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped);

  // Dummy: not used
  virtual mcsv1_UDAF::ReturnCode createUserData(UserData*& userData, int32_t& length)
  {
    return mcsv1_UDAF::SUCCESS;
  }
 private:
  datatypes::Charset cs;
};

// moda returns the modal value of the dataset. If more than one value
// have the same maximum number of occurances, then the one closest to
// AVG wins. If two are the same distance from AVG, then the smaller wins.
class moda : public mcsv1_UDAF
{
 public:
  // Defaults OK
  moda() : mcsv1_UDAF(){};
  virtual ~moda(){};

  virtual mcsv1_UDAF::ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

  virtual ReturnCode reset(mcsv1Context* context)
  {
    return getImpl(context)->reset(context);
  }

  virtual mcsv1_UDAF::ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn)
  {
    return getImpl(context)->nextValue(context, valsIn);
  }

  virtual mcsv1_UDAF::ReturnCode subEvaluate(mcsv1Context* context, const UserData* valIn)
  {
    return getImpl(context)->subEvaluate(context, valIn);
  }

  virtual mcsv1_UDAF::ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut)
  {
    return getImpl(context)->evaluate(context, valOut);
  }

  virtual mcsv1_UDAF::ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
  {
    return getImpl(context)->dropValue(context, valsDropped);
  }

  mcsv1_UDAF::ReturnCode createUserData(UserData*& userData, int32_t& length)
  {
    userData = new ModaData;
    length = sizeof(ModaData);
    return mcsv1_UDAF::SUCCESS;
  }

  mcsv1_UDAF* getImpl(mcsv1Context* context);

 protected:
  Moda_impl_T<int8_t> moda_impl_int8;
  Moda_impl_T<int16_t> moda_impl_int16;
  Moda_impl_T<int32_t> moda_impl_int32;
  Moda_impl_T<int64_t> moda_impl_int64;
  Moda_impl_T<int128_t> moda_impl_int128;
  Moda_impl_T<uint8_t> moda_impl_uint8;
  Moda_impl_T<uint16_t> moda_impl_uint16;
  Moda_impl_T<uint32_t> moda_impl_uint32;
  Moda_impl_T<uint64_t> moda_impl_uint64;
  Moda_impl_T<float> moda_impl_float;
  Moda_impl_T<double> moda_impl_double;
  Moda_impl_T<long double> moda_impl_longdouble;
  Moda_impl_T<string> moda_impl_string;
};

};  // namespace mcsv1sdk

#undef EXPORT
