/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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
   MA 02110-1301, USA.
*/

//
// C++ Implementation: rowgroup
//
// Description:
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//

//#define NDEBUG
#include <sstream>
#include <iterator>
using namespace std;

#include <boost/shared_array.hpp>
#include <numeric>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "nullvaluemanip.h"
#include "rowgroup.h"
#include "dataconvert.h"
#include "columnwidth.h"

namespace rowgroup
{
using cscType = execplan::CalpontSystemCatalog::ColDataType;

StringStore::StringStore() : empty(true), fUseStoreStringMutex(false)
{
}

StringStore::StringStore(const StringStore&)
{
  throw logic_error("Don't call StringStore copy ctor");
}

StringStore& StringStore::operator=(const StringStore&)
{
  throw logic_error("Don't call StringStore operator=");
}

StringStore::~StringStore()
{
#if 0
    // for mem usage debugging
    uint32_t i;
    uint64_t inUse = 0, allocated = 0;

    for (i = 0; i < mem.size(); i++)
    {
        MemChunk* tmp = (MemChunk*) mem.back().get();
        inUse += tmp->currentSize;
        allocated += tmp->capacity;
    }

    if (allocated > 0)
        cout << "~SS: " << inUse << "/" << allocated << " = " << (float) inUse / (float) allocated << endl;

#endif
}

uint64_t StringStore::storeString(const uint8_t* data, uint32_t len)
{
  MemChunk* lastMC = NULL;
  uint64_t ret = 0;

  empty = false;  // At least a NULL is being stored.

  // Sometimes the caller actually wants "" to be returned.......   argggghhhh......
  // if (len == 0)
  //	return numeric_limits<uint32_t>::max();

  if ((len == 8 || len == 9) && *((uint64_t*)data) == *((uint64_t*)joblist::CPNULLSTRMARK.c_str()))
    return numeric_limits<uint64_t>::max();

  //@bug6065, make StringStore::storeString() thread safe
  boost::mutex::scoped_lock lk(fMutex, boost::defer_lock);

  if (fUseStoreStringMutex)
    lk.lock();

  if (mem.size() > 0)
    lastMC = (MemChunk*)mem.back().get();

  if ((len + 4) >= CHUNK_SIZE)
  {
    shared_array<uint8_t> newOne(new uint8_t[len + sizeof(MemChunk) + 4]);
    longStrings.push_back(newOne);
    lastMC = (MemChunk*)longStrings.back().get();
    lastMC->capacity = lastMC->currentSize = len + 4;
    memcpy(lastMC->data, &len, 4);
    memcpy(lastMC->data + 4, data, len);
    // High bit to mark a long string
    ret = 0x8000000000000000;
    ret += longStrings.size() - 1;
  }
  else
  {
    if ((lastMC == NULL) || (lastMC->capacity - lastMC->currentSize < (len + 4)))
    {
      // mem usage debugging
      // if (lastMC)
      // cout << "Memchunk efficiency = " << lastMC->currentSize << "/" << lastMC->capacity << endl;
      shared_array<uint8_t> newOne(new uint8_t[CHUNK_SIZE + sizeof(MemChunk)]);
      mem.push_back(newOne);
      lastMC = (MemChunk*)mem.back().get();
      lastMC->currentSize = 0;
      lastMC->capacity = CHUNK_SIZE;
      memset(lastMC->data, 0, CHUNK_SIZE);
    }

    ret = ((mem.size() - 1) * CHUNK_SIZE) + lastMC->currentSize;

    // If this ever happens then we have big problems
    if (ret & 0x8000000000000000)
      throw logic_error("StringStore memory exceeded.");

    memcpy(&(lastMC->data[lastMC->currentSize]), &len, 4);
    memcpy(&(lastMC->data[lastMC->currentSize]) + 4, data, len);
    /*
    cout << "stored: '" << hex;
    for (uint32_t i = 0; i < len ; i++) {
            cout << (char) lastMC->data[lastMC->currentSize + i];
    }
    cout << "' at position " << lastMC->currentSize << " len " << len << dec << endl;
    */
    lastMC->currentSize += len + 4;
  }

  return ret;
}

void StringStore::serialize(ByteStream& bs) const
{
  uint64_t i;
  MemChunk* mc;

  bs << (uint64_t)mem.size();
  bs << (uint8_t)empty;

  for (i = 0; i < mem.size(); i++)
  {
    mc = (MemChunk*)mem[i].get();
    bs << (uint64_t)mc->currentSize;
    // cout << "serialized " << mc->currentSize << " bytes\n";
    bs.append(mc->data, mc->currentSize);
  }

  bs.setLongStrings(longStrings);
}

void StringStore::deserialize(ByteStream& bs)
{
  uint64_t i;
  uint64_t count;
  uint64_t size;
  uint8_t* buf;
  MemChunk* mc;
  uint8_t tmp8;

  // mem.clear();
  bs >> count;
  mem.resize(count);
  bs >> tmp8;
  empty = (bool)tmp8;

  for (i = 0; i < count; i++)
  {
    bs >> size;
    // cout << "deserializing " << size << " bytes\n";
    buf = bs.buf();
    mem[i].reset(new uint8_t[size + sizeof(MemChunk)]);
    mc = (MemChunk*)mem[i].get();
    mc->currentSize = size;
    mc->capacity = size;
    memcpy(mc->data, buf, size);
    bs.advance(size);
  }

  longStrings = bs.getLongStrings();
  return;
}

void StringStore::clear()
{
  vector<shared_array<uint8_t> > emptyv;
  vector<shared_array<uint8_t> > emptyv2;
  mem.swap(emptyv);
  longStrings.swap(emptyv2);
  empty = true;
}

UserDataStore::UserDataStore() : fUseUserDataMutex(false)
{
}

UserDataStore::~UserDataStore()
{
}

uint32_t UserDataStore::storeUserData(mcsv1sdk::mcsv1Context& context,
                                      boost::shared_ptr<mcsv1sdk::UserData> data, uint32_t len)
{
  uint32_t ret = 0;

  if (len == 0 || data == NULL)
  {
    return numeric_limits<uint32_t>::max();
  }

  boost::mutex::scoped_lock lk(fMutex, boost::defer_lock);

  if (fUseUserDataMutex)
    lk.lock();

  StoreData storeData;
  storeData.length = len;
  storeData.functionName = context.getName();
  storeData.userData = data;
  vStoreData.push_back(storeData);

  ret = vStoreData.size();

  return ret;
}

boost::shared_ptr<mcsv1sdk::UserData> UserDataStore::getUserData(uint32_t off) const
{
  if (off == std::numeric_limits<uint32_t>::max())
    return boost::shared_ptr<mcsv1sdk::UserData>();

  if ((vStoreData.size() < off) || off == 0)
    return boost::shared_ptr<mcsv1sdk::UserData>();

  return vStoreData[off - 1].userData;
}

void UserDataStore::serialize(ByteStream& bs) const
{
  size_t i;

  bs << (uint32_t)vStoreData.size();

  for (i = 0; i < vStoreData.size(); ++i)
  {
    const StoreData& storeData = vStoreData[i];
    bs << storeData.length;
    bs << storeData.functionName;
    storeData.userData->serialize(bs);
  }
}

void UserDataStore::deserialize(ByteStream& bs)
{
  size_t i;
  uint32_t cnt;
  bs >> cnt;

  //	vStoreData.clear();
  vStoreData.resize(cnt);

  for (i = 0; i < cnt; i++)
  {
    bs >> vStoreData[i].length;
    bs >> vStoreData[i].functionName;

    // We don't have easy access to the context here, so we do our own lookup
    if (vStoreData[i].functionName.length() == 0)
    {
      throw std::logic_error("UserDataStore::deserialize: has empty name");
    }

    mcsv1sdk::UDAF_MAP::iterator funcIter = mcsv1sdk::UDAFMap::getMap().find(vStoreData[i].functionName);

    if (funcIter == mcsv1sdk::UDAFMap::getMap().end())
    {
      std::ostringstream errmsg;
      errmsg << "UserDataStore::deserialize: " << vStoreData[i].functionName << " is undefined";
      throw std::logic_error(errmsg.str());
    }

    mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
    mcsv1sdk::UserData* userData = NULL;
    rc = funcIter->second->createUserData(userData, vStoreData[i].length);

    if (rc != mcsv1sdk::mcsv1_UDAF::SUCCESS)
    {
      std::ostringstream errmsg;
      errmsg << "UserDataStore::deserialize: " << vStoreData[i].functionName << " createUserData failed("
             << rc << ")";
      throw std::logic_error(errmsg.str());
    }

    userData->unserialize(bs);
    vStoreData[i].userData = boost::shared_ptr<mcsv1sdk::UserData>(userData);
  }

  return;
}

RGData::RGData()
{
  // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
}

RGData::RGData(const RowGroup& rg, uint32_t rowCount)
{
  // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
  rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);

  if (rg.usesStringTable() && rowCount > 0)
    strings.reset(new StringStore());

#ifdef VALGRIND
  /* In a PM-join, we can serialize entire tables; not every value has been
   * filled in yet.  Need to look into that.  Valgrind complains that
   * those bytes are uninitialized, this suppresses that error.
   */
  memset(rowData.get(), 0, rg.getDataSize(rowCount));  // XXXPAT: make valgrind happy temporarily
#endif
  memset(rowData.get(), 0, rg.getDataSize(rowCount));  // XXXPAT: make valgrind happy temporarily
}

RGData::RGData(const RowGroup& rg)
{
  // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
  rowData.reset(new uint8_t[rg.getMaxDataSize()]);

  if (rg.usesStringTable())
    strings.reset(new StringStore());

#ifdef VALGRIND
  /* In a PM-join, we can serialize entire tables; not every value has been
   * filled in yet.  Need to look into that.  Valgrind complains that
   * those bytes are uninitialized, this suppresses that error.
   */
  memset(rowData.get(), 0, rg.getMaxDataSize());
#endif
}

void RGData::reinit(const RowGroup& rg, uint32_t rowCount)
{
  rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);

  if (rg.usesStringTable())
    strings.reset(new StringStore());
  else
    strings.reset();

#ifdef VALGRIND
  /* In a PM-join, we can serialize entire tables; not every value has been
   * filled in yet.  Need to look into that.  Valgrind complains that
   * those bytes are uninitialized, this suppresses that error.
   */
  memset(rowData.get(), 0, rg.getDataSize(rowCount));
#endif
}

void RGData::reinit(const RowGroup& rg)
{
  reinit(rg, 8192);
}

RGData::RGData(const RGData& r) : rowData(r.rowData), strings(r.strings), userDataStore(r.userDataStore)
{
  // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
}

RGData::~RGData()
{
  // cout << "rgdata-- = " << __sync_sub_and_fetch(&rgDataCount, 1) << endl;
}

void RGData::serialize(ByteStream& bs, uint32_t amount) const
{
  // cout << "serializing!\n";
  bs << (uint32_t)RGDATA_SIG;
  bs << (uint32_t)amount;
  bs.append(rowData.get(), amount);

  if (strings)
  {
    bs << (uint8_t)1;
    strings->serialize(bs);
  }
  else
    bs << (uint8_t)0;

  if (userDataStore)
  {
    bs << (uint8_t)1;
    userDataStore->serialize(bs);
  }
  else
    bs << (uint8_t)0;
}

void RGData::deserialize(ByteStream& bs, uint32_t defAmount)
{
  uint32_t amount, sig;
  uint8_t* buf;
  uint8_t tmp8;

  bs.peek(sig);

  if (sig == RGDATA_SIG)
  {
    bs >> sig;
    bs >> amount;
    rowData.reset(new uint8_t[std::max(amount, defAmount)]);
    buf = bs.buf();
    memcpy(rowData.get(), buf, amount);
    bs.advance(amount);
    bs >> tmp8;

    if (tmp8)
    {
      strings.reset(new StringStore());
      strings->deserialize(bs);
    }
    else
      strings.reset();

    // UDAF user data
    bs >> tmp8;

    if (tmp8)
    {
      userDataStore.reset(new UserDataStore());
      userDataStore->deserialize(bs);
    }
    else
      userDataStore.reset();
  }

  return;
}

void RGData::clear()
{
  rowData.reset();
  strings.reset();
}

// UserDataStore is only used for UDAF.
// Just in time construction because most of the time we don't need one.
UserDataStore* RGData::getUserDataStore()
{
  if (!userDataStore)
  {
    userDataStore.reset(new UserDataStore);
  }

  return userDataStore.get();
}

Row::Row() : data(NULL), strings(NULL), userDataStore(NULL)
{
}

Row::Row(const Row& r)
 : columnCount(r.columnCount)
 , baseRid(r.baseRid)
 , oldOffsets(r.oldOffsets)
 , stOffsets(r.stOffsets)
 , offsets(r.offsets)
 , colWidths(r.colWidths)
 , types(r.types)
 , charsetNumbers(r.charsetNumbers)
 , charsets(r.charsets)
 , data(r.data)
 , scale(r.scale)
 , precision(r.precision)
 , strings(r.strings)
 , useStringTable(r.useStringTable)
 , hasCollation(r.hasCollation)
 , hasLongStringField(r.hasLongStringField)
 , sTableThreshold(r.sTableThreshold)
 , forceInline(r.forceInline)
 , userDataStore(NULL)
{
}

Row::~Row()
{
}

Row& Row::operator=(const Row& r)
{
  columnCount = r.columnCount;
  baseRid = r.baseRid;
  oldOffsets = r.oldOffsets;
  stOffsets = r.stOffsets;
  offsets = r.offsets;
  colWidths = r.colWidths;
  types = r.types;
  charsetNumbers = r.charsetNumbers;
  charsets = r.charsets;
  data = r.data;
  scale = r.scale;
  precision = r.precision;
  strings = r.strings;
  useStringTable = r.useStringTable;
  hasCollation = r.hasCollation;
  hasLongStringField = r.hasLongStringField;
  sTableThreshold = r.sTableThreshold;
  forceInline = r.forceInline;
  return *this;
}

string Row::toString(uint32_t rownum) const
{
  ostringstream os;
  uint32_t i;

  // os << getRid() << ": ";
  os << "[" << std::setw(5) << rownum << std::setw(0) << "]: ";
  os << (int)useStringTable << ": ";

  for (i = 0; i < columnCount; i++)
  {
    if (isNullValue(i))
      os << "NULL ";
    else
      switch (types[i])
      {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        {
          const utils::ConstString tmp = getConstString(i);
          os << "(" << tmp.length() << ") '";
          os.write(tmp.str(), tmp.length());
          os << "' ";
          break;
        }

        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT: os << getFloatField(i) << " "; break;

        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE: os << getDoubleField(i) << " "; break;

        case CalpontSystemCatalog::LONGDOUBLE: os << getLongDoubleField(i) << " "; break;

        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        {
          uint32_t len = getVarBinaryLength(i);
          const uint8_t* val = getVarBinaryField(i);
          os << "0x" << hex;

          while (len-- > 0)
          {
            os << (uint32_t)(*val >> 4);
            os << (uint32_t)(*val++ & 0x0F);
          }

          os << " " << dec;
          break;
        }

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
          if (colWidths[i] == datatypes::MAXDECIMALWIDTH)
          {
            datatypes::Decimal dec(0, scale[i], precision[i], getBinaryField<int128_t>(i));
            os << dec << " ";
            break;
          }
          // fallthrough
        default: os << getIntField(i) << " "; break;
      }
  }

  return os.str();
}

string Row::toCSV() const
{
  ostringstream os;

  for (uint32_t i = 0; i < columnCount; i++)
  {
    if (i > 0)
    {
      os << ",";
    }

    if (isNullValue(i))
      os << "NULL";
    else
      switch (types[i])
      {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR: os << getStringField(i).c_str(); break;

        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT: os << getFloatField(i); break;

        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE: os << getDoubleField(i); break;

        case CalpontSystemCatalog::LONGDOUBLE: os << getLongDoubleField(i); break;

        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        {
          uint32_t len = getVarBinaryLength(i);
          const uint8_t* val = getVarBinaryField(i);
          os << "0x" << hex;

          while (len-- > 0)
          {
            os << (uint32_t)(*val >> 4);
            os << (uint32_t)(*val++ & 0x0F);
          }

          os << dec;
          break;
        }

        default: os << getIntField(i); break;
      }
  }

  return os.str();
}

void Row::initToNull()
{
  uint32_t i;

  for (i = 0; i < columnCount; i++)
  {
    switch (types[i])
    {
      case CalpontSystemCatalog::TINYINT: data[offsets[i]] = joblist::TINYINTNULL; break;

      case CalpontSystemCatalog::SMALLINT:
        *((int16_t*)&data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL);
        break;

      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
        *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL);
        break;

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
        *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::FLOATNULL);
        break;

      case CalpontSystemCatalog::DATE:
        *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::DATENULL);
        break;

      case CalpontSystemCatalog::BIGINT:
        if (precision[i] != 9999)
          *((uint64_t*)&data[offsets[i]]) = joblist::BIGINTNULL;
        else  // work around for count() in outer join result.
          *((uint64_t*)&data[offsets[i]]) = 0;

        break;

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE: *((uint64_t*)&data[offsets[i]]) = joblist::DOUBLENULL; break;

      case CalpontSystemCatalog::LONGDOUBLE:
        *((long double*)&data[offsets[i]]) = joblist::LONGDOUBLENULL;
        break;

      case CalpontSystemCatalog::DATETIME: *((uint64_t*)&data[offsets[i]]) = joblist::DATETIMENULL; break;

      case CalpontSystemCatalog::TIMESTAMP: *((uint64_t*)&data[offsets[i]]) = joblist::TIMESTAMPNULL; break;

      case CalpontSystemCatalog::TIME: *((uint64_t*)&data[offsets[i]]) = joblist::TIMENULL; break;

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::TEXT:
      case CalpontSystemCatalog::STRINT:
      {
        if (inStringTable(i))
        {
          setStringField(joblist::CPNULLSTRMARK, i);
          break;
        }

        uint32_t len = getColumnWidth(i);

        switch (len)
        {
          case 1: data[offsets[i]] = joblist::CHAR1NULL; break;

          case 2: *((uint16_t*)&data[offsets[i]]) = joblist::CHAR2NULL; break;

          case 3:
          case 4: *((uint32_t*)&data[offsets[i]]) = joblist::CHAR4NULL; break;

          case 5:
          case 6:
          case 7:
          case 8: *((uint64_t*)&data[offsets[i]]) = joblist::CHAR8NULL; break;

          default:
            *((uint64_t*)&data[offsets[i]]) = *((uint64_t*)joblist::CPNULLSTRMARK.c_str());
            memset(&data[offsets[i] + 8], 0, len - 8);
            break;
        }

        break;
      }

      case CalpontSystemCatalog::VARBINARY:
      case CalpontSystemCatalog::BLOB: *((uint16_t*)&data[offsets[i]]) = 0; break;

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        uint32_t len = getColumnWidth(i);

        switch (len)
        {
          case 1: data[offsets[i]] = joblist::TINYINTNULL; break;

          case 2: *((int16_t*)&data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL); break;

          case 4: *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL); break;

          case 16:
          {
            int128_t* s128ValuePtr = (int128_t*)(&data[offsets[i]]);
            datatypes::TSInt128::storeUnaligned(s128ValuePtr, datatypes::Decimal128Null);
          }
          break;
          default: *((int64_t*)&data[offsets[i]]) = static_cast<int64_t>(joblist::BIGINTNULL); break;
        }

        break;
      }

      case CalpontSystemCatalog::UTINYINT: data[offsets[i]] = joblist::UTINYINTNULL; break;

      case CalpontSystemCatalog::USMALLINT: *((uint16_t*)&data[offsets[i]]) = joblist::USMALLINTNULL; break;

      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT: *((uint32_t*)&data[offsets[i]]) = joblist::UINTNULL; break;

      case CalpontSystemCatalog::UBIGINT: *((uint64_t*)&data[offsets[i]]) = joblist::UBIGINTNULL; break;

      default:
        ostringstream os;
        os << "Row::initToNull(): got bad column type (" << types[i] << ").  Width=" << getColumnWidth(i)
           << endl;
        os << toString();
        throw logic_error(os.str());
    }
  }
}

template <cscDataType cscDT, int width>
inline bool Row::isNullValue_offset(uint32_t offset) const
{
  ostringstream os;
  os << "Row::isNullValue(): got bad column type at offset(";
  os << offset;
  os << ").  Width=";
  os << width << endl;
  throw logic_error(os.str());
}

template <>
inline bool Row::isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 16>(uint32_t offset) const
{
  const int128_t* intPtr = reinterpret_cast<const int128_t*>(&data[offset]);
  return datatypes::Decimal::isWideDecimalNullValue(*intPtr);
}

template <>
inline bool Row::isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 8>(uint32_t offset) const
{
  return (*reinterpret_cast<int64_t*>(&data[offset]) == static_cast<int64_t>(joblist::BIGINTNULL));
}

template <>
inline bool Row::isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 4>(uint32_t offset) const
{
  return (*reinterpret_cast<int32_t*>(&data[offset]) == static_cast<int32_t>(joblist::INTNULL));
}

template <>
inline bool Row::isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 2>(uint32_t offset) const
{
  return (*reinterpret_cast<int16_t*>(&data[offset]) == static_cast<int16_t>(joblist::SMALLINTNULL));
}

template <>
inline bool Row::isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 1>(uint32_t offset) const
{
  return (data[offset] == joblist::TINYINTNULL);
}

bool Row::isNullValue(uint32_t colIndex) const
{
  switch (types[colIndex])
  {
    case CalpontSystemCatalog::TINYINT: return (data[offsets[colIndex]] == joblist::TINYINTNULL);

    case CalpontSystemCatalog::SMALLINT:
      return (*((int16_t*)&data[offsets[colIndex]]) == static_cast<int16_t>(joblist::SMALLINTNULL));

    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT:
      return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::INTNULL));

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
      return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::FLOATNULL));

    case CalpontSystemCatalog::DATE:
      return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::DATENULL));

    case CalpontSystemCatalog::BIGINT:
      return (*((int64_t*)&data[offsets[colIndex]]) == static_cast<int64_t>(joblist::BIGINTNULL));

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
      return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::DOUBLENULL);

    case CalpontSystemCatalog::DATETIME:
      return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::DATETIMENULL);

    case CalpontSystemCatalog::TIMESTAMP:
      return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::TIMESTAMPNULL);

    case CalpontSystemCatalog::TIME: return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::TIMENULL);

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::STRINT:
    {
      uint32_t len = getColumnWidth(colIndex);

      if (inStringTable(colIndex))
      {
        uint64_t offset;
        offset = *((uint64_t*)&data[offsets[colIndex]]);
        return strings->isNullValue(offset);
      }

      if (data[offsets[colIndex]] == 0)  // empty string
        return true;

      switch (len)
      {
        case 1: return (data[offsets[colIndex]] == joblist::CHAR1NULL);

        case 2: return (*((uint16_t*)&data[offsets[colIndex]]) == joblist::CHAR2NULL);

        case 3:
        case 4: return (*((uint32_t*)&data[offsets[colIndex]]) == joblist::CHAR4NULL);

        case 5:
        case 6:
        case 7:
        case 8: return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::CHAR8NULL);
        default:
          return (*((uint64_t*)&data[offsets[colIndex]]) == *((uint64_t*)joblist::CPNULLSTRMARK.c_str()));
      }

      break;
    }

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      // TODO MCOL-641 Allmighty hack.
      switch (getColumnWidth(colIndex))
      {
        // MCOL-641
        case 16: return isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 16>(offsets[colIndex]);
        case 1: return (data[offsets[colIndex]] == joblist::TINYINTNULL);

        case 2: return (*((int16_t*)&data[offsets[colIndex]]) == static_cast<int16_t>(joblist::SMALLINTNULL));

        case 4: return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::INTNULL));

        default: return (*((int64_t*)&data[offsets[colIndex]]) == static_cast<int64_t>(joblist::BIGINTNULL));
      }

      break;
    }

    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARBINARY:
    {
      uint32_t pos = offsets[colIndex];

      if (inStringTable(colIndex))
      {
        uint64_t offset;
        offset = *((uint64_t*)&data[pos]);
        return strings->isNullValue(offset);
      }

      if (*((uint16_t*)&data[pos]) == 0)
        return true;
      else if ((strncmp((char*)&data[pos + 2], joblist::CPNULLSTRMARK.c_str(), 8) == 0) &&
               *((uint16_t*)&data[pos]) == joblist::CPNULLSTRMARK.length())
        return true;

      break;
    }

    case CalpontSystemCatalog::UTINYINT: return (data[offsets[colIndex]] == joblist::UTINYINTNULL);

    case CalpontSystemCatalog::USMALLINT:
      return (*((uint16_t*)&data[offsets[colIndex]]) == joblist::USMALLINTNULL);

    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: return (*((uint32_t*)&data[offsets[colIndex]]) == joblist::UINTNULL);

    case CalpontSystemCatalog::UBIGINT:
      return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::UBIGINTNULL);

    case CalpontSystemCatalog::LONGDOUBLE:
      return (*((long double*)&data[offsets[colIndex]]) == joblist::LONGDOUBLENULL);
      break;

    default:
    {
      ostringstream os;
      os << "Row::isNullValue(): got bad column type (";
      os << types[colIndex];
      os << ").  Width=";
      os << getColumnWidth(colIndex) << endl;
      throw logic_error(os.str());
    }
  }

  return false;
}

uint64_t Row::getNullValue(uint32_t colIndex) const
{
  return utils::getNullValue(types[colIndex], getColumnWidth(colIndex));
}

/* This fcn might produce overflow warnings from the compiler, but that's OK.
 * The overflow is intentional...
 */
int64_t Row::getSignedNullValue(uint32_t colIndex) const
{
  return utils::getSignedNullValue(types[colIndex], getColumnWidth(colIndex));
}

bool Row::equals(const Row& r2, uint32_t lastCol) const
{
  // This check fires with empty r2 only.
  if (lastCol >= columnCount)
    return true;

  // If there are no strings in the row, then we can just memcmp the whole row.
  // hasCollation is true if there is any column of type CHAR, VARCHAR or TEXT
  // useStringTable is true if any field declared > max inline field size, including BLOB
  // For memcmp to be correct, both must be false.
  if (!hasCollation && !useStringTable && !r2.hasCollation && !r2.useStringTable)
    return !(memcmp(&data[offsets[0]], &r2.data[offsets[0]], offsets[lastCol + 1] - offsets[0]));

  // There are strings involved, so we need to check each column
  // because binary equality is not equality for many charsets/collations
  for (uint32_t col = 0; col <= lastCol; col++)
  {
    cscDataType columnType = getColType(col);
    if (UNLIKELY(typeHasCollation(columnType)))
    {
      datatypes::Charset cs(getCharset(col));
      if (cs.strnncollsp(getConstString(col), r2.getConstString(col)))
      {
        return false;
      }
    }
    else if (UNLIKELY(columnType == execplan::CalpontSystemCatalog::BLOB))
    {
      if (!getConstString(col).eq(r2.getConstString(col)))
        return false;
    }
    else
    {
      if (UNLIKELY(columnType == execplan::CalpontSystemCatalog::LONGDOUBLE))
      {
        if (getLongDoubleField(col) != r2.getLongDoubleField(col))
          return false;
      }
      else if (UNLIKELY(datatypes::isWideDecimalType(columnType, colWidths[col])))
      {
        if (*getBinaryField<int128_t>(col) != *r2.getBinaryField<int128_t>(col))
          return false;
      }
      else if (getUintField(col) != r2.getUintField(col))
      {
        return false;
      }
    }
  }
  return true;
}

const CHARSET_INFO* Row::getCharset(uint32_t col) const
{
  if (charsets[col] == NULL)
  {
    const_cast<CHARSET_INFO**>(charsets)[col] = &datatypes::Charset(charsetNumbers[col]).getCharset();
  }
  return charsets[col];
}

RowGroup::RowGroup()
 : columnCount(0)
 , data(NULL)
 , rgData(NULL)
 , strings(NULL)
 , useStringTable(true)
 , hasCollation(false)
 , hasLongStringField(false)
 , sTableThreshold(20)
{
  // 1024 is too generous to waste.
  oldOffsets.reserve(10);
  oids.reserve(10);
  keys.reserve(10);
  types.reserve(10);
  charsetNumbers.reserve(10);
  charsets.reserve(10);
  scale.reserve(10);
  precision.reserve(10);
}

RowGroup::RowGroup(uint32_t colCount, const vector<uint32_t>& positions, const vector<uint32_t>& roids,
                   const vector<uint32_t>& tkeys, const vector<CalpontSystemCatalog::ColDataType>& colTypes,
                   const vector<uint32_t>& csNumbers, const vector<uint32_t>& cscale,
                   const vector<uint32_t>& cprecision, uint32_t stringTableThreshold, bool stringTable,
                   const vector<bool>& forceInlineData)
 : columnCount(colCount)
 , data(NULL)
 , oldOffsets(positions)
 , oids(roids)
 , keys(tkeys)
 , types(colTypes)
 , charsetNumbers(csNumbers)
 , scale(cscale)
 , precision(cprecision)
 , rgData(NULL)
 , strings(NULL)
 , sTableThreshold(stringTableThreshold)
{
  uint32_t i;

  forceInline.reset(new bool[columnCount]);

  if (forceInlineData.empty())
    for (i = 0; i < columnCount; i++)
      forceInline[i] = false;
  else
    for (i = 0; i < columnCount; i++)
      forceInline[i] = forceInlineData[i];

  colWidths.resize(columnCount);
  stOffsets.resize(columnCount + 1);
  stOffsets[0] = 2;  // 2-byte rid
  hasLongStringField = false;
  hasCollation = false;

  for (i = 0; i < columnCount; i++)
  {
    colWidths[i] = positions[i + 1] - positions[i];

    if (colWidths[i] >= sTableThreshold && !forceInline[i])
    {
      hasLongStringField = true;
      stOffsets[i + 1] = stOffsets[i] + 8;
    }
    else
      stOffsets[i + 1] = stOffsets[i] + colWidths[i];

    if (colHasCollation(i))
    {
      hasCollation = true;
    }
  }

  useStringTable = (stringTable && hasLongStringField);
  offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);

  // Set all the charsets to NULL for jit initialization.
  charsets.insert(charsets.begin(), charsetNumbers.size(), NULL);
}

RowGroup::RowGroup(const RowGroup& r)
 : columnCount(r.columnCount)
 , data(r.data)
 , oldOffsets(r.oldOffsets)
 , stOffsets(r.stOffsets)
 , colWidths(r.colWidths)
 , oids(r.oids)
 , keys(r.keys)
 , types(r.types)
 , charsetNumbers(r.charsetNumbers)
 , charsets(r.charsets)
 , scale(r.scale)
 , precision(r.precision)
 , rgData(r.rgData)
 , strings(r.strings)
 , useStringTable(r.useStringTable)
 , hasCollation(r.hasCollation)
 , hasLongStringField(r.hasLongStringField)
 , sTableThreshold(r.sTableThreshold)
 , forceInline(r.forceInline)
{
  // stOffsets and oldOffsets are sometimes empty...
  // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
  offsets = 0;

  if (useStringTable && !stOffsets.empty())
    offsets = &stOffsets[0];
  else if (!useStringTable && !oldOffsets.empty())
    offsets = &oldOffsets[0];
}

RowGroup& RowGroup::operator=(const RowGroup& r)
{
  columnCount = r.columnCount;
  oldOffsets = r.oldOffsets;
  stOffsets = r.stOffsets;
  colWidths = r.colWidths;
  oids = r.oids;
  keys = r.keys;
  types = r.types;
  charsetNumbers = r.charsetNumbers;
  charsets = r.charsets;
  data = r.data;
  scale = r.scale;
  precision = r.precision;
  rgData = r.rgData;
  strings = r.strings;
  useStringTable = r.useStringTable;
  hasCollation = r.hasCollation;
  hasLongStringField = r.hasLongStringField;
  sTableThreshold = r.sTableThreshold;
  forceInline = r.forceInline;
  // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
  offsets = 0;

  if (useStringTable && !stOffsets.empty())
    offsets = &stOffsets[0];
  else if (!useStringTable && !oldOffsets.empty())
    offsets = &oldOffsets[0];

  return *this;
}

RowGroup::RowGroup(ByteStream& bs)
 : columnCount(0)
 , data(nullptr)
 , rgData(nullptr)
 , strings(nullptr)
 , useStringTable(true)
 , hasCollation(false)
 , hasLongStringField(false)
 , sTableThreshold(20)
{
  this->deserialize(bs);
}

RowGroup::~RowGroup()
{
}

void RowGroup::resetRowGroup(uint64_t rid)
{
  *((uint32_t*)&data[rowCountOffset]) = 0;
  *((uint64_t*)&data[baseRidOffset]) = rid;
  *((uint16_t*)&data[statusOffset]) = 0;
  *((uint32_t*)&data[dbRootOffset]) = 0;

  if (strings)
    strings->clear();
}

void RowGroup::serialize(ByteStream& bs) const
{
  bs << columnCount;
  serializeInlineVector<uint32_t>(bs, oldOffsets);
  serializeInlineVector<uint32_t>(bs, stOffsets);
  serializeInlineVector<uint32_t>(bs, colWidths);
  serializeInlineVector<uint32_t>(bs, oids);
  serializeInlineVector<uint32_t>(bs, keys);
  serializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
  serializeInlineVector<uint32_t>(bs, charsetNumbers);
  serializeInlineVector<uint32_t>(bs, scale);
  serializeInlineVector<uint32_t>(bs, precision);
  bs << (uint8_t)useStringTable;
  bs << (uint8_t)hasCollation;
  bs << (uint8_t)hasLongStringField;
  bs << sTableThreshold;
  bs.append((uint8_t*)&forceInline[0], sizeof(bool) * columnCount);
}

void RowGroup::deserialize(ByteStream& bs)
{
  uint8_t tmp8;

  bs >> columnCount;
  deserializeInlineVector<uint32_t>(bs, oldOffsets);
  deserializeInlineVector<uint32_t>(bs, stOffsets);
  deserializeInlineVector<uint32_t>(bs, colWidths);
  deserializeInlineVector<uint32_t>(bs, oids);
  deserializeInlineVector<uint32_t>(bs, keys);
  deserializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
  deserializeInlineVector<uint32_t>(bs, charsetNumbers);
  deserializeInlineVector<uint32_t>(bs, scale);
  deserializeInlineVector<uint32_t>(bs, precision);
  bs >> tmp8;
  useStringTable = (bool)tmp8;
  bs >> tmp8;
  hasCollation = (bool)tmp8;
  bs >> tmp8;
  hasLongStringField = (bool)tmp8;
  bs >> sTableThreshold;
  forceInline.reset(new bool[columnCount]);
  memcpy(&forceInline[0], bs.buf(), sizeof(bool) * columnCount);
  bs.advance(sizeof(bool) * columnCount);
  // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
  offsets = 0;

  if (useStringTable && !stOffsets.empty())
    offsets = &stOffsets[0];
  else if (!useStringTable && !oldOffsets.empty())
    offsets = &oldOffsets[0];

  // Set all the charsets to NULL for jit initialization.
  charsets.insert(charsets.begin(), charsetNumbers.size(), NULL);
}

void RowGroup::serializeRGData(ByteStream& bs) const
{
  // cout << "****** serializing\n" << toString() << en
  //	if (useStringTable || !hasLongStringField)
  rgData->serialize(bs, getDataSize());
  //	else {
  //		uint64_t size;
  //		RGData *compressed = convertToStringTable(&size);
  //		compressed->serialize(bs, size);
  //		if (compressed != rgData)
  //			delete compressed;
  //	}
}

uint32_t RowGroup::getDataSize() const
{
  return headerSize + (getRowCount() * offsets[columnCount]);
}

uint32_t RowGroup::getDataSize(uint64_t n) const
{
  return headerSize + (n * offsets[columnCount]);
}

uint32_t RowGroup::getMaxDataSize() const
{
  return headerSize + (8192 * offsets[columnCount]);
}

uint32_t RowGroup::getMaxDataSizeWithStrings() const
{
  return headerSize + (8192 * oldOffsets[columnCount]);
}

uint32_t RowGroup::getEmptySize() const
{
  return headerSize;
}

uint32_t RowGroup::getStatus() const
{
  return *((uint16_t*)&data[statusOffset]);
}

void RowGroup::setStatus(uint16_t err)
{
  *((uint16_t*)&data[statusOffset]) = err;
}

uint32_t RowGroup::getColumnWidth(uint32_t col) const
{
  return colWidths[col];
}

uint32_t RowGroup::getColumnCount() const
{
  return columnCount;
}

string RowGroup::toString(const std::vector<uint64_t>& used) const
{
  ostringstream os;
  ostream_iterator<int> oIter1(os, "\t");

  os << "columncount = " << columnCount << endl;
  os << "oids:\t\t";
  copy(oids.begin(), oids.end(), oIter1);
  os << endl;
  os << "keys:\t\t";
  copy(keys.begin(), keys.end(), oIter1);
  os << endl;
  os << "offsets:\t";
  copy(&offsets[0], &offsets[columnCount + 1], oIter1);
  os << endl;
  os << "colWidths:\t";
  copy(colWidths.begin(), colWidths.end(), oIter1);
  os << endl;
  os << "types:\t\t";
  copy(types.begin(), types.end(), oIter1);
  os << endl;
  os << "scales:\t\t";
  copy(scale.begin(), scale.end(), oIter1);
  os << endl;
  os << "precisions:\t";
  copy(precision.begin(), precision.end(), oIter1);
  os << endl;

  if (useStringTable)
    os << "uses a string table\n";
  else
    os << "doesn't use a string table\n";
  if (!used.empty())
    os << "sparse\n";

  // os << "strings = " << hex << (int64_t) strings << "\n";
  // os << "data = " << (int64_t) data << "\n" << dec;
  if (data != NULL)
  {
    Row r;
    initRow(&r);
    getRow(0, &r);
    os << "rowcount = " << getRowCount() << endl;
    if (!used.empty())
    {
      uint64_t cnt =
          std::accumulate(used.begin(), used.end(), 0ULL,
                          [](uint64_t a, uint64_t bits) { return a + __builtin_popcountll(bits); });
      os << "sparse row count = " << cnt << endl;
    }
    os << "base rid = " << getBaseRid() << endl;
    os << "status = " << getStatus() << endl;
    os << "dbroot = " << getDBRoot() << endl;
    os << "row data...\n";

    uint32_t max_cnt = used.empty() ? getRowCount() : (used.size() * 64);
    for (uint32_t i = 0; i < max_cnt; i++)
    {
      if (!used.empty() && !(used[i / 64] & (1ULL << (i % 64))))
        continue;
      os << r.toString(i) << endl;
      r.nextRow();
    }
  }

  return os.str();
}

boost::shared_array<int> makeMapping(const RowGroup& r1, const RowGroup& r2)
{
  shared_array<int> ret(new int[r1.getColumnCount()]);
  // bool reserved[r2.getColumnCount()];
  bool* reserved = (bool*)alloca(r2.getColumnCount() * sizeof(bool));
  uint32_t i, j;

  for (i = 0; i < r2.getColumnCount(); i++)
    reserved[i] = false;

  for (i = 0; i < r1.getColumnCount(); i++)
  {
    for (j = 0; j < r2.getColumnCount(); j++)
      if ((r1.getKeys()[i] == r2.getKeys()[j]) && !reserved[j])
      {
        ret[i] = j;
        reserved[j] = true;
        break;
      }

    if (j == r2.getColumnCount())
      ret[i] = -1;
  }

  return ret;
}

void applyMapping(const boost::shared_array<int>& mapping, const Row& in, Row* out)
{
  applyMapping(mapping.get(), in, out);
}

void applyMapping(const std::vector<int>& mapping, const Row& in, Row* out)
{
  applyMapping((int*)&mapping[0], in, out);
}

void applyMapping(const int* mapping, const Row& in, Row* out)
{
  uint32_t i;

  for (i = 0; i < in.getColumnCount(); i++)
    if (mapping[i] != -1)
    {
      if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY ||
                   in.getColTypes()[i] == execplan::CalpontSystemCatalog::BLOB ||
                   in.getColTypes()[i] == execplan::CalpontSystemCatalog::TEXT))
        out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), mapping[i]);
      else if (UNLIKELY(in.isLongString(i)))
        out->setStringField(in.getConstString(i), mapping[i]);
      else if (UNLIKELY(in.isShortString(i)))
        out->setUintField(in.getUintField(i), mapping[i]);
      else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
        out->setLongDoubleField(in.getLongDoubleField(i), mapping[i]);
      // WIP this doesn't look right b/c we can pushdown colType
      // Migrate to offset based methods here
      // code precision 2 width convertor
      else if (UNLIKELY(datatypes::isWideDecimalType(in.getColTypes()[i], in.getColumnWidth(i))))
        out->setBinaryField_offset(in.getBinaryField<int128_t>(i), 16, out->getOffset(mapping[i]));
      else if (in.isUnsigned(i))
        out->setUintField(in.getUintField(i), mapping[i]);
      else
        out->setIntField(in.getIntField(i), mapping[i]);
    }
}

RowGroup& RowGroup::operator+=(const RowGroup& rhs)
{
  boost::shared_array<bool> tmp;
  uint32_t i, j;
  // not appendable if data is set
  assert(!data);

  tmp.reset(new bool[columnCount + rhs.columnCount]);

  for (i = 0; i < columnCount; i++)
    tmp[i] = forceInline[i];

  for (j = 0; j < rhs.columnCount; i++, j++)
    tmp[i] = rhs.forceInline[j];

  forceInline.swap(tmp);

  columnCount += rhs.columnCount;
  oids.insert(oids.end(), rhs.oids.begin(), rhs.oids.end());
  keys.insert(keys.end(), rhs.keys.begin(), rhs.keys.end());
  types.insert(types.end(), rhs.types.begin(), rhs.types.end());
  charsetNumbers.insert(charsetNumbers.end(), rhs.charsetNumbers.begin(), rhs.charsetNumbers.end());
  charsets.insert(charsets.end(), rhs.charsets.begin(), rhs.charsets.end());
  scale.insert(scale.end(), rhs.scale.begin(), rhs.scale.end());
  precision.insert(precision.end(), rhs.precision.begin(), rhs.precision.end());
  colWidths.insert(colWidths.end(), rhs.colWidths.begin(), rhs.colWidths.end());

  //    +4  +4  +8       +2 +4  +8
  // (2, 6, 10, 18) + (2, 4, 8, 16) = (2, 6, 10, 18, 20, 24, 32)
  for (i = 1; i < rhs.stOffsets.size(); i++)
  {
    stOffsets.push_back(stOffsets.back() + rhs.stOffsets[i] - rhs.stOffsets[i - 1]);
    oldOffsets.push_back(oldOffsets.back() + rhs.oldOffsets[i] - rhs.oldOffsets[i - 1]);
  }

  hasLongStringField = rhs.hasLongStringField || hasLongStringField;
  useStringTable = rhs.useStringTable || useStringTable;
  hasCollation = rhs.hasCollation || hasCollation;
  offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);

  return *this;
}

RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs)
{
  RowGroup temp(lhs);
  return temp += rhs;
}

uint32_t RowGroup::getDBRoot() const
{
  return *((uint32_t*)&data[dbRootOffset]);
}

void RowGroup::addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList)
{
  execplan::ColumnResult* cr;

  rowgroup::Row row;
  initRow(&row);
  uint32_t rowCount = getRowCount();
  uint32_t columnCount = getColumnCount();

  for (uint32_t i = 0; i < rowCount; i++)
  {
    getRow(i, &row);

    for (uint32_t j = 0; j < columnCount; j++)
    {
      int idx = sysDataList.findColumn(getOIDs()[j]);

      if (idx >= 0)
      {
        cr = sysDataList.sysDataVec[idx];
      }
      else
      {
        cr = new execplan::ColumnResult();
        cr->SetColumnOID(getOIDs()[j]);
        sysDataList.push_back(cr);
      }

      // @todo more data type checking. for now only check string, midint and bigint
      switch ((getColTypes()[j]))
      {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        {
          switch (getColumnWidth(j))
          {
            case 1: cr->PutData(row.getUintField<1>(j)); break;

            case 2: cr->PutData(row.getUintField<2>(j)); break;

            case 4: cr->PutData(row.getUintField<4>(j)); break;

            case 8: cr->PutData(row.getUintField<8>(j)); break;
            case 16:

            default:
            {
              string s = row.getStringField(j);
              cr->PutStringData(string(s.c_str(), strlen(s.c_str())));
            }
          }

          break;
        }

        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::UINT: cr->PutData(row.getIntField<4>(j)); break;

        case CalpontSystemCatalog::DATE: cr->PutData(row.getUintField<4>(j)); break;

        default: cr->PutData(row.getIntField<8>(j));
      }

      cr->PutRid(row.getFileRelativeRid());
    }
  }
}

const CHARSET_INFO* RowGroup::getCharset(uint32_t col)
{
  if (charsets[col] == NULL)
  {
    charsets[col] = &datatypes::Charset(charsetNumbers[col]).getCharset();
  }
  return charsets[col];
}

void RowGroup::setDBRoot(uint32_t dbroot)
{
  *((uint32_t*)&data[dbRootOffset]) = dbroot;
}

RGData RowGroup::duplicate()
{
  RGData ret(*this, getRowCount());

  if (useStringTable)
  {
    // this isn't a straight memcpy of everything b/c it might be remapping strings.
    // think about a big memcpy + a remap operation; might be faster.
    Row r1, r2;
    RowGroup rg(*this);
    rg.setData(&ret);
    rg.resetRowGroup(getBaseRid());
    rg.setStatus(getStatus());
    rg.setRowCount(getRowCount());
    rg.setDBRoot(getDBRoot());
    initRow(&r1);
    initRow(&r2);
    getRow(0, &r1);
    rg.getRow(0, &r2);

    for (uint32_t i = 0; i < getRowCount(); i++)
    {
      copyRow(r1, &r2);
      r1.nextRow();
      r2.nextRow();
    }
  }
  else
    memcpy(ret.rowData.get(), data, getDataSize());

  return ret;
}

void Row::setStringField(const std::string& val, uint32_t colIndex)
{
  uint64_t offset;
  uint64_t length;

  // length = strlen(val.c_str()) + 1;
  length = val.length();

  if (length > getColumnWidth(colIndex))
    length = getColumnWidth(colIndex);

  if (inStringTable(colIndex))
  {
    offset = strings->storeString((const uint8_t*)val.data(), length);
    *((uint64_t*)&data[offsets[colIndex]]) = offset;
    //		cout << " -- stored offset " << *((uint32_t *) &data[offsets[colIndex]])
    //				<< " length " << *((uint32_t *) &data[offsets[colIndex] + 4])
    //				<< endl;
  }
  else
  {
    memcpy(&data[offsets[colIndex]], val.data(), length);
    memset(&data[offsets[colIndex] + length], 0, offsets[colIndex + 1] - (offsets[colIndex] + length));
  }
}

void RowGroup::append(RGData& rgd)
{
  RowGroup tmp(*this);
  Row src, dest;

  tmp.setData(&rgd);
  initRow(&src);
  initRow(&dest);
  tmp.getRow(0, &src);
  getRow(getRowCount(), &dest);

  for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow())
  {
    // cerr << "appending row: " << src.toString() << endl;
    copyRow(src, &dest);
  }

  setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup& rg)
{
  append(*rg.getRGData());
}

void RowGroup::append(RGData& rgd, uint32_t startPos)
{
  RowGroup tmp(*this);
  Row src, dest;

  tmp.setData(&rgd);
  initRow(&src);
  initRow(&dest);
  tmp.getRow(0, &src);
  getRow(startPos, &dest);

  for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow())
  {
    // cerr << "appending row: " << src.toString() << endl;
    copyRow(src, &dest);
  }

  setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup& rg, uint32_t startPos)
{
  append(*rg.getRGData(), startPos);
}

RowGroup RowGroup::truncate(uint32_t cols)
{
  idbassert(cols <= columnCount);

  RowGroup ret(*this);
  ret.columnCount = cols;
  ret.oldOffsets.resize(cols + 1);
  ret.stOffsets.resize(cols + 1);
  ret.colWidths.resize(cols);
  ret.oids.resize(cols);
  ret.keys.resize(cols);
  ret.types.resize(cols);
  ret.charsetNumbers.resize(cols);
  ret.charsets.resize(cols);
  ret.scale.resize(cols);
  ret.precision.resize(cols);
  ret.forceInline.reset(new bool[cols]);
  memcpy(ret.forceInline.get(), forceInline.get(), cols * sizeof(bool));

  ret.hasLongStringField = false;
  ret.hasCollation = false;

  for (uint32_t i = 0; i < columnCount && (!ret.hasLongStringField || !ret.hasCollation); i++)
  {
    if (colWidths[i] >= sTableThreshold && !forceInline[i])
    {
      ret.hasLongStringField = true;
    }

    if (colHasCollation(i))
    {
      ret.hasCollation = true;
    }
  }

  ret.useStringTable = (ret.useStringTable && ret.hasLongStringField);
  ret.offsets = (ret.useStringTable ? &ret.stOffsets[0] : &ret.oldOffsets[0]);
  return ret;
}

}  // namespace rowgroup

// vim:ts=4 sw=4:
