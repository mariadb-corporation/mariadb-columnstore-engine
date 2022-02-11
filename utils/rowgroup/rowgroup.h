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
// C++ Interface: rowgroup
//
// Description:
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008

#ifndef ROWGROUP_H_
#define ROWGROUP_H_

#include <vector>
#include <string>
#include <stdexcept>
//#define NDEBUG
#include <cassert>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread/mutex.hpp>
#include <cmath>
#include <cfloat>
#ifdef __linux__
#include <execinfo.h>
#endif

#if defined(_MSC_VER) && !defined(isnan)
#define isnan _isnan
#endif

#include "hasher.h"

#include "joblisttypes.h"
#include "bytestream.h"
#include "calpontsystemcatalog.h"
#include "exceptclasses.h"
#include "mcsv1_udaf.h"

#include "branchpred.h"
#include "datatypes/mcs_int128.h"

#include "../winport/winport.h"

#include "collation.h"
#include "common/hashfamily.h"

// Workaround for my_global.h #define of isnan(X) causing a std::std namespace

namespace rowgroup
{
const int16_t rgCommonSize = 8192;

/*
    The RowGroup family of classes encapsulate the data moved through the
    system.

     - RowGroup specifies the format of the data primarily (+ some other metadata),
     - RGData (aka RowGroup Data) encapsulates the data,
     - Row is used to extract fields from the data and iterate.

    JobListFactory instantiates the RowGroups to be used by each stage of processing.
    RGDatas are passed between stages, and their RowGroup instances are used
    to interpret them.

    Historically, row data was just a chunk of contiguous memory, a uint8_t *.
    Every field had a fixed width, which allowed for quick offset
    calculation when assigning or retrieving individual fields.  That worked
    well for a few years, but at some point it became common to declare
    all strings as max-length, and to manipulate them in queries.

    Having fixed-width fields, even for strings, required an unreasonable
    amount of memory.  RGData & StringStore were introduced to handle strings
    more efficiently, at least with respect to memory.  The row data would
    still be a uint8_t *, and columns would be fixed-width, but string fields
    above a certain width would contain a 'Pointer' that referenced a string in
    StringStore.  Strings are stored efficiently in StringStore, so there is
    no longer wasted space.

    StringStore comes with a different inefficiency however.  When a value
    is overwritten, the original string cannot be freed independently of the
    others, so it continues to use space.  If values are only set once, as is
    the typical case, then StringStore is efficient.  When it is necessary
    to overwrite string fields, it is possible to configure these classes
    to use the original data format so that old string fields do not accumulate
    in memory.  Of course, be careful, because blobs and text fields in CS are
    declared as 2GB strings!

    A single RGData contains up to one 'logical block' worth of data,
    which is 8192 rows.  One RGData is usually treated as one unit of work by
    PrimProc and the JobSteps, but the rows an RGData contains and how many are
    treated as a work unit depend on the operation being done.

    For example, PrimProc works in units of 8192 contiguous rows
    that come from disk.  If half of the rows were filtered out, then the
    RGData it passes to the next stage would only contain 4096 rows.

    Others build results incrementally before passing them along, such as
    group-by.  If one group contains 11111 values, then group-by will
    return 2 RGDatas for that group, one with 8192 rows, and one with 2919.

    Note: There is no synchronization in any of these classes for obvious
    performance reasons.  Likewise, although it's technically safe for many
    readers to access an RGData simultaneously, that would not be an
    efficient thing to do.  Try to stick to designs where a single RGData
    is used by a single thread at a time.
*/

// VS'08 carps that struct MemChunk is not default copyable because of the zero-length array.
// This may be so, and we'll get link errors if someone trys, but so far no one has.
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4200)
#endif

// Helper to get a value from nested vector pointers.
template <typename T>
inline T derefFromTwoVectorPtrs(const std::vector<T>* outer, const std::vector<T>* inner, const T innerIdx)
{
  auto outerIdx = inner->operator[](innerIdx);
  return outer->operator[](outerIdx);
}

class StringStore
{
 public:
  StringStore();
  virtual ~StringStore();

  inline std::string getString(uint64_t offset) const;
  uint64_t storeString(const uint8_t* data, uint32_t length);  // returns the offset
  inline const uint8_t* getPointer(uint64_t offset) const;
  inline uint32_t getStringLength(uint64_t offset) const;
  inline utils::ConstString getConstString(uint64_t offset) const
  {
    return utils::ConstString((const char*)getPointer(offset), getStringLength(offset));
  }
  inline bool isEmpty() const;
  inline uint64_t getSize() const;
  inline bool isNullValue(uint64_t offset) const;

  void clear();

  void serialize(messageqcpp::ByteStream&) const;
  void deserialize(messageqcpp::ByteStream&);

  //@bug6065, make StringStore::storeString() thread safe
  void useStoreStringMutex(bool b)
  {
    fUseStoreStringMutex = b;
  }
  bool useStoreStringMutex() const
  {
    return fUseStoreStringMutex;
  }

  // This is an overlay b/c the underlying data needs to be any size,
  // and alloc'd in one chunk.  data can't be a separate dynamic chunk.
  // NOTE: Change here, requires a change in 'bytestream.h'.
  struct MemChunk
  {
    uint32_t currentSize;
    uint32_t capacity;
    uint8_t data[];
  };

 private:
  std::string empty_str;

  StringStore(const StringStore&);
  StringStore& operator=(const StringStore&);
  static constexpr const uint32_t CHUNK_SIZE = 64 * 1024;  // allocators like powers of 2

  std::vector<boost::shared_array<uint8_t>> mem;

  // To store strings > 64KB (BLOB/TEXT)
  std::vector<boost::shared_array<uint8_t>> longStrings;
  bool empty;
  bool fUseStoreStringMutex;  //@bug6065, make StringStore::storeString() thread safe
  boost::mutex fMutex;
};

// Where we store user data for UDA(n)F
class UserDataStore
{
  // length represents the fixed portion length of userData.
  // There may be variable length data in containers or other
  // user created structures.
  struct StoreData
  {
    int32_t length;
    std::string functionName;
    boost::shared_ptr<mcsv1sdk::UserData> userData;
    StoreData() : length(0)
    {
    }
    StoreData(const StoreData& rhs)
    {
      length = rhs.length;
      functionName = rhs.functionName;
      userData = rhs.userData;
    }
  };

 public:
  UserDataStore();
  virtual ~UserDataStore();

  void serialize(messageqcpp::ByteStream&) const;
  void deserialize(messageqcpp::ByteStream&);

  // Set to make UserDataStore thread safe
  void useUserDataMutex(bool b)
  {
    fUseUserDataMutex = b;
  }
  bool useUserDataMutex() const
  {
    return fUseUserDataMutex;
  }

  // Returns the offset
  uint32_t storeUserData(mcsv1sdk::mcsv1Context& context, boost::shared_ptr<mcsv1sdk::UserData> data,
                         uint32_t length);

  boost::shared_ptr<mcsv1sdk::UserData> getUserData(uint32_t offset) const;

 private:
  UserDataStore(const UserDataStore&);
  UserDataStore& operator=(const UserDataStore&);

  std::vector<StoreData> vStoreData;

  bool fUseUserDataMutex;
  boost::mutex fMutex;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

class RowGroup;
class Row;

/* TODO: OO the rowgroup data to the extent there's no measurable performance hit. */
class RGData
{
 public:
  RGData();  // useless unless followed by an = or a deserialize operation
  RGData(const RowGroup& rg, uint32_t rowCount);  // allocates memory for rowData
  explicit RGData(const RowGroup& rg);
  RGData(const RGData&);
  virtual ~RGData();

  inline RGData& operator=(const RGData&);

  // amount should be the # returned by RowGroup::getDataSize()
  void serialize(messageqcpp::ByteStream&, uint32_t amount) const;

  // the 'hasLengthField' is there b/c PM aggregation (and possibly others) currently sends
  // inline data with a length field.  Once that's converted to string table format, that
  // option can go away.
  void deserialize(messageqcpp::ByteStream&, uint32_t amount = 0);  // returns the # of bytes read

  inline uint64_t getStringTableMemUsage();
  void clear();
  void reinit(const RowGroup& rg);
  void reinit(const RowGroup& rg, uint32_t rowCount);
  inline void setStringStore(boost::shared_ptr<StringStore>& ss)
  {
    strings = ss;
  }

  // this will use the pre-configured Row to figure out where row # num is, then set the Row
  // to point to it.  It's a shortcut around using a RowGroup to do the same thing for cases
  // where it's inconvenient to instantiate one.
  inline void getRow(uint32_t num, Row* row);

  //@bug6065, make StringStore::storeString() thread safe
  void useStoreStringMutex(bool b)
  {
    if (strings)
      strings->useStoreStringMutex(b);
  }
  bool useStoreStringMutex() const
  {
    return (strings ? (strings->useStoreStringMutex()) : false);
  }

  UserDataStore* getUserDataStore();
  // make UserDataStore::storeData() thread safe
  void useUserDataMutex(bool b)
  {
    if (userDataStore)
      userDataStore->useUserDataMutex(b);
  }
  bool useUserDataMutex() const
  {
    return (userDataStore ? (userDataStore->useUserDataMutex()) : false);
  }

  boost::shared_array<uint8_t> rowData;
  boost::shared_ptr<StringStore> strings;
  boost::shared_ptr<UserDataStore> userDataStore;

 private:
  // boost::shared_array<uint8_t> rowData;
  // boost::shared_ptr<StringStore> strings;

  // Need sig to support backward compat.  RGData can deserialize both forms.
  static const uint32_t RGDATA_SIG = 0xffffffff;  // won't happen for 'old' Rowgroup data

  friend class RowGroup;
};

class Row
{
 public:
  struct Pointer
  {
    inline Pointer() : data(NULL), strings(NULL), userDataStore(NULL)
    {
    }

    // Pointer(uint8_t*) implicitly makes old code compatible with the string table impl;
    inline Pointer(uint8_t* d) : data(d), strings(NULL), userDataStore(NULL)
    {
    }
    inline Pointer(uint8_t* d, StringStore* s) : data(d), strings(s), userDataStore(NULL)
    {
    }
    inline Pointer(uint8_t* d, StringStore* s, UserDataStore* u) : data(d), strings(s), userDataStore(u)
    {
    }
    uint8_t* data;
    StringStore* strings;
    UserDataStore* userDataStore;
  };

  Row();
  Row(const Row&);
  ~Row();

  Row& operator=(const Row&);
  bool operator==(const Row&) const;

  // void setData(uint8_t *rowData, StringStore *ss);
  inline void setData(const Pointer&);  // convenience fcn, can go away
  inline uint8_t* getData() const;

  inline void setPointer(const Pointer&);
  inline Pointer getPointer() const;

  inline void nextRow();
  inline uint32_t getColumnWidth(uint32_t colIndex) const;
  inline uint32_t getColumnCount() const;
  inline uint32_t getSize() const;  // this is only accurate if there is no string table
  // if a string table is being used, getRealSize() takes into account variable-length strings
  inline uint32_t getRealSize() const;
  inline uint32_t getOffset(uint32_t colIndex) const;
  inline uint32_t getScale(uint32_t colIndex) const;
  inline uint32_t getPrecision(uint32_t colIndex) const;
  inline execplan::CalpontSystemCatalog::ColDataType getColType(uint32_t colIndex) const;
  inline execplan::CalpontSystemCatalog::ColDataType* getColTypes();
  inline const execplan::CalpontSystemCatalog::ColDataType* getColTypes() const;
  inline uint32_t getCharsetNumber(uint32_t colIndex) const;

  // this returns true if the type is not CHAR or VARCHAR
  inline bool isCharType(uint32_t colIndex) const;
  inline bool isUnsigned(uint32_t colIndex) const;
  inline bool isShortString(uint32_t colIndex) const;
  inline bool isLongString(uint32_t colIndex) const;

  bool colHasCollation(uint32_t colIndex) const
  {
    return datatypes::typeHasCollation(getColType(colIndex));
  }

  template <int len>
  inline uint64_t getUintField(uint32_t colIndex) const;
  inline uint64_t getUintField(uint32_t colIndex) const;
  template <int len>
  inline int64_t getIntField(uint32_t colIndex) const;
  inline int64_t getIntField(uint32_t colIndex) const;
  // Get a signed 64-bit integer column value, convert to the given
  // floating point data type T (e.g. float, double, long double)
  // and divide it according to the scale.
  template <typename T>
  inline T getScaledSInt64FieldAsXFloat(uint32_t colIndex, uint32_t scale) const
  {
    const T d = getIntField(colIndex);
    if (!scale)
      return d;
    return d / datatypes::scaleDivisor<T>(scale);
  }
  template <typename T>
  inline T getScaledSInt64FieldAsXFloat(uint32_t colIndex) const
  {
    return getScaledSInt64FieldAsXFloat<T>(colIndex, getScale(colIndex));
  }
  // Get an unsigned 64-bit integer column value, convert to the given
  // floating point data type T (e.g. float, double, long double)
  // and divide it according to the scale.
  template <typename T>
  inline T getScaledUInt64FieldAsXFloat(uint32_t colIndex, uint32_t scale) const
  {
    const T d = getUintField(colIndex);
    if (!scale)
      return d;
    return d / datatypes::scaleDivisor<T>(scale);
  }
  template <typename T>
  inline T getScaledUInt64FieldAsXFloat(uint32_t colIndex) const
  {
    return getScaledUInt64FieldAsXFloat<T>(colIndex, getScale(colIndex));
  }
  template <typename T>
  inline bool equals(T* value, uint32_t colIndex) const;
  template <int len>
  inline bool equals(uint64_t val, uint32_t colIndex) const;
  inline bool equals(long double val, uint32_t colIndex) const;
  inline bool equals(const int128_t& val, uint32_t colIndex) const;

  inline double getDoubleField(uint32_t colIndex) const;
  inline float getFloatField(uint32_t colIndex) const;
  inline datatypes::Decimal getDecimalField(uint32_t colIndex) const
  {
    if (LIKELY(getColumnWidth(colIndex) == datatypes::MAXDECIMALWIDTH))
      return datatypes::Decimal(0, (int)getScale(colIndex), getPrecision(colIndex),
                                getBinaryField<int128_t>(colIndex));
    return datatypes::Decimal(datatypes::TSInt64(getIntField(colIndex)), (int)getScale(colIndex),
                              getPrecision(colIndex));
  }
  inline long double getLongDoubleField(uint32_t colIndex) const;
  inline void storeInt128FieldIntoPtr(uint32_t colIndex, uint8_t* x) const;
  inline void getInt128Field(uint32_t colIndex, int128_t& x) const;
  inline datatypes::TSInt128 getTSInt128Field(uint32_t colIndex) const;

  inline uint64_t getBaseRid() const;
  inline uint64_t getRid() const;
  inline uint16_t getRelRid() const;             // returns a rid relative to this logical block
  inline uint64_t getExtentRelativeRid() const;  // returns a rid relative to the extent it's in
  inline uint64_t getFileRelativeRid() const;    // returns a file-relative rid
  inline void getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum,
                          uint16_t* rowNum);

  template <int len>
  void setUintField(uint64_t val, uint32_t colIndex);

  /* Note: these 2 fcns avoid 1 array lookup per call.  Using them only
  in projection on the PM resulted in a 2.8% performance gain on
  the queries listed in bug 2223.
  TODO: apply them everywhere else possible, and write equivalents
  for the other types as well as the getters.
  */
  template <int len>
  void setUintField_offset(uint64_t val, uint32_t offset);
  template <typename T>
  void setIntField_offset(const T val, const uint32_t offset);
  inline void nextRow(uint32_t size);
  inline void prevRow(uint32_t size, uint64_t number);

  inline void setUintField(uint64_t val, uint32_t colIndex);
  template <int len>
  void setIntField(int64_t, uint32_t colIndex);
  inline void setIntField(int64_t, uint32_t colIndex);

  inline void setDoubleField(double val, uint32_t colIndex);
  inline void setFloatField(float val, uint32_t colIndex);
  inline void setDecimalField(double val, uint32_t colIndex){};  // TODO: Do something here
  inline void setLongDoubleField(const long double& val, uint32_t colIndex);
  inline void setInt128Field(const int128_t& val, uint32_t colIndex);

  inline void setRid(uint64_t rid);

  // TODO: remove this (string is not efficient for this), use getConstString() instead
  inline std::string getStringField(uint32_t colIndex) const
  {
    return getConstString(colIndex).toString();
  }

  inline utils::ConstString getConstString(uint32_t colIndex) const;
  inline utils::ConstString getShortConstString(uint32_t colIndex) const;
  void setStringField(const std::string& val, uint32_t colIndex);
  inline void setStringField(const utils::ConstString& str, uint32_t colIndex);
  template <typename T>
  inline void setBinaryField(const T* value, uint32_t width, uint32_t colIndex);
  template <typename T>
  inline void setBinaryField(const T* value, uint32_t colIndex);
  template <typename T>
  inline void setBinaryField_offset(const T* value, uint32_t width, uint32_t colIndex);
  // support VARBINARY
  // Add 2-byte length at the CHARSET_INFO*beginning of the field.  NULL and zero length field are
  // treated the same, could use one of the length bit to distinguish these two cases.
  inline std::string getVarBinaryStringField(uint32_t colIndex) const;
  inline void setVarBinaryField(const std::string& val, uint32_t colIndex);
  // No string construction is necessary for better performance.
  inline uint32_t getVarBinaryLength(uint32_t colIndex) const;
  inline const uint8_t* getVarBinaryField(uint32_t colIndex) const;
  inline const uint8_t* getVarBinaryField(uint32_t& len, uint32_t colIndex) const;
  inline void setVarBinaryField(const uint8_t* val, uint32_t len, uint32_t colIndex);

  // inline std::string getBinaryField(uint32_t colIndex) const;
  template <typename T>
  inline T* getBinaryField(uint32_t colIndex) const;
  // To simplify parameter type deduction.
  template <typename T>
  inline T* getBinaryField(T* argtype, uint32_t colIndex) const;
  template <typename T>
  inline T* getBinaryField_offset(uint32_t offset) const;

  inline boost::shared_ptr<mcsv1sdk::UserData> getUserData(uint32_t colIndex) const;
  inline void setUserData(mcsv1sdk::mcsv1Context& context, boost::shared_ptr<mcsv1sdk::UserData> userData,
                          uint32_t len, uint32_t colIndex);

  uint64_t getNullValue(uint32_t colIndex) const;
  bool isNullValue(uint32_t colIndex) const;
  template <cscDataType cscDT, int width>
  inline bool isNullValue_offset(uint32_t offset) const;

  // when NULLs are pulled out via getIntField(), they come out with these values.
  // Ex: the 1-byte int null value is 0x80.  When it gets cast to an int64_t
  // it becomes 0xffffffffffffff80, which won't match anything returned by getNullValue().
  int64_t getSignedNullValue(uint32_t colIndex) const;

  // copy data in srcIndex field to destIndex, all data type
  inline void copyField(uint32_t destIndex, uint32_t srcIndex) const;

  // copy data in srcIndex field to destAddr, all data type
  // inline void copyField(uint8_t* destAddr, uint32_t srcIndex) const;

  // an adapter for code that uses the copyField call above;
  // that's not string-table safe, this one is
  inline void copyField(Row& dest, uint32_t destIndex, uint32_t srcIndex) const;

  template <typename T>
  inline void copyBinaryField(Row& dest, uint32_t destIndex, uint32_t srcIndex) const;

  std::string toString(uint32_t rownum = 0) const;
  std::string toCSV() const;

  /* These fcns are used only in joins.  The RID doesn't matter on the side that
  gets hashed.  We steal that field here to "mark" a row. */
  inline void markRow();
  inline void zeroRid();
  inline bool isMarked();
  void initToNull();

  inline void usesStringTable(bool b)
  {
    useStringTable = b;
  }
  inline bool usesStringTable() const
  {
    return useStringTable;
  }
  inline bool hasLongString() const
  {
    return hasLongStringField;
  }

  // these are for cases when you already know the type definitions are the same.
  // a fcn to check the type defs seperately doesn't exist yet.  No normalization.
  inline uint64_t hash(uint32_t lastCol) const;  // generates a hash for cols [0-lastCol]
  inline uint64_t hash() const;                  // generates a hash for all cols
  inline void colUpdateHasher(datatypes::MariaDBHasher& hM, const utils::Hasher_r& h, const uint32_t col,
                              uint32_t& intermediateHash) const;
  inline void colUpdateHasherTypeless(datatypes::MariaDBHasher& hasher, uint32_t keyColsIdx,
                                      const std::vector<uint32_t>& keyCols,
                                      const std::vector<uint32_t>* smallSideKeyColumnsIds,
                                      const std::vector<uint32_t>* smallSideColumnsWidths) const;
  inline uint64_t hashTypeless(const std::vector<uint32_t>& keyCols,
                               const std::vector<uint32_t>* smallSideKeyColumnsIds,
                               const std::vector<uint32_t>* smallSideColumnsWidths) const
  {
    datatypes::MariaDBHasher h;
    for (uint32_t i = 0; i < keyCols.size(); i++)
      colUpdateHasherTypeless(h, i, keyCols, smallSideKeyColumnsIds, smallSideColumnsWidths);
    return h.finalize();
  }

  bool equals(const Row&, uint32_t lastCol) const;
  inline bool equals(const Row&) const;

  inline void setUserDataStore(UserDataStore* u)
  {
    userDataStore = u;
  }

  const CHARSET_INFO* getCharset(uint32_t col) const;

 private:
  uint32_t columnCount;
  uint64_t baseRid;

  // Note, the mem behind these pointer fields is owned by RowGroup not Row
  uint32_t* oldOffsets;
  uint32_t* stOffsets;
  uint32_t* offsets;
  uint32_t* colWidths;
  execplan::CalpontSystemCatalog::ColDataType* types;
  uint32_t* charsetNumbers;
  CHARSET_INFO** charsets;
  uint8_t* data;
  uint32_t* scale;
  uint32_t* precision;

  StringStore* strings;
  bool useStringTable;
  bool hasCollation;
  bool hasLongStringField;
  uint32_t sTableThreshold;
  boost::shared_array<bool> forceInline;
  inline bool inStringTable(uint32_t col) const;

  UserDataStore* userDataStore;  // For UDAF

  friend class RowGroup;
};

inline Row::Pointer Row::getPointer() const
{
  return Pointer(data, strings, userDataStore);
}
inline uint8_t* Row::getData() const
{
  return data;
}

inline void Row::setPointer(const Pointer& p)
{
  data = p.data;
  strings = p.strings;
  bool hasStrings = (strings != 0);

  if (useStringTable != hasStrings)
  {
    useStringTable = hasStrings;
    offsets = (useStringTable ? stOffsets : oldOffsets);
  }

  userDataStore = p.userDataStore;
}

inline void Row::setData(const Pointer& p)
{
  setPointer(p);
}

inline void Row::nextRow()
{
  data += offsets[columnCount];
}

inline uint32_t Row::getColumnCount() const
{
  return columnCount;
}

inline uint32_t Row::getColumnWidth(uint32_t col) const
{
  return colWidths[col];
}

inline uint32_t Row::getSize() const
{
  return offsets[columnCount];
}

inline uint32_t Row::getRealSize() const
{
  if (!useStringTable)
    return getSize();

  uint32_t ret = 2;

  for (uint32_t i = 0; i < columnCount; i++)
  {
    if (!inStringTable(i))
      ret += getColumnWidth(i);
    else
      ret += getConstString(i).length();
  }

  return ret;
}

inline uint32_t Row::getScale(uint32_t col) const
{
  return scale[col];
}

inline uint32_t Row::getPrecision(uint32_t col) const
{
  return precision[col];
}

inline execplan::CalpontSystemCatalog::ColDataType Row::getColType(uint32_t colIndex) const
{
  return types[colIndex];
}

inline execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes()
{
  return types;
}

inline const execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes() const
{
  return types;
}

inline uint32_t Row::getCharsetNumber(uint32_t col) const
{
  return charsetNumbers[col];
}

inline bool Row::isCharType(uint32_t colIndex) const
{
  return datatypes::isCharType(types[colIndex]);
}

inline bool Row::isUnsigned(uint32_t colIndex) const
{
  return datatypes::isUnsigned(types[colIndex]);
}

inline bool Row::isShortString(uint32_t colIndex) const
{
  return (getColumnWidth(colIndex) <= 8 && isCharType(colIndex));
}

inline bool Row::isLongString(uint32_t colIndex) const
{
  return (getColumnWidth(colIndex) > 8 && isCharType(colIndex));
}

inline bool Row::inStringTable(uint32_t col) const
{
  return strings && getColumnWidth(col) >= sTableThreshold && !forceInline[col];
}

template <typename T>
inline bool Row::equals(T* value, uint32_t colIndex) const
{
  return *reinterpret_cast<T*>(&data[offsets[colIndex]]) == *value;
}

template <int len>
inline bool Row::equals(uint64_t val, uint32_t colIndex) const
{
  /* I think the compiler will optimize away the switch stmt */
  switch (len)
  {
    case 1: return data[offsets[colIndex]] == val;

    case 2: return *((uint16_t*)&data[offsets[colIndex]]) == val;

    case 4: return *((uint32_t*)&data[offsets[colIndex]]) == val;

    case 8: return *((uint64_t*)&data[offsets[colIndex]]) == val;
    default: idbassert(0); throw std::logic_error("Row::equals(): bad length.");
  }
}

inline bool Row::equals(long double val, uint32_t colIndex) const
{
  return *((long double*)&data[offsets[colIndex]]) == val;
}

inline bool Row::equals(const int128_t& val, uint32_t colIndex) const
{
  return *((int128_t*)&data[offsets[colIndex]]) == val;
}

template <int len>
inline uint64_t Row::getUintField(uint32_t colIndex) const
{
  /* I think the compiler will optimize away the switch stmt */
  switch (len)
  {
    case 1: return data[offsets[colIndex]];

    case 2: return *((uint16_t*)&data[offsets[colIndex]]);

    case 4: return *((uint32_t*)&data[offsets[colIndex]]);

    case 8: return *((uint64_t*)&data[offsets[colIndex]]);
    default: idbassert(0); throw std::logic_error("Row::getUintField(): bad length.");
  }
}

inline uint64_t Row::getUintField(uint32_t colIndex) const
{
  switch (getColumnWidth(colIndex))
  {
    case 1: return data[offsets[colIndex]];

    case 2: return *((uint16_t*)&data[offsets[colIndex]]);

    case 4: return *((uint32_t*)&data[offsets[colIndex]]);
    case 8: return *((uint64_t*)&data[offsets[colIndex]]);

    default: idbassert(0); throw std::logic_error("Row::getUintField(): bad length.");
  }
}

template <int len>
inline int64_t Row::getIntField(uint32_t colIndex) const
{
  /* I think the compiler will optimize away the switch stmt */
  switch (len)
  {
    case 1: return (int8_t)data[offsets[colIndex]];

    case 2: return *((int16_t*)&data[offsets[colIndex]]);

    case 4: return *((int32_t*)&data[offsets[colIndex]]);

    case 8: return *((int64_t*)&data[offsets[colIndex]]);

    default:
      std::cout << "Row::getIntField getColumnWidth(colIndex) " << getColumnWidth(colIndex) << std::endl;
      idbassert(0);
      throw std::logic_error("Row::getIntField(): bad length.");
  }
}

inline int64_t Row::getIntField(uint32_t colIndex) const
{
  /* I think the compiler will optimize away the switch stmt */
  switch (getColumnWidth(colIndex))
  {
    case 1: return (int8_t)data[offsets[colIndex]];

    case 2: return *((int16_t*)&data[offsets[colIndex]]);

    case 4: return *((int32_t*)&data[offsets[colIndex]]);

    case 8: return *((int64_t*)&data[offsets[colIndex]]);

    default: idbassert(0); throw std::logic_error("Row::getIntField(): bad length.");
  }
}

template <typename T>
inline void Row::setBinaryField(const T* value, uint32_t width, uint32_t colIndex)
{
  memcpy(&data[offsets[colIndex]], value, width);
}

template <typename T>
inline void Row::setBinaryField(const T* value, uint32_t colIndex)
{
  *reinterpret_cast<T*>(&data[offsets[colIndex]]) = *value;
}

template <>
inline void Row::setBinaryField<int128_t>(const int128_t* value, uint32_t colIndex)
{
  datatypes::TSInt128::assignPtrPtr(&data[offsets[colIndex]], value);
}

// This method !cannot! be applied to uint8_t* buffers.
template <typename T>
inline void Row::setBinaryField_offset(const T* value, uint32_t width, uint32_t offset)
{
  *reinterpret_cast<T*>(&data[offset]) = *value;
}

template <>
inline void Row::setBinaryField_offset<uint8_t>(const uint8_t* value, uint32_t width, uint32_t offset)
{
  memcpy(&data[offset], value, width);
}

template <>
inline void Row::setBinaryField_offset<int128_t>(const int128_t* value, uint32_t width, uint32_t offset)
{
  datatypes::TSInt128::assignPtrPtr(&data[offset], value);
}

inline utils::ConstString Row::getShortConstString(uint32_t colIndex) const
{
  const char* src = (const char*)&data[offsets[colIndex]];
  return utils::ConstString(src, strnlen(src, getColumnWidth(colIndex)));
}

inline utils::ConstString Row::getConstString(uint32_t colIndex) const
{
  return inStringTable(colIndex) ? strings->getConstString(*((uint64_t*)&data[offsets[colIndex]]))
                                 : getShortConstString(colIndex);
}

inline void Row::colUpdateHasher(datatypes::MariaDBHasher& hM, const utils::Hasher_r& h, const uint32_t col,
                                 uint32_t& intermediateHash) const
{
  switch (getColType(col))
  {
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      CHARSET_INFO* cs = getCharset(col);
      hM.add(cs, getConstString(col));
      break;
    }
    default:
    {
      intermediateHash = h((const char*)&data[offsets[col]], colWidths[col], intermediateHash);
      break;
    }
  }
}

inline void Row::colUpdateHasherTypeless(datatypes::MariaDBHasher& h, uint32_t keyColsIdx,
                                         const std::vector<uint32_t>& keyCols,
                                         const std::vector<uint32_t>* smallSideKeyColumnsIds,
                                         const std::vector<uint32_t>* smallSideColumnsWidths) const
{
  auto rowKeyColIdx = keyCols[keyColsIdx];
  auto largeSideColType = getColType(rowKeyColIdx);
  switch (largeSideColType)
  {
    case datatypes::SystemCatalog::CHAR:
    case datatypes::SystemCatalog::VARCHAR:
    case datatypes::SystemCatalog::BLOB:
    case datatypes::SystemCatalog::TEXT:
    {
      CHARSET_INFO* cs = getCharset(rowKeyColIdx);
      h.add(cs, getConstString(rowKeyColIdx));
      break;
    }
    case datatypes::SystemCatalog::DECIMAL:
    {
      auto width = getColumnWidth(rowKeyColIdx);
      if (datatypes::isWideDecimalType(largeSideColType, width))
      {
        bool joinHasSkewedKeyColumn = (smallSideColumnsWidths);
        datatypes::TSInt128 val = getTSInt128Field(rowKeyColIdx);
        if (joinHasSkewedKeyColumn &&
            width != derefFromTwoVectorPtrs(smallSideColumnsWidths, smallSideKeyColumnsIds, keyColsIdx))
        {
          if (val.getValue() >= std::numeric_limits<int64_t>::min() &&
              val.getValue() <= std::numeric_limits<uint64_t>::max())
          {
            h.add(&my_charset_bin, (const char*)&val.getValue(), datatypes::MAXLEGACYWIDTH);
          }
          else
            h.add(&my_charset_bin, (const char*)&val.getValue(), datatypes::MAXDECIMALWIDTH);
        }
        else
          h.add(&my_charset_bin, (const char*)&val.getValue(), datatypes::MAXDECIMALWIDTH);
      }
      else
      {
        int64_t val = getIntField(rowKeyColIdx);
        h.add(&my_charset_bin, (const char*)&val, datatypes::MAXLEGACYWIDTH);
      }

      break;
    }
    default:
    {
      if (isUnsigned(rowKeyColIdx))
      {
        uint64_t val = getUintField(rowKeyColIdx);
        h.add(&my_charset_bin, (const char*)&val, datatypes::MAXLEGACYWIDTH);
      }
      else
      {
        int64_t val = getIntField(rowKeyColIdx);
        h.add(&my_charset_bin, (const char*)&val, datatypes::MAXLEGACYWIDTH);
      }

      break;
    }
  }
}

inline void Row::setStringField(const utils::ConstString& str, uint32_t colIndex)
{
  uint64_t offset;

  // TODO: add multi-byte safe truncation here
  uint32_t length = str.length();
  if (length > getColumnWidth(colIndex))
    length = getColumnWidth(colIndex);

  if (inStringTable(colIndex))
  {
    offset = strings->storeString((const uint8_t*)str.str(), length);
    *((uint64_t*)&data[offsets[colIndex]]) = offset;
    //		cout << " -- stored offset " << *((uint32_t *) &data[offsets[colIndex]])
    //				<< " length " << *((uint32_t *) &data[offsets[colIndex] + 4])
    //				<< endl;
  }
  else
  {
    memcpy(&data[offsets[colIndex]], str.str(), length);
    memset(&data[offsets[colIndex] + length], 0, offsets[colIndex + 1] - (offsets[colIndex] + length));
  }
}

template <typename T>
inline T* Row::getBinaryField(uint32_t colIndex) const
{
  return getBinaryField_offset<T>(offsets[colIndex]);
}

template <typename T>
inline T* Row::getBinaryField(T* argtype, uint32_t colIndex) const
{
  return getBinaryField_offset<T>(offsets[colIndex]);
}

template <typename T>
inline T* Row::getBinaryField_offset(uint32_t offset) const
{
  return reinterpret_cast<T*>(&data[offset]);
}

inline std::string Row::getVarBinaryStringField(uint32_t colIndex) const
{
  if (inStringTable(colIndex))
    return getConstString(colIndex).toString();

  return std::string((char*)&data[offsets[colIndex] + 2], *((uint16_t*)&data[offsets[colIndex]]));
}

inline uint32_t Row::getVarBinaryLength(uint32_t colIndex) const
{
  if (inStringTable(colIndex))
    return strings->getStringLength(*((uint64_t*)&data[offsets[colIndex]]));
  ;

  return *((uint16_t*)&data[offsets[colIndex]]);
}

inline const uint8_t* Row::getVarBinaryField(uint32_t colIndex) const
{
  if (inStringTable(colIndex))
    return strings->getPointer(*((uint64_t*)&data[offsets[colIndex]]));

  return &data[offsets[colIndex] + 2];
}

inline const uint8_t* Row::getVarBinaryField(uint32_t& len, uint32_t colIndex) const
{
  if (inStringTable(colIndex))
  {
    len = strings->getStringLength(*((uint64_t*)&data[offsets[colIndex]]));
    return getVarBinaryField(colIndex);
  }
  else
  {
    len = *((uint16_t*)&data[offsets[colIndex]]);
    return &data[offsets[colIndex] + 2];
  }
}

inline boost::shared_ptr<mcsv1sdk::UserData> Row::getUserData(uint32_t colIndex) const
{
  if (!userDataStore)
  {
    return boost::shared_ptr<mcsv1sdk::UserData>();
  }

  return userDataStore->getUserData(*((uint32_t*)&data[offsets[colIndex]]));
}

inline double Row::getDoubleField(uint32_t colIndex) const
{
  return *((double*)&data[offsets[colIndex]]);
}

inline float Row::getFloatField(uint32_t colIndex) const
{
  return *((float*)&data[offsets[colIndex]]);
}

inline long double Row::getLongDoubleField(uint32_t colIndex) const
{
  return *((long double*)&data[offsets[colIndex]]);
}

inline void Row::storeInt128FieldIntoPtr(uint32_t colIndex, uint8_t* x) const
{
  datatypes::TSInt128::assignPtrPtr(x, &data[offsets[colIndex]]);
}

inline void Row::getInt128Field(uint32_t colIndex, int128_t& x) const
{
  datatypes::TSInt128::assignPtrPtr(&x, &data[offsets[colIndex]]);
}

inline datatypes::TSInt128 Row::getTSInt128Field(uint32_t colIndex) const
{
  const int128_t* ptr = getBinaryField<int128_t>(colIndex);
  return datatypes::TSInt128(ptr);
}

inline uint64_t Row::getRid() const
{
  return baseRid + *((uint16_t*)data);
}

inline uint16_t Row::getRelRid() const
{
  return *((uint16_t*)data);
}

inline uint64_t Row::getBaseRid() const
{
  return baseRid;
}

inline void Row::markRow()
{
  *((uint16_t*)data) = 0xffff;
}

inline void Row::zeroRid()
{
  *((uint16_t*)data) = 0;
}

inline bool Row::isMarked()
{
  return *((uint16_t*)data) == 0xffff;
}

/* Begin speculative code! */
inline uint32_t Row::getOffset(uint32_t colIndex) const
{
  return offsets[colIndex];
}

template <int len>
inline void Row::setUintField_offset(uint64_t val, uint32_t offset)
{
  switch (len)
  {
    case 1: data[offset] = val; break;

    case 2: *((uint16_t*)&data[offset]) = val; break;

    case 4: *((uint32_t*)&data[offset]) = val; break;

    case 8: *((uint64_t*)&data[offset]) = val; break;

    default: idbassert(0); throw std::logic_error("Row::setUintField called on a non-uint32_t field");
  }
}

template <typename T>
inline void Row::setIntField_offset(const T val, const uint32_t offset)
{
  *((T*)&data[offset]) = val;
}

inline void Row::nextRow(uint32_t size)
{
  data += size;
}

inline void Row::prevRow(uint32_t size, uint64_t number = 1)
{
  data -= size * number;
}

template <int len>
inline void Row::setUintField(uint64_t val, uint32_t colIndex)
{
  switch (len)
  {
    case 1: data[offsets[colIndex]] = val; break;

    case 2: *((uint16_t*)&data[offsets[colIndex]]) = val; break;

    case 4: *((uint32_t*)&data[offsets[colIndex]]) = val; break;

    case 8: *((uint64_t*)&data[offsets[colIndex]]) = val; break;

    default: idbassert(0); throw std::logic_error("Row::setUintField called on a non-uint32_t field");
  }
}

inline void Row::setUintField(uint64_t val, uint32_t colIndex)
{
  switch (getColumnWidth(colIndex))
  {
    case 1: data[offsets[colIndex]] = val; break;

    case 2: *((uint16_t*)&data[offsets[colIndex]]) = val; break;

    case 4: *((uint32_t*)&data[offsets[colIndex]]) = val; break;

    case 8: *((uint64_t*)&data[offsets[colIndex]]) = val; break;

    default: idbassert(0); throw std::logic_error("Row::setUintField: bad length");
  }
}

template <int len>
inline void Row::setIntField(int64_t val, uint32_t colIndex)
{
  switch (len)
  {
    case 1: *((int8_t*)&data[offsets[colIndex]]) = val; break;

    case 2: *((int16_t*)&data[offsets[colIndex]]) = val; break;

    case 4: *((int32_t*)&data[offsets[colIndex]]) = val; break;

    case 8: *((int64_t*)&data[offsets[colIndex]]) = val; break;

    default: idbassert(0); throw std::logic_error("Row::setIntField: bad length");
  }
}

inline void Row::setIntField(int64_t val, uint32_t colIndex)
{
  switch (getColumnWidth(colIndex))
  {
    case 1: *((int8_t*)&data[offsets[colIndex]]) = val; break;

    case 2: *((int16_t*)&data[offsets[colIndex]]) = val; break;

    case 4: *((int32_t*)&data[offsets[colIndex]]) = val; break;

    case 8: *((int64_t*)&data[offsets[colIndex]]) = val; break;

    default: idbassert(0); throw std::logic_error("Row::setIntField: bad length");
  }
}

inline void Row::setDoubleField(double val, uint32_t colIndex)
{
  *((double*)&data[offsets[colIndex]]) = val;
}

inline void Row::setFloatField(float val, uint32_t colIndex)
{
  // N.B. There is a bug in boost::any or in gcc where, if you store a nan, you will get back a nan,
  //  but not necessarily the same bits that you put in. This only seems to be for float (double seems
  //  to work).
  if (std::isnan(val))
    setUintField<4>(joblist::FLOATNULL, colIndex);
  else
    *((float*)&data[offsets[colIndex]]) = val;
}

inline void Row::setLongDoubleField(const long double& val, uint32_t colIndex)
{
  uint8_t* p = &data[offsets[colIndex]];
  *reinterpret_cast<long double*>(p) = val;
#ifdef MASK_LONGDOUBLE
  memset(p + 10, 0, 6);
#endif
}

inline void Row::setInt128Field(const int128_t& val, uint32_t colIndex)
{
  setBinaryField<int128_t>(&val, colIndex);
}

inline void Row::setVarBinaryField(const std::string& val, uint32_t colIndex)
{
  if (inStringTable(colIndex))
    setStringField(val, colIndex);
  else
  {
    *((uint16_t*)&data[offsets[colIndex]]) = static_cast<uint16_t>(val.length());
    memcpy(&data[offsets[colIndex] + 2], val.data(), val.length());
  }
}

inline void Row::setVarBinaryField(const uint8_t* val, uint32_t len, uint32_t colIndex)
{
  if (len > getColumnWidth(colIndex))
    len = getColumnWidth(colIndex);

  if (inStringTable(colIndex))
  {
    uint64_t offset = strings->storeString(val, len);
    *((uint64_t*)&data[offsets[colIndex]]) = offset;
  }
  else
  {
    *((uint16_t*)&data[offsets[colIndex]]) = len;
    memcpy(&data[offsets[colIndex] + 2], val, len);
  }
}

inline void Row::setUserData(mcsv1sdk::mcsv1Context& context, boost::shared_ptr<mcsv1sdk::UserData> userData,
                             uint32_t len, uint32_t colIndex)
{
  if (!userDataStore)
  {
    return;
  }

  uint32_t offset = userDataStore->storeUserData(context, userData, len);
  *((uint32_t*)&data[offsets[colIndex]]) = offset;
  *((uint32_t*)&data[offsets[colIndex] + 4]) = len;
}

inline void Row::copyField(uint32_t destIndex, uint32_t srcIndex) const
{
  uint32_t n = offsets[destIndex + 1] - offsets[destIndex];
  memmove(&data[offsets[destIndex]], &data[offsets[srcIndex]], n);
}

inline void Row::copyField(Row& out, uint32_t destIndex, uint32_t srcIndex) const
{
  if (UNLIKELY(types[srcIndex] == execplan::CalpontSystemCatalog::VARBINARY ||
               types[srcIndex] == execplan::CalpontSystemCatalog::BLOB ||
               types[srcIndex] == execplan::CalpontSystemCatalog::TEXT))
  {
    out.setVarBinaryField(getVarBinaryStringField(srcIndex), destIndex);
  }
  else if (UNLIKELY(isLongString(srcIndex)))
  {
    out.setStringField(getConstString(srcIndex), destIndex);
  }
  else if (UNLIKELY(isShortString(srcIndex)))
  {
    out.setUintField(getUintField(srcIndex), destIndex);
  }
  else if (UNLIKELY(types[srcIndex] == execplan::CalpontSystemCatalog::LONGDOUBLE))
  {
    out.setLongDoubleField(getLongDoubleField(srcIndex), destIndex);
  }
  else if (UNLIKELY(datatypes::isWideDecimalType(types[srcIndex], colWidths[srcIndex])))
  {
    copyBinaryField<int128_t>(out, destIndex, srcIndex);
  }
  else
  {
    out.setIntField(getIntField(srcIndex), destIndex);
  }
}

template <typename T>
inline void Row::copyBinaryField(Row& out, uint32_t destIndex, uint32_t srcIndex) const
{
  out.setBinaryField(getBinaryField<T>(srcIndex), destIndex);
}

inline void Row::setRid(uint64_t rid)
{
  *((uint16_t*)data) = rid & 0xffff;
}

inline uint64_t Row::hash() const
{
  return hash(columnCount - 1);
}

inline uint64_t Row::hash(uint32_t lastCol) const
{
  // Use two hash classes. MariaDBHasher for text-based
  // collation-aware data types and Hasher_r for all other data types.
  // We deliver a hash that is a combination of both hashers' results.
  utils::Hasher_r h;
  datatypes::MariaDBHasher hM;
  uint32_t intermediateHash = 0;

  // Sometimes we ask this to hash 0 bytes, and it comes through looking like
  // lastCol = -1.  Return 0.
  if (lastCol >= columnCount)
    return 0;

  for (uint32_t i = 0; i <= lastCol; i++)
    colUpdateHasher(hM, h, i, intermediateHash);

  return utils::HashFamily(h, intermediateHash, lastCol << 2, hM).finalize();
}

inline bool Row::equals(const Row& r2) const
{
  return equals(r2, columnCount - 1);
}

/** @brief RowGroup is a lightweight interface for processing packed row data

        A RowGroup is an interface for parsing and/or modifying row data as described at the top
        of this file.  Its lifecycle can be tied to a producer or consumer's lifecycle.
        Only one instance is required to process any number of blocks with a
        given column configuration.  The column configuration is specified in the
        constructor, and the block data to process is specified through the
        setData() function.	 It will not copy or take ownership of the data it processes;
        the caller should do that.

        Row and RowGroup share some bits.  RowGroup owns the memory they share.
*/
class RowGroup : public messageqcpp::Serializeable
{
 public:
  /** @brief The default ctor.  It does nothing.  Need to init by assignment or deserialization */
  RowGroup();

  /** @brief The RowGroup ctor, which specifies the column config to process

  @param colCount The number of columns
  @param positions An array specifying the offsets within the packed data
              of a row where each column begins.  It should have colCount + 1
              entries.  The first offset is 2, because a row begins with a 2-byte
              RID.  The last entry should be the offset of the last column +
              its length, which is also the size of the entire row including the rid.
  @param coids An array of oids for each column.
  @param tkeys An array of unique id for each column.
  @param colTypes An array of COLTYPEs for each column.
  @param charsetNumbers an Array of the lookup numbers for the charset/collation object.
  @param scale An array specifying the scale of DECIMAL types (0 for non-decimal)
  @param precision An array specifying the precision of DECIMAL types (0 for non-decimal)
  */

  RowGroup(uint32_t colCount, const std::vector<uint32_t>& positions, const std::vector<uint32_t>& cOids,
           const std::vector<uint32_t>& tkeys,
           const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colTypes,
           const std::vector<uint32_t>& charsetNumbers, const std::vector<uint32_t>& scale,
           const std::vector<uint32_t>& precision, uint32_t stringTableThreshold, bool useStringTable = true,
           const std::vector<bool>& forceInlineData = std::vector<bool>());

  /** @brief The copiers.  It copies metadata, not the row data */
  RowGroup(const RowGroup&);

  /** @brief Assignment operator.  It copies metadata, not the row data */
  RowGroup& operator=(const RowGroup&);

  explicit RowGroup(messageqcpp::ByteStream& bs);

  ~RowGroup();

  inline void initRow(Row*, bool forceInlineData = false) const;
  inline uint32_t getRowCount() const;
  inline void incRowCount();
  inline void setRowCount(uint32_t num);
  inline void getRow(uint32_t rowNum, Row*) const;
  inline uint32_t getRowSize() const;
  inline uint32_t getRowSizeWithStrings() const;
  inline uint64_t getBaseRid() const;
  void setData(RGData* rgd);
  inline void setData(uint8_t* d);
  inline uint8_t* getData() const;
  inline RGData* getRGData() const;

  uint32_t getStatus() const;
  void setStatus(uint16_t);

  uint32_t getDBRoot() const;
  void setDBRoot(uint32_t);

  uint32_t getDataSize() const;
  uint32_t getDataSize(uint64_t n) const;
  uint32_t getMaxDataSize() const;
  uint32_t getMaxDataSizeWithStrings() const;
  uint32_t getEmptySize() const;

  // this returns the size of the row data with the string table
  inline uint64_t getSizeWithStrings() const;
  inline uint64_t getSizeWithStrings(uint64_t n) const;

  // sets the row count to 0 and the baseRid to something
  // effectively initializing whatever chunk of memory
  // data points to
  void resetRowGroup(uint64_t baseRid);

  /* The Serializeable interface */
  void serialize(messageqcpp::ByteStream&) const;
  void deserialize(messageqcpp::ByteStream&);

  uint32_t getColumnWidth(uint32_t col) const;
  uint32_t getColumnCount() const;
  inline const std::vector<uint32_t>& getOffsets() const;
  inline const std::vector<uint32_t>& getOIDs() const;
  inline const std::vector<uint32_t>& getKeys() const;
  inline const std::vector<uint32_t>& getColWidths() const;
  inline execplan::CalpontSystemCatalog::ColDataType getColType(uint32_t colIndex) const;
  inline const std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes() const;
  inline std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes();
  inline const std::vector<uint32_t>& getCharsetNumbers() const;
  inline uint32_t getCharsetNumber(uint32_t colIndex) const;
  inline boost::shared_array<bool>& getForceInline();
  static inline uint32_t getHeaderSize()
  {
    return headerSize;
  }

  // this returns true if the type is CHAR or VARCHAR
  inline bool isCharType(uint32_t colIndex) const;
  inline bool isUnsigned(uint32_t colIndex) const;
  inline bool isShortString(uint32_t colIndex) const;
  inline bool isLongString(uint32_t colIndex) const;

  bool colHasCollation(uint32_t colIndex) const
  {
    return datatypes::typeHasCollation(getColType(colIndex));
  }

  inline const std::vector<uint32_t>& getScale() const;
  inline const std::vector<uint32_t>& getPrecision() const;

  inline bool usesStringTable() const;
  inline void setUseStringTable(bool);

  //	RGData *convertToInlineData(uint64_t *size = NULL) const;  // caller manages the memory returned by
  //this 	void convertToInlineDataInPlace(); 	RGData *convertToStringTable(uint64_t *size = NULL) const; 	void
  //convertToStringTableInPlace();
  void serializeRGData(messageqcpp::ByteStream&) const;
  inline uint32_t getStringTableThreshold() const;

  void append(RGData&);
  void append(RowGroup&);
  void append(RGData&, uint pos);  // insert starting at position 'pos'
  void append(RowGroup&, uint pos);

  RGData duplicate();  // returns a copy of the attached RGData

  std::string toString(const std::vector<uint64_t>& used = {}) const;

  /** operator+=
   *
   * append the metadata of another RowGroup to this RowGroup
   */
  RowGroup& operator+=(const RowGroup& rhs);

  // returns a RowGroup with only the first cols columns.  Useful for generating a
  // RowGroup where the first cols make up a key of some kind, and the rest is irrelevant.
  RowGroup truncate(uint32_t cols);

  /** operator<
   *
   * Orders RG's based on baseRid
   */
  inline bool operator<(const RowGroup& rhs) const;

  void addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList);

  /* Base RIDs are now a combination of partition#, segment#, extent#, and block#. */
  inline void setBaseRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                         const uint16_t& blockNum);
  inline void getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum);

  inline void setStringStore(boost::shared_ptr<StringStore>);

  const CHARSET_INFO* getCharset(uint32_t col);

 private:
  uint32_t columnCount;
  uint8_t* data;

  std::vector<uint32_t> oldOffsets;  // inline data offsets
  std::vector<uint32_t> stOffsets;   // string table offsets
  uint32_t* offsets;                 // offsets either points to oldOffsets or stOffsets
  std::vector<uint32_t> colWidths;
  // oids: the real oid of the column, may have duplicates with alias.
  // This oid is necessary for front-end to decide the real column width.
  std::vector<uint32_t> oids;
  // keys: the unique id for pair(oid, alias). bug 1632.
  // Used to map the projected column and rowgroup index
  std::vector<uint32_t> keys;
  std::vector<execplan::CalpontSystemCatalog::ColDataType> types;
  // For string collation
  std::vector<uint32_t> charsetNumbers;
  std::vector<CHARSET_INFO*> charsets;

  // DECIMAL support.  For non-decimal fields, the values are 0.
  std::vector<uint32_t> scale;
  std::vector<uint32_t> precision;

  // string table impl
  RGData* rgData;
  StringStore* strings;  // note, strings and data belong to rgData
  bool useStringTable;
  bool hasCollation;
  bool hasLongStringField;
  uint32_t sTableThreshold;
  boost::shared_array<bool> forceInline;

  static const uint32_t headerSize = 18;
  static const uint32_t rowCountOffset = 0;
  static const uint32_t baseRidOffset = 4;
  static const uint32_t statusOffset = 12;
  static const uint32_t dbRootOffset = 14;
};

inline uint64_t convertToRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                             const uint16_t& blockNum);
inline void getLocationFromRid(uint64_t rid, uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                               uint16_t* blockNum);

// returns the first rid of the logical block specified by baseRid
inline uint64_t getExtentRelativeRid(uint64_t baseRid);

// returns the first rid of the logical block specified by baseRid
inline uint64_t getFileRelativeRid(uint64_t baseRid);

/** operator+
 *
 * add the metadata of 2 RowGroups together and return a new RowGroup
 */
RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs);

boost::shared_array<int> makeMapping(const RowGroup& r1, const RowGroup& r2);
void applyMapping(const boost::shared_array<int>& mapping, const Row& in, Row* out);
void applyMapping(const std::vector<int>& mapping, const Row& in, Row* out);
void applyMapping(const int* mapping, const Row& in, Row* out);

/* PL 8/10/09: commented the asserts for now b/c for the fcns that are called
every row, they're a measurable performance penalty */
inline uint32_t RowGroup::getRowCount() const
{
  // 	idbassert(data);
  // 	if (!data) throw std::logic_error("RowGroup::getRowCount(): data is NULL!");
  return *((uint32_t*)&data[rowCountOffset]);
}

inline void RowGroup::incRowCount()
{
  // 	idbassert(data);
  ++(*((uint32_t*)&data[rowCountOffset]));
}

inline void RowGroup::setRowCount(uint32_t num)
{
  // 	idbassert(data);
  *((uint32_t*)&data[rowCountOffset]) = num;
}

inline void RowGroup::getRow(uint32_t rowNum, Row* r) const
{
  // 	idbassert(data);
  if (useStringTable != r->usesStringTable())
    initRow(r);

  r->baseRid = getBaseRid();
  r->data = &(data[headerSize + (rowNum * offsets[columnCount])]);
  r->strings = strings;
  r->userDataStore = rgData->userDataStore.get();
}

inline void RowGroup::setData(uint8_t* d)
{
  data = d;
  strings = NULL;
  rgData = NULL;
  setUseStringTable(false);
}

inline void RowGroup::setData(RGData* rgd)
{
  data = rgd->rowData.get();
  strings = rgd->strings.get();
  rgData = rgd;
}

inline uint8_t* RowGroup::getData() const
{
  // assert(!useStringTable);
  return data;
}

inline RGData* RowGroup::getRGData() const
{
  return rgData;
}

inline void RowGroup::setUseStringTable(bool b)
{
  useStringTable = (b && hasLongStringField);
  // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
  offsets = 0;

  if (useStringTable && !stOffsets.empty())
    offsets = &stOffsets[0];
  else if (!useStringTable && !oldOffsets.empty())
    offsets = &oldOffsets[0];

  if (!useStringTable)
    strings = NULL;
}

inline uint64_t RowGroup::getBaseRid() const
{
  return *((uint64_t*)&data[baseRidOffset]);
}

inline bool RowGroup::operator<(const RowGroup& rhs) const
{
  return (getBaseRid() < rhs.getBaseRid());
}

void RowGroup::initRow(Row* r, bool forceInlineData) const
{
  r->columnCount = columnCount;

  if (LIKELY(!types.empty()))
  {
    r->colWidths = (uint32_t*)&colWidths[0];
    r->types = (execplan::CalpontSystemCatalog::ColDataType*)&(types[0]);
    r->charsetNumbers = (uint32_t*)&(charsetNumbers[0]);
    r->charsets = (CHARSET_INFO**)&(charsets[0]);
    r->scale = (uint32_t*)&(scale[0]);
    r->precision = (uint32_t*)&(precision[0]);
  }

  if (forceInlineData)
  {
    r->useStringTable = false;
    r->oldOffsets = (uint32_t*)&(oldOffsets[0]);
    r->stOffsets = (uint32_t*)&(stOffsets[0]);
    r->offsets = (uint32_t*)&(oldOffsets[0]);
  }
  else
  {
    r->useStringTable = useStringTable;
    r->oldOffsets = (uint32_t*)&(oldOffsets[0]);
    r->stOffsets = (uint32_t*)&(stOffsets[0]);
    r->offsets = offsets;
  }

  r->hasLongStringField = hasLongStringField;
  r->sTableThreshold = sTableThreshold;
  r->forceInline = forceInline;
  r->hasCollation = hasCollation;
}

inline uint32_t RowGroup::getRowSize() const
{
  return offsets[columnCount];
}

inline uint32_t RowGroup::getRowSizeWithStrings() const
{
  return oldOffsets[columnCount];
}

inline uint64_t RowGroup::getSizeWithStrings(uint64_t n) const
{
  if (strings == NULL)
    return getDataSize(n);
  else
    return getDataSize(n) + strings->getSize();
}

inline uint64_t RowGroup::getSizeWithStrings() const
{
  return getSizeWithStrings(getRowCount());
}

inline bool RowGroup::isCharType(uint32_t colIndex) const
{
  return datatypes::isCharType(types[colIndex]);
}

inline bool RowGroup::isUnsigned(uint32_t colIndex) const
{
  return datatypes::isUnsigned(types[colIndex]);
}

inline bool RowGroup::isShortString(uint32_t colIndex) const
{
  return ((getColumnWidth(colIndex) <= 7 && types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
          (getColumnWidth(colIndex) <= 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR));
}

inline bool RowGroup::isLongString(uint32_t colIndex) const
{
  return ((getColumnWidth(colIndex) > 7 && types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
          (getColumnWidth(colIndex) > 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR) ||
          types[colIndex] == execplan::CalpontSystemCatalog::VARBINARY ||
          types[colIndex] == execplan::CalpontSystemCatalog::BLOB ||
          types[colIndex] == execplan::CalpontSystemCatalog::TEXT);
}

inline bool RowGroup::usesStringTable() const
{
  return useStringTable;
}

inline const std::vector<uint32_t>& RowGroup::getOffsets() const
{
  return oldOffsets;
}

inline const std::vector<uint32_t>& RowGroup::getOIDs() const
{
  return oids;
}

inline const std::vector<uint32_t>& RowGroup::getKeys() const
{
  return keys;
}

inline execplan::CalpontSystemCatalog::ColDataType RowGroup::getColType(uint32_t colIndex) const
{
  return types[colIndex];
}

inline const std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes() const
{
  return types;
}

inline std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes()
{
  return types;
}

inline const std::vector<uint32_t>& RowGroup::getCharsetNumbers() const
{
  return charsetNumbers;
}

inline uint32_t RowGroup::getCharsetNumber(uint32_t colIndex) const
{
  return charsetNumbers[colIndex];
}

inline const std::vector<uint32_t>& RowGroup::getScale() const
{
  return scale;
}

inline const std::vector<uint32_t>& RowGroup::getPrecision() const
{
  return precision;
}

inline const std::vector<uint32_t>& RowGroup::getColWidths() const
{
  return colWidths;
}

inline boost::shared_array<bool>& RowGroup::getForceInline()
{
  return forceInline;
}

inline uint64_t convertToRid(const uint32_t& partitionNum, const uint16_t& segmentNum, const uint8_t& exNum,
                             const uint16_t& blNum)
{
  uint64_t partNum = partitionNum, segNum = segmentNum, extentNum = exNum, blockNum = blNum;

  // extentNum gets trunc'd to 6 bits, blockNums to 10 bits
  extentNum &= 0x3f;
  blockNum &= 0x3ff;

  return (partNum << 32) | (segNum << 16) | (extentNum << 10) | blockNum;
}

inline void RowGroup::setBaseRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                                 const uint16_t& blockNum)
{
  *((uint64_t*)&data[baseRidOffset]) = convertToRid(partNum, segNum, extentNum, blockNum);
}

inline uint32_t RowGroup::getStringTableThreshold() const
{
  return sTableThreshold;
}

inline void RowGroup::setStringStore(boost::shared_ptr<StringStore> ss)
{
  if (useStringTable)
  {
    rgData->setStringStore(ss);
    strings = rgData->strings.get();
  }
}

inline void getLocationFromRid(uint64_t rid, uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                               uint16_t* blockNum)
{
  if (partNum)
    *partNum = rid >> 32;

  if (segNum)
    *segNum = rid >> 16;

  if (extentNum)
    *extentNum = (rid >> 10) & 0x3f;

  if (blockNum)
    *blockNum = rid & 0x3ff;
}

inline void RowGroup::getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum)
{
  getLocationFromRid(getBaseRid(), partNum, segNum, extentNum, blockNum);
}

// returns the first RID of the logical block identified by baseRid
inline uint64_t getExtentRelativeRid(uint64_t baseRid)
{
  uint64_t blockNum = baseRid & 0x3ff;
  return (blockNum << 13);
}

inline uint64_t Row::getExtentRelativeRid() const
{
  return rowgroup::getExtentRelativeRid(baseRid) | (getRelRid() & 0x1fff);
}

// returns the first RID of the logical block identified by baseRid
inline uint64_t getFileRelativeRid(uint64_t baseRid)
{
  uint64_t extentNum = (baseRid >> 10) & 0x3f;
  uint64_t blockNum = baseRid & 0x3ff;
  return (extentNum << 23) | (blockNum << 13);
}

inline uint64_t Row::getFileRelativeRid() const
{
  return rowgroup::getFileRelativeRid(baseRid) | (getRelRid() & 0x1fff);
}

inline void Row::getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum,
                             uint16_t* rowNum)
{
  getLocationFromRid(baseRid, partNum, segNum, extentNum, blockNum);

  if (rowNum)
    *rowNum = getRelRid();
}

inline void copyRow(const Row& in, Row* out, uint32_t colCount)
{
  if (&in == out)
    return;

  out->setRid(in.getRelRid());

  if (!in.usesStringTable() && !out->usesStringTable())
  {
    memcpy(out->getData(), in.getData(), std::min(in.getOffset(colCount), out->getOffset(colCount)));
    return;
  }

  for (uint32_t i = 0; i < colCount; i++)
  {
    if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY ||
                 in.getColTypes()[i] == execplan::CalpontSystemCatalog::BLOB ||
                 in.getColTypes()[i] == execplan::CalpontSystemCatalog::TEXT ||
                 in.getColTypes()[i] == execplan::CalpontSystemCatalog::CLOB))
    {
      out->setVarBinaryField(in.getVarBinaryStringField(i), i);
    }
    else if (UNLIKELY(in.isLongString(i)))
    {
      out->setStringField(in.getConstString(i), i);
    }
    else if (UNLIKELY(in.isShortString(i)))
    {
      out->setUintField(in.getUintField(i), i);
    }
    else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
    {
      out->setLongDoubleField(in.getLongDoubleField(i), i);
    }
    else if (UNLIKELY(datatypes::isWideDecimalType(in.getColType(i), in.getColumnWidth(i))))
    {
      in.copyBinaryField<int128_t>(*out, i, i);
    }
    else
    {
      out->setIntField(in.getIntField(i), i);
    }
  }
}

inline void copyRow(const Row& in, Row* out)
{
  copyRow(in, out, std::min(in.getColumnCount(), out->getColumnCount()));
}

inline std::string StringStore::getString(uint64_t off) const
{
  uint32_t length;

  if (off == std::numeric_limits<uint64_t>::max())
    return joblist::CPNULLSTRMARK;

  MemChunk* mc;

  if (off & 0x8000000000000000)
  {
    // off = off - 0x8000000000000000;
    off &= ~0x8000000000000000;

    if (longStrings.size() <= off)
      return joblist::CPNULLSTRMARK;

    mc = (MemChunk*)longStrings[off].get();
    memcpy(&length, mc->data, 4);
    return std::string((char*)mc->data + 4, length);
  }

  uint64_t chunk = off / CHUNK_SIZE;
  uint64_t offset = off % CHUNK_SIZE;

  // this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
  // what gets returned, it just can't go out of bounds.
  if (mem.size() <= chunk)
    return joblist::CPNULLSTRMARK;

  mc = (MemChunk*)mem[chunk].get();

  memcpy(&length, &mc->data[offset], 4);

  if ((offset + length) > mc->currentSize)
    return joblist::CPNULLSTRMARK;

  return std::string((char*)&(mc->data[offset]) + 4, length);
}

inline const uint8_t* StringStore::getPointer(uint64_t off) const
{
  if (off == std::numeric_limits<uint64_t>::max())
    return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

  uint64_t chunk = off / CHUNK_SIZE;
  uint64_t offset = off % CHUNK_SIZE;
  MemChunk* mc;

  if (off & 0x8000000000000000)
  {
    // off = off - 0x8000000000000000;
    off &= ~0x8000000000000000;

    if (longStrings.size() <= off)
      return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

    mc = (MemChunk*)longStrings[off].get();
    return mc->data + 4;
  }

  // this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
  // what gets returned, it just can't go out of bounds.
  if (UNLIKELY(mem.size() <= chunk))
    return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

  mc = (MemChunk*)mem[chunk].get();

  if (offset > mc->currentSize)
    return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

  return &(mc->data[offset]) + 4;
}

inline bool StringStore::isNullValue(uint64_t off) const
{
  uint32_t length;

  if (off == std::numeric_limits<uint64_t>::max())
    return true;

  // Long strings won't be NULL
  if (off & 0x8000000000000000)
    return false;

  uint32_t chunk = off / CHUNK_SIZE;
  uint32_t offset = off % CHUNK_SIZE;
  MemChunk* mc;

  if (mem.size() <= chunk)
    return true;

  mc = (MemChunk*)mem[chunk].get();
  memcpy(&length, &mc->data[offset], 4);

  if (length == 0)
    return true;

  if (length < 8)
    return false;

  if ((offset + length) > mc->currentSize)
    return true;

  if (mc->data[offset + 4] == 0)  // "" = NULL string for some reason...
    return true;
  return (memcmp(&mc->data[offset + 4], joblist::CPNULLSTRMARK.c_str(), 8) == 0);
}

inline uint32_t StringStore::getStringLength(uint64_t off) const
{
  uint32_t length;
  MemChunk* mc;

  if (off == std::numeric_limits<uint64_t>::max())
    return 0;

  if (off & 0x8000000000000000)
  {
    // off = off - 0x8000000000000000;
    off &= ~0x8000000000000000;

    if (longStrings.size() <= off)
      return 0;

    mc = (MemChunk*)longStrings[off].get();
    memcpy(&length, mc->data, 4);
  }
  else
  {
    uint64_t chunk = off / CHUNK_SIZE;
    uint64_t offset = off % CHUNK_SIZE;

    if (mem.size() <= chunk)
      return 0;

    mc = (MemChunk*)mem[chunk].get();
    memcpy(&length, &mc->data[offset], 4);
  }

  return length;
}

inline bool StringStore::isEmpty() const
{
  return empty;
}

inline uint64_t StringStore::getSize() const
{
  uint32_t i;
  uint64_t ret = 0;
  MemChunk* mc;

  ret += sizeof(MemChunk) * mem.size();
  for (i = 0; i < mem.size(); i++)
  {
    mc = (MemChunk*)mem[i].get();
    ret += mc->capacity;
  }

  ret += sizeof(MemChunk) * longStrings.size();
  for (i = 0; i < longStrings.size(); i++)
  {
    mc = (MemChunk*)longStrings[i].get();
    ret += mc->capacity;
  }

  return ret;
}

inline RGData& RGData::operator=(const RGData& r)
{
  rowData = r.rowData;
  strings = r.strings;
  userDataStore = r.userDataStore;
  return *this;
}

inline void RGData::getRow(uint32_t num, Row* row)
{
  uint32_t size = row->getSize();
  row->setData(
      Row::Pointer(&rowData[RowGroup::getHeaderSize() + (num * size)], strings.get(), userDataStore.get()));
}

}  // namespace rowgroup

#endif
// vim:ts=4 sw=4:
