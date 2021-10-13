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

#pragma once

#include <cstdint>

#include "dataconvert.h"
#include "hasher.h"
#include "mcs_basic_types.h"
#include "mcs_decimal.h"
#include "mcsv1_udaf.h"

namespace rowgroup
{
class StringStore;
class UserDataStore;

class Row
{
  public:
    struct Pointer
    {
        Pointer() = default;

        // Pointer(uint8_t*) implicitly makes old code compatible with the string table impl;
        Pointer(uint8_t* d) : data(d)
        {
        }
        Pointer(uint8_t* d, StringStore* s) : data(d), strings(s)
        {
        }
        Pointer(uint8_t* d, StringStore* s, UserDataStore* u) : data(d), strings(s), userDataStore(u)
        {
        }

        uint8_t* data = nullptr;
        StringStore* strings = nullptr;
        UserDataStore* userDataStore = nullptr;
    };

    Row() = default;
    Row(const Row&);
    ~Row() = default;

    Row& operator=(const Row&);
    bool operator==(const Row&) const;

    void setData(const Pointer&);  // convenience fcn, can go away
    uint8_t* getData() const;

    void setPointer(const Pointer&);
    Pointer getPointer() const;

    void nextRow();
    uint32_t getColumnWidth(uint32_t colIndex) const;
    uint32_t getColumnCount() const;
    uint32_t getSize() const;  // this is only accurate if there is no string table
    // if a string table is being used, getRealSize() takes into account variable-length strings
    uint32_t getRealSize() const;
    uint32_t getOffset(uint32_t colIndex) const;
    uint32_t getScale(uint32_t colIndex) const;
    uint32_t getPrecision(uint32_t colIndex) const;
    execplan::CalpontSystemCatalog::ColDataType getColType(uint32_t colIndex) const;
    execplan::CalpontSystemCatalog::ColDataType* getColTypes();
    const execplan::CalpontSystemCatalog::ColDataType* getColTypes() const;
    uint32_t getCharsetNumber(uint32_t colIndex) const;

    // this returns true if the type is not CHAR or VARCHAR
    bool isCharType(uint32_t colIndex) const;
    bool isUnsigned(uint32_t colIndex) const;
    bool isShortString(uint32_t colIndex) const;
    bool isLongString(uint32_t colIndex) const;

    bool colHasCollation(uint32_t colIndex) const;

    template <int len>
    uint64_t getUintField(uint32_t colIndex) const;
    uint64_t getUintField(uint32_t colIndex) const;
    template <int len>
    int64_t getIntField(uint32_t colIndex) const;
    int64_t getIntField(uint32_t colIndex) const;
    // Get a signed 64-bit integer column value, convert to the given
    // floating point data type T (e.g. float, double, long double)
    // and divide it according to the scale.
    template <typename T>
    T getScaledSInt64FieldAsXFloat(uint32_t colIndex, uint32_t scale) const
    {
        const T d = getIntField(colIndex);
        if (!scale)
            return d;
        return d / datatypes::scaleDivisor<T>(scale);
    }
    template <typename T>
    T getScaledSInt64FieldAsXFloat(uint32_t colIndex) const
    {
        return getScaledSInt64FieldAsXFloat<T>(colIndex, getScale(colIndex));
    }
    // Get an unsigned 64-bit integer column value, convert to the given
    // floating point data type T (e.g. float, double, long double)
    // and divide it according to the scale.
    template <typename T>
    T getScaledUInt64FieldAsXFloat(uint32_t colIndex, uint32_t scale) const
    {
        const T d = getUintField(colIndex);
        if (!scale)
            return d;
        return d / datatypes::scaleDivisor<T>(scale);
    }
    template <typename T>
    T getScaledUInt64FieldAsXFloat(uint32_t colIndex) const
    {
        return getScaledUInt64FieldAsXFloat<T>(colIndex, getScale(colIndex));
    }
    template <typename T>
    bool equals(T* value, uint32_t colIndex) const;
    template <int len>
    bool equals(uint64_t val, uint32_t colIndex) const;
    bool equals(long double val, uint32_t colIndex) const;
    bool equals(const int128_t& val, uint32_t colIndex) const;

    double getDoubleField(uint32_t colIndex) const;
    float getFloatField(uint32_t colIndex) const;
    datatypes::Decimal getDecimalField(uint32_t colIndex) const;
    long double getLongDoubleField(uint32_t colIndex) const;
    void storeInt128FieldIntoPtr(uint32_t colIndex, uint8_t* x) const;
    void getInt128Field(uint32_t colIndex, int128_t& x) const;
    datatypes::TSInt128 getTSInt128Field(uint32_t colIndex) const;

    uint64_t getBaseRid() const;
    uint64_t getRid() const;
    uint16_t getRelRid() const;             // returns a rid relative to this logical block
    uint64_t getExtentRelativeRid() const;  // returns a rid relative to the extent it's in
    uint64_t getFileRelativeRid() const;    // returns a file-relative rid
    void getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum,
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

    void nextRow(uint32_t size);
    void prevRow(uint32_t size, uint64_t number = 1);

    void setUintField(uint64_t val, uint32_t colIndex);

    template <int len>
    void setIntField(int64_t, uint32_t colIndex);
    void setIntField(int64_t, uint32_t colIndex);

    void setDoubleField(double val, uint32_t colIndex);
    void setFloatField(float val, uint32_t colIndex);
    void setDecimalField(double val, uint32_t colIndex){};  // TODO: Do something here
    void setLongDoubleField(const long double& val, uint32_t colIndex);
    void setInt128Field(const int128_t& val, uint32_t colIndex);
    void setRid(uint64_t rid);

    // TODO: remove this (string is not efficient for this), use getConstString() instead
    std::string getStringField(uint32_t colIndex) const
    {
        return getConstString(colIndex).toString();
    }

    utils::ConstString getConstString(uint32_t colIndex) const;
    utils::ConstString getShortConstString(uint32_t colIndex) const;
    void setStringField(const std::string& val, uint32_t colIndex);
    void setStringField(const utils::ConstString& str, uint32_t colIndex);
    template <typename T>
    void setBinaryField(const T* value, uint32_t width, uint32_t colIndex);
    template <typename T>
    void setBinaryField(const T* value, uint32_t colIndex);
    template <typename T>
    void setBinaryField_offset(const T* value, uint32_t width, uint32_t colIndex);
    // support VARBINARY
    // Add 2-byte length at the CHARSET_INFO*beginning of the field.  NULL and zero length field are
    // treated the same, could use one of the length bit to distinguish these two cases.
    std::string getVarBinaryStringField(uint32_t colIndex) const;
    void setVarBinaryField(const std::string& val, uint32_t colIndex);
    // No string construction is necessary for better performance.
    uint32_t getVarBinaryLength(uint32_t colIndex) const;
    const uint8_t* getVarBinaryField(uint32_t colIndex) const;
    const uint8_t* getVarBinaryField(uint32_t& len, uint32_t colIndex) const;
    void setVarBinaryField(const uint8_t* val, uint32_t len, uint32_t colIndex);

    template <typename T>
    T* getBinaryField(uint32_t colIndex) const;
    // To simplify parameter type deduction.
    template <typename T>
    T* getBinaryField(T* argtype, uint32_t colIndex) const;
    template <typename T>
    T* getBinaryField_offset(uint32_t offset) const;

    boost::shared_ptr<mcsv1sdk::UserData> getUserData(uint32_t colIndex) const;
    void setUserData(mcsv1sdk::mcsv1Context& context, boost::shared_ptr<mcsv1sdk::UserData> userData,
                     uint32_t len, uint32_t colIndex);

    uint64_t getNullValue(uint32_t colIndex) const;
    bool isNullValue(uint32_t colIndex) const;
    template <cscDataType cscDT, int width>
    bool isNullValue_offset(uint32_t offset) const;

    // when NULLs are pulled out via getIntField(), they come out with these values.
    // Ex: the 1-byte int null value is 0x80.  When it gets cast to an int64_t
    // it becomes 0xffffffffffffff80, which won't match anything returned by getNullValue().
    int64_t getSignedNullValue(uint32_t colIndex) const;

    // copy data in srcIndex field to destIndex, all data type
    void copyField(uint32_t destIndex, uint32_t srcIndex) const;
    // an adapter for code that uses the copyField call above;
    // that's not string-table safe, this one is
    void copyField(Row& dest, uint32_t destIndex, uint32_t srcIndex) const;

    template <typename T>
    void copyBinaryField(Row& dest, uint32_t destIndex, uint32_t srcIndex) const;

    std::string toString(uint32_t rownum = 0) const;
    std::string toCSV() const;

    /* These fcns are used only in joins.  The RID doesn't matter on the side that
    gets hashed.  We steal that field here to "mark" a row. */
    void markRow();
    void zeroRid();
    bool isMarked();
    void initToNull();

    void usesStringTable(bool b)
    {
        useStringTable = b;
    }
    bool usesStringTable() const
    {
        return useStringTable;
    }
    bool hasLongString() const
    {
        return hasLongStringField;
    }

    // these are for cases when you already know the type definitions are the same.
    // a fcn to check the type defs seperately doesn't exist yet.  No normalization.
    uint64_t hash(uint32_t lastCol) const;  // generates a hash for cols [0-lastCol]
    uint64_t hash() const;                  // generates a hash for all cols
    void colUpdateHasher(datatypes::MariaDBHasher& hM, const utils::Hasher_r& h, const uint32_t col,
                         uint32_t& intermediateHash) const;
    void colUpdateHasherTypeless(datatypes::MariaDBHasher& hasher, uint32_t keyColsIdx,
                                 const std::vector<uint32_t>& keyCols,
                                 const std::vector<uint32_t>* smallSideKeyColumnsIds,
                                 const std::vector<uint32_t>* smallSideColumnsWidths) const;
    uint64_t hashTypeless(const std::vector<uint32_t>& keyCols,
                          const std::vector<uint32_t>* smallSideKeyColumnsIds,
                          const std::vector<uint32_t>* smallSideColumnsWidths) const;

    bool equals(const Row&, uint32_t lastCol) const;
    bool equals(const Row&) const;

    void setUserDataStore(UserDataStore* u)
    {
        userDataStore = u;
    }

    const CHARSET_INFO* getCharset(uint32_t col) const;

  private:
    bool inStringTable(uint32_t col) const;

  private:
    uint32_t columnCount = 0;
    uint64_t baseRid = 0;

    // Note, the mem behind these pointer fields is owned by RowGroup not Row
    uint32_t* oldOffsets = nullptr;
    uint32_t* stOffsets = nullptr;
    uint32_t* offsets = nullptr;
    uint32_t* colWidths = nullptr;
    execplan::CalpontSystemCatalog::ColDataType* types = nullptr;
    uint32_t* charsetNumbers = nullptr;
    CHARSET_INFO** charsets = nullptr;
    uint8_t* data = nullptr;
    uint32_t* scale = nullptr;
    uint32_t* precision = nullptr;

    StringStore* strings = nullptr;
    bool useStringTable = false;
    bool hasCollation = false;
    bool hasLongStringField = false;
    uint32_t sTableThreshold = 20;
    boost::shared_array<bool> forceInline;
    UserDataStore* userDataStore = nullptr;  // For UDAF

    friend class RowGroup;
};

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

// This method !cannot! be applied to uint8_t* buffers.
template <typename T>
inline void Row::setBinaryField_offset(const T* value, uint32_t width, uint32_t offset)
{
    *reinterpret_cast<T*>(&data[offset]) = *value;
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

template <typename T>
inline void Row::setIntField_offset(const T val, const uint32_t offset)
{
    *((T*)&data[offset]) = val;
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
inline void Row::copyBinaryField(Row& out, uint32_t destIndex, uint32_t srcIndex) const
{
    out.setBinaryField(getBinaryField<T>(srcIndex), destIndex);
}

template <>
inline void Row::setBinaryField<int128_t>(const int128_t* value, uint32_t colIndex)
{
    datatypes::TSInt128::assignPtrPtr(&data[offsets[colIndex]], value);
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

}  // namespace rowgroup