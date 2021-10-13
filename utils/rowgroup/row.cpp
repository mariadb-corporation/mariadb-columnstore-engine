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

#include "row.h"

#include "hashfamily.h"
#include "helpers.h"
#include "nullvaluemanip.h"
#include "stringstore.h"
#include "userdatastore.h"

namespace rowgroup
{
// Helper to get a value from nested vector pointers.
template <typename T>
T derefFromTwoVectorPtrs(const std::vector<T>* outer, const std::vector<T>* inner, const T innerIdx)
{
    auto outerIdx = inner->operator[](innerIdx);
    return outer->operator[](outerIdx);
}

Row::Pointer Row::getPointer() const
{
    return Pointer(data, strings, userDataStore);
}

uint8_t* Row::getData() const
{
    return data;
}

void Row::setPointer(const Pointer& p)
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

void Row::setData(const Pointer& p)
{
    setPointer(p);
}

void Row::nextRow()
{
    data += offsets[columnCount];
}

uint32_t Row::getColumnCount() const
{
    return columnCount;
}

uint32_t Row::getColumnWidth(uint32_t col) const
{
    return colWidths[col];
}

uint32_t Row::getSize() const
{
    return offsets[columnCount];
}

uint32_t Row::getRealSize() const
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

uint32_t Row::getScale(uint32_t col) const
{
    return scale[col];
}

uint32_t Row::getPrecision(uint32_t col) const
{
    return precision[col];
}

execplan::CalpontSystemCatalog::ColDataType Row::getColType(uint32_t colIndex) const
{
    return types[colIndex];
}

execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes()
{
    return types;
}

const execplan::CalpontSystemCatalog::ColDataType* Row::getColTypes() const
{
    return types;
}

uint32_t Row::getCharsetNumber(uint32_t col) const
{
    return charsetNumbers[col];
}

bool Row::isCharType(uint32_t colIndex) const
{
    return datatypes::isCharType(types[colIndex]);
}

bool Row::isUnsigned(uint32_t colIndex) const
{
    return datatypes::isUnsigned(types[colIndex]);
}

bool Row::isShortString(uint32_t colIndex) const
{
    return (getColumnWidth(colIndex) <= 8 && isCharType(colIndex));
}

bool Row::isLongString(uint32_t colIndex) const
{
    return (getColumnWidth(colIndex) > 8 && isCharType(colIndex));
}

bool Row::inStringTable(uint32_t col) const
{
    return strings && getColumnWidth(col) >= sTableThreshold && !forceInline[col];
}

bool Row::equals(long double val, uint32_t colIndex) const
{
    return *((long double*)&data[offsets[colIndex]]) == val;
}

bool Row::equals(const int128_t& val, uint32_t colIndex) const
{
    return *((int128_t*)&data[offsets[colIndex]]) == val;
}

uint64_t Row::getUintField(uint32_t colIndex) const
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

int64_t Row::getIntField(uint32_t colIndex) const
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

utils::ConstString Row::getShortConstString(uint32_t colIndex) const
{
    const char* src = (const char*)&data[offsets[colIndex]];
    return utils::ConstString(src, strnlen(src, getColumnWidth(colIndex)));
}

utils::ConstString Row::getConstString(uint32_t colIndex) const
{
    return inStringTable(colIndex) ? strings->getConstString(*((uint64_t*)&data[offsets[colIndex]]))
                                   : getShortConstString(colIndex);
}

void Row::colUpdateHasher(datatypes::MariaDBHasher& hM, const utils::Hasher_r& h,
                          const uint32_t col, uint32_t& intermediateHash) const
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

void Row::colUpdateHasherTypeless(datatypes::MariaDBHasher& h, uint32_t keyColsIdx,
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
                width != derefFromTwoVectorPtrs(smallSideColumnsWidths, smallSideKeyColumnsIds,
                                                keyColsIdx))
            {
                if (val.getValue() >= std::numeric_limits<int64_t>::min() &&
                    val.getValue() <= std::numeric_limits<uint64_t>::max())
                {
                    h.add(&my_charset_bin, (const char*)&val.getValue(), datatypes::MAXLEGACYWIDTH);
                }
                else
                    h.add(&my_charset_bin, (const char*)&val.getValue(),
                          datatypes::MAXDECIMALWIDTH);
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

void Row::setStringField(const utils::ConstString& str, uint32_t colIndex)
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
        //				<< " length " << *((uint32_t *) &data[offsets[colIndex] +
        // 4])
        //				<< endl;
    }
    else
    {
        memcpy(&data[offsets[colIndex]], str.str(), length);
        memset(&data[offsets[colIndex] + length], 0,
               offsets[colIndex + 1] - (offsets[colIndex] + length));
    }
}

std::string Row::getVarBinaryStringField(uint32_t colIndex) const
{
    if (inStringTable(colIndex))
        return getConstString(colIndex).toString();

    return std::string((char*)&data[offsets[colIndex] + 2], *((uint16_t*)&data[offsets[colIndex]]));
}

uint32_t Row::getVarBinaryLength(uint32_t colIndex) const
{
    if (inStringTable(colIndex))
        return strings->getStringLength(*((uint64_t*)&data[offsets[colIndex]]));
    ;

    return *((uint16_t*)&data[offsets[colIndex]]);
}

const uint8_t* Row::getVarBinaryField(uint32_t colIndex) const
{
    if (inStringTable(colIndex))
        return strings->getPointer(*((uint64_t*)&data[offsets[colIndex]]));

    return &data[offsets[colIndex] + 2];
}

const uint8_t* Row::getVarBinaryField(uint32_t& len, uint32_t colIndex) const
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

boost::shared_ptr<mcsv1sdk::UserData> Row::getUserData(uint32_t colIndex) const
{
    if (!userDataStore)
    {
        return boost::shared_ptr<mcsv1sdk::UserData>();
    }

    return userDataStore->getUserData(*((uint32_t*)&data[offsets[colIndex]]));
}

double Row::getDoubleField(uint32_t colIndex) const
{
    return *((double*)&data[offsets[colIndex]]);
}

float Row::getFloatField(uint32_t colIndex) const
{
    return *((float*)&data[offsets[colIndex]]);
}

long double Row::getLongDoubleField(uint32_t colIndex) const
{
    return *((long double*)&data[offsets[colIndex]]);
}

void Row::storeInt128FieldIntoPtr(uint32_t colIndex, uint8_t* x) const
{
    datatypes::TSInt128::assignPtrPtr(x, &data[offsets[colIndex]]);
}

void Row::getInt128Field(uint32_t colIndex, int128_t& x) const
{
    datatypes::TSInt128::assignPtrPtr(&x, &data[offsets[colIndex]]);
}

datatypes::TSInt128 Row::getTSInt128Field(uint32_t colIndex) const
{
    const int128_t* ptr = getBinaryField<int128_t>(colIndex);
    return datatypes::TSInt128(ptr);
}

uint64_t Row::getRid() const
{
    return baseRid + *((uint16_t*)data);
}

uint16_t Row::getRelRid() const
{
    return *((uint16_t*)data);
}

uint64_t Row::getBaseRid() const
{
    return baseRid;
}

void Row::markRow()
{
    *((uint16_t*)data) = 0xffff;
}

void Row::zeroRid()
{
    *((uint16_t*)data) = 0;
}

bool Row::isMarked()
{
    return *((uint16_t*)data) == 0xffff;
}

/* Begin speculative code! */
uint32_t Row::getOffset(uint32_t colIndex) const
{
    return offsets[colIndex];
}

void Row::nextRow(uint32_t size)
{
    data += size;
}

void Row::prevRow(uint32_t size, uint64_t number)
{
    data -= size * number;
}

void Row::setUintField(uint64_t val, uint32_t colIndex)
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

void Row::setIntField(int64_t val, uint32_t colIndex)
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

void Row::setDoubleField(double val, uint32_t colIndex)
{
    *((double*)&data[offsets[colIndex]]) = val;
}

void Row::setFloatField(float val, uint32_t colIndex)
{
    // N.B. There is a bug in boost::any or in gcc where, if you store a nan, you will get back a
    // nan,
    //  but not necessarily the same bits that you put in. This only seems to be for float (double
    //  seems to work).
    if (std::isnan(val))
        setUintField<4>(joblist::FLOATNULL, colIndex);
    else
        *((float*)&data[offsets[colIndex]]) = val;
}

void Row::setLongDoubleField(const long double& val, uint32_t colIndex)
{
    uint8_t* p = &data[offsets[colIndex]];
    *reinterpret_cast<long double*>(p) = val;
#ifdef MASK_LONGDOUBLE
    memset(p + 10, 0, 6);
#endif
}

void Row::setInt128Field(const int128_t& val, uint32_t colIndex)
{
    setBinaryField<int128_t>(&val, colIndex);
}

void Row::setVarBinaryField(const std::string& val, uint32_t colIndex)
{
    if (inStringTable(colIndex))
        setStringField(val, colIndex);
    else
    {
        *((uint16_t*)&data[offsets[colIndex]]) = static_cast<uint16_t>(val.length());
        memcpy(&data[offsets[colIndex] + 2], val.data(), val.length());
    }
}

void Row::setVarBinaryField(const uint8_t* val, uint32_t len, uint32_t colIndex)
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

void Row::setUserData(mcsv1sdk::mcsv1Context& context,
                      boost::shared_ptr<mcsv1sdk::UserData> userData, uint32_t len,
                      uint32_t colIndex)
{
    if (!userDataStore)
    {
        return;
    }

    uint32_t offset = userDataStore->storeUserData(context, userData, len);
    *((uint32_t*)&data[offsets[colIndex]]) = offset;
    *((uint32_t*)&data[offsets[colIndex] + 4]) = len;
}

void Row::copyField(uint32_t destIndex, uint32_t srcIndex) const
{
    uint32_t n = offsets[destIndex + 1] - offsets[destIndex];
    memmove(&data[offsets[destIndex]], &data[offsets[srcIndex]], n);
}

void Row::copyField(Row& out, uint32_t destIndex, uint32_t srcIndex) const
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

void Row::setRid(uint64_t rid)
{
    *((uint16_t*)data) = rid & 0xffff;
}

uint64_t Row::hash() const
{
    return hash(columnCount - 1);
}

uint64_t Row::hash(uint32_t lastCol) const
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

uint64_t Row::hashTypeless(const std::vector<uint32_t>& keyCols,
                           const std::vector<uint32_t>* smallSideKeyColumnsIds,
                           const std::vector<uint32_t>* smallSideColumnsWidths) const
{
    datatypes::MariaDBHasher h;
    for (uint32_t i = 0; i < keyCols.size(); i++)
        colUpdateHasherTypeless(h, i, keyCols, smallSideKeyColumnsIds, smallSideColumnsWidths);
    return h.finalize();
}

bool Row::equals(const Row& r2) const
{
    return equals(r2, columnCount - 1);
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
            case execplan::CalpontSystemCatalog::CHAR:
            case execplan::CalpontSystemCatalog::VARCHAR:
            {
                const utils::ConstString tmp = getConstString(i);
                os << "(" << tmp.length() << ") '";
                os.write(tmp.str(), tmp.length());
                os << "' ";
                break;
            }

            case execplan::CalpontSystemCatalog::FLOAT:
            case execplan::CalpontSystemCatalog::UFLOAT: os << getFloatField(i) << " "; break;

            case execplan::CalpontSystemCatalog::DOUBLE:
            case execplan::CalpontSystemCatalog::UDOUBLE: os << getDoubleField(i) << " "; break;

            case execplan::CalpontSystemCatalog::LONGDOUBLE:
                os << getLongDoubleField(i) << " ";
                break;

            case execplan::CalpontSystemCatalog::VARBINARY:
            case execplan::CalpontSystemCatalog::BLOB:
            case execplan::CalpontSystemCatalog::TEXT:
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

            case execplan::CalpontSystemCatalog::DECIMAL:
            case execplan::CalpontSystemCatalog::UDECIMAL:
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
            case execplan::CalpontSystemCatalog::CHAR:
            case execplan::CalpontSystemCatalog::VARCHAR: os << getStringField(i).c_str(); break;

            case execplan::CalpontSystemCatalog::FLOAT:
            case execplan::CalpontSystemCatalog::UFLOAT: os << getFloatField(i); break;

            case execplan::CalpontSystemCatalog::DOUBLE:
            case execplan::CalpontSystemCatalog::UDOUBLE: os << getDoubleField(i); break;

            case execplan::CalpontSystemCatalog::LONGDOUBLE: os << getLongDoubleField(i); break;

            case execplan::CalpontSystemCatalog::VARBINARY:
            case execplan::CalpontSystemCatalog::BLOB:
            case execplan::CalpontSystemCatalog::TEXT:
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
        case execplan::CalpontSystemCatalog::TINYINT:
            data[offsets[i]] = joblist::TINYINTNULL;
            break;

        case execplan::CalpontSystemCatalog::SMALLINT:
            *((int16_t*)&data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL);
            break;

        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::INT:
            *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL);
            break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
            *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::FLOATNULL);
            break;

        case execplan::CalpontSystemCatalog::DATE:
            *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::DATENULL);
            break;

        case execplan::CalpontSystemCatalog::BIGINT:
            if (precision[i] != 9999)
                *((uint64_t*)&data[offsets[i]]) = joblist::BIGINTNULL;
            else  // work around for count() in outer join result.
                *((uint64_t*)&data[offsets[i]]) = 0;

            break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
            *((uint64_t*)&data[offsets[i]]) = joblist::DOUBLENULL;
            break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
            *((long double*)&data[offsets[i]]) = joblist::LONGDOUBLENULL;
            break;

        case execplan::CalpontSystemCatalog::DATETIME:
            *((uint64_t*)&data[offsets[i]]) = joblist::DATETIMENULL;
            break;

        case execplan::CalpontSystemCatalog::TIMESTAMP:
            *((uint64_t*)&data[offsets[i]]) = joblist::TIMESTAMPNULL;
            break;

        case execplan::CalpontSystemCatalog::TIME:
            *((uint64_t*)&data[offsets[i]]) = joblist::TIMENULL;
            break;

        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        case execplan::CalpontSystemCatalog::STRINT:
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

        case execplan::CalpontSystemCatalog::VARBINARY:
        case execplan::CalpontSystemCatalog::BLOB: *((uint16_t*)&data[offsets[i]]) = 0; break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            uint32_t len = getColumnWidth(i);

            switch (len)
            {
            case 1: data[offsets[i]] = joblist::TINYINTNULL; break;

            case 2:
                *((int16_t*)&data[offsets[i]]) = static_cast<int16_t>(joblist::SMALLINTNULL);
                break;

            case 4: *((int32_t*)&data[offsets[i]]) = static_cast<int32_t>(joblist::INTNULL); break;

            case 16:
            {
                int128_t* s128ValuePtr = (int128_t*)(&data[offsets[i]]);
                datatypes::TSInt128::storeUnaligned(s128ValuePtr, datatypes::Decimal128Null);
            }
            break;
            default:
                *((int64_t*)&data[offsets[i]]) = static_cast<int64_t>(joblist::BIGINTNULL);
                break;
            }

            break;
        }

        case execplan::CalpontSystemCatalog::UTINYINT:
            data[offsets[i]] = joblist::UTINYINTNULL;
            break;

        case execplan::CalpontSystemCatalog::USMALLINT:
            *((uint16_t*)&data[offsets[i]]) = joblist::USMALLINTNULL;
            break;

        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UINT:
            *((uint32_t*)&data[offsets[i]]) = joblist::UINTNULL;
            break;

        case execplan::CalpontSystemCatalog::UBIGINT:
            *((uint64_t*)&data[offsets[i]]) = joblist::UBIGINTNULL;
            break;

        default:
            ostringstream os;
            os << "Row::initToNull(): got bad column type (" << types[i]
               << ").  Width=" << getColumnWidth(i) << endl;
            os << toString();
            throw logic_error(os.str());
        }
    }
}

bool Row::isNullValue(uint32_t colIndex) const
{
    switch (types[colIndex])
    {
    case execplan::CalpontSystemCatalog::TINYINT:
        return (data[offsets[colIndex]] == joblist::TINYINTNULL);

    case execplan::CalpontSystemCatalog::SMALLINT:
        return (*((int16_t*)&data[offsets[colIndex]]) ==
                static_cast<int16_t>(joblist::SMALLINTNULL));

    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
        return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::INTNULL));

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
        return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::FLOATNULL));

    case execplan::CalpontSystemCatalog::DATE:
        return (*((int32_t*)&data[offsets[colIndex]]) == static_cast<int32_t>(joblist::DATENULL));

    case execplan::CalpontSystemCatalog::BIGINT:
        return (*((int64_t*)&data[offsets[colIndex]]) == static_cast<int64_t>(joblist::BIGINTNULL));

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
        return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::DOUBLENULL);

    case execplan::CalpontSystemCatalog::DATETIME:
        return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::DATETIMENULL);

    case execplan::CalpontSystemCatalog::TIMESTAMP:
        return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::TIMESTAMPNULL);

    case execplan::CalpontSystemCatalog::TIME:
        return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::TIMENULL);

    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::STRINT:
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
            return (*((uint64_t*)&data[offsets[colIndex]]) ==
                    *((uint64_t*)joblist::CPNULLSTRMARK.c_str()));
        }

        break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
        // TODO MCOL-641 Allmighty hack.
        switch (getColumnWidth(colIndex))
        {
        // MCOL-641
        case 16:
            return isNullValue_offset<execplan::CalpontSystemCatalog::DECIMAL, 16>(
                offsets[colIndex]);
        case 1: return (data[offsets[colIndex]] == joblist::TINYINTNULL);

        case 2:
            return (*((int16_t*)&data[offsets[colIndex]]) ==
                    static_cast<int16_t>(joblist::SMALLINTNULL));

        case 4:
            return (*((int32_t*)&data[offsets[colIndex]]) ==
                    static_cast<int32_t>(joblist::INTNULL));

        default:
            return (*((int64_t*)&data[offsets[colIndex]]) ==
                    static_cast<int64_t>(joblist::BIGINTNULL));
        }

        break;
    }

    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::VARBINARY:
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

    case execplan::CalpontSystemCatalog::UTINYINT:
        return (data[offsets[colIndex]] == joblist::UTINYINTNULL);

    case execplan::CalpontSystemCatalog::USMALLINT:
        return (*((uint16_t*)&data[offsets[colIndex]]) == joblist::USMALLINTNULL);

    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
        return (*((uint32_t*)&data[offsets[colIndex]]) == joblist::UINTNULL);

    case execplan::CalpontSystemCatalog::UBIGINT:
        return (*((uint64_t*)&data[offsets[colIndex]]) == joblist::UBIGINTNULL);

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
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
        //				<< " length " << *((uint32_t *) &data[offsets[colIndex] +
        // 4])
        //				<< endl;
    }
    else
    {
        memcpy(&data[offsets[colIndex]], val.data(), length);
        memset(&data[offsets[colIndex] + length], 0,
               offsets[colIndex + 1] - (offsets[colIndex] + length));
    }
}

bool Row::equals(const Row& r2, uint32_t lastCol) const
{
    // This check fires with empty r2 only.
    if (lastCol >= columnCount)
        return true;

    // If there are no strings in the row, then we can just memcmp the whole row.
    // hasCollation is true if there is any column of type CHAR, VARCHAR or TEXT
    // useStringTable is true if any field declared > max  field size, including BLOB
    // For memcmp to be correct, both must be false.
    if (!hasCollation && !useStringTable && !r2.hasCollation && !r2.useStringTable)
        return !(
            memcmp(&data[offsets[0]], &r2.data[offsets[0]], offsets[lastCol + 1] - offsets[0]));

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
        const_cast<CHARSET_INFO**>(charsets)[col] =
            &datatypes::Charset(charsetNumbers[col]).getCharset();
    }
    return charsets[col];
}

uint64_t Row::getFileRelativeRid() const
{
    return rowgroup::getFileRelativeRid(baseRid) | (getRelRid() & 0x1fff);
}

void Row::getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum,
                      uint16_t* rowNum)
{
    getLocationFromRid(baseRid, partNum, segNum, extentNum, blockNum);

    if (rowNum)
        *rowNum = getRelRid();
}

uint64_t Row::getExtentRelativeRid() const
{
    return rowgroup::getExtentRelativeRid(baseRid) | (getRelRid() & 0x1fff);
}

bool Row::colHasCollation(uint32_t colIndex) const
{
    return datatypes::typeHasCollation(getColType(colIndex));
}

datatypes::Decimal Row::getDecimalField(uint32_t colIndex) const
{
    if (LIKELY(getColumnWidth(colIndex) == datatypes::MAXDECIMALWIDTH))
        return datatypes::Decimal(0, (int)getScale(colIndex), getPrecision(colIndex),
                                  getBinaryField<int128_t>(colIndex));
    return datatypes::Decimal(datatypes::TSInt64(getIntField(colIndex)), (int)getScale(colIndex),
                              getPrecision(colIndex));
}

}  // namespace rowgroup